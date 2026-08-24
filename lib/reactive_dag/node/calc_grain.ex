defmodule ReactiveDag.Node.CalcGrain do
  @moduledoc """
  A grain entry that is a CALCULATION, evaluated on a change rather than read
  from the datastore.

  Most grains are columns, and a change carries them directly: project the diff
  onto a map and the values are there. A calculation is different — it is an Ash
  `expr` the datastore normally evaluates, so a fold grouping by one used to fall
  back to reading live rows, which cannot answer for a deleted row and names only
  where a moved row landed.

  It does not have to. A calculation over attributes of the same row is a function
  of values the diff already holds, and Ash can evaluate it in the BEAM: build a
  record from the diff and `Ash.load/3` the calculation onto it. No query, no
  round trip, and a destroyed row's prior side evaluates exactly as well as a live
  one's.

  ## Why `Ash.load/3` rather than evaluating the expr directly

  The expr is reachable — `Ash.Resource.Info.calculation/2` hands back
  `{Ash.Resource.Calculation.Expression, [expr: …]}` — but its refs are
  unresolved (`%Ash.Query.Ref{resource: nil}`), so `Ash.Expr.eval/2` refuses them
  with "Invalid reference kind". Resolving refs is query-planning work, and
  reimplementing it here would be a second, worse copy of Ash's own evaluator.

  `Ash.load/3` on an in-memory struct does the whole job — including calculations
  that depend on other calculations — and stays correct as Ash's expression
  language grows. Measured: a `if kind == "osc_actual" …` calculation evaluates
  correctly on a struct that was never in the database.

  ## What is NOT supported

  A calculation that traverses a RELATIONSHIP, or a custom calculation module
  doing its own reads. Both need data the change does not carry, so they keep the
  live-read fallback rather than being half-evaluated against nils — a wrong unit
  is worse than a wide one, because a fold that claims the wrong unit reconciles a
  live one away.

  This is decided per cell at assembly (`Node.group_key_plan/2` marks the entry
  `{:calc, name}`), and per change here: an evaluation that errors or comes back
  nil yields no unit, and the caller falls back.
  """

  require Logger

  @doc """
  Project a change onto a grain that may contain calculations.

  `plan` is the assembled `group_key_plan` — `{:attr, name, string?}` and
  `{:calc, name}` entries in group order. `row` is the diff projected to a map
  (`Diff.before/1` or `Diff.after_/1`).

  Returns the grain's values in plan order, or `nil` when any entry cannot be
  resolved — the caller then reads live rather than claiming a partial unit.
  """
  @spec values([tuple()], map(), module()) :: [term()] | nil
  def values(plan, row, resource) when is_map(row) do
    calcs = for {:calc, name} <- plan, do: name

    with {:ok, loaded} <- resolve(calcs, row, resource) do
      plan
      |> Enum.map(fn
        {:attr, name, _string?} -> Map.get(row, name)
        {:calc, name} -> Map.get(loaded, name)
      end)
      |> then(fn values -> if Enum.any?(values, &is_nil/1), do: nil, else: values end)
    else
      :error -> nil
    end
  end

  # No calculations in the grain: nothing to evaluate, and the row already holds
  # every value.
  defp resolve([], row, _resource), do: {:ok, row}

  defp resolve(calcs, row, resource) do
    # A struct, not the bare map: `Ash.load/3` dispatches on the resource, and the
    # attributes the calculation reads come off the struct's own fields. Values
    # absent from the diff stay nil, which is the same thing the datastore would
    # see for a column the action did not accept.
    record = struct(resource, Map.put(row, :__meta__, %Ecto.Schema.Metadata{state: :loaded, schema: resource}))

    case Ash.load(record, calcs, authorize?: false, lazy?: true) do
      {:ok, loaded} ->
        {:ok, Map.merge(row, Map.new(calcs, &{&1, Map.get(loaded, &1)}))}

      {:error, reason} ->
        # A calculation needing a relationship or its own read lands here. Not an
        # error to propagate: the caller falls back to a live read, which is what
        # happened before calculations were evaluable at all.
        Logger.debug(fn ->
          "reactive_dag: could not evaluate #{inspect(calcs)} on a change " <>
            "(#{inspect(reason)}); this claim falls back to a live read"
        end)

        :error
    end
  rescue
    e ->
      Logger.debug(fn ->
        "reactive_dag: #{inspect(calcs)} raised on a change " <>
          "(#{Exception.message(e)}); this claim falls back to a live read"
      end)

      :error
  end
end

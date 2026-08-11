defmodule ReactiveDag.Attestation.Op do
  @moduledoc """
  The recompute of an ATTESTED cell — the derived view an `attested` combinator
  or a `gate:`d edge lowers to. Thin DB glue: read the three inputs, run the
  pure `ReactiveDag.Attestation.Evaluation`, write ordinary spine rows.

  The cell's meta carries `attested: %{over: raw_cell_id, requirement:
  %Requirement{}}` (resolved at graph assembly). Inputs are `[raw, eligibility,
  store leaf]` — so a scan, a role change, or a signing all dirty this cell and
  the drain recomputes it the same way.

  ## What it writes

  One spine row per admission, in the requirement's status vocabulary — so
  verdict rollup, first-class coverage, and the freshness spine apply to
  attested views unchanged. The mapping depends on the view's MODE
  (`spine_status/3`):

    * `:require` (blocking, the default) — `covered` / `pending` / `refused`.
      A not-yet-signed row is withheld: consumers of the signed set read
      `covered` and see nothing for it.
    * `:annotate` (non-blocking) — `covered` / `unsigned` / `refused`. Best
      effort: an unsigned row FLOWS, distinguished from signed rather than
      withheld. A rejection still bites — data someone said is WRONG is a
      different thing from data nobody has vouched for, and passing it through
      as best-effort would launder the objection.

  Affirmed rows are put with `strength: "attested"` in the writer opts: the
  spine-only default writer drops it (strength is a host extension column), a
  host writer stamps it — which keeps "the machinery assigns the strength"
  inside the existing `CoordinationWriter` seam.

  Rows vanished from the raw cell are retired (delete): an attested view has no
  claim about data that no longer exists — the attestation RECORD survives in
  the store (append-only history), only the projection row goes.
  """
  @behaviour ReactiveDag.Op

  alias ReactiveDag.{Attestation, Op, Tuple}
  alias ReactiveDag.Attestation.{Evaluation, Requirement, Scope}

  @impl true
  def recompute(cell, _keys) do
    meta = attested_meta(cell)
    %{over: over, requirement: %Requirement{} = req} = meta
    mode = Map.get(meta, :mode, :require)

    raw_rows = Tuple.rows(over)
    stances = Attestation.stances(over)
    eligibility = Tuple.all_keys(to_string(req.signers))
    statuses = Requirement.statuses(req)
    now = DateTime.utc_now()

    by_key =
      case req.scope do
        :key ->
          raw_rows
          |> Evaluation.evaluate(stances, eligibility, req, now)
          |> Map.new(fn %{scope: {:key, k}} = a -> {k, a} end)

        _filter ->
          # SET-LEVEL: one admission per scope INSTANCE (the whole set, or one
          # set per eligibility row), each evaluated against the rows its
          # filter currently selects — so the basis moves when the set does.
          by_scope = Enum.group_by(stances, & &1.scope)

          for {inst_key, key_scope} <- instances(req, eligibility), into: %{} do
            scope = {:filter, key_scope}
            selected = Scope.select(scope, raw_rows)
            scope_stances = Map.get(by_scope, scope, [])

            {inst_key,
             Evaluation.evaluate_scope(scope, selected, scope_stances, eligibility, req, now)}
          end
      end

    Tuple.reconcile(to_string(cell_id(cell)), Map.keys(by_key),
      upsert: fn key ->
        %{state: state} = by_key[key]

        opts =
          case state do
            :affirmed -> [status: spine_status(state, mode, statuses), strength: "attested"]
            _ -> [status: spine_status(state, mode, statuses)]
          end

        # a writer that reports the changed boolean keeps propagation
        # O(real changes); with the spine-only default (:ok) every key
        # propagates — correct, just less scoped.
        Op.put(cell, key, opts) in [true, :ok]
      end
    )
  end

  @doc """
  The SCOPE INSTANCES of a filter-shaped requirement — `{instance_key,
  key_scope}` pairs, one view row each. Pure: `{:filter, ks}` yields the single
  instance (keyed by the requirement's `instance_key`); `{:filter_by, fun}`
  derives one per eligibility key (nil — or a clause that doesn't match the
  key — skips; deduped by instance key, first wins).
  """
  @spec instances(Requirement.t(), [String.t()]) :: [{String.t(), term()}]
  def instances(%Requirement{scope: {:filter, key_scope}} = req, _eligibility) do
    [{req.instance_key || "all", key_scope}]
  end

  def instances(%Requirement{scope: {:filter_by, fun}}, eligibility) do
    eligibility
    |> Enum.map(&instance_of(fun, &1))
    |> Enum.reject(&is_nil/1)
    |> Enum.uniq_by(&elem(&1, 0))
  end

  # "nil skips" must hold for a CLAUSE MISS too: the fun is mapped over EVERY
  # eligibility key, and a host fn written for its expected key shape would
  # otherwise crash the drain on the first unexpected row. Only the host fn's
  # own miss is a skip — a FunctionClauseError from deeper code re-raises.
  defp instance_of(fun, eligibility_key) do
    fun.(eligibility_key)
  rescue
    e in FunctionClauseError ->
      info = Function.info(fun)

      if e.module == info[:module] and e.function == info[:name] and e.arity == 1 do
        nil
      else
        reraise e, __STACKTRACE__
      end
  end

  @doc """
  The admission → spine-status projection, per mode. Pure — the one place the
  blocking/non-blocking distinction lives (force evaluation is identical in
  both; a mode only changes what a not-yet-signed row projects to).
  """
  @spec spine_status(:affirmed | :pending | :refused, :require | :annotate, map()) :: String.t()
  def spine_status(:affirmed, _mode, statuses), do: statuses.affirmed
  def spine_status(:pending, :require, statuses), do: statuses.pending
  def spine_status(:pending, :annotate, statuses), do: statuses.unsigned
  def spine_status(:refused, _mode, statuses), do: statuses.refused

  defp attested_meta(%{meta: %{attested: a}}), do: a

  defp attested_meta(cell) do
    raise ArgumentError,
          "#{inspect(cell_id(cell))} reached Attestation.Op without attested meta — " <>
            "was the graph assembled by ReactiveDag.Node.graph/2 (which resolves requirements)?"
  end

  defp cell_id(%{id: id}), do: id
  defp cell_id(id) when is_binary(id), do: id
end

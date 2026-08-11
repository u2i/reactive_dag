defmodule ReactiveDag.Graph do
  @moduledoc """
  Pure DAG construction: a list of `ReactiveDag.Cell` → a `ReactiveDag.Plan`.

  No live data — only the plan (cells, parent edges, depths). Validates that
  every referenced input exists and the graph is acyclic (raises `ArgumentError`
  on a dangling input or cycle; a host's DSL transformer should catch these at
  compile time, but the runtime builder re-checks so a hand-built plan can't
  wedge the drain).
  """

  alias ReactiveDag.{Cell, Plan}

  @spec build([Cell.t()]) :: Plan.t()
  def build(cells) when is_list(cells) do
    by_id = Map.new(cells, &{&1.id, &1})
    validate_inputs!(cells, by_id)

    %Plan{cells: by_id, parents: build_parents(cells), depths: build_depths(by_id)}
  end

  @doc """
  The parents whose keys should be dirtied when `child` changed, applying the
  host's `key_rule` module (a `ReactiveDag.KeyRule` impl). The rule sees the
  parent, the specific child input, and the changed keys, and returns `:all`
  (whole-cell recompute, the `"*"` wildcard) or `{:keys, mapped}`.

  Returns `[{parent_id, [key]}]`. `key_rule` defaults to identity mapping.
  """
  @spec dirty_parents(Plan.t(), Cell.id(), [String.t()], module()) ::
          [{Cell.id(), [String.t()]}]
  def dirty_parents(%Plan{} = plan, child_id, keys, key_rule \\ ReactiveDag.KeyRule) do
    plan.parents
    |> Map.get(child_id, [])
    |> Enum.map(fn parent_id ->
      parent = Map.fetch!(plan.cells, parent_id)

      case apply_rule(key_rule, parent, child_id, keys) do
        :all -> {parent_id, ["*"]}
        {:keys, mapped} -> {parent_id, mapped}
      end
    end)
  end

  defp apply_rule(mod, parent, child, keys), do: mod.rule(parent, child, keys)

  # ---- parents: inverse of each cell's inputs, EXCLUDING reference edges ----
  # A cell's `inputs` are every edge — used for validation + depth ordering +
  # reading. But a REFERENCE input (listed in `meta.reference_inputs`) is read as
  # CONTEXT, not recomputed on: a change to it must NOT dirty this cell (e.g. an
  # expensive/non-deterministic LLM node that consults mutable reference data). So
  # the propagation graph (`parents`) omits reference edges — the node still reads
  # the current value when it recomputes for other reasons, it just isn't triggered
  # BY that value changing.
  defp build_parents(cells) do
    Enum.reduce(cells, %{}, fn cell, acc ->
      refs = MapSet.new(cell.meta[:reference_inputs] || [])

      cell.inputs
      |> Enum.reject(&MapSet.member?(refs, &1))
      |> Enum.reduce(acc, fn input_id, acc2 ->
        Map.update(acc2, input_id, [cell.id], &[cell.id | &1])
      end)
    end)
    |> Map.new(fn {k, v} -> {k, Enum.reverse(v)} end)
  end

  defp validate_inputs!(cells, by_id) do
    for cell <- cells, input <- cell.inputs, not Map.has_key?(by_id, input) do
      raise ArgumentError, "cell #{inspect(cell.id)} references unknown input #{inspect(input)}"
    end

    :ok
  end

  # ---- depths: longest path from a leaf, memoized, with cycle detection ----
  defp build_depths(by_id) do
    Enum.reduce(Map.keys(by_id), %{}, fn id, memo ->
      {depth, memo} = depth_of(id, by_id, memo, MapSet.new())
      Map.put(memo, id, depth)
    end)
  end

  defp depth_of(id, by_id, memo, on_stack) do
    cond do
      Map.has_key?(memo, id) -> {Map.fetch!(memo, id), memo}
      MapSet.member?(on_stack, id) -> raise ArgumentError, "cycle at cell #{inspect(id)}"
      true -> compute_depth(Map.fetch!(by_id, id), id, by_id, memo, on_stack)
    end
  end

  # Field access (cell.inputs), not a %Cell{} match — a host may pass its OWN
  # cell struct (cascade keeps Cascade.Engine.Cell); the graph math only needs
  # `.id` and `.inputs`.
  defp compute_depth(%{inputs: []}, id, _by_id, memo, _on_stack), do: {0, Map.put(memo, id, 0)}

  defp compute_depth(%{inputs: inputs}, id, by_id, memo, on_stack) do
    on_stack = MapSet.put(on_stack, id)

    {max_input_depth, memo} =
      Enum.reduce(inputs, {-1, memo}, fn input_id, {acc_max, acc_memo} ->
        {d, acc_memo} = depth_of(input_id, by_id, acc_memo, on_stack)
        {max(acc_max, d), acc_memo}
      end)

    depth = max_input_depth + 1
    {depth, Map.put(memo, id, depth)}
  end
end

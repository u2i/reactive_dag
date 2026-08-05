defmodule ReactiveDag.Drain do
  @moduledoc """
  The reactive propagation loop — the heart of the substrate, shared by both
  hosts.

  Given a compiled `Plan` and a host config (`recompute` strategy + `key_rule`),
  drain the frontier to empty:

    1. Pick the dirty cell with the smallest depth (`Frontier.next_cell`) — no
       cell recomputes while an input is still dirty (topological order, no
       external scheduler).
    2. Atomically claim its dirty keys (`Frontier.claim` — delete-returning).
    3. Recompute via the host's `RecomputeStrategy` → the keys that changed.
    4. Propagate: mark the changed keys on the cell's parents, applying the
       host's `KeyRule` (`Graph.dirty_parents`).
    5. Repeat until empty.

  A leaf carries no recompute — a source writes its tuples and marks parents
  dirty directly, so a leaf shouldn't appear in the frontier; if one does, its
  claimed keys are treated as changed and just propagated.

  `run/2` opts:
    * `:recompute` — a `ReactiveDag.RecomputeStrategy` module (required unless the
      graph is leaves-only).
    * `:key_rule`  — a `ReactiveDag.KeyRule` module (default: identity mapping).
    * `:on_step`   — optional `(cell, step) -> any` for tracing, called after each
      cell recomputes. `step` is a map:
        * `:claimed`      — the keys claimed from the frontier this pass
        * `:changed`      — the keys the recompute reported as changed
        * `:triggered_by` — the cell_id whose propagation last dirtied THIS cell
          (nil for the drain's initial frontier), so a trace can reconstruct the
          causal propagation tree without the frontier carrying reasons. The drain
          tracks this itself: when it propagates from A to a parent B, it remembers
          "B's next recompute was triggered by A".
        * `:duration_us` — microseconds the cell's recompute took (the drain
          brackets the `RecomputeStrategy` call).
  """

  require Logger

  alias ReactiveDag.{Cell, Frontier, Graph, Plan}

  @max_passes 100_000

  @spec run(Plan.t(), keyword()) :: {:ok, non_neg_integer()}
  def run(%Plan{} = plan, opts \\ []) do
    do_run(plan, opts, 0, %{})
  end

  defp do_run(_plan, _opts, passes, _cause) when passes >= @max_passes do
    raise "reactive_dag drain exceeded #{@max_passes} passes — likely a cycle or a " <>
            "recompute that keeps re-dirtying its own inputs"
  end

  defp do_run(plan, opts, passes, cause) do
    case Frontier.next_cell(plan.depths) do
      nil ->
        {:ok, passes}

      cell_id ->
        # A whole-cell claim ("*") SUBSUMES any co-claimed specific keys: if "*"
        # is present, the claim is normalized to exactly ["*"]. Recompute strategies
        # test `keys == ["*"]` to mean "recompute the whole cell", so a stray "*"
        # riding alongside real keys must collapse — otherwise the whole-cell branch
        # is missed and the specific keys are processed as if "*" were a real key.
        claimed = Frontier.claim(cell_id)
        keys = if "*" in claimed, do: ["*"], else: claimed

        cause =
          if keys != [] do
            cell = Map.fetch!(plan.cells, cell_id)
            {changed, us} = timed(fn -> recompute(cell, keys, opts) end)
            # A WHOLE-CELL claim ("*") propagates :all: a whole recompute can DELETE
            # keys, and per-key propagation only carries survivors — it would strand
            # a vanished key in the parent. So escalate downstream when the claim was
            # whole, regardless of the per-parent key_rule.
            prop = if "*" in keys, do: :all, else: changed
            parents = propagate(plan, cell_id, prop, opts)

            if f = opts[:on_step] do
              f.(cell, %{
                claimed: keys,
                changed: changed,
                triggered_by: Map.get(cause, cell_id),
                duration_us: us
              })
            end

            # Every parent we just dirtied was triggered by this cell.
            Enum.reduce(parents, cause, fn parent_id, acc -> Map.put(acc, parent_id, cell_id) end)
          else
            cause
          end

        do_run(plan, opts, passes + 1, cause)
    end
  end

  defp timed(fun) do
    t0 = System.monotonic_time(:microsecond)
    result = fun.()
    {result, System.monotonic_time(:microsecond) - t0}
  end

  defp recompute(%Cell{leaf?: true}, keys, _opts), do: keys

  defp recompute(cell, keys, opts) do
    case opts[:recompute] do
      nil ->
        Logger.warning("reactive_dag: cell #{inspect(cell.id)} has no recompute strategy; passing keys through")
        keys

      strategy ->
        {:ok, changed} = strategy.recompute(cell, keys)
        changed
    end
  end

  # Each clause returns the list of parent cell_ids it dirtied (so the drain can
  # record them as triggered-by this cell).
  defp propagate(_plan, _cell_id, [], _opts), do: []

  # :all — the cell was claimed whole; every parent recomputes whole too.
  defp propagate(plan, cell_id, :all, _opts) do
    parents = Map.get(plan.parents, cell_id, [])
    Enum.each(parents, &Frontier.mark_dirty(&1, ["*"], "propagated (all) from #{cell_id}"))
    parents
  end

  defp propagate(plan, cell_id, changed, opts) do
    key_rule = opts[:key_rule] || ReactiveDag.KeyRule

    parents = Graph.dirty_parents(plan, cell_id, changed, key_rule)

    Enum.each(parents, fn {parent_id, keys} ->
      Frontier.mark_dirty(parent_id, keys, "propagated from #{cell_id}")
    end)

    Enum.map(parents, fn {parent_id, _keys} -> parent_id end)
  end
end

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
    * `:on_step`   — optional `(cell, claimed, changed) -> any` for tracing.
  """

  require Logger

  alias ReactiveDag.{Cell, Frontier, Graph, Plan}

  @max_passes 100_000

  @spec run(Plan.t(), keyword()) :: {:ok, non_neg_integer()}
  def run(%Plan{} = plan, opts \\ []) do
    do_run(plan, opts, 0)
  end

  defp do_run(_plan, _opts, passes) when passes >= @max_passes do
    raise "reactive_dag drain exceeded #{@max_passes} passes — likely a cycle or a " <>
            "recompute that keeps re-dirtying its own inputs"
  end

  defp do_run(plan, opts, passes) do
    case Frontier.next_cell(plan.depths) do
      nil ->
        {:ok, passes}

      cell_id ->
        keys = Frontier.claim(cell_id)

        if keys != [] do
          cell = Map.fetch!(plan.cells, cell_id)
          changed = recompute(cell, keys, opts)
          # A WHOLE-CELL claim ("*") propagates :all: a whole recompute can DELETE
          # keys, and per-key propagation only carries survivors — it would strand
          # a vanished key in the parent. So escalate downstream when the claim was
          # whole, regardless of the per-parent key_rule.
          prop = if "*" in keys, do: :all, else: changed
          propagate(plan, cell_id, prop, opts)
          if f = opts[:on_step], do: f.(cell, keys, changed)
        end

        do_run(plan, opts, passes + 1)
    end
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

  defp propagate(_plan, _cell_id, [], _opts), do: :ok

  # :all — the cell was claimed whole; every parent recomputes whole too.
  defp propagate(plan, cell_id, :all, _opts) do
    plan.parents
    |> Map.get(cell_id, [])
    |> Enum.each(&Frontier.mark_dirty(&1, ["*"], "propagated (all) from #{cell_id}"))
  end

  defp propagate(plan, cell_id, changed, opts) do
    key_rule = opts[:key_rule] || ReactiveDag.KeyRule

    plan
    |> Graph.dirty_parents(cell_id, changed, key_rule)
    |> Enum.each(fn {parent_id, keys} ->
      Frontier.mark_dirty(parent_id, keys, "propagated from #{cell_id}")
    end)
  end
end

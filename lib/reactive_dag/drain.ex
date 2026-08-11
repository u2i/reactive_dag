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

  Returns `{:ok, %ReactiveDag.Drain.Report{}}` — the processing trace: one
  step per cell recompute (cell, pass, claimed, changed, triggered_by,
  duration_us), in execution order, plus run totals. The drain knows all of
  this as it works; the report is that knowledge kept instead of discarded.
  Persistence is the host's (an Oban job's meta, a run table) — the library
  reports, the host records.

  ## Concurrency

  The per-cell claim is atomic (a `DELETE … RETURNING`): a dirty KEY is
  consumed exactly once. But the pick-then-claim PAIR is not serialized — two
  concurrent drains over the same graph can select the same cell and both
  recompute it (each claiming a disjoint slice of its keys). Run ONE drain at a
  time per graph (both hosts do: a single worker), or make recomputes
  idempotent so a doubled recompute is merely wasted work.

  `run/2` opts:
    * `:recompute` — a `ReactiveDag.RecomputeStrategy` module (required unless the
      graph is leaves-only).
    * `:key_rule`  — a `ReactiveDag.KeyRule` module (default: identity mapping).
    * `:on_step`   — optional `(cell, step) -> any` for STREAMING (live progress
      UI) — called after each cell recomputes with the same fields the report
      step carries (minus `:cell`/`:pass`, which the report adds). The full
      trace arrives in the report either way; use `on_step` only when you need
      it before the drain finishes.
    * `:max_passes` — runaway guard (default #{100_000}): exceeding it raises
      `ReactiveDag.Drain.RunawayError`, whose `:report` field carries the
      partial trace — the step list showing which cells keep re-dirtying each
      other is exactly the diagnostic for the cycle the guard suspects.
  """

  require Logger

  alias ReactiveDag.{Cell, Drain.Report, Frontier, Graph, Plan}

  @max_passes 100_000

  defmodule RunawayError do
    @moduledoc """
    The drain exceeded its pass budget — likely a cycle, or a recompute that
    keeps re-dirtying its own inputs. `:report` carries the PARTIAL trace up to
    the abort: `report.steps`' tail shows exactly which cells keep triggering
    each other, which is the diagnostic for the loop this error suspects.
    """
    defexception [:message, :report]
  end

  @spec run(Plan.t(), keyword()) :: {:ok, Report.t()}
  def run(%Plan{} = plan, opts \\ []) do
    t0 = System.monotonic_time(:microsecond)
    do_run(plan, opts, 0, %{}, [], t0)
  end

  defp do_run(plan, opts, passes, cause, steps, t0) do
    max = opts[:max_passes] || @max_passes

    if passes >= max do
      report = %Report{
        steps: Enum.reverse(steps),
        passes: passes,
        duration_us: System.monotonic_time(:microsecond) - t0
      }

      recent = report.steps |> Enum.take(-10) |> Enum.map(& &1.cell) |> Enum.uniq()

      raise RunawayError,
        message:
          "reactive_dag drain exceeded #{max} passes — likely a cycle or a recompute " <>
            "that keeps re-dirtying its own inputs (recently: #{inspect(recent)}; " <>
            "the exception's :report holds the full partial trace)",
        report: report
    else
      drain_pass(plan, opts, passes, cause, steps, t0)
    end
  end

  defp drain_pass(plan, opts, passes, cause, steps, t0) do
    case Frontier.next_cell(plan.depths) do
      nil ->
        {:ok,
         %Report{
           steps: Enum.reverse(steps),
           passes: passes,
           duration_us: System.monotonic_time(:microsecond) - t0
         }}

      cell_id ->
        # A whole-cell claim ("*") SUBSUMES any co-claimed specific keys: if "*"
        # is present, the claim is normalized to exactly ["*"]. Recompute strategies
        # test `keys == ["*"]` to mean "recompute the whole cell", so a stray "*"
        # riding alongside real keys must collapse — otherwise the whole-cell branch
        # is missed and the specific keys are processed as if "*" were a real key.
        claimed = Frontier.claim(cell_id)
        keys = if "*" in claimed, do: ["*"], else: claimed

        {cause, steps} =
          cond do
            keys == [] ->
              {cause, steps}

            not Map.has_key?(plan.cells, cell_id) ->
              # A STALE frontier row — the dirty table outlives any one plan, so
              # a renamed/removed cell, a drain over a subgraph plan, or a source
              # writing to an old leaf id can leave ids this plan doesn't know.
              # The claim above already consumed the rows; log what was dropped
              # and keep draining rather than crashing after destroying the work
              # item (and claiming, not skipping, is what prevents the same row
              # from being re-selected forever).
              Logger.warning(
                "reactive_dag: frontier holds cell #{inspect(cell_id)} absent from this plan; " <>
                  "dropping its claimed dirty keys #{inspect(keys)}"
              )

              {cause, steps}

            true ->
              cell = Map.fetch!(plan.cells, cell_id)
              {{changed, meta}, us} = timed(fn -> recompute(cell, keys, opts) end)
              # A WHOLE-CELL claim ("*") propagates :all: a whole recompute can DELETE
              # keys, and per-key propagation only carries survivors — it would strand
              # a vanished key in the parent. So escalate downstream when the claim was
              # whole, regardless of the per-parent key_rule.
              prop = if "*" in keys, do: :all, else: changed
              parents = propagate(plan, cell_id, prop, opts)

              step = %{
                claimed: keys,
                changed: changed,
                triggered_by: Map.get(cause, cell_id),
                duration_us: us,
                meta: meta
              }

              if f = opts[:on_step], do: f.(cell, step)

              # Every parent we just dirtied was triggered by this cell.
              cause =
                Enum.reduce(parents, cause, fn parent_id, acc ->
                  Map.put(acc, parent_id, cell_id)
                end)

              {cause, [Map.merge(step, %{cell: cell_id, pass: passes}) | steps]}
          end

        do_run(plan, opts, passes + 1, cause, steps, t0)
    end
  end

  defp timed(fun) do
    t0 = System.monotonic_time(:microsecond)
    result = fun.()
    {result, System.monotonic_time(:microsecond) - t0}
  end

  defp recompute(%Cell{leaf?: true}, keys, _opts), do: {keys, %{}}

  defp recompute(cell, keys, opts) do
    case opts[:recompute] do
      nil ->
        # a DERIVED cell reached with no strategy is a config error, not a
        # recoverable state — passing keys through silently propagates values
        # nothing recomputed. (Leaves never reach here; a leaves-only graph
        # needs no :recompute.)
        raise ArgumentError,
              "reactive_dag: derived cell #{inspect(cell.id)} claimed dirty keys but " <>
                "Drain.run was given no :recompute strategy"

      strategy ->
        case strategy.recompute(cell, keys) do
          {:ok, changed} -> {changed, %{}}
          # a strategy may report what the work cost (tokens, retries, cache
          # hits); the library carries the map without interpreting it
          {:ok, changed, meta} when is_map(meta) -> {changed, meta}
        end
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

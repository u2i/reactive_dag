if Code.ensure_loaded?(Oban.Worker) do
  defmodule ReactiveDag.ReprocessWorker do
    @moduledoc """
    Re-derive a cell's rows without any input having changed — *the code moved,
    not the data*.

    The frontier says "an input moved, redo this". That is the wrong sentence for
    a changed prompt, a fixed fold, or a suspect result: the inputs are identical
    and the function is not. Mechanically the mark is the same, but the reason
    matters, and a job is the honest place to put it.

    ## Selecting what to redo

    Whole cell, and everything below it:

        %{"cell" => "budget_rollups"} |> ReactiveDag.ReprocessWorker.new() |> Oban.insert()

    A slice — the common case, and what `slice` exists for:

        %{"cell" => "budget_rollups", "where" => %{"fiscal_year" => "FY25"}}
        |> ReactiveDag.ReprocessWorker.new()
        |> Oban.insert()

    Or exact keys, when a UI has already chosen them:

        %{"cell" => "budget_rollups", "keys" => ["gf|FY25", "water|FY25"]}

    ## Whole-cell means everything downstream

    A `"*"` claim propagates `:all`, so reprocessing a cell re-derives every cell
    beneath it. That is usually what "the code changed" means and occasionally
    much more work than intended — a slice or an explicit key list keeps it
    proportional, which is the whole point of the engine.

    ## It invalidates first

    A `per_key` node skips rows whose declared inputs have not moved — and after
    a prompt change they have not. Marking alone would therefore skip exactly the
    rows you asked it to redo.

    So the stored fingerprint is CLEARED on the selected keys before they are
    marked. That is not a bypass: a null fingerprint means "no valid prior
    result", which is precisely true once the code that produced it has changed.
    The recompute then runs for the ordinary reason, and stores a fresh
    fingerprint as it always would.

    `invalidated` in the telemetry says how many rows that touched — 0 on a node
    with no fingerprint column, which needs no invalidation to recompute.

    ## Telemetry

    `[:reactive_dag, :reprocess, :start]` with `system_time`, and
    `[:reactive_dag, :reprocess, :stop]` with `claimed`, `invalidated` and
    `changed`, so a dashboard can say *"queued 412, 88 actually moved"*.

    Both carry `cell`, `reason` and the job's own `args` — a reprocess is
    usually one leg of something a host named, and only the job knows what.
    """
    # Unique on the same terms as `ScanWorker`: a reprocess of the same cell and
    # selection, queued twice, is the same work twice. Two DIFFERENT selections
    # of one cell are different args and both run.
    use Oban.Worker,
      queue: :scans,
      max_attempts: 1,
      unique: [
        period: :infinity,
        fields: [:args, :queue, :worker],
        states: :incomplete
      ]

    require Logger

    alias ReactiveDag.{Drain, Frontier, Graph, Job}
    alias ReactiveDag.Node.Rows

    @impl Oban.Worker
    def perform(%Oban.Job{args: args}) do
      cell_id = Map.fetch!(args, "cell")
      plan = Job.plan(args, __MODULE__)
      reason = Map.get(args, "reason", "reprocess")

      case plan.cells[cell_id] do
        nil ->
          # Not a failure to retry: the graph will not grow this cell on the next
          # attempt either.
          Logger.warning("reactive_dag: cannot reprocess #{cell_id} — no such cell in this plan")
          :ok

        cell ->
          keys = select(cell, args)

          # Clear the stored fingerprints FIRST, or a `per_key` node skips the
          # very rows we just claimed: its fingerprint answers "did the input
          # move?" and after a code change it did not. A null fingerprint means
          # "no valid prior result", which is exactly true here.
          invalidated = invalidate(cell, keys)

          mark(plan, cell_id, keys, reason)

          t0 = System.monotonic_time(:microsecond)

          :telemetry.execute(
            [:reactive_dag, :reprocess, :start],
            %{system_time: System.system_time()},
            %{cell: cell_id, args: args, reason: reason}
          )

          {:ok, report} = Drain.run(plan, Job.drain_opts(args))

          :telemetry.execute(
            [:reactive_dag, :reprocess, :stop],
            %{
              duration_us: System.monotonic_time(:microsecond) - t0,
              claimed: claimed_count(keys),
              invalidated: length(invalidated),
              changed: ReactiveDag.Drain.Report.changed_total(report),
              passes: report.passes
            },
            %{cell: cell_id, args: args, reason: reason, report: report}
          )

          :ok
      end
    end

    # Explicit keys win; a `where` filter selects them from the node's own rows;
    # neither means the whole cell.
    defp select(_cell, %{"keys" => keys}) when is_list(keys) and keys != [], do: keys

    defp select(cell, %{"where" => %{} = where}) when map_size(where) > 0 do
      Rows.keys_where(cell, Enum.map(where, fn {k, v} -> {String.to_existing_atom(k), v} end))
    end

    defp select(_cell, _args), do: ["*"]

    # A whole-cell claim propagates `:all` to parents; specific keys go through
    # the key rule, exactly as a scan's would. Marking the leaf without its
    # parents would strand the change one level up.
    defp mark(_plan, _cell_id, [], _reason), do: :ok

    defp mark(plan, cell_id, keys, reason) do
      Frontier.mark_dirty(cell_id, keys, reason)

      for {parent, parent_keys} <-
            Graph.dirty_parents(plan, cell_id, keys, ReactiveDag.Node.KeyRule) do
        Frontier.mark_dirty(parent, parent_keys, reason)
      end

      :ok
    end

    # `"*"` is not a key list, so everything the cell holds is invalidated.
    defp invalidate(cell, ["*"]), do: Rows.invalidate(cell, :all)
    defp invalidate(cell, keys), do: Rows.invalidate(cell, keys)

    defp claimed_count(["*"]), do: nil
    defp claimed_count(keys), do: length(keys)
  end
end

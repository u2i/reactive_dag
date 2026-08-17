if Code.ensure_loaded?(Oban.Worker) do
  defmodule ReactiveDag.DrainWorker do
    @moduledoc """
    The Oban job that drains the frontier and nothing else.

    `dirties_on` writes a dirty mark inside the write's transaction and stops.
    Something has to consume it, and until this existed nothing did: the mark sat
    in the frontier until the next `ScanWorker` sweep happened along. A write-fed
    leaf was therefore durable but not prompt — correct on the next sweep, stale
    until then, and for a graph with no polling source on that cadence there may
    be no next sweep at all (u2i/reactive_dag#142).

    ## Why the adjacent tools are all wrong

    A host reaching for this finds four things, three of which fail quietly:

      * `ScanWorker` with `%{"cell" => id}` resolves a scanner off the graph and
        returns `{:cancel, :no_scanner}`. A `dirties_on` leaf has no scanner *by
        design*, so that is a cancelled job which looks like a completed one and
        recomputes nothing.
      * `ReprocessWorker` works, and does damage on the way: it INVALIDATES
        fingerprints before marking. That is right for "the code changed" and
        wrong here — the data genuinely moved, the mark is already correct, and
        invalidating forces recompute of rows nothing touched.
      * doing it in the write transaction holds `Frontier.with_lock/2` — a
        cluster-wide advisory lock — under a user request.
      * doing it synchronously makes the caller wait on a recompute of everything
        downstream.

    So: a job that does exactly `Drain.run/2`, and is safe to enqueue from inside
    a transaction.

    ## Scheduling it

    Declaratively, from the option that creates the obligation:

        reactive do
          id :attestations
          dirties_on [:create, :destroy], schedule_drain: true
        end

    The enqueue then joins the write's transaction, so it commits atomically with
    the mark — an INSERT is cheap, unlike the drain it schedules. A rolled-back
    write leaves neither.

    By hand, which is the same thing:

        ReactiveDag.DrainWorker.enqueue()

    ## Why the frontier is not an argument

    `Drain.run/2` takes no cell scope: a drain reads `SELECT DISTINCT cell_id`
    and processes every dirty cell, whoever marked it. So this job carries no
    cell, and there is nothing to key per-cell jobs on — N of them would each do
    the same global work.

    That is also what makes coalescing trivial. Oban's uniqueness over
    `(worker, args, queue)` with empty args means a burst of N writes enqueues at
    most one pending drain, and the one that runs covers every mark that arrived
    before it started.

    ## A mark arriving mid-drain is not lost

    Worth stating, because it decides the uniqueness config and the obvious
    reasoning is wrong.

    `states: :incomplete` includes `executing`, so a write landing while a drain
    runs does NOT enqueue a second job. That would strand the mark if a drain
    snapshotted the frontier at its start — but it does not: `Drain.run/2` loops
    on `Frontier.next_cell/1`, which re-reads the dirty set every pass and always
    takes the SHALLOWEST dirty cell. A leaf is shallower than anything the drain
    is working through, so a mark written on one is claimed by the pass after it
    lands.

    The residual gap is a mark arriving after the final `next_cell/1` has already
    returned `nil`. That waits for the next drain — bounded by whatever else is
    scheduled, and the narrowest version of the window this worker exists to
    close.

    Excluding `executing` to close even that was the first version of this, and
    it is worse: Oban warns that a partial state list breaks uniqueness (any
    missing in-flight state does), and the coalescing it buys back is the thing
    that keeps a bulk import from enqueueing thousands of drains.

    Two drains never overlap in effect anyway: `Frontier.with_lock/2` serialises
    them cluster-wide and the loser stands down. Nothing is lost by standing down
    — the frontier is a set, and whoever holds the lock drains the marks the
    other would have.
    """
    use Oban.Worker,
      queue: :drain,
      max_attempts: 1,
      unique: [
        period: :infinity,
        fields: [:args, :queue, :worker],
        # The named group, matching `ScanWorker`. A hand-listed subset warns —
        # any missing in-flight state can break uniqueness — and excluding
        # `executing` is unnecessary here: see the moduledoc on why a mark
        # arriving mid-drain is still claimed.
        states: :incomplete
      ]

    require Logger

    alias ReactiveDag.Drain

    @doc """
    Enqueue a drain, coalescing with any already pending.

    Safe to call inside a transaction: it is one INSERT, and Oban's uniqueness
    turns a burst into a single pending job. Returns `{:ok, job}` — including
    when the job was a duplicate, which Oban reports as a conflict rather than an
    error.

    `opts` are passed to `new/2`, so a host can add `schedule_in:` to debounce
    further, or `"plan_mfa"` via args where the configured default is wrong.
    """
    @spec enqueue(map(), keyword()) :: {:ok, Oban.Job.t()} | {:error, term()}
    def enqueue(args \\ %{}, opts \\ []) do
      args |> __MODULE__.new(opts) |> Oban.insert()
    end

    @impl Oban.Worker
    def perform(%Oban.Job{args: args}) do
      plan = ReactiveDag.Job.plan(args, __MODULE__)
      t0 = System.monotonic_time(:microsecond)

      :telemetry.execute(
        [:reactive_dag, :drain_job, :start],
        %{system_time: System.system_time()},
        %{args: args}
      )

      # The same lock the sweep takes, for the same reason: two nodes draining at
      # once duplicates work and can interleave passes. The loser stands down —
      # the frontier is a set, so the winner drains what the loser would have.
      case ReactiveDag.Frontier.with_lock(fn ->
             Drain.run(plan, ReactiveDag.Job.drain_opts(args))
           end) do
        {:ok, {:ok, report}} ->
          :telemetry.execute(
            [:reactive_dag, :drain_job, :stop],
            %{
              duration_us: System.monotonic_time(:microsecond) - t0,
              passes: report.passes
            },
            # `args` verbatim, like `ScanWorker`: a host's activity page groups by
            # a run id only the enqueuer knows, and without this a handler can see
            # that a drain finished but not what it belonged to.
            %{args: args, report: report}
          )

          :ok

        :busy ->
          # Not a failure. Another node holds the lock and will drain these marks;
          # re-enqueueing would spin against it.
          Logger.info("reactive_dag: drain skipped, another node holds the lock")
          :ok
      end
    end
  end
end

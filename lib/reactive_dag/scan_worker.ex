if Code.ensure_loaded?(Oban.Worker) do
  defmodule ReactiveDag.ScanWorker do
    @moduledoc """
    The Oban job that polls one scanner and drains what it changed.

    Every host that scans grew this worker independently, and each time it was
    the same five lines of engine logic wrapped in that host's own observability:
    poll, normalise the return shape, mark the frontier, propagate to parents,
    drain. Providing it means a host schedules scans without re-deriving the
    loop — and gets the poll/drain split right by construction rather than by
    reading the guide.

    ## Scheduling it

    `ReactiveDag.Source.crontab/2` reads the cadence each leaf declared and emits
    entries for Oban's cron plugin:

        config :my_app, Oban,
          queues: [scans: 1],
          plugins: [
            {Oban.Plugins.Cron,
             crontab: ReactiveDag.Source.crontab(MyApp.Dag.plan(), ReactiveDag.ScanWorker)}
          ]

    A single-concurrency `:scans` queue is the usual choice: two concurrent polls
    of the same upstream are wasted requests, and the drain is cheaper batched.

    ## Running one on demand

        %{"cell" => "agenda_docs"} |> ReactiveDag.ScanWorker.new() |> Oban.insert()

    or with a wider bound than the leaf's standing default:

        %{"cell" => "agenda_docs", "opts" => %{"recent" => false}}
        |> ReactiveDag.ScanWorker.new()
        |> Oban.insert()

    ## What it does not own

    The plan, because a job argument cannot carry one — see `:plan_mfa` below.

    Domain observability. A host that audits its crawls, records a run id, or
    enqueues follow-up work wraps this rather than extending it: call
    `ReactiveDag.Source.refresh/3` and `ReactiveDag.Drain.run/2` directly inside
    your own worker, which is all this module does. It exists to save you writing
    it, not to stop you.

    ## Telemetry

    Emits `[:reactive_dag, :scan, :stop]` with `changed` and `passes`, and
    `[:reactive_dag, :scan, :exception]` on failure. The drain inside emits its
    own events, so a host attaching to `[:reactive_dag, :drain, :stop]` sees the
    recompute trace without attaching here at all.
    """
    use Oban.Worker, queue: :scans, max_attempts: 1

    require Logger

    alias ReactiveDag.{Drain, Source}

    @impl Oban.Worker
    def perform(%Oban.Job{args: args}) do
      cell_id = Map.fetch!(args, "cell")
      plan = ReactiveDag.Job.plan(args, __MODULE__)
      opts = poll_opts(args)

      t0 = System.monotonic_time(:microsecond)

      try do
        case Source.refresh(plan, cell_id, opts) do
          {:ok, result} ->
            # Drain even when nothing changed: another source may have marked
            # cells this job is now the first to reach, and an empty frontier is
            # a no-op anyway.
            {:ok, report} = Drain.run(plan, ReactiveDag.Job.drain_opts(args))

            :telemetry.execute(
              [:reactive_dag, :scan, :stop],
              %{
                duration_us: System.monotonic_time(:microsecond) - t0,
                changed: length(result.changed),
                passes: report.passes
              },
              %{cell: cell_id, unreachable: result.unreachable, report: report}
            )

            warn_unreachable(cell_id, result)
            :ok

          {:error, :no_scanner} ->
            # Not a failure to retry: the graph says this cell has no scanner, and
            # it will not have one on the next attempt either.
            Logger.warning("reactive_dag: #{cell_id} has no scanner; nothing to poll")
            :ok

          {:error, reason} ->
            {:error, reason}
        end
      rescue
        e ->
          :telemetry.execute(
            [:reactive_dag, :scan, :exception],
            %{duration_us: System.monotonic_time(:microsecond) - t0},
            %{cell: cell_id, reason: e}
          )

          reraise e, __STACKTRACE__
      end
    end

    # An outage is not a quiet success: the poll wrote nothing for the upstreams
    # it could not reach, so those keys are STALE rather than absent, and nothing
    # downstream will recompute to reveal it.
    defp warn_unreachable(_cell_id, %{unreachable: []}), do: :ok

    defp warn_unreachable(cell_id, %{unreachable: upstreams}) do
      Logger.warning(
        "reactive_dag: scan of #{cell_id} could not reach #{length(upstreams)} upstream(s): " <>
          "#{inspect(upstreams)}. Nothing was written for them, so their rows are stale " <>
          "rather than gone — which is correct, and invisible unless you surface it."
      )
    end

    # JSON round-trips string keys; the poll contract takes a keyword list. Only
    # the keys a scanner declares are meaningful, so this is a straight
    # translation rather than a schema.
    defp poll_opts(%{"opts" => %{} = opts}) do
      for {k, v} <- opts, do: {String.to_atom(k), v}
    end

    defp poll_opts(_args), do: []

  end
end

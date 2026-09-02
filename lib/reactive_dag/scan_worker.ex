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

    Domain observability — auditing crawls, recording run ids, enqueuing
    follow-up work. Not because those belong outside a library, but because this
    module is a convenience over two public calls and has no opinion about them.

    Most of that needs one thing: *the loop finished, here is what happened*.
    `[:reactive_dag, :scan, :stop]` carries a `ReactiveDag.ScanRun` under `run`
    — the poll and the drain it triggered as ONE value, which is what a
    broadcast, a durable scan record or a follow-up enqueue actually wants. The
    same facts are also present as flat keys (`cell`, `args`, `unreachable`,
    `detail`, `report`) for handlers written before the struct existed.

    `ScanRun.total/2` is the one that needed a value rather than a payload: a
    run's cost lives in BOTH phases — the crawl's own spend in `detail`, its
    downstream recomputes' in the report's steps — and adding them was left to
    every caller. `:start` covers the same work at
    the other end, for anything a person watches while the poll runs. Anything inside the poll itself — wrapping each HTTP request,
    mirroring listing pages — belongs in your `poll/1`, which the library never
    looks inside.

    Where that is not enough, call `ReactiveDag.Source.refresh/3` and
    `ReactiveDag.Cascade.run/3` directly: that is all this module does. It exists
    to save you writing the loop, not to stop you writing a different one.

    ## Watching a sweep

    A sweep is one job that can run for minutes, so `:start` and `:stop` bracket
    the whole run and say nothing about what is happening inside it.
    `[:reactive_dag, :scan, :source_stop]` fires as each source finishes,
    carrying that source's own result — which source went, how long it took, and
    what it found. That is the progress signal and the per-source record both.

    `:stop` then carries `results` (`%{module => result}`) as well as the
    aggregate, so a host that only wants the end state has it in one payload.

    ## Three outcomes

    | the scanner returns | the job | why |
    |---|---|---|
    | `{:ok, result}` | `:ok` | it looked |
    | `{:error, :not_scannable}` | `{:cancel, reason}` | it cannot look, and retrying will not change that |
    | `{:error, {:not_scannable, why}}` | `{:cancel, reason}` | …and it can say why |
    | `{:error, reason}` | `{:error, reason}` | it failed; a retry might work |

    A source with no credential configured, or an integration not enabled for
    this tenant, is not a fault: retrying cannot conjure a missing credential,
    and burning every attempt to land in `discarded` reads as *"something is
    broken"* when the honest answer is *"this was never going to work"*.

    The judgement belongs to the SCANNER, because only it knows the difference
    between an upstream that is down and one that was never configured.

    `:stop` still fires for an unscannable source, carrying `not_scannable:` in
    its metadata — it is a completed scan that found nothing, and a host
    recording scan results wants the row. An outage is not a quiet success, and
    neither is a missing credential.

    ## Telemetry

    | event | measurements | metadata |
    |---|---|---|
    | `[:reactive_dag, :scan, :start]` | `system_time` | `cell`, `args` |
    | `[:reactive_dag, :scan, :stop]` | `duration_us`, `changed`, `passes` | `cell`, `args`, `unreachable`, `detail`, `report`, `run` |
    | `[:reactive_dag, :scan, :exception]` | `duration_us` | `cell`, `args`, `reason` |
    | `[:reactive_dag, :scan, :source_stop]` | `duration_us` | `source`, `result` |
    | `[:reactive_dag, :scan, :progress]` | `done`, `total` | `cell`, `label`, `source` |

    `:progress` comes from a SCANNER, via `ReactiveDag.Source.progress/3`, and is
    the only signal from inside one poll: a crawl of 700 documents is otherwise a
    single `:source_stop` that fires once it is already over. Nothing emits it
    unless a scanner chooses to.

    `:source_stop` fires once per source inside a sweep, as it finishes. It comes
    from `Source.poll_all/2` rather than this worker, so a host calling that
    directly gets the same signal.

    A poll can run for minutes, so `:start` is what lets a page show a crawl as
    in-flight rather than appearing only once it is over.

    **`args` is the job's own arguments, verbatim.** A scan is often one leg of a
    RUN whose id the enqueuer chose — `crontab/3` takes `args:` for exactly this
    — and only the job carries it. A handler that sees `cell` alone knows which
    cell finished and not which run it belonged to, so it cannot write the row,
    address the broadcast or group the trace, and the work has to fork this
    worker instead of attaching to it.

    The drain inside emits its own events, so a host attaching to
    `[:reactive_dag, :cascade, :stop]` sees the recompute trace without attaching
    here at all.
    """
    # UNIQUE per (args, queue, worker) while a job is available or executing.
    #
    # Two things this covers. A cron entry that fires while the previous run of
    # it is still going — a crawl taking longer than its own cadence — enqueues
    # a duplicate that would poll the same upstreams concurrently. And on a
    # MULTI-NODE cluster every node's Cron plugin inserts the entry at the same
    # minute, so without this an N-node deploy runs every sweep N times.
    #
    # `states: :incomplete` rather than the default `:successful`: the question
    # is "is this already queued or running", not "did an identical job succeed
    # recently". A sweep SHOULD run again next hour; it should not run twice at
    # once. (Oban's named group, rather than a hand-listed set — it warns that a
    # literal list misses `:suspended`, which is exactly the kind of gap that
    # would show up only under load.)
    use Oban.Worker,
      queue: :scans,
      max_attempts: 1,
      unique: [
        period: :infinity,
        fields: [:args, :queue, :worker],
        states: :incomplete
      ]

    require Logger

    alias ReactiveDag.Source

    @impl Oban.Worker
    def perform(%Oban.Job{args: %{"sweep" => true} = args}) do
      plan = ReactiveDag.Job.plan(args, __MODULE__)
      opts = poll_opts(args)

      :telemetry.execute(
        [:reactive_dag, :scan, :start],
        %{system_time: System.system_time()},
        %{cell: :sweep, args: args}
      )

      # ONE job, every source, in graph order, then ONE drain. Sources run
      # sequentially in this process, so a source declaring `depends_on` another
      # genuinely sees what it wrote — which N independent cron entries cannot
      # promise however they are sorted.
      # The lock is around the WHOLE sweep, not just the drain: two nodes polling
      # the same upstreams at the same minute is duplicated external I/O, which
      # is the expensive half. Oban's uniqueness already stops the duplicate
      # ENQUEUE; this covers a host that triggers a sweep by hand, or one whose
      # jobs were inserted before the unique constraint existed.
      # SCOPED to the plan's tenant, so tenants sweep CONCURRENTLY. The lock
      # exists to stop two nodes polling the same upstreams at the same minute;
      # two different tenants polling their own upstreams is not that, and one
      # global lock would make them queue behind each other — the opposite of
      # why a graph is per tenant.
      case ReactiveDag.Lock.with_lock(fn -> sweep(plan, opts, args) end,
             scope: lock_scope(plan)
           ) do
        {:ok, result} ->
          result

        :busy ->
          # not a failure: another node is running this sweep, and the frontier
          # is a set — nothing is lost by standing down
          Logger.info("reactive_dag: sweep skipped, another node holds the lock")
          :ok
      end
    end

    def perform(%Oban.Job{args: args}) do
      cell_id = Map.fetch!(args, "cell")
      plan = ReactiveDag.Job.plan(args, __MODULE__)
      opts = poll_opts(args)

      t0 = System.monotonic_time(:microsecond)

      # A poll can run for minutes, so "it started" is a thing a person watches
      # for. Without this the only observable moment is the end, and a page
      # showing a crawl looks idle while the machine is busy.
      :telemetry.execute(
        [:reactive_dag, :scan, :start],
        %{system_time: System.system_time()},
        %{cell: cell_id, args: args}
      )

      try do
        # The host's wrapper, if it configured one, is present for the POLL
        # only — the part a telemetry handler cannot be inside. It may add
        # options (a collector's pid, say), which is the reason it takes the
        # poll rather than the whole job: nothing a wrapper starts should
        # outlive the fetch it was started for. See `ReactiveDag.Job.around_poll/2`.
        polled =
          ReactiveDag.Job.around_poll(args, fn extra ->
            Source.refresh(plan, cell_id, Keyword.merge(opts, extra))
          end)

        case polled do
          {:ok, result} ->
            # NO DRAIN HERE. `Source.refresh/3` enqueues a cascade per changed
            # leaf, in the poll's own transaction — so the propagation is
            # already scheduled by the time this returns, and running a second
            # engine over the same graph would duplicate it.
            #
            # This also removes the old "drain even when nothing changed" step:
            # there is no shared queue left for another source to have filled,
            # so an unchanged poll now costs nothing at all.

            :telemetry.execute(
              [:reactive_dag, :scan, :stop],
              %{
                duration_us: System.monotonic_time(:microsecond) - t0,
                changed: length(result.changed),
                # `passes: 0` — a scan no longer drains. It observes and
                # enqueues; the cascades it started report themselves under
                # `[:reactive_dag, :cascade, :stop]`. Reporting a borrowed
                # number here would say this job did work it did not do.
                passes: 0
              },
              # `args` verbatim: a scan is often one leg of a RUN whose id the
              # enqueuer chose, and only the job carries it. Without this a
              # handler knows which cell finished and not which run it belonged
              # to, so it cannot write the row, address the broadcast, or group
              # the trace — and the work has to fork the worker instead.
              #
              # `detail` is what the POLL reported about its own work — token
              # spend for a crawler that classifies with a model, rows scanned,
              # anything. Forwarded because this event is the only place it can
              # reach a live consumer: `report` covers the DRAIN, and a scan's
              # own cost appears in no step. Without it a host could roll spend
              # up after a sweep (`Source.detail_total/2`) but never show it as
              # the scan finished.
              %{
                cell: cell_id,
                args: args,
                # WHICH GRAPH this scan was. A handler recording the run needs it
                # to pass to `Insights.record/2`, and the plan is the only thing
                # that knows — a job argument cannot carry one.
                tenant: plan.tenant,
                unreachable: result.unreachable,
                detail: result[:detail] || %{},
                report: nil,
                # The poll and the drain as ONE value — see `ReactiveDag.ScanRun`.
                # The flat keys above stay for handlers written before it; this
                # is what a new one should read, and it is the only thing that
                # can answer "what did this RUN cost" without the caller adding
                # the two phases together itself.
                run: %ReactiveDag.ScanRun{
                  cell: cell_id,
                  changed: result.changed,
                  unreachable: result.unreachable,
                  detail: result[:detail] || %{},
                  report: nil,
                  duration_us: System.monotonic_time(:microsecond) - t0
                }
              }
            )

            warn_unreachable(cell_id, result)
            :ok

          {:error, :no_scanner} ->
            # The graph says this cell has no scanner, and it will not have one
            # on the next attempt either.
            Logger.warning("reactive_dag: #{cell_id} has no scanner; nothing to poll")
            {:cancel, :no_scanner}

          {:error, reason} ->
            unscannable(reason, cell_id, args, t0)
        end
      rescue
        e ->
          :telemetry.execute(
            [:reactive_dag, :scan, :exception],
            %{duration_us: System.monotonic_time(:microsecond) - t0},
            %{cell: cell_id, args: args, reason: e}
          )

          reraise e, __STACKTRACE__
      end
    end

    # THREE outcomes, not two. A scan can succeed, fail in a way that retrying
    # might fix (a timeout, a 503), or be structurally unscannable — no
    # credential configured, an integration not enabled for this tenant. The
    # third is not a fault: retrying cannot conjure a missing credential, and
    # burning every attempt to land in `discarded` reads as "something is
    # broken" when the honest answer is "this was never going to work"
    # (u2i/reactive_dag#122).
    #
    # A scanner says so by returning `{:error, :not_scannable}` — or
    # `{:error, {:not_scannable, reason}}` when it can say why. The judgement
    # belongs to the scanner because only it knows the difference between an
    # upstream that is down and one that was never configured.
    #
    # `:stop` still fires: an unscannable source is a COMPLETED scan that found
    # nothing, and a host recording scan results wants the row.
    defp unscannable({:not_scannable, why} = reason, cell_id, args, t0) do
      Logger.info("reactive_dag: #{cell_id} is not scannable (#{inspect(why)}); not retrying")
      emit_stop(cell_id, args, t0, reason)
      {:cancel, reason}
    end

    defp unscannable(:not_scannable, cell_id, args, t0) do
      Logger.info("reactive_dag: #{cell_id} is not scannable; not retrying")
      emit_stop(cell_id, args, t0, :not_scannable)
      {:cancel, :not_scannable}
    end

    defp unscannable(reason, _cell_id, _args, _t0), do: {:error, reason}

    defp emit_stop(cell_id, args, t0, reason) do
      duration_us = System.monotonic_time(:microsecond) - t0

      :telemetry.execute(
        [:reactive_dag, :scan, :stop],
        %{duration_us: duration_us, changed: 0, passes: 0},
        # `detail: %{}` here too, which this used to omit — a handler reading it
        # got `nil` from an unscannable source and a map from every other, for
        # no reason a caller could infer. Building the payload from a
        # `%ScanRun{}` makes that kind of drift a compile-time concern rather
        # than a matter of keeping two literals in step.
        %{
          cell: cell_id,
          args: args,
          unreachable: [],
          detail: %{},
          report: nil,
          not_scannable: reason,
          # `report: nil` — an unscannable source completes WITHOUT draining, so
          # `report` stays nil rather than a host inferring propagation from
          # a zero pass count that means something else.
          run: %ReactiveDag.ScanRun{
            cell: cell_id,
            not_scannable: reason,
            duration_us: duration_us
          }
        }
      )
    end

    # An outage is not a quiet success: the poll wrote nothing for the upstreams
    # it could not reach, so those keys are STALE rather than absent, and nothing
    # downstream will recompute to reveal it.
    # The advisory-lock key: the dirty table for an untenanted plan (the previous
    # behaviour, byte for byte), else that table AND the tenant — so tenants do
    # not contend and two nodes on one tenant still do.
    defp lock_scope(%ReactiveDag.Plan{tenant: "*"}), do: nil
    defp lock_scope(%ReactiveDag.Plan{tenant: t}), do: {:tenant, t}
    defp lock_scope(_plan), do: nil

    defp sweep(plan, opts, args) do
      t0 = System.monotonic_time(:microsecond)

      # A sweep polls too, so the host's wrapper belongs here on the same terms
      # — one wrapper for the whole sweep, since that is what the job is.
      polled =
        ReactiveDag.Job.around_poll(args, fn extra ->
          Source.poll_all(plan, Keyword.merge(opts, extra))
        end)

      case polled do
        {:ok, results} ->
          # As above: `poll_all/2` has already enqueued a cascade per changed
          # leaf.
          changed =
            results |> Map.values() |> Enum.flat_map(&Map.get(&1, :changed, [])) |> Enum.uniq()

          :telemetry.execute(
            [:reactive_dag, :scan, :stop],
            %{
              duration_us: System.monotonic_time(:microsecond) - t0,
              changed: length(changed),
              passes: 0
            },
            # `results` in full, not just its keys. It is `%{module => result}`
            # — each source's own `changed`, `unreachable` and whatever else it
            # reported — and it was computed for a total, then thrown away one
            # line later. A host wanting a per-source record attaches one
            # handler; one that does not, ignores the key
            # (u2i/reactive_dag#133).
            %{
              cell: :sweep,
              args: args,
              # WHICH GRAPH this sweep was — same reason as the single-cell scan
              # above: a handler recording the run needs it for
              # `Insights.record/2`, and only the plan knows.
              tenant: plan.tenant,
              sources: Map.keys(results),
              results: results,
              report: nil
            }
          )

          :ok

        {:error, failures} ->
          # one bad source does not cancel the others' work: `poll_all/2` has
          # already polled everything it could
          Logger.warning("reactive_dag: sweep had failing sources: #{inspect(failures)}")
          {:error, failures}
      end
    end

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

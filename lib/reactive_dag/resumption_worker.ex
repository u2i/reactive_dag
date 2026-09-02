if Code.ensure_loaded?(Oban.Worker) do
  defmodule ReactiveDag.ResumptionWorker do
    @moduledoc """
    The Oban job that resumes a suspended cascade — and the one place expensive
    work runs.

    A cascade that reached something it could not finish inline recorded a
    suspension and committed. This picks that up: it reads every suspension at
    the stopping point, does the work ONCE, writes the result, cascades onward
    from that write, and discharges exactly the rows it read.

    ## The shape, and why each part is where it is

        1. suspensions = Suspension.at(point)     one read
           []  ->  exit. A duplicate job, and an ordinary outcome.

        2. ids = their ids                        remembered BEFORE the work

        3. recompute                              OUTSIDE any transaction
                                                    ← this is the whole point

        4. transaction:
             write the result
             cascade onward to the next stop
             discharge(ids)                       by id, never by point

    **Step 3 outside a transaction** is the redesign. The drain held one open
    across the recompute, so a nine-minute extraction sat inside it until Neon
    closed the connection at five. Here nothing is open while the model runs.

    **Step 4's `discharge(ids)`** is what makes the append-only table safe. A
    change arriving during step 3 writes a new suspension at the same point;
    deleting by point would take it too, discarding a change nobody observed.
    Deleting by the ids read in step 1 cannot: the new row is not in the list,
    and the next job picks it up.

    ## The two reasons differ only in step 3

    `:expensive` runs the recompute. `:approval` skips it — the write already
    happened, the gate merely opened, so there is nothing to compute and the
    job only propagates. Same read, same discharge, same everything else, which
    is the argument for one table and one worker rather than two of each.

    ## Uniqueness IS `:infinity` here

    Unlike `ReactiveDag.CascadeWorker`, whose args name a change and must not
    coalesce forever. These args name a STOPPING POINT. A second change to the
    same point writes a second suspension row but must not enqueue a second
    job: the pending job will read both when it runs.

    That is the whole coalescing story, and it is why the args deliberately
    carry no version and no suspension id. A job queued at 12:00 and run at
    12:05 must act on what is true at 12:05.

    ## It may run twice, and that is safe

    There is no lock. At-least-once delivery over idempotent work is a stronger
    position than exactly-once scheduling, because it degrades gracefully
    instead of depending on job-state bookkeeping staying correct across a node
    death. Three things make the second run harmless:

      * a duplicate finds no suspensions and exits at step 1;
      * payload writes upsert, so two identical writes are one write;
      * an expensive op is expected to be CONTENT-ADDRESSED — keyed on its
        input's digest, checked before the spend — so a duplicate is a cache
        hit rather than a repeated bill.

    That last one is a host obligation, not a library guarantee. A node
    declared `suspends` whose op derives its cache key from anything other than
    its inputs breaks it silently, and the symptom is a bill.
    """
    use Oban.Worker,
      queue: :reactive_dag,
      max_attempts: 3,
      unique: [
        period: :infinity,
        fields: [:args, :queue, :worker],
        states: :incomplete
      ]

    require Logger

    alias ReactiveDag.{Cascade, Job, Plan, Suspension}

    @doc """
    Enqueue a resumption for one stopping point.

    Coalescing is the point's, not Oban's: several suspensions at one point
    produce one job, because the args name the point rather than any of them.
    """
    @spec enqueue(map(), keyword()) :: {:ok, Oban.Job.t()} | {:error, term()}
    def enqueue(point, opts \\ []) do
      %{
        "tenant" => point.tenant,
        "waiting" => point.waiting,
        "resource" => point.resource,
        "row_uuid" => point.row_uuid
      }
      |> then(&if opts[:plan_mfa], do: Map.put(&1, "plan_mfa", opts[:plan_mfa]), else: &1)
      |> __MODULE__.new(Keyword.take(opts, [:schedule_in, :priority, :queue]))
      |> Oban.insert()
    end

    @impl Oban.Worker
    def perform(%Oban.Job{args: args}) do
      plan = Job.plan(args, __MODULE__)

      point = %{
        tenant: Map.fetch!(args, "tenant"),
        waiting: Map.fetch!(args, "waiting"),
        resource: Map.fetch!(args, "resource"),
        row_uuid: Map.fetch!(args, "row_uuid")
      }

      case Suspension.at(point) do
        [] ->
          # ORDINARY, not an error: the work was done and discharged by another
          # run of this same job. Logged at debug because a duplicate is
          # expected under at-least-once delivery, not a symptom.
          Logger.debug(fn ->
            "reactive_dag: nothing suspended at #{point.waiting} for #{point.row_uuid}; " <>
              "this resumption is a duplicate"
          end)

          :ok

        suspensions ->
          resume(plan, point, suspensions, args)
      end
    end

    defp resume(plan, point, suspensions, args) do
      ids = Enum.map(suspensions, & &1.id)
      cell_id = cell_for(plan, point.waiting)

      cond do
        is_nil(cell_id) ->
          # The graph no longer has the cell this suspension names. Discharging
          # is right: nothing can ever resume it, and leaving the rows would
          # accumulate a point that never clears.
          Logger.warning(
            "reactive_dag: #{point.waiting} is suspended but not in the plan; " <>
              "discharging #{length(ids)} suspension(s) that can never resume"
          )

          Suspension.transaction(fn -> Suspension.discharge(ids) end)
          :ok

        true ->
          do_resume(plan, point, cell_id, suspensions, ids, args)
      end
    end

    defp do_resume(plan, point, cell_id, suspensions, ids, args) do
      reasons = suspensions |> Enum.map(& &1.reason) |> Enum.uniq()

      t0 = System.monotonic_time(:microsecond)

      :telemetry.execute(
        [:reactive_dag, :cascade, :resumed],
        %{suspensions: length(ids), system_time: System.system_time()},
        %{waiting: point.waiting, cell: cell_id, tenant: point.tenant, reasons: reasons}
      )

      # `feedback_lap:` hands the loop accounting back to the cascade. The lap
      # count survives ONLY on the suspension rows — each trip around a loop
      # through this cell is a separate, individually-successful cascade — so
      # if it is not read here and passed on, every resumption starts at lap 0
      # and an oscillating loop runs forever as a chain of healthy-looking
      # jobs. Max, not first: several suspensions coalescing into this one
      # recompute must not let a fresh lap-0 row launder an over-budget chain.
      feedback_lap = suspensions |> Enum.map(&Map.get(&1, :lap, 0)) |> Enum.max()

      opts =
        [tenant: point.tenant, feedback_lap: feedback_lap] ++
          Keyword.take(cascade_opts(args), [:plan_mfa])

      # The origin for the onward cascade: this cell, the keys the suspensions
      # named, and their versions.
      keys = keys_from(suspensions, point)

      # `resuming:` is what lets this cell run at all: it declares `suspends`,
      # so without it the cascade would suspend it again on sight and the point
      # would never clear. `Cascade.run/3` honours it by recomputing that cell
      # FIRST, with no transaction open, and only then opening one for the fast
      # subtree below.
      origin = %{cell: cell_id, keys: keys, versions: version_map(suspensions, keys)}

      case Cascade.run(plan, [origin], opts ++ [resuming: cell_id]) do
        {:error, {:resumption_failed, _cell, reason}} ->
          # NOT discharged. The suspensions stay and Oban retries — which is the
          # difference between slow work and fast: a suspension committed before
          # the work began, so there is something to retry FROM. Fast work has
          # no such record and depends on its source re-observing.
          Logger.warning(
            "reactive_dag: resumption of #{point.waiting} failed; " <>
              "#{length(ids)} suspension(s) kept for the retry"
          )

          {:error, reason}

        {:ok, _report} ->
          Suspension.transaction(fn -> Suspension.discharge(ids) end)

          :telemetry.execute(
            [:reactive_dag, :cascade, :resumption_done],
            %{duration_us: System.monotonic_time(:microsecond) - t0, discharged: length(ids)},
            %{waiting: point.waiting, cell: cell_id, tenant: point.tenant}
          )

          # A cascade that stops AGAIN downstream — one slow cell feeding
          # another — schedules its own resumption from inside `Cascade.run/3`.
          # Doing it here as well would enqueue each point twice: harmless,
          # because the second job finds the suspensions already discharged and
          # exits, but wasteful and misleading in the queue.
          :ok
      end
    end

    # `"*"` subsumes: one unattributable suspension means the whole cell.
    defp keys_from(suspensions, point) do
      if point.row_uuid == "*" or Enum.any?(suspensions, &(&1.version_id == "*")) do
        ["*"]
      else
        [point.row_uuid]
      end
    end

    defp version_map(_suspensions, ["*"]), do: %{}

    # The EARLIEST version for the key — `at/1` returns them in insertion order,
    # so the first is the change that succeeded the last settled state. That is
    # what a consumer must reprice from; a later one describes a state the graph
    # never saw settled.
    defp version_map(suspensions, [key]) do
      case Enum.find(suspensions, &(&1.version_id != "*")) do
        nil -> %{}
        %{version_id: vid} -> %{key => vid}
      end
    end

    # A suspension names its cell by RESOURCE. Assembly guarantees exactly one
    # suspendable cell per resource, so this lookup cannot be ambiguous.
    defp cell_for(%Plan{cells: cells}, waiting) do
      Enum.find_value(cells, fn {id, cell} ->
        cond do
          to_string(id) == waiting -> id
          cell.meta[:resource] && inspect(cell.meta[:resource]) == waiting -> id
          true -> nil
        end
      end)
    end

    defp cascade_opts(args) do
      case Map.get(args, "plan_mfa") do
        nil -> []
        mfa -> [plan_mfa: mfa]
      end
    end
  end
end

defmodule ReactiveDag.Cascade do
  @moduledoc """
  A CHANGE, propagated to completion — in one transaction, in memory, stopping
  only where it must.

  Something writes a row. Everything downstream that can run, runs: the graph is
  walked shallowest-first, each cell recomputed and its parents queued, until
  either there is nothing left or the walk reaches a node that cannot be
  finished inline. That node's work is recorded as a suspension and the branch
  stops. The transaction closes AT the suspension, not around it — everything
  that ran is durable, and nothing slow is inside.

  ## What this replaces, and why

  The drain read a queue of CONCLUSIONS. A row `(cell, key)` meant "this cell
  needs recomputing", computed by walking the graph at mark time — so the
  conclusion aged between being written and being read, and the drain's job was
  to find work rather than to do it. Three costs followed, and one of them was
  a production failure: claim, recompute and propagate shared one transaction,
  so a nine-minute extraction held a connection until the database closed it.

  A cascade starts from an explicit origin instead. It never asks the database
  what needs doing; it is told what changed and follows the consequences. The
  only thing the database holds is where it had to stop.

  ## The walk

      run(plan, origins)
        │
        ├─ pop the SHALLOWEST pending cell
        │    merge everything else queued for it first
        │
        ├─ suspends? ──yes──> record one suspension per changed row
        │                     stop THIS branch; others carry on
        │
        └─ no ──> recompute in a savepoint
                  queue each parent with the units this change touched

  Three properties worth naming, because none held before:

    * **A diamond recomputes its apex once.** Two inputs of one cell changing in
      the same cascade merge before it runs. The queue could only manage this by
      luck — two marks coalesced if they happened to land before the claim.
    * **Depth ordering is exact.** In memory, not re-derived by a
      `SELECT DISTINCT` per pass.
    * **A suspension truncates one branch.** The rest of the graph keeps running
      and commits.

  ## Failure

  This changes what a failure costs, and the change is real. A drain claimed
  work with a `DELETE` before doing it, so a crash rolled back and the item
  returned: the queue remembered what was outstanding by still holding it.

  A cascade holds nothing. A failure mid-walk rolls back the whole subtree and
  leaves no trace — the change is lost unless its source observes it again.
  That is acceptable because sources are idempotent and re-observe on a
  schedule: a crawl finding the same document with the same fingerprint writes
  nothing, and one finding it changed re-triggers the cascade. But the recovery
  story for fast work is now re-observation, not retry.

  Slow work is different, and deliberately so: its suspension is committed
  before the job runs, so a crashed resumption resumes from a row that still
  exists.
  """

  require Logger

  alias ReactiveDag.{Cell, Report, Graph, Plan, Suspension}

  @max_steps 100_000

  defmodule RunawayError do
    @moduledoc """
    The cascade exceeded its step budget — likely a cycle, or a recompute that
    keeps re-dirtying its own inputs. `:report` carries the PARTIAL trace up to
    the abort: `report.steps`' tail shows exactly which cells keep triggering
    each other, which is the diagnostic for the loop this error suspects.
    """
    defexception [:message, :report]
  end

  @typedoc """
  Where a cascade starts: a cell, the keys of it that changed, and the version
  recording each change.

  `versions` may be empty, and a key with no version yields a suspension
  carrying `"*"` — a resumption that recomputes the whole cell rather than the
  rows that moved.
  """
  @type origin :: %{
          required(:cell) => String.t(),
          required(:keys) => [String.t()],
          optional(:versions) => %{String.t() => String.t()},
          optional(:diffs) => map()
        }

  @doc """
  Propagate `origins` through `plan`, in one transaction, to completion.

  Returns the trace. `report.suspended` names every point where the cascade
  stopped, which is the only part of it the database also knows.
  """
  @spec run(Plan.t(), [origin()], keyword()) :: {:ok, Report.t()}
  def run(%Plan{} = plan, origins, opts \\ []) when is_list(origins) do
    t0 = System.monotonic_time(:microsecond)

    :telemetry.execute(
      [:reactive_dag, :cascade, :start],
      %{system_time: System.system_time(), origins: length(origins)},
      %{cells: map_size(plan.cells), origins: Enum.map(origins, & &1.cell)}
    )

    try do
      # A RESUMPTION runs its expensive cell FIRST, and outside the
      # transaction. This is the whole redesign in one line: the drain held a
      # transaction open across a nine-minute extraction until the database
      # closed the connection, and the only way not to is to do that work with
      # nothing open.
      #
      # What follows — the fast subtree below it — is transactional as usual.
      # So a resumption is two phases, not one: the slow thing, then the
      # cascade from what it produced.
      {origins, pre_steps} = run_resuming_cell(plan, origins, opts)

      result =
        Suspension.transaction(fn -> walk(plan, origins, opts, t0, pre_steps) end)

      emit_stop(result, t0)
      schedule_resumptions(result, opts)
      result
    catch
      # A resumption's own recompute failed, before any transaction opened.
      # Reported as a value so the caller leaves the suspensions in place and
      # the job retries — the failure is the work's, not the cascade's, and
      # nothing has been written to roll back.
      :throw, {:resumption_failed, cell_id, reason} ->
        {:error, {:resumption_failed, cell_id, reason}}

      kind, reason ->
        :telemetry.execute(
          [:reactive_dag, :cascade, :exception],
          %{duration_us: System.monotonic_time(:microsecond) - t0},
          %{kind: kind, reason: reason, report: partial_report(reason)}
        )

        :erlang.raise(kind, reason, __STACKTRACE__)
    end
  end

  # ---- the walk ----

  defp walk(plan, origins, opts, t0, pre_steps) do
    pending = Enum.reduce(origins, %{}, &merge_origin(&2, &1))

    do_walk(plan, pending, opts, %{}, pre_steps, [], 0, t0)
  end

  # The expensive recompute, run with NO transaction open, before the cascade
  # begins. Only a resumption reaches this: `opts[:resuming]` names the cell
  # whose suspension is being cleared, and it is the one cell allowed to run
  # despite declaring `suspends` — otherwise it would suspend again on sight and
  # the point would never clear.
  #
  # Returns the origins the cascade should actually start from: this cell's
  # CHANGED keys, so the walk continues from what the work produced rather than
  # re-running it. A cell that changed nothing yields no origin, and the cascade
  # is a no-op — correct, and the ordinary outcome when a resumption's input has
  # already been superseded.
  defp run_resuming_cell(plan, origins, opts) do
    case Keyword.get(opts, :resuming) do
      nil ->
        {origins, []}

      cell_id ->
        {mine, others} = Enum.split_with(origins, &(&1.cell == cell_id))

        case mine do
          [] ->
            {others, []}

          [origin | _] ->
            cell = Map.get(plan.cells, cell_id)
            run_outside_transaction(plan, cell_id, cell, origin, opts, others)
        end
    end
  end

  defp run_outside_transaction(_plan, _cell_id, nil, _origin, _opts, others), do: {others, []}

  defp run_outside_transaction(plan, cell_id, cell, origin, opts, others) do
    keys = Enum.sort(Map.get(origin, :keys, []))
    versions = Map.get(origin, :versions, %{})

    # Same resolution the in-walk path does: a resumption is handed the versions
    # its suspensions recorded, and they are references until read.
    origin_diffs = Map.merge(resolve_versions(plan, cell, versions), Map.get(origin, :diffs, %{}))

    :telemetry.execute(
      [:reactive_dag, :cascade, :cell_start],
      %{claimed: length(keys), system_time: System.system_time()},
      %{cell: cell_id, step: 0, claimed_keys: keys, outside_transaction: true}
    )

    {{outcome, diffs, new_versions}, us} =
      timed(fn ->
        ReactiveDag.Node.Payload.collecting_diffs_and_versions(fn ->
          {narrowed, claimed_groups} = derive_units(plan, cell_id, keys, origin_diffs)

          recompute(
            cell,
            narrowed,
            opts
            |> Keyword.merge(Plan.frontier_opts(plan))
            |> Keyword.put(:claimed_groups, claimed_groups)
          )
        end)
      end)

    case outcome do
      {:failed, reason} ->
        # NOT discharged by the caller: this returns without changed keys, so
        # the resumption's suspensions stay and the job retries. That is the
        # difference from fast work — slow work keeps its retry, because its
        # suspension committed before the work began.
        Logger.warning(
          "reactive_dag: resuming #{cell_id} failed (#{brief(reason)}); its " <>
            "suspensions stay for the next attempt"
        )

        :telemetry.execute(
          [:reactive_dag, :cascade, :cell_failed],
          %{duration_us: us},
          %{cell: cell_id, step: 0, reason: reason, claimed: keys}
        )

        throw({:resumption_failed, cell_id, reason})

      {changed, meta} ->
        step = %{
          cell: cell_id,
          pass: 0,
          claimed: keys,
          changed: changed,
          triggered_by: nil,
          duration_us: us,
          op: cell.op,
          depth: Map.get(plan.depths, cell_id),
          meta: meta
        }

        :telemetry.execute(
          [:reactive_dag, :cascade, :step],
          %{duration_us: us, claimed: length(keys), changed: length(changed)},
          %{cell: cell_id, step: 0, changed_keys: changed, outside_transaction: true}
        )

        # Continue from this cell's PARENTS, seeded as origins. The cell itself
        # is not re-queued: it has just run, and queueing it would suspend it
        # again.
        #
        # NOTHING CHANGED means nothing to propagate. `Graph.dirty_parents/5`
        # does not short-circuit on an empty key list — it returns every parent
        # paired with `[]` — so without this a resumption whose work produced no
        # change would still recompute everything below it. The ordinary case
        # for that is a resumption whose input was superseded before the job
        # ran, which is common rather than exotic.
        merged_versions = Map.merge(versions, new_versions)

        parent_origins =
          case changed do
            [] ->
              []

            changed ->
              plan
              |> Graph.dirty_parents(cell_id, changed, ReactiveDag.Node.KeyRule, Map.merge(origin_diffs, diffs))
              |> Enum.map(fn {parent_id, mapped} ->
                %{
                  cell: parent_id,
                  keys: entries_for(plan, parent_id, cell_id, mapped, changed, merged_versions),
                  versions: merged_versions,
                  diffs: diffs,
                  from: cell_id
                }
              end)
          end

        {others ++ parent_origins, [step]}
    end
  end

  defp do_walk(_plan, pending, _opts, _cause, steps, suspended, n, t0)
       when map_size(pending) == 0 do
    {:ok,
     %Report{
       steps: Enum.reverse(steps),
       suspended: Enum.reverse(suspended),
       passes: n,
       duration_us: System.monotonic_time(:microsecond) - t0
     }}
  end

  defp do_walk(plan, pending, opts, cause, steps, suspended, n, t0) do
    if n >= Keyword.get(opts, :max_steps, @max_steps) do
      report = %Report{
        steps: Enum.reverse(steps),
        suspended: Enum.reverse(suspended),
        passes: n,
        duration_us: System.monotonic_time(:microsecond) - t0
      }

      raise RunawayError,
        message:
          "reactive_dag: cascade exceeded #{n} steps without settling — " <>
            "the last cells to run were " <>
            inspect(steps |> Enum.take(10) |> Enum.map(& &1.cell) |> Enum.uniq()) <>
            ". This is a cycle, or a recompute that re-dirties its own inputs.",
        report: report
    end

    # SHALLOWEST first, and everything queued for that cell merged before it
    # runs. The merge is what makes a diamond recompute its apex once: two
    # inputs changing in one cascade arrive as one unit of work, not two.
    {cell_id, work} = shallowest(plan, pending)
    pending = Map.delete(pending, cell_id)
    cell = Map.get(plan.cells, cell_id)

    cond do
      is_nil(cell) ->
        # A cell named by an origin or an edge that the plan does not have.
        # Dropped rather than raised: a graph can shrink between a write and its
        # cascade, and losing one branch is better than losing the transaction.
        Logger.warning(
          "reactive_dag: cascade reached #{inspect(cell_id)}, which is not in the plan; " <>
            "dropping that branch"
        )

        do_walk(plan, pending, opts, cause, steps, suspended, n + 1, t0)

      reason = suspends_for(cell, opts) ->
        points = suspend(plan, cell_id, work, reason)
        do_walk(plan, pending, opts, cause, steps, points ++ suspended, n + 1, t0)

      true ->
        {pending, steps, cause} =
          recompute_and_queue(plan, cell_id, cell, work, opts, pending, steps, cause, n)

        do_walk(plan, pending, opts, cause, steps, suspended, n + 1, t0)
    end
  end

  # ---- one cell ----

  defp recompute_and_queue(plan, cell_id, cell, work, opts, pending, steps, cause, n) do
    keys = Enum.sort(work.keys)

    # A version REFERENCE has to become a diff before this cell can narrow by
    # it. The reference travels through the walk because it is small and
    # durable; the diff is read here, once, at the moment the cell needs it.
    #
    # Missing this was a silent bug: a parent handed a version but no diff falls
    # straight through `derive_units/4`'s `map_size(diffs) == 0` clause and
    # recomputes its claimed keys — which for a fold means claiming the CHILD's
    # row key as if it were a unit, and producing nothing.
    diffs = Map.merge(resolve_versions(plan, cell, work.versions), work.diffs)

    {keys, claimed_groups} = derive_units(plan, cell_id, keys, diffs)

    :telemetry.execute(
      [:reactive_dag, :cascade, :cell_start],
      %{claimed: length(keys), system_time: System.system_time()},
      %{cell: cell_id, step: n, claimed_keys: keys}
    )

    {{outcome, new_diffs, new_versions}, us} =
      timed(fn ->
        ReactiveDag.Node.Payload.collecting_diffs_and_versions(fn ->
          Suspension.savepoint(fn ->
            recompute(
              cell,
              keys,
              opts
              |> Keyword.merge(Plan.frontier_opts(plan))
              |> Keyword.put(:claimed_groups, claimed_groups)
            )
          end)
        end)
      end)

    case outcome do
      {:failed, reason} ->
        # CONTAINED. The savepoint rolled this cell back; the rest of the
        # cascade keeps going and commits. The branch below it does not run —
        # nothing changed there to propagate.
        Logger.warning(
          "reactive_dag: #{cell_id} failed (#{brief(reason)}); its branch stops, " <>
            "the rest of this cascade continues"
        )

        :telemetry.execute(
          [:reactive_dag, :cascade, :cell_failed],
          %{duration_us: us},
          %{cell: cell_id, step: n, reason: reason, claimed: keys}
        )

        {pending, steps, cause}

      {changed, meta} ->
        merged = Map.merge(diffs, new_diffs)

        # The versions describing THIS cell's changed rows, from either source:
        # what its own write recorded, or — for a leaf, which recomputes
        # nothing — what the origin handed in. Taking only the former drops the
        # origin's versions at the very first hop, and every suspension
        # downstream carries `"*"` and resumes whole-cell. Silently, since a
        # whole-cell resumption is correct.
        versions = Map.merge(work.versions, new_versions)

        # A recompute reporting NO change propagates nothing — the point of
        # reporting changed keys rather than claimed ones. `dirty_parents/5`
        # pairs every parent with `[]` rather than returning none, so this has
        # to be checked here.
        parents =
          if changed == [],
            do: [],
            else: Graph.dirty_parents(plan, cell_id, changed, ReactiveDag.Node.KeyRule, merged)

        pending =
          Enum.reduce(parents, pending, fn {parent_id, mapped}, acc ->
            merge_pending(acc, parent_id, cell_id, %{
              keys: entries_for(plan, parent_id, cell_id, mapped, changed, versions),
              versions: versions,
              diffs: merged
            })
          end)

        step = %{
          cell: cell_id,
          pass: n,
          claimed: keys,
          changed: changed,
          triggered_by: Map.get(cause, cell_id),
          duration_us: us,
          op: cell.op,
          depth: Map.get(plan.depths, cell_id),
          meta: meta
        }

        :telemetry.execute(
          [:reactive_dag, :cascade, :step],
          %{duration_us: us, claimed: length(keys), changed: length(changed)},
          %{cell: cell_id, step: n, changed_keys: changed, triggered_by: step.triggered_by}
        )

        cause =
          Enum.reduce(parents, cause, fn {parent_id, _}, acc ->
            Map.put_new(acc, parent_id, cell_id)
          end)

        {pending, [step | steps], cause}
    end
  end

  # ---- suspending ----

  # One suspension per CHANGED ROW, not one per stopping point: the row is what
  # a resumption narrows by. Several rows feeding one slow cell therefore leave
  # several rows at one point, and the job that resumes reads them all and does
  # the work once.
  defp suspend(plan, cell_id, work, reason) do
    tenant = Suspension.tenant(Plan.frontier_opts(plan))
    waiting = resource_name(plan, cell_id)

    entries =
      case work.keys do
        ["*"] -> [{"*", "*", nil}]
        keys -> Enum.map(keys, &{&1, Map.get(work.versions, &1, "*"), &1})
      end

    points =
      for {row_uuid, version_id, _key} <- entries do
        source = work.from || cell_id

        point = %{
          tenant: tenant,
          waiting: waiting,
          resource: resource_name(plan, source),
          row_uuid: row_uuid
        }

        Suspension.record(point, version_id, reason)

        Map.put(point, :reason, reason)
      end

    :telemetry.execute(
      [:reactive_dag, :cascade, :suspended],
      %{count: length(points)},
      %{cell: cell_id, waiting: waiting, reason: reason, tenant: tenant}
    )

    points
  end

  # What a suspension calls this cell. The RESOURCE, not the cell id, so a host
  # can name a stopping point without knowing the graph's internal names —
  # assembly guarantees one cell per suspendable resource, so this cannot be
  # ambiguous.
  defp resource_name(plan, cell_id) do
    case Map.get(plan.cells, cell_id) do
      %Cell{meta: %{resource: resource}} when not is_nil(resource) -> inspect(resource)
      _ -> to_string(cell_id)
    end
  end

  # `:expensive` wins when a node declares both: there is no point asking a
  # person to approve work that has not been done yet.
  #
  # `opts[:skip_gate]` names a cell whose APPROVAL gate this cascade clears —
  # the origin of a change a person made themselves. It never clears
  # `:expensive`: who wrote the row has no bearing on how long the work takes.
  defp suspends_for(cell, opts) do
    case suspends_for(cell) do
      :approval ->
        if Keyword.get(opts, :skip_gate) == cell.id, do: nil, else: :approval

      other ->
        other
    end
  end

  defp suspends_for(%Cell{meta: %{suspends: reasons}}) when is_map(reasons) do
    cond do
      Map.has_key?(reasons, :expensive) -> :expensive
      Map.has_key?(reasons, :approval) -> :approval
      true -> nil
    end
  end

  defp suspends_for(_), do: nil

  # ---- pending set ----

  # An origin's change is its OWN — nothing upstream propagated it here — so
  # `from` is the origin cell itself. A suspension at an origin therefore names
  # that cell as both what stopped and what changed, which is exactly right for
  # a source-fed node that is itself too slow to run inline.
  defp merge_origin(pending, origin) do
    merge_pending(pending, origin.cell, Map.get(origin, :from) || origin.cell, %{
      keys: Map.get(origin, :keys, []),
      versions: Map.get(origin, :versions, %{}),
      diffs: Map.get(origin, :diffs, %{})
    })
  end

  # `from` is WHICH CELL'S CHANGE this work represents — the child that
  # propagated, not the parent about to run. A suspension records both: `waiting`
  # is the cell that stopped, `resource` is what changed. Neither implies the
  # other, and reading the second off the first would name the wrong resource in
  # every suspension.
  #
  # The FIRST arrival wins when several children queue the same parent. That is a
  # real limitation rather than a considered choice: the suspension names one
  # changed resource, so a parent stopped by two different inputs in one cascade
  # records only the first. It is the sibling problem, and it is open.
  defp merge_pending(pending, cell_id, from, work) do
    {keys, versions} = split_entries(work.keys, work.versions)

    Map.update(
      pending,
      cell_id,
      %{keys: keys, versions: versions, diffs: work.diffs, from: from},
      fn existing ->
        %{
          keys: merge_keys(existing.keys, keys),
          versions: Map.merge(existing.versions, versions),
          diffs: Map.merge(existing.diffs, work.diffs),
          from: existing.from
        }
      end
    )
  end

  # `"*"` SUBSUMES everything: a whole-cell claim already covers each key, and
  # carrying both would recompute the cell and then claim to have recomputed
  # rows it never looked at individually.
  defp merge_keys(a, b) do
    merged = Enum.uniq(a ++ b)
    if "*" in merged, do: ["*"], else: Enum.sort(merged)
  end

  # An entry is a bare key or `{key, version_id}`. Splitting here means the rest
  # of the walk deals in one shape.
  defp split_entries(entries, base_versions) do
    Enum.reduce(entries, {[], base_versions}, fn
      {key, version_id}, {keys, versions} when is_binary(version_id) ->
        {[key | keys], Map.put(versions, key, version_id)}

      {key, _}, {keys, versions} ->
        {[key | keys], versions}

      key, {keys, versions} when is_binary(key) ->
        {[key | keys], versions}
    end)
    |> then(fn {keys, versions} -> {Enum.reverse(keys), versions} end)
  end

  defp shallowest(plan, pending) do
    {cell_id, work} =
      Enum.min_by(pending, fn {cell_id, _} -> Map.get(plan.depths, cell_id, 1_000_000) end)

    {cell_id, work}
  end

  # ---- moved verbatim from Drain ----
  #
  # These four are the consumer-side derivation: how a cell turns a CHANGE into
  # the units of its own work. They are unchanged because they were already
  # right, and already mutation-tested — the redesign is about when they run,
  # not what they compute.

  defp derive_units(_plan, _cell_id, ["*"], _diffs), do: {["*"], %{}}
  defp derive_units(_plan, _cell_id, keys, diffs) when map_size(diffs) == 0, do: {keys, %{}}

  defp derive_units(plan, cell_id, keys, diffs) do
    cell = Map.get(plan.cells, cell_id)

    case grain_of(cell) do
      nil ->
        {keys, %{}}

      {group_plan, resource} ->
        key_fn = unit_key_fn(cell)

        derived =
          for k <- keys,
              d = diffs[k],
              group <- ReactiveDag.Node.Diff.groups(d, group_plan, resource),
              reduce: %{} do
            acc -> Map.update(acc, key_fn.(group), [group], &[group | &1])
          end

        # ALL-OR-NOTHING, enforced upstream by `entries_for/6`: it refuses to
        # queue version-bearing keys unless EVERY changed key carried one, so a
        # unit that gets here has a diff for all of its keys. Mixing derived
        # units with underived keys would be the dangerous state — some units
        # claimed, the rest silently stranded — and it is prevented at the queue
        # rather than patched here.
        case Map.keys(derived) do
          [] -> {keys, %{}}
          units -> {units, derived}
        end
    end
  end

  # A version reference was written by the cell whose ROW changed, so the reader
  # that resolves it is that cell's — this cell for its own write, one of its
  # INPUTS for a propagated change. Try this cell first, then its inputs; a
  # reference nothing can read leaves the claim un-narrowed rather than failing
  # it.
  defp resolve_versions(_plan, nil, _versions), do: %{}

  defp resolve_versions(plan, cell, versions) when map_size(versions) > 0 do
    readers =
      [cell.meta[:version_diff] | Enum.map(cell.inputs, &input_reader(plan, &1))]
      |> Enum.reject(&is_nil/1)
      |> Enum.uniq()

    if readers == [] do
      %{}
    else
      for {k, vid} <- versions,
          is_binary(vid),
          diff = Enum.find_value(readers, &read_version(&1, vid)),
          into: %{} do
        {k, diff}
      end
    end
  end

  defp resolve_versions(_plan, _cell, _versions), do: %{}

  defp input_reader(plan, input_id) do
    case Map.get(plan.cells, input_id) do
      nil -> nil
      input -> input.meta[:version_diff]
    end
  end

  defp read_version(reader, version_id) do
    case reader do
      {m, f, a} -> apply(m, f, [version_id | a])
      fun when is_function(fun, 1) -> fun.(version_id)
    end
  rescue
    e ->
      Logger.warning(
        "reactive_dag: version_diff reader failed for #{inspect(version_id)} " <>
          "(#{Exception.message(e)}); this claim will not be narrowed"
      )

      nil
  end

  defp grain_of(nil), do: nil

  defp grain_of(%Cell{meta: meta}) do
    with :group <- meta[:key_rule],
         %{} = src <- meta[:over_source],
         group_plan when is_list(group_plan) <- src[:group_key_plan],
         resource when not is_nil(resource) <- src[:resource] do
      {group_plan, resource}
    else
      _ -> nil
    end
  end

  defp unit_key_fn(%Cell{meta: meta}) do
    spec = meta[:reduce] || meta[:join]

    ReactiveDag.Node.Recompute.Declarative.key_fn(
      spec && Map.get(spec, :key),
      spec && Map.get(spec, :key_prefix)
    )
  end

  # Whether to hand a parent the CHILD's keys plus versions (so it can derive
  # its own units) or the rule-mapped keys.
  #
  # ALL-OR-NOTHING: a partial version set falls back to the mapped keys.
  # Attaching some contributing versions and not others derives SOME of the
  # parent's units, and a fold that recomputes a subset reconciles the rest
  # away — an untouched rollup row destroyed by a change that never concerned
  # it.
  defp entries_for(plan, parent_id, child_id, mapped_keys, changed, versions) do
    parent = Map.get(plan.cells, parent_id)

    cond do
      "*" in mapped_keys ->
        mapped_keys

      not derives_units?(parent, child_id) ->
        mapped_keys

      true ->
        entries = for k <- changed, vid = versions[k], do: {k, vid}
        if entries != [] and length(entries) == length(changed), do: entries, else: mapped_keys
    end
  end

  defp derives_units?(nil, _child_id), do: false

  defp derives_units?(%Cell{meta: meta}, child_id) do
    with :group <- meta[:key_rule],
         %{} = src <- meta[:over_source],
         ^child_id <- meta[:over] || child_id,
         group_plan when is_list(group_plan) <- src[:group_key_plan] do
      Enum.all?(group_plan, &match?({:attr, _, _}, &1))
    else
      _ -> false
    end
  end

  defp recompute(%Cell{leaf?: true}, keys, _opts), do: {keys, %{}}

  defp recompute(cell, keys, opts) do
    case ReactiveDag.Node.Recompute.recompute(cell, keys, opts) do
      {:ok, changed} when is_list(changed) ->
        {changed, %{}}

      {:ok, changed, meta} when is_list(changed) and is_map(meta) ->
        {changed, meta}

      # CONTAINED, not raised. Must be a RETURNED value: an exception inside a
      # nested transaction aborts the outer one, so only this shape can be
      # isolated by the savepoint.
      {:error, reason} ->
        {:failed, reason}

      other ->
        raise ArgumentError, """
        reactive_dag: recomputing #{inspect(cell.id)} returned a value the cascade cannot use.

            got:      #{inspect(other, limit: 5)}
            expected: {:ok, changed_keys}  |  {:ok, changed_keys, meta_map}

        `changed_keys` is a list of the keys whose output actually changed
        (returning every claimed key is always correct, just less efficient).
        `meta_map` is anything the node wants to report about the work — the
        library carries it onto the step without interpreting it.

        This is almost always a `compute Mod` whose `recompute/2` returned
        something else. A recompute that FAILED should raise or return
        `{:error, reason}` rather than something unrecognised.
        """
    end
  end

  # ---- reporting ----

  defp emit_stop({:ok, %Report{} = report}, t0) do
    :telemetry.execute(
      [:reactive_dag, :cascade, :stop],
      %{
        duration_us: System.monotonic_time(:microsecond) - t0,
        steps: length(report.steps),
        suspended: length(report.suspended),
        changed: Report.changed_total(report)
      },
      %{report: report, cells_touched: Report.cells(report)}
    )
  end

  defp emit_stop(_other, _t0), do: :ok

  # A suspension nobody resumes is a cascade that silently stopped forever.
  #
  # This lives HERE rather than in `CascadeWorker` because a cascade has more
  # than one caller — a source poll and a reprocess both run one directly — and
  # a suspension recorded by any of them needs the same job. Leaving it to the
  # caller meant exactly one of three paths scheduled anything, and the other
  # two wrote rows that would never be read.
  #
  # AFTER the transaction commits, which the `try` block's ordering guarantees:
  # a job that started first would read no suspensions and exit as an ordinary
  # duplicate, while the real work sat unclaimed.
  #
  # Failure is logged, not raised. The suspensions are already durable, so the
  # cost of a queue being down is a delay someone must notice — which is what
  # `Suspension.points/1` is for — rather than a lost cascade.
  defp schedule_resumptions({:ok, %Report{suspended: [_ | _] = points}}, opts) do
    scheduler = Keyword.get(opts, :resumption_scheduler) || default_scheduler()

    if scheduler do
      # DEFENSIVE, not load-bearing: the walk merges everything queued for a
      # cell before it runs, so one cascade cannot currently reach one stopping
      # point twice. Kept because the scheduler must be idempotent per point
      # regardless of how the walk happens to batch — and because a mutation
      # removing this line passes the suite, which is worth saying out loud
      # rather than leaving as apparent coverage.
      points
      |> Enum.uniq_by(&Map.take(&1, [:tenant, :waiting, :resource, :row_uuid]))
      |> Enum.each(fn point ->
        try do
          scheduler.(point, Keyword.take(opts, [:plan_mfa]))
        rescue
          e ->
            Logger.warning(
              "reactive_dag: #{point.waiting} suspended but its resumption could not " <>
                "be enqueued (#{Exception.message(e)}). The suspension stands; " <>
                "nothing will resume it until something enqueues one."
            )
        end
      end)
    end

    :ok
  end

  defp schedule_resumptions(_result, _opts), do: :ok

  # nil when Oban is absent — the library works without it, and a host that
  # drives resumptions itself passes `resumption_scheduler:`.
  defp default_scheduler do
    if Code.ensure_loaded?(ReactiveDag.ResumptionWorker) do
      &ReactiveDag.ResumptionWorker.enqueue/2
    end
  end

  defp partial_report(%RunawayError{report: report}), do: report
  defp partial_report(_reason), do: nil

  @brief_limit 400

  # A reason rendered at a BOUNDED length. The changeset that surfaced this
  # carried an entire LLM extraction, and an 8KB log line to report a dropped
  # socket is its own defect.
  defp brief(reason) do
    reason
    |> inspect(limit: 8, printable_limit: 200)
    |> String.slice(0, @brief_limit)
  end

  defp timed(fun) do
    t0 = System.monotonic_time(:microsecond)
    result = fun.()
    {result, System.monotonic_time(:microsecond) - t0}
  end
end

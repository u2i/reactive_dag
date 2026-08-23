defmodule ReactiveDag.Drain do
  @moduledoc """
  The reactive propagation loop — the heart of the substrate, shared by both
  hosts.

  Given a compiled `Plan`, drain the frontier to empty:

    1. Pick the dirty cell with the smallest depth (`Frontier.next_cell`) — no
       cell recomputes while an input is still dirty (topological order, no
       external scheduler).
    2. Atomically claim its dirty keys (`Frontier.claim` — delete-returning).
    3. Recompute it (`ReactiveDag.Node.Recompute`, dispatching on what the
       node DECLARED) → the keys that changed.
    4. Propagate: mark the changed keys on the cell's parents, applying the
       node's declared key rule (`Graph.dirty_parents`).
    5. Repeat until empty.

  Steps 2–4 run in ONE savepoint per cell, so a cell that fails leaves the
  frontier exactly as it found it. A claim is a delete: without that, a
  transient failure — a deadlock, a timeout, an upstream 503 — consumes the
  work item and those keys go silently stale.

  A recompute reports failure two ways, and they mean different things:

    * it RAISES — the drain rolls that cell back and re-raises. Something is
      wrong with the graph or the host, and stopping is right.
    * it returns `{:error, reason}` — the drain rolls that cell back, records
      it, and CARRIES ON with every other cell. The failed cell is excluded
      from selection for the rest of the run (its keys are still dirty, so it
      would otherwise be re-selected forever) and retried by the next drain.

  The second is what lets a fallible unit live in the graph. A poll that could
  not reach its upstream is one cell staying dirty while the rest of the
  cascade runs — the containment `ReactiveDag.Source.poll_all/2` gives a sweep,
  expressed where the work happens. It must be a RETURNED value: an exception
  inside a nested transaction aborts the outer one, so only a value can be
  isolated.

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
  recompute it (each claiming a disjoint slice of its keys).

  So run ONE drain at a time per graph. On a single node that is a single
  worker; across a CLUSTER it is `ReactiveDag.Frontier.with_lock/2`, a Postgres
  advisory lock that `ReactiveDag.ScanWorker`'s sweep already takes:

      case Frontier.with_lock(fn -> Drain.run(plan, opts) end) do
        {:ok, {:ok, report}} -> report
        :busy -> :already_draining
      end

  `:busy` is not an error. The frontier is a set rather than a queue, so
  anything this drain would have claimed is still there for whoever holds the
  lock — a caller that retries on `:busy` retries work already in progress.

  Failing that, make recomputes idempotent so a doubled recompute is merely
  wasted work.

  ## One engine

  There is no strategy to supply. A node's `reactive` block declares what it
  computes and how its changes propagate, and the drain reads that — so the same
  loop serves a per-key LLM pipeline and a set-based SQL model without either
  one bringing its own dispatch.

  Earlier versions took `recompute:`/`key_rule:` modules. Both hosts ended up
  passing the library's own, because what actually varied between them was
  DATA — a module named in `compute`, a combinator, a key rule — not control
  flow. A pluggable engine that everyone plugs the same thing into is just an
  indirection.

  `run/2` opts:
    * `:max_passes` — runaway guard (default #{100_000}): exceeding it raises
      `ReactiveDag.Drain.RunawayError`, whose `:report` field carries the
      partial trace — the step list showing which cells keep re-dirtying each
      other is exactly the diagnostic for the cycle the guard suspects.
    * `:force` — a cell id, a list of them, or `:all`: these cells' claimed keys
      propagate WHETHER OR NOT the recompute reported them changed. For a
      RE-RUN, where the point is that the graph catches up rather than that the
      work is redone.

      It does not make an op work harder. One that memoises on something the
      library cannot see — an md5-keyed cache, a content digest — still skips,
      and should: the expensive call is rarely what a re-run is after. What
      `:force` overrides is the conclusion drawn from `changed == []`, which
      otherwise stops the cascade at that cell. So a host that recomputed a cell
      out-of-band, or cleared a payload by hand, can drain and have everything
      downstream reflect it:

          Drain.run(plan, force: "transcript_record")

      Only the named cells are forced. Their parents propagate on their own
      verdicts, so a genuinely unchanged consumer still stops the cascade —
      forcing transitively would recompute the whole downstream graph on every
      re-run and make change detection pointless past the first hop.

  ## Telemetry

  The drain emits `:telemetry` events, so a host observes it without threading a
  callback through every call site — a dashboard, a metrics backend and a log
  can all attach independently, and none of them changes how the drain is
  invoked.

  | event | measurements | metadata |
  |---|---|---|
  | `[:reactive_dag, :drain, :start]` | `system_time` | `cells` (count in the plan) |
  | `[:reactive_dag, :drain, :step]` | `duration_us`, `claimed`, `changed` | `cell`, `pass`, `changed_keys`, `triggered_by`, `step` |
  | `[:reactive_dag, :drain, :stop]` | `duration_us`, `passes`, `steps`, `changed` | `report`, `cells_touched` |
  | `[:reactive_dag, :drain, :cell_failed]` | `duration_us` | `cell`, `pass`, `reason`, `claimed` |
  | `[:reactive_dag, :drain, :exception]` | `duration_us` | `kind`, `reason`, `report` |

  `:step` carries the changed KEYS, not just their count, because that is what
  makes a consumer incremental: a dashboard that knows which cells moved reads
  only those instead of re-reading the graph.

      :telemetry.attach("my-drain-log", [:reactive_dag, :drain, :stop], fn _e, m, meta, _ ->
        Logger.info("drained \#{length(meta.cells_touched)} cells in \#{m.duration_us}us")
      end, nil)

  `:exception` fires for a `RunawayError` too, carrying the partial report — a
  monitor should see the runaway, not just the crash.

  This replaces an earlier `:on_step` option. A closure threaded through `run/2`
  could only serve whoever owned that call site — a dashboard, a metrics backend
  and a log could not all have one, and adding a second consumer meant editing
  every place the drain was invoked. Telemetry has no such limit, and a caller
  who wants a plain closure can attach one in three lines.
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

    :telemetry.execute(
      [:reactive_dag, :drain, :start],
      %{system_time: System.system_time()},
      %{cells: map_size(plan.cells)}
    )

    try do
      result = do_run(plan, opts, 0, %{}, [], t0, [])
      emit_stop(result, t0)
      result
    catch
      kind, reason ->
        :telemetry.execute(
          [:reactive_dag, :drain, :exception],
          %{duration_us: System.monotonic_time(:microsecond) - t0},
          %{kind: kind, reason: reason, report: partial_report(reason)}
        )

        :erlang.raise(kind, reason, __STACKTRACE__)
    end
  end

  defp emit_stop({:ok, %Report{} = report}, t0) do
    :telemetry.execute(
      [:reactive_dag, :drain, :stop],
      %{
        duration_us: System.monotonic_time(:microsecond) - t0,
        passes: report.passes,
        steps: length(report.steps),
        changed: Report.changed_total(report)
      },
      %{report: report, cells_touched: Report.cells(report)}
    )
  end

  defp emit_stop(_other, _t0), do: :ok

  # a RunawayError carries the partial trace; anything else has none to give.
  defp partial_report(%RunawayError{report: report}), do: report
  defp partial_report(_reason), do: nil

  defp do_run(plan, opts, passes, cause, steps, t0, failed) do
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
      drain_pass(plan, opts, passes, cause, steps, t0, failed)
    end
  end

  defp drain_pass(plan, opts, passes, cause, steps, t0, failed) do
    case Frontier.next_cell(plan.depths, failed, Plan.frontier_opts(plan)) do
      nil ->
        {:ok,
         %Report{
           steps: Enum.reverse(steps),
           passes: passes,
           duration_us: System.monotonic_time(:microsecond) - t0
         }}

      cell_id ->
        # ONE TRANSACTION over claim → recompute → propagate.
        #
        # A claim is a DELETE, so it consumes the work item BEFORE the work
        # happens. Without this a recompute that raises — a deadlock, a timeout,
        # an upstream 503 — leaves those keys gone from the frontier and
        # silently stale, and a failure between the recompute and its
        # propagation loses the parents' marks too. Rolling back does not put
        # them back; it means they were never taken.
        #
        # The connection is held for the length of the recompute, which is
        # affordable only because drains are SERIALIZED (`with_lock/2`) — one
        # connection, not one per concurrent drain. Readers are unaffected:
        # Postgres readers never block on the row locks a DELETE takes.
        # ONE SAVEPOINT per cell, over claim → recompute → propagate. A cell
        # that reports a failure rolls all three back: its keys were never
        # taken, so the next drain retries them, and every cell already
        # recomputed this run is untouched.
        outcome =
          Frontier.savepoint(fn ->
            drain_cell(plan, opts, passes, cause, steps, cell_id, failed)
          end)

        case outcome do
          # Rolled back, so its keys are still dirty — which is why it must be
          # EXCLUDED for the rest of this run, or `next_cell` hands back the
          # same cell forever and the drain spins to the runaway guard.
          {:error, {:cell_failed, id}} ->
            do_run(plan, opts, passes + 1, cause, steps, t0, [id | failed])

          {next_cause, next_steps, next_failed} ->
            do_run(plan, opts, passes + 1, next_cause, next_steps, t0, next_failed)
        end
    end
  end

  defp drain_cell(plan, opts, passes, cause, steps, cell_id, failed) do
    # A whole-cell claim ("*") SUBSUMES any co-claimed specific keys: if "*"
    # is present, the claim is normalized to exactly ["*"]. Recompute strategies
    # test `keys == ["*"]` to mean "recompute the whole cell", so a stray "*"
    # riding alongside real keys must collapse — otherwise the whole-cell branch
    # is missed and the specific keys are processed as if "*" were a real key.
    entries = Frontier.claim_with_diffs(cell_id, Plan.frontier_opts(plan))
    claimed = Enum.map(entries, &elem(&1, 0))
    keys = if "*" in claimed, do: ["*"], else: claimed

    # The diffs a change arrived WITH. There are two producers of "what moved",
    # and a cell can see either:
    #
    #   * a host writing a row itself (`dirties_on`) attaches the diff to the
    #     mark, because nothing else in the graph saw that write;
    #   * a payload write inside this recompute records its own
    #     (`Payload.collecting_diffs/1`), collected below.
    #
    # This cell's OWN writes are what its parents propagate from, so those win on
    # key collision — but a claimed key this recompute did not rewrite still
    # carries the change it came in with, which is what makes an externally-written
    # row propagate precisely.
    #
    # RESOLVED from the version, not copied onto the mark: the queue row holds a
    # reference, so the diff is read here from the versioned resource's own
    # record. A cell declaring no `version_diff` reader yields nothing, and its
    # claims narrow no further than the keys themselves.
    claimed_diffs = resolve_versions(plan, cell_or_nil(plan, cell_id), entries)

    # DERIVE this cell's own units from the changes it was marked with.
    #
    # A propagated mark names the CHILD row that changed and references its
    # version — `(cell, uuid, version)`. This is where that becomes work: read the
    # version, project it onto this cell's declared grain, and the units fall out
    # as VALUES (`Diff.groups/2`). Nothing composite was stored, so nothing has to
    # be split apart, and the values scope the fold's read exactly rather than to
    # a per-column hull.
    #
    # `{keys, groups}` — the keys this cell recomputes, and the group values
    # behind them. Unchanged from `keys` for a cell that derives nothing.
    {keys, claimed_groups} = derive_units(plan, cell_id, keys, claimed_diffs)

    cond do
      keys == [] ->
        {cause, steps, failed}

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

        {cause, steps, failed}

      true ->
        cell = Map.fetch!(plan.cells, cell_id)

        # The plan's tenant rides with the recompute. It is the PLAN's, not the
        # cell's — every tenant's plan holds identical cells — so it can only
        # enter here, where both are in scope.
        #
        # `collecting_diffs/1` wraps the WHOLE recompute, which is what makes one
        # mechanism cover every rung: a declarative fold, a `per_key` action and a
        # `compute` op writing its own rows all reach the same payload write, so
        # all three yield diffs without any of them knowing.
        {{outcome, diffs, versions}, us} =
          timed(fn ->
            ReactiveDag.Node.Payload.collecting_diffs_and_versions(fn ->
              recompute(
                cell,
                keys,
                opts
                |> Keyword.merge(Plan.frontier_opts(plan))
                |> Keyword.put(:claimed_groups, claimed_groups)
              )
            end)
          end)

        case outcome do
          {:failed, reason} ->
            # CONTAINED. The savepoint inside `recompute/3` already rolled this
            # cell back, so its keys are still dirty and the next drain retries
            # them. Failing the whole drain here would throw away every cell
            # already recomputed this pass — which is exactly what
            # `Source.poll_all/2` refuses to do for a sweep, and a fallible cell
            # deserves the same.
            Logger.warning(
              "reactive_dag: #{cell_id} failed to recompute (#{inspect(reason)}); " <>
                "its keys stay dirty for the next drain"
            )

            :telemetry.execute(
              [:reactive_dag, :drain, :cell_failed],
              %{duration_us: us},
              %{cell: cell_id, pass: passes, reason: reason, claimed: keys}
            )

            # AS AN ERROR, so the savepoint around this cell rolls back — the
            # claim included. That is the whole mechanism: the keys were never
            # taken, so the next drain retries them.
            {:error, {:cell_failed, cell_id}}

          {changed, meta} ->
            recomputed(
              plan,
              opts,
              passes,
              cause,
              steps,
              cell_id,
              cell,
              keys,
              Map.merge(claimed_diffs, diffs),
              versions,
              changed,
              meta,
              us,
              failed
            )
        end
    end
  end

  defp recomputed(
         plan,
         opts,
         passes,
         cause,
         steps,
         cell_id,
         cell,
         keys,
         diffs,
         versions,
         changed,
         meta,
         us,
         failed
       ) do
    # A WHOLE-CELL claim ("*") propagates :all: a whole recompute can DELETE
    # keys, and per-key propagation only carries survivors — it would strand
    # a vanished key in the parent. So escalate downstream when the claim was
    # whole, regardless of the per-parent key_rule.
    prop =
      cond do
        "*" in keys -> :all
        forced?(opts, cell_id) -> forced_prop(keys, changed)
        true -> changed
      end

    parents = propagate(plan, cell_id, prop, opts, diffs, versions)

    step = %{
      claimed: keys,
      changed: changed,
      triggered_by: Map.get(cause, cell_id),
      duration_us: us,
      # `op` and `depth` are both in hand here and neither is
      # recoverable from a step alone — a consumer would need the plan
      # to look them up, and a durable processing log is usually read
      # long after the plan that produced it has moved on
      # (u2i/reactive_dag#114).
      op: cell.op,
      depth: Map.get(plan.depths, cell_id),
      meta: meta
    }

    :telemetry.execute(
      [:reactive_dag, :drain, :step],
      %{duration_us: us, claimed: length(keys), changed: length(changed)},
      %{
        cell: cell_id,
        pass: passes,
        # the KEYS, not just the count: a consumer that knows which
        # keys moved can read only those, which is the whole point
        changed_keys: changed,
        triggered_by: Map.get(cause, cell_id),
        step: step
      }
    )

    # Every parent we just dirtied was triggered by this cell.
    cause =
      Enum.reduce(parents, cause, fn parent_id, acc ->
        Map.put(acc, parent_id, cell_id)
      end)

    {cause, [Map.merge(step, %{cell: cell_id, pass: passes}) | steps], failed}
  end

  # `force:` names cells whose claimed keys propagate WHETHER OR NOT the
  # recompute reported them changed.
  #
  # It does not make the op work harder. An op that memoises — an md5-keyed cache,
  # a content digest the library never sees — still skips, and it SHOULD: the
  # expensive call is not what a re-run is usually after. What force overrides is
  # the conclusion drawn from `changed == []`, which without it stops the cascade
  # at this cell.
  #
  # That is exactly the situation a host is in when it re-runs a cell out-of-band
  # and then wants the graph to catch up: the work is already done, so the op has
  # nothing to report, and everything downstream would sit unrecomputed against a
  # payload that DID move. Cascade's `Rerun` hand-marked the immediate parents to
  # get around it, which reaches one level and reads like duplicated propagation.
  #
  # Only the FORCED cell is forced. Its parents propagate on their own verdicts,
  # so a genuinely unchanged consumer still stops the cascade — the alternative
  # (force everything transitively) would recompute the whole downstream graph on
  # every re-run and make change detection pointless past the first hop.
  defp forced?(opts, cell_id) do
    case opts[:force] do
      nil -> false
      :all -> true
      cells when is_list(cells) -> cell_id in cells
      cell -> cell == cell_id
    end
  end

  # The keys the claim was for, not the ones the op admitted to. `changed` is a
  # subset of `keys` (an op reports what moved), so the union is just `keys` —
  # except an op may legitimately report a key it was not claimed for (a
  # whole-cell pass discovering a new one), and dropping that would be a
  # regression.
  defp forced_prop(keys, changed), do: Enum.uniq(keys ++ changed)

  defp timed(fun) do
    t0 = System.monotonic_time(:microsecond)
    result = fun.()
    {result, System.monotonic_time(:microsecond) - t0}
  end

  defp recompute(%Cell{leaf?: true}, keys, _opts), do: {keys, %{}}

  defp recompute(cell, keys, opts) do
    case ReactiveDag.Node.Recompute.recompute(cell, keys, opts) do
      {:ok, changed} when is_list(changed) ->
        {changed, %{}}

      # a recompute may report what the work cost (tokens, retries, cache hits);
      # the library carries the map without interpreting it
      {:ok, changed, meta} when is_list(changed) and is_map(meta) ->
        {changed, meta}

      # CONTAINED, not raised. Must be a RETURNED value: an exception inside a
      # nested transaction aborts the outer one, so only this shape can be
      # isolated by the savepoint. `ReactiveDag.Source`'s `poll/1` already
      # returns errors "contained, not raised" for the same reason.
      {:error, reason} ->
        {:failed, reason}

      other ->
        # NAMED, rather than a CaseClauseError from inside the drain. The two
        # accepted shapes differ only in arity, so the mistake this catches is
        # nearly always a `compute` module that started reporting meta (or
        # stopped) while something else still returns the other shape — and a
        # bare CaseClauseError says which VALUE was unmatched without saying
        # which cell produced it. In a drain over a dozen cells that is the
        # whole question.
        raise ArgumentError, """
        reactive_dag: recomputing #{inspect(cell.id)} returned a value the drain cannot use.

            got:      #{inspect(other, limit: 5)}
            expected: {:ok, changed_keys}  |  {:ok, changed_keys, meta_map}

        `changed_keys` is a list of the keys whose output actually changed
        (returning every claimed key is always correct, just less efficient).
        `meta_map` is anything the node wants to report about the work — the
        library carries it onto the step without interpreting it.

        This is almost always a `compute Mod` whose `recompute/2` returned
        something else. A recompute that FAILED should raise rather than return
        an error tuple: the drain has already claimed these keys, so a swallowed
        failure marks them clean over work that did not happen.
        """
    end
  end

  defp cell_or_nil(plan, cell_id), do: Map.get(plan.cells, cell_id)

  # `{key, version_id}` pairs → `%{key => diff}`, through the node's own reader.
  #
  # A version belongs to the resource whose row changed, so the reader is that
  # node's declaration — the same node whose `version_id` wrote the reference.
  # A reader that raises costs precision, not the drain: the claim simply is not
  # narrowed, which is the same outcome as declaring no reader at all.
  # This cell's OWN units, derived from the changes its marks referenced.
  #
  # Returns `{keys, groups}`: the keys to recompute, and `%{unit => [group_values]}`
  # for the ones derived from a version. A cell that derives nothing gets its
  # claimed keys back unchanged and an empty map — which is every cell except a
  # fold whose grain is a plain field list.
  #
  # The units are computed HERE rather than at mark time, and that is the model: a
  # mark says which child row changed and references the change; the shape of this
  # cell's work is its own business, declared in its own grain, so it is derived
  # where that grain lives. Nothing composite is ever stored, so nothing has to be
  # split apart — and the group VALUES survive, which is what scopes the fold's
  # read exactly instead of to a per-column hull.
  defp derive_units(_plan, _cell_id, ["*"], _diffs), do: {["*"], %{}}
  defp derive_units(_plan, _cell_id, keys, diffs) when map_size(diffs) == 0, do: {keys, %{}}

  defp derive_units(plan, cell_id, keys, diffs) do
    cell = Map.get(plan.cells, cell_id)

    case grain_of(cell) do
      nil ->
        {keys, %{}}

      grain ->
        key_fn = unit_key_fn(cell)

        derived =
          for k <- keys,
              d = diffs[k],
              group <- ReactiveDag.Node.Diff.groups(d, grain),
              reduce: %{} do
            acc -> Map.update(acc, key_fn.(group), [group], &[group | &1])
          end

        # ALL-OR-NOTHING, and the invariant is enforced upstream: `parent_entries/6`
        # refuses to write version-bearing marks unless EVERY changed key carried
        # one, so a claim that gets here has a diff for all of its keys. Mixing
        # derived units with undrived keys would be the dangerous state — some
        # units claimed, the rest silently stranded — and it is prevented at the
        # mark rather than patched here.
        #
        # A derivation yielding nothing (a nil in the grain) falls back to the
        # claimed keys, which is what the cell would have recomputed anyway.
        case Map.keys(derived) do
          [] -> {keys, %{}}
          units -> {units, derived}
        end
    end
  end

  # The cell's grain as a plain field list, or nil when it cannot be projected
  # onto a map — a `{:calc, _}` the datastore evaluates, or a `%Join{}` whose
  # sides are picked rather than grouped.
  defp grain_of(nil), do: nil

  defp grain_of(%Cell{meta: meta}) do
    with :group <- meta[:key_rule],
         %{} = src <- meta[:over_source],
         plan when is_list(plan) <- src[:group_key_plan],
         true <- Enum.all?(plan, &match?({:attr, _, _}, &1)) do
      Enum.map(plan, fn {:attr, name, _string?} -> name end)
    else
      _ -> nil
    end
  end

  # How this cell NAMES a unit. Still a string — a payload row is found by its key
  # and a claim is reported in the drain's log — but derived here and never stored
  # in the queue, so it round-trips through nothing.
  defp unit_key_fn(%Cell{meta: meta}) do
    spec = meta[:reduce] || meta[:join]

    ReactiveDag.Node.Recompute.Declarative.key_fn(
      spec && Map.get(spec, :key),
      spec && Map.get(spec, :key_prefix)
    )
  end

  defp resolve_versions(_plan, nil, _entries), do: %{}

  # A mark's version reference was written by the cell whose ROW changed, so the
  # reader that resolves it is that cell's — the claimed cell itself for a source
  # mark (`dirties_on`), one of its INPUTS for a propagated one. Try the claimed
  # cell first, then its inputs; a reference nothing can read leaves the claim
  # un-narrowed rather than failing it.
  defp resolve_versions(plan, cell, entries) do
    readers =
      [cell.meta[:version_diff] | Enum.map(cell.inputs, &input_reader(plan, &1))]
      |> Enum.reject(&is_nil/1)
      |> Enum.uniq()

    if readers == [] do
      %{}
    else
      for {k, vid} <- entries,
          is_binary(vid),
          diff = Enum.find_value(readers, &read_version(&1, vid)),
          into: %{} do
        {k, diff}
      end
    end
  end

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

  # Each clause returns the list of parent cell_ids it dirtied (so the drain can
  # record them as triggered-by this cell).
  defp propagate(_plan, _cell_id, [], _opts, _diffs, _versions), do: []

  # :all — the cell was claimed whole; every parent recomputes whole too.
  defp propagate(plan, cell_id, :all, _opts, _diffs, _versions) do
    parents = Map.get(plan.parents, cell_id, [])
    opts = Plan.frontier_opts(plan)

    Enum.each(
      parents,
      &Frontier.mark_dirty(&1, ["*"], "propagated (all) from #{cell_id}", opts)
    )

    parents
  end

  defp propagate(plan, cell_id, changed, _opts, diffs, versions) do
    parents = Graph.dirty_parents(plan, cell_id, changed, ReactiveDag.Node.KeyRule, diffs)

    opts = Plan.frontier_opts(plan)

    Enum.each(parents, fn {parent_id, keys} ->
      Frontier.mark_dirty(
        parent_id,
        parent_entries(plan, parent_id, cell_id, keys, changed, versions),
        "propagated from #{cell_id}",
        opts
      )
    end)

    Enum.map(parents, fn {parent_id, _keys} -> parent_id end)
  end

  # WHAT a parent's mark says.
  #
  # For a parent whose grain the change can be projected onto, the mark names the
  # CHILD row that changed and references its version — `(cell, uuid, version)` —
  # and the parent derives its own units when it drains, from the version and its
  # own declared grain (`Diff.groups/2`).
  #
  # That is the whole reason no composite key is stored: a fold's unit is a tuple
  # of column values with no storable name, so filing a mark UNDER the unit forces
  # a `"|"` serialization that the consumer must then split apart — and splitting
  # can only recover a per-column hull, so the fold over-reads. Deriving in the
  # consumer keeps the values, and the values scope exactly.
  #
  # The child's key is used only when a version backs it. Without one the parent
  # cannot derive anything from a child key it does not group by, so the
  # pre-existing behaviour stands: mark the parent's own keys as the rule mapped
  # them.
  defp parent_entries(plan, parent_id, child_id, mapped_keys, changed, versions) do
    parent = Map.get(plan.cells, parent_id)

    cond do
      "*" in mapped_keys ->
        mapped_keys

      not derives_units?(parent, child_id) ->
        mapped_keys

      true ->
        entries = for k <- changed, vid = versions[k], do: {k, vid}

        # Every changed key must carry a version, or the parent would derive units
        # for some rows and silently miss the rest. A partial set is worse than
        # none: the missing rows' units are never claimed at all.
        if entries != [] and length(entries) == length(changed) do
          entries
        else
          mapped_keys
        end
    end
  end

  # A parent DERIVES its units from a child's change when its grain is a plain
  # field list over that child — the same condition `KeyRule`'s diff path uses.
  # A `{:calc, _}` grain or a `%Join{}` spec is evaluated by the datastore, not by
  # arithmetic on a map, so those keep the mapped-key mark.
  defp derives_units?(nil, _child_id), do: false

  defp derives_units?(%Cell{meta: meta}, child_id) do
    with :group <- meta[:key_rule],
         %{} = src <- meta[:over_source],
         ^child_id <- meta[:over] || child_id,
         plan when is_list(plan) <- src[:group_key_plan] do
      Enum.all?(plan, &match?({:attr, _, _}, &1))
    else
      _ -> false
    end
  end
end

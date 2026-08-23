defmodule ReactiveDag.Insights do
  @moduledoc """
  The engine, viewed from outside: what the graph LOOKS like, what state each
  cell is in, and what the last drains actually did.

  Everything here is a READ. Nothing new is computed — the plan already carries
  the structure and depths, each cell's own resource already carries its rows,
  and `ReactiveDag.Drain.Report` is already a complete causal trace. This module
  is those three assembled into the shape a human (or a dashboard, a mix task,
  an alerting check) actually asks for.

  Per-cell state is read through `ReactiveDag.Node.Rows`, which reads the node's
  resource under the DAG's own cell keys. A node with no `:status` column
  reports its keys with a `nil` status — the count is still the truth about how
  many units the cell holds.

  Deliberately UI-free: no Phoenix, no rendering, no assumptions about a web
  layer. `reactive_dag_dashboard` renders this; a host with no web layer at all
  can still call `summary/1` from a mix task or a health check.

  ## Retaining runs

  `record/1` keeps the last N runs in a rolling in-memory window, so a host gets
  a processing log without writing storage of its own. It takes what the engine
  hands back, in either of the two shapes the engine produces:

      # a scan — the poll and the drain it triggered
      :telemetry.attach("scans", [:reactive_dag, :scan, :stop], fn _e, _m, meta, _ ->
        ReactiveDag.Insights.record(meta.run)
      end, nil)

      # a drain someone triggered directly, with no poll in front of it
      {:ok, report} = ReactiveDag.Drain.run(plan, opts)
      ReactiveDag.Insights.record(report)

  The buffer holds `%ReactiveDag.ScanRun{}` either way. A bare `%Report{}` is a
  drain with no poll around it, so it is wrapped in a run at the boundary rather
  than stored as a second shape — `recent/1` returns ONE shape, and a consumer
  reads `run.report` for the drain and `run.duration_us`, `run.changed`,
  `run.unreachable`, `run.detail` for the poll without testing which kind of
  entry it got.

  ## Why the whole run, and not just the drain

  The buffer used to hold `%Report{}` alone, so a host with a `%ScanRun{}`
  unwrapped it and threw the envelope away. That discarded everything the POLL
  did: its wall time (usually most of a scan's — the drain is the cheap half),
  the keys it found changed, what it cost, and its `unreachable` list. A
  two-minute scan of an upstream it could not reach logged as `0 cells · 0
  changed · 6.1ms`, identical to a scan that looked at everything and found
  nothing. That is precisely the honest-gap failure `ReactiveDag.Source` warns
  about, reintroduced by the observer.

  `recent/1`'s entries therefore carry the run, and `polled?` says whether there
  was a poll at all, so "this drain reported no changes" and "this scan could
  not look" stay different sentences.

  This is an opt-in observer, not a durable log: the buffer is per-BEAM-node,
  in-memory, and lost on restart. A host that needs history stores the run where
  its runs already live.
  """

  alias ReactiveDag.{Drain.Report, Frontier, Node.Rows, Plan, ScanRun}

  @default_keep 20
  @failing_sample 10

  @typedoc """
  One retained run, as `recent/1` returns it.

    * `run` — the `%ReactiveDag.ScanRun{}`. Always present, always this struct,
      whichever shape was recorded.
    * `at` — when `record/1` was called (wall clock, for ordering a log).
    * `polled?` — was there a poll? `true` for a scan, `false` for a bare drain
      recorded on its own. The one field that tells the two apart, so nothing
      has to infer it from a nil `cell` or an empty `changed`.

  The drain is `run.report` — `nil` when a scan never drained (see
  `ReactiveDag.ScanRun.drained?/1`), so a consumer reading it must handle nil.
  """
  @type entry :: %{
          run: ScanRun.t(),
          at: DateTime.t(),
          polled?: boolean(),
          tenant: String.t() | nil
        }

  @typedoc "A cell's observable state."
  @type cell_status :: %{
          id: String.t(),
          depth: non_neg_integer(),
          inputs: [String.t()],
          leaf?: boolean(),
          op: atom() | nil,
          statuses: %{(String.t() | nil) => non_neg_integer()},
          key_count: non_neg_integer(),
          rows: :stored | :elsewhere | :unreadable,
          failing_sample: [String.t()]
        }

  # ── structure ───────────────────────────────────────────────────────────────

  @doc """
  The graph's shape: cells grouped by depth, in execution order.

  Depth is the longest path from a leaf, so every cell in a level can only
  depend on shallower ones — which is exactly the order the drain runs them in,
  and therefore the order worth drawing them in.
  """
  @spec levels(Plan.t()) :: [{non_neg_integer(), [ReactiveDag.Cell.t()]}]
  def levels(%Plan{cells: cells, depths: depths}) do
    cells
    |> Map.values()
    |> Enum.group_by(&Map.get(depths, &1.id, 0))
    |> Enum.map(fn {depth, cs} -> {depth, Enum.sort_by(cs, & &1.id)} end)
    |> Enum.sort_by(&elem(&1, 0))
  end

  @doc """
  The graph's edges as `{from, to}` pairs — an input edge points from the input
  cell TO the cell that reads it, i.e. the direction change flows.
  """
  @spec edges(Plan.t()) :: [{String.t(), String.t()}]
  def edges(%Plan{cells: cells}) do
    for {id, cell} <- cells, input <- cell.inputs, do: {input, id}
  end

  # ── per-cell state ──────────────────────────────────────────────────────────

  @doc """
  One cell's observable state: its declaration (depth, inputs, shape) plus what
  it currently holds (status histogram, key count, and a small sample of failing
  keys for the drawer).

  Reads the node's own resource, and pushes the reduction into the datastore: a
  histogram is a `DISTINCT` plus one `COUNT` per status, and the failing sample
  is a filtered `LIMIT`. No row is loaded to be counted, which matters most for
  a node whose payload is a blob — decoding it to discard it was the bulk of the
  old cost.

  Still several small queries per cell, so `summary/1` over a large graph is
  many round trips. That is the remaining cost, and it is a different one.
  """
  @spec cell_status(Plan.t(), String.t()) :: cell_status() | nil
  def cell_status(%Plan{cells: cells, depths: depths}, cell_id) do
    case Map.get(cells, cell_id) do
      nil -> nil
      cell -> build_status(cell, Map.get(depths, cell_id, 0))
    end
  end

  defp build_status(cell, depth) do
    {statuses, rows} =
      case rows_kind(cell) do
        # No table of its own — a `compose` node, whose nested legs hold the rows.
        # Reading it is not a failure, and an empty histogram here means "nothing
        # lives here", which is a different sentence from "I could not look".
        :elsewhere ->
          {%{}, :elsewhere}

        :stored ->
          case safe(fn -> Rows.status_histogram(cell) end) do
            {:ok, statuses} -> {statuses, :stored}
            :error -> {%{}, :unreadable}
          end
      end

    %{
      id: cell.id,
      depth: depth,
      inputs: cell.inputs,
      leaf?: cell.leaf? == true,
      # `op` is a core %Cell{} field, not meta (Cell's Access impl flattens the
      # two, but the struct is the honest read here)
      op: cell.op,
      statuses: statuses,
      # summed from the histogram rather than counted again: the histogram is
      # already one COUNT per status, so the total is free.
      key_count: statuses |> Map.values() |> Enum.sum(),
      # WHY the count is what it is. A zero from a real table, a node that keeps
      # its rows elsewhere, and a read that failed are three different states,
      # and a consumer collapsing them shows an alarm for two non-problems.
      rows: rows,
      failing_sample: failing_sample(cell, statuses)
    }
  end

  # A cell with no attributes has no table to read. Since rc.39 that means a
  # `compose` node's leg — every cell that computes something owns its rows — so
  # it is a shape, not a fault. Mirrors the guard `Rows.queryable/1` uses.
  defp rows_kind(cell) do
    resource = cell.meta[:resource]

    if is_nil(resource) or Ash.Resource.Info.attributes(resource) == [],
      do: :elsewhere,
      else: :stored
  end

  # only pay for the sample when something is actually failing. A nil status is
  # "this node has no status column", not a failure, so it never samples.
  defp failing_sample(cell, statuses) do
    failing = Map.keys(statuses) -- ["present", nil]

    if failing == [] do
      []
    else
      safe(fn -> Rows.keys_by_status(cell, failing, limit: @failing_sample) end, [])
    end
  end

  @doc """
  Every cell's state, in execution order — the dashboard's landing view.

  One status query per cell. Fine for the graphs this library is built for
  (tens of cells); for a very large graph, page it with `cell_status/2` instead.
  """
  @spec summary(Plan.t()) :: [cell_status()]
  def summary(%Plan{cells: cells, depths: depths}) do
    cells
    |> Map.values()
    |> Enum.sort_by(&{Map.get(depths, &1.id, 0), &1.id})
    |> Enum.map(&build_status(&1, Map.get(depths, &1.id, 0)))
  end

  @doc """
  Cell ids with dirty keys waiting — what the NEXT drain would work on.

  Reads the frontier rather than the nodes themselves: a cell is pending because
  something dirtied it, whether or not its rows have changed yet.
  """
  @spec pending(Plan.t()) :: [String.t()]
  def pending(%Plan{cells: cells}) do
    safe(
      fn ->
        Frontier.dirty_cells() |> Enum.filter(&Map.has_key?(cells, &1)) |> Enum.sort()
      end,
      []
    )
  end

  # ── retained runs ───────────────────────────────────────────────────────────

  @doc """
  Keep a run in the rolling in-memory window (default #{@default_keep},
  configurable with `config :reactive_dag, insights_keep: n`).

  Takes either shape the engine produces:

    * a `%ReactiveDag.ScanRun{}` — a scan, kept whole. The poll's duration,
      changed keys, cost `detail` and `unreachable` list are the point: they are
      most of what a scan did, and unwrapping to the report throws them away.
    * a bare `%ReactiveDag.Drain.Report{}` — a drain someone triggered directly,
      with no poll in front of it. Wrapped in a `%ScanRun{}` here so `recent/1`
      returns one shape; `polled?` on the entry is `false`.

  Opt-in: neither the scan nor the drain persists anything on its own. Returns
  its argument unchanged, so it drops into a pipeline either way:

      plan |> ReactiveDag.Drain.run(opts) |> then(fn {:ok, r} -> Insights.record(r) end)
  """
  @spec record(ScanRun.t(), keyword()) :: ScanRun.t()
  @spec record(Report.t(), keyword()) :: Report.t()
  def record(run_or_report, opts \\ [])

  def record(%ScanRun{} = run, opts) do
    put(run, true, opts)
    run
  end

  def record(%Report{} = report, opts) do
    # A drain with no poll around it. `changed`/`unreachable`/`detail` stay at
    # their empty defaults, which is honest — there was no poll to report them —
    # and `duration_us` is the drain's own, so a log line's duration column means
    # the same thing on both kinds of row.
    put(%ScanRun{report: report, duration_us: report.duration_us}, false, opts)
    report
  end

  defp put(run, polled?, opts) do
    ensure_table()
    :ets.insert(table(), {counter(), stamp(run, polled?, tenant_of(opts))})
    trim()
    :ok
  end

  # `:tenant` names WHICH GRAPH this run was. One buffer holds every tenant's
  # runs — it is a process-wide window, not a per-plan one — so without this a
  # host running several graphs cannot tell whose drain a log line describes.
  #
  # `nil` for a host with one graph, which is most of them, and what `recent/1`
  # returns unfiltered. A plan's own tenant is `"*"` when it has none, but that
  # is the FRONTIER's spelling for "untenanted" and belongs in the frontier: an
  # entry says `nil` for "this host does not divide its graphs", which reads
  # correctly in a log line and sorts out of a filter cleanly.
  defp tenant_of(opts) do
    case Keyword.get(opts, :tenant) do
      nil -> nil
      "*" -> nil
      t when is_binary(t) -> t
      t -> to_string(t)
    end
  end

  @doc """
  The most recent runs, newest first (default all retained).

  Each entry is a `t:entry/0`: the `%ReactiveDag.ScanRun{}`, when it was
  recorded, and whether a poll produced it.

      for %{run: run, at: at, polled?: polled?} <- Insights.recent(20) do
        %{
          at: at,
          kind: if(polled?, do: :scan, else: :drain),
          # the whole run's wall time: a scan's poll AND its drain
          duration_us: run.duration_us,
          # the POLL. `cell` is what was scanned; the rest is what it found, what
          # it could not reach, and what it cost. All at their empty defaults on
          # a bare drain, where there was no poll to report them.
          scanned: run.cell,
          changed: length(run.changed),
          unreachable: run.unreachable,
          detail: run.detail,
          # the DRAIN — `report` is nil when a scan never drained, so guard it
          passes: run.report && run.report.passes,
          steps: (run.report && run.report.steps) || [],
          # …and either phase's cost, or both summed
          tokens_in: ReactiveDag.ScanRun.total(run, :tokens_in)
        }
      end

  `run.duration_us` is the WHOLE run — for a scan, the poll plus its drain. The
  drain's own share is `run.report.duration_us`, and the gap between the two is
  the poll, which is usually the larger number by orders of magnitude. Showing
  the drain's figure as the run's is what made a two-minute scan render as
  `6.1ms`.

  `run.unreachable` is `[{upstream, reason}]` — non-empty means the poll could
  NOT see everything it meant to (`ReactiveDag.ScanRun.complete?/1`), and a
  consumer that renders it the same as an empty one is reporting a gap as a
  clean run.

  ## Tenants

  Pass `tenant:` to see ONE GRAPH's runs. The buffer is process-wide and holds
  every tenant's, so a host running several graphs and reading unfiltered gets
  another tenant's drain in the log it is showing for this one.

      Insights.recent(25, tenant: "village")

  The filter is applied BEFORE the limit, so twenty-five means twenty-five of
  that tenant's — not twenty-five of everyone's, then whichever of them match.

  Without it every entry comes back, which is right for a host with one graph
  (its entries carry no tenant) and right for an operator who wants to see
  everything at once.
  """
  @spec recent(pos_integer() | :all, keyword()) :: [entry()]
  def recent(limit \\ :all, opts \\ [])

  def recent(limit, opts) do
    ensure_table()

    entries =
      table()
      |> :ets.tab2list()
      |> Enum.sort_by(&elem(&1, 0), :desc)
      |> Enum.map(&elem(&1, 1))
      |> filter_tenant(tenant_of(opts))

    case limit do
      :all -> entries
      n when is_integer(n) -> Enum.take(entries, n)
    end
  end

  defp filter_tenant(entries, nil), do: entries

  # An entry recorded before its host declared tenants carries no tenant at all,
  # and is NOT claimed by whichever graph happens to be on screen: showing an
  # untenanted run as the village's asserts something the recording never said.
  defp filter_tenant(entries, tenant),
    do: Enum.filter(entries, &(Map.get(&1, :tenant) == tenant))

  @doc """
  The most recent run, or `nil` if none has been recorded.

  `tenant:` scopes it to one graph, like `recent/2`.
  """
  @spec last_run(keyword()) :: entry() | nil
  def last_run(opts \\ []), do: recent(1, opts) |> List.first()

  @doc "Forget every retained run."
  @spec forget_runs() :: :ok
  def forget_runs do
    ensure_table()
    :ets.delete_all_objects(table())
    :ok
  end

  # ── internals ───────────────────────────────────────────────────────────────

  @table :reactive_dag_insights_runs
  defp table, do: @table

  defp stamp(run, polled?, tenant),
    do: %{run: run, at: DateTime.utc_now(), polled?: polled?, tenant: tenant}

  # `get_env` with an explicitly-stored nil returns nil, not the default (a test
  # or a release config that clears the key does exactly that), so fall back on
  # the value rather than only on absence.
  defp keep do
    case Application.get_env(:reactive_dag, :insights_keep) do
      n when is_integer(n) and n > 0 -> n
      _ -> @default_keep
    end
  end

  # a monotonic ordering key: ETS has no insertion order, and two reports can
  # land in the same microsecond.
  defp counter, do: :erlang.unique_integer([:monotonic, :positive])

  defp ensure_table do
    case :ets.whereis(@table) do
      :undefined ->
        # public + named so any process can record; a caller racing us to create
        # it is fine, hence the rescue.
        :ets.new(@table, [:named_table, :public, :ordered_set])
        :ok

      _tid ->
        :ok
    end
  rescue
    ArgumentError -> :ok
  end

  defp trim do
    excess = :ets.info(table(), :size) - keep()

    if excess > 0 do
      table()
      |> :ets.tab2list()
      |> Enum.sort_by(&elem(&1, 0))
      |> Enum.take(excess)
      |> Enum.each(fn {k, _} -> :ets.delete(table(), k) end)
    end

    :ok
  end

  # a node's resource may be unreadable in the context this is called from (a
  # policy, an unmigrated table, a data layer that isn't up) — a dashboard
  # should degrade to "structure only" rather than crash the page.
  defp safe(fun) do
    {:ok, fun.()}
  rescue
    _ -> :error
  catch
    _, _ -> :error
  end

  # the degrade-to-a-default form, for reads whose failure needs no distinction
  defp safe(fun, default) do
    case safe(fun) do
      {:ok, value} -> value
      :error -> default
    end
  end
end

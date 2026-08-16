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

  ## Retaining reports

  `Drain.run/2` returns a `%Report{}` and most callers discard it — the drain
  deliberately does not persist anything (see `ReactiveDag.Drain.Report`: the
  library reports, the host records). For a rolling window without a host
  writing its own storage, `record/1` keeps the last N in memory:

      {:ok, report} = ReactiveDag.Drain.run(plan, opts)
      ReactiveDag.Insights.record(report)

  `recent/1` then reads them back, newest first. This is an opt-in observer, not
  a durable log: the buffer is per-node, in-memory, and lost on restart. A host
  that needs history stores the report where its runs already live.
  """

  alias ReactiveDag.{Drain.Report, Frontier, Node.Rows, Plan}

  @default_keep 20
  @failing_sample 10

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
        # No table by DESIGN — a write-elsewhere or escape-hatch node
        # (`Ash.DataLayer.Simple`, no attributes). Reading it is not a failure,
        # and an empty histogram here means "nothing lives here", which is a
        # different sentence from "I could not look".
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

  # A node with no attributes has no table to read — that is the declared
  # write-elsewhere / escape-hatch shape, not a fault. Mirrors the guard
  # `Rows.queryable/1` uses.
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

  # ── drain reports ───────────────────────────────────────────────────────────

  @doc """
  Keep `report` in the rolling in-memory window (default #{@default_keep},
  configurable with `config :reactive_dag, insights_keep: n`).

  Opt-in: the drain persists nothing on its own. Returns the report, so it
  drops into a pipeline:

      plan |> ReactiveDag.Drain.run(opts) |> then(fn {:ok, r} -> Insights.record(r) end)
  """
  @spec record(Report.t()) :: Report.t()
  def record(%Report{} = report) do
    ensure_table()
    :ets.insert(table(), {counter(), stamp(report)})
    trim()
    report
  end

  @doc "The most recent reports, newest first (default all retained)."
  @spec recent(pos_integer() | :all) :: [%{report: Report.t(), at: DateTime.t()}]
  def recent(limit \\ :all) do
    ensure_table()

    entries =
      table()
      |> :ets.tab2list()
      |> Enum.sort_by(&elem(&1, 0), :desc)
      |> Enum.map(&elem(&1, 1))

    case limit do
      :all -> entries
      n when is_integer(n) -> Enum.take(entries, n)
    end
  end

  @doc "The most recent report, or `nil` if none has been recorded."
  @spec last_report() :: %{report: Report.t(), at: DateTime.t()} | nil
  def last_report, do: recent(1) |> List.first()

  @doc "Forget every retained report."
  @spec forget_reports() :: :ok
  def forget_reports do
    ensure_table()
    :ets.delete_all_objects(table())
    :ok
  end

  # ── internals ───────────────────────────────────────────────────────────────

  @table :reactive_dag_insights_reports
  defp table, do: @table

  defp stamp(report), do: %{report: report, at: DateTime.utc_now()}

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

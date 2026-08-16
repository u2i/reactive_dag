defmodule ReactiveDag.Source do
  @moduledoc """
  A **scanner** — the third seam, alongside `ReactiveDag.RecomputeStrategy` and
  `ReactiveDag.KeyRule`.

  A source reads external state (a fleet API, a cloud estate, a repo, an LLM) and
  writes a **leaf cell**'s rows in a *poll* phase deliberately OUTSIDE the drain:

    1. **poll** — run each source's `poll/1`: fetch → write its leaf's rows →
       return the leaf keys that CHANGED (so the caller can mark parents dirty).
       Sources are independent; a failure is contained to its own leaf.
    2. **drain** — the engine recomputes everything downstream from the dirty
       frontier (`ReactiveDag.Drain`). No source runs here.

  This split is a design invariant, not an accident: the drain is pure set/graph
  computation over rows already written — deterministic, re-runnable, and it
  never fails on a network outage. Effectful, non-deterministic, fallible I/O
  (that's every scanner) stays in phase 1.

  The scanner↔leaf binding has ONE home, chosen by cardinality:

    * **1:1 (the common case) — inline on the leaf.** `source :fleet_scan` /
      `driver MyApp.Sources.FleetScan` in the leaf's `reactive` block co-locates
      the leaf and its scanner in one declaration (they travel together).
      `ReactiveDag.Source.drivers/2` reads these off the graph (each leaf cell's
      `meta.driver`).
    * **fan-out (rare) — on the driver.** A scanner that writes cells no single
      leaf owns (e.g. many guarantee sub-cells) has no inline `driver` and names
      its cells via its own `leaf_cells/1`; the host passes it as an `extra`
      driver.

  Either way `verify!/2` confirms every named leaf is a real cell in the built
  plan (an inline driver's leaf is the node it's declared on).

  ## The contract

  Two apps (a data pipeline and a compliance model) independently grew the same
  three-callback shape — `id` / `leaf_cells` / `poll → changed-keys` — which is
  why it lives here rather than in either app. A single-leaf source (the common
  case) may export `leaf_cell/0` instead of `leaf_cells/1`; `cells_of/2`
  resolves whichever is present.

      defmodule MyApp.Sources.FleetScan do
        @behaviour ReactiveDag.Source

        @impl true
        def id, do: :fleet_scan

        @impl true
        def leaf_cells(_graph), do: ["machines"]

        @impl true
        def poll(_opts) do
          # fetch the fleet, write the "machines" leaf's rows, return changed keys
          {:ok, %{changed: ["host-1", "host-7"]}}
        rescue
          e -> {:error, Exception.message(e)}
        end
      end

  ## Fan-out and multi-leaf sources

  `leaf_cells/1` takes the lowered graph and returns a **list** of cell ids, so a
  source that feeds many leaves (e.g. one per discovered kind) computes them from
  the graph, and a source that direct-writes several cells lists them all. A
  single-leaf source returns `[one_id]`. This list is what `verify/2` checks.
  """

  @typedoc "The lowered graph — a `ReactiveDag.Plan` (or any map with a `:cells` map keyed by id)."
  @type graph :: %{cells: %{optional(String.t()) => struct()}}

  @doc "Stable id of this source (matches the `source :id` binding where declared)."
  @callback id() :: atom()

  @doc """
  The authoritative set of cell ids this source feeds, given the lowered `graph` —
  the binding `verify/2` validates. Single-leaf sources return `[leaf]`; fan-out
  sources compute their per-instance leaves from the graph; multi-leaf sources
  list every cell they write.

  OPTIONAL: a single-leaf source may instead export `leaf_cell/0` (the common
  case — one scanner, one leaf) and skip this; `cells_of/2` resolves whichever
  the module exports. A module must export at least one of the two.
  """
  @callback leaf_cells(graph()) :: [String.t()]

  @doc """
  Single-leaf fallback for `leaf_cells/1`: the ONE cell id this source feeds.
  For the common one-scanner-one-leaf driver, this is the whole binding — no
  graph-dependent computation to write.
  """
  @callback leaf_cell() :: String.t() | atom()

  @doc """
  Poll the external source: fetch → write the leaf tuples → return the leaf keys
  that changed. `{:error, reason}` when it couldn't run at all (no credential,
  API down) — contained, not raised, so one bad source doesn't abort a refresh.
  `arg` is source-specific (a since-timestamp, a manifest path, opts).

  A multi-upstream source that could observe SOME of its inputs reports the
  others under the optional `unreachable:` key (`{upstream_label, reason}`
  pairs) — the honest-gap discipline: a scan that couldn't look must never
  render as a scan that found nothing, so write what you observed, retire
  nothing you couldn't see, and surface the outage for the host to display.
  """
  @callback poll(arg :: term()) ::
              {:ok,
               %{
                 :changed => [String.t()],
                 optional(:unreachable) => [{String.t(), term()}]
               }}
              | {:error, term()}

  @doc """
  Optional lineage for display: where this source's data comes from, as a map like
  `%{label: "Fleet · Huntress", url: "https://…", store: "Tigris"}` (any subset).
  Not implemented = origin unknown.
  """
  @callback origin() :: map() | nil

  @optional_callbacks origin: 0, leaf_cells: 1, leaf_cell: 0

  @doc """
  The cells `source` feeds in `graph` — the resolver behind `verify!/2`. Uses
  the module's `leaf_cells/1` when exported, else the single-leaf `leaf_cell/0`
  fallback (as `[to_string(leaf_cell())]`). Raises `ArgumentError` when the
  module exports neither.
  """
  @spec cells_of(module(), graph()) :: [String.t()]
  def cells_of(source, graph) do
    # ensure_loaded: function_exported?/3 is false for a merely-unloaded module.
    cond do
      not Code.ensure_loaded?(source) ->
        raise ArgumentError, "reactive_dag: source #{inspect(source)} is not a loadable module"

      function_exported?(source, :leaf_cells, 1) ->
        source.leaf_cells(graph)

      function_exported?(source, :leaf_cell, 0) ->
        [to_string(source.leaf_cell())]

      true ->
        raise ArgumentError,
              "reactive_dag: source #{inspect(source)} exports neither leaf_cells/1 nor " <>
                "leaf_cell/0 — a source must name the cell(s) it feeds"
    end
  end

  @doc """
  Verify a `scan Mod` declaration on the leaf `cell_id`: the module must be a
  loadable `ReactiveDag.Source`, and its own `leaf_cells/1` must claim this leaf.

  The second half matters more than it looks. A scanner already knows which
  cells it feeds; `scan` states the same fact from the other side. Two
  statements of one fact can disagree, so this is the check that they don't —
  a scanner refactored to feed `"agenda_docs_v2"` while a resource still
  declares `scan` fails at assembly rather than polling into a cell nobody
  reads.

  Called by `ReactiveDag.Node.graph/2` for every cell carrying a `scan`, so a
  host declaring scanners in the DSL needs no `verify!/2` call of its own.
  """
  @spec verify_scan!(module(), String.t(), graph()) :: :ok
  def verify_scan!(source, cell_id, graph) do
    unless Code.ensure_loaded?(source) do
      raise ArgumentError,
            "reactive_dag: cell #{inspect(cell_id)} declares `scan #{inspect(source)}`, " <>
              "which is not a loadable module."
    end

    unless function_exported?(source, :poll, 1) do
      raise ArgumentError,
            "reactive_dag: cell #{inspect(cell_id)} declares `scan #{inspect(source)}`, " <>
              "which does not implement `ReactiveDag.Source` (no poll/1). A scan names " <>
              "the source that FEEDS this leaf; use `compute`/`run` for a node that " <>
              "computes its own rows."
    end

    claimed = cells_of(source, graph)

    unless cell_id in claimed do
      raise ArgumentError,
            "reactive_dag: cell #{inspect(cell_id)} declares `scan #{inspect(source)}`, but " <>
              "that source feeds #{inspect(claimed)} — not this leaf. The scanner and the " <>
              "leaf disagree about which cells it writes; fix whichever is stale."
    end

    :ok
  end

  @doc """
  Poll every scanner the PLAN declares (via `scan Mod` on its leaves), in the
  poll phase before a drain.

  Scanners are found from the graph rather than a list the host maintains
  alongside it — the list is the thing that drifts. A source feeding many
  leaves appears once, however many leaves declare it.

  Returns `{:ok, %{module => result}}`, or `{:error, failures}` where failures
  are `{module, reason}`: one scanner failing must not silently cancel the
  others, and must not look like success.
  """
  @spec poll_all(graph(), keyword()) :: {:ok, map()} | {:error, [{module(), term()}]}
  def poll_all(graph, opts \\ []) do
    standing = standing_args(graph)

    {oks, errors} =
      graph
      |> scanners()
      |> Enum.map(fn mod ->
        # the leaf's declared `args:` are the DEFAULT; the caller's win. That is
        # what keeps a routine `poll_all(plan)` cheap without any call site
        # remembering the bound, while still letting a deliberate deep pass say
        # so — `poll_all(plan, recent: false)`.
        {mod, safe_poll(mod, Keyword.merge(Map.get(standing, mod, []), opts))}
      end)
      |> Enum.split_with(fn {_mod, result} -> match?({:ok, _}, result) end)

    case errors do
      [] -> {:ok, Map.new(oks, fn {mod, {:ok, r}} -> {mod, r} end)}
      _ -> {:error, Enum.map(errors, fn {mod, {:error, reason}} -> {mod, reason} end)}
    end
  end

  @doc """
  Poll the scanner feeding ONE cell — the "re-run this scanner" affordance.

  `poll_all/2` is the routine sweep. This is what a host wires a button to: a
  dashboard has a cell in hand, not a source module, and a human asking to
  refresh is asking about *this leaf*, not about every scanner in the graph.

      # routine, on the declared cadence
      Source.poll_all(plan)

      # a human pressed "refresh", accepting the cheap default
      Source.poll_cell(plan, "agenda_docs")

      # ...or asked for the deep pass
      Source.poll_cell(plan, "agenda_docs", recent: false)

  The leaf's declared `args:` apply exactly as they do in `poll_all/2`, with the
  caller's opts winning — so a button that passes nothing gets the cheap pass,
  and one that passes `recent: false` gets the expensive one.

  Returns `{:ok, result}`, `{:error, reason}` if the poll failed, or
  `{:error, :no_scanner}` when the cell declares none — which a host should
  render as "no refresh available" rather than as a failure.

  Note a source feeding several leaves is polled whole: `poll/1` takes options,
  not a cell, so asking for one leaf runs whatever that scanner does. The
  scanner narrows itself through `args:` if that matters.
  """
  @spec poll_cell(graph(), String.t(), keyword()) ::
          {:ok, map()} | {:error, term()} | {:error, :no_scanner}
  def poll_cell(graph, cell_id, opts \\ []) do
    case graph.cells[cell_id] do
      nil ->
        {:error, :no_scanner}

      cell ->
        case cell.meta[:scan] do
          nil -> {:error, :no_scanner}
          mod -> safe_poll(mod, Keyword.merge(cell.meta[:scan_args] || [], opts))
        end
    end
  end

  @doc """
  Poll a cell's scanner AND mark what changed — the whole poll half of the
  two-phase loop, in one call.

  `poll_cell/3` returns what the scanner said and stops, leaving every host to
  hand-write the same three steps: normalise the return shape, mark the
  frontier, propagate to parents. That loop is documented in the guide and was
  provided nowhere, which is why every host grew a worker to hold it.

      {:ok, result} = Source.refresh(plan, "agenda_docs")
      #=> %{changed: ["a", "b"], marked: %{"agenda_docs" => ["a", "b"]},
      #     unreachable: []}

  Then drain — separately, and deliberately so. The poll/drain split is a design
  invariant (external I/O must not sit inside a depth-ordered recompute), and a
  host polling several sources usually wants ONE drain after all of them rather
  than one each.

  ## Return shapes

  A single-leaf source returns a flat key list; a fan-out source returns
  `%{leaf_id => keys}`. Both are in the `Source` contract, so both are
  normalised here rather than in each host.

  `:reason` labels the frontier rows (default `"scan"`), which is what makes a
  drain's trace say *why* a cell was dirty.

  Nothing is marked for an unreachable upstream, because the scanner reported no
  keys for it — an outage propagates nothing, which is the honest gap holding by
  construction rather than by rule.
  """
  @spec refresh(graph(), String.t(), keyword()) ::
          {:ok, %{changed: [String.t()], marked: map(), unreachable: list()}}
          | {:error, term()}
  def refresh(graph, cell_id, opts \\ []) do
    {reason, poll_opts} = Keyword.pop(opts, :reason, "scan")
    key_rule = Keyword.get(poll_opts, :key_rule, ReactiveDag.KeyRule)
    poll_opts = Keyword.delete(poll_opts, :key_rule)

    case poll_cell(graph, cell_id, poll_opts) do
      {:ok, result} ->
        marked = mark(graph, cell_id, result, reason, key_rule)

        {:ok,
         %{
           changed: marked |> Map.values() |> List.flatten() |> Enum.uniq(),
           marked: marked,
           unreachable: Map.get(result, :unreachable, []),
           # A scanner using `Rows.reconcile/3` gets a `%{created:, updated:,
           # revived:, retired:}` breakdown; it reaches here only if the scanner
           # passes it back in its poll result, since `poll/1` is the host's own
           # function and the library never sees the reconcile.
           detail: Map.get(result, :detail)
         }}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # A flat list belongs to the cell that was polled; a map names its own leaves.
  defp mark(graph, cell_id, result, reason, key_rule) do
    by_leaf =
      case Map.get(result, :changed, []) do
        keys when is_list(keys) -> %{cell_id => keys}
        %{} = by_leaf -> Map.new(by_leaf, fn {leaf, keys} -> {to_string(leaf), keys} end)
      end

    for {leaf, keys} <- by_leaf, keys != [], into: %{} do
      ReactiveDag.Frontier.mark_dirty(leaf, keys, reason)

      # `dirty_parents/5` COMPUTES each parent's claim; marking it is the
      # caller's. Dropping the return here would mark the leaf and strand the
      # change one level up — the cascade would stop before it started.
      for {parent, parent_keys} <- ReactiveDag.Graph.dirty_parents(graph, leaf, keys, key_rule) do
        ReactiveDag.Frontier.mark_dirty(parent, parent_keys, reason)
      end

      {leaf, keys}
    end
  end

  @doc """
  What a host needs to render a scan control for each cell that has one:
  `%{cell_id => %{source:, args:, every:, origin:}}`.

  The library describes; the host renders. A cell with no scanner is absent, and
  a scanner declaring no `args:`/`every:` reports them empty — so a leaf cheap
  enough to run whole gets a plain "refresh" and no misleading range picker,
  without the dashboard having to know which scanners are expensive.

  `origin:` is the source's own `origin/0` when it implements it, so a control
  can say *where* it is about to fetch from.
  """
  @spec controls(graph()) :: %{String.t() => map()}
  def controls(graph) do
    for {id, cell} <- graph.cells,
        mod = cell.meta[:scan],
        not is_nil(mod),
        into: %{} do
      {id,
       %{
         source: mod,
         args: cell.meta[:scan_args] || [],
         every: cell.meta[:scan_every],
         origin: origin_of(mod)
       }}
    end
  end

  defp origin_of(mod) do
    if Code.ensure_loaded?(mod) and function_exported?(mod, :origin, 0), do: mod.origin()
  end

  @doc """
  Every scannable unit of work in the plan: `%{cell:, source:, args:, every:}`.

  One entry per cell that declares a `scan`, whether or not it declares a
  cadence. This is the list a host schedules from, renders controls from, and
  triggers ad-hoc runs from — `crontab/2` is a projection of the subset that
  declared `every:`.

      Source.scan_jobs(plan)
      #=> [%{cell: "agenda_docs", source: MyApp.Crawler,
      #      args: [recent: true], every: "0 * * * *"}]

  Keyed by CELL rather than by source, because a source feeding several leaves
  is several units of work — each with its own declared bound.
  """
  @spec scan_jobs(graph()) :: [
          %{cell: String.t(), source: module(), args: keyword(), every: String.t() | nil}
        ]
  def scan_jobs(graph) do
    for {id, cell} <- graph.cells,
        mod = cell.meta[:scan],
        not is_nil(mod) do
      %{
        cell: id,
        source: mod,
        args: cell.meta[:scan_args] || [],
        every: cell.meta[:scan_every]
      }
    end
    |> Enum.sort_by(& &1.cell)
  end

  @doc """
  The Oban-style crontab entries a plan's leaves declare, as
  `{cron, worker, args: %{"cell" => id}}`.

  The library **never schedules anything**. A leaf declaring `every:` states how
  often a routine poll *should* run; this collects those declarations into data
  the host hands to its own scheduler:

      plugins: [
        {Oban.Plugins.Cron, crontab: ReactiveDag.Source.crontab(plan, MyApp.ScanWorker)}
      ]

  Emitting data rather than inserting jobs keeps the library out of the host's
  supervision tree and out of its deploy story — and lets a host filter, rewrite
  or ignore the entries, which it could not do if they were already scheduled.

  The worker receives `%{"cell" => "agenda_docs"}` and is expected to poll that
  one leaf. A leaf declaring no `every:` contributes nothing, which is the
  correct outcome for a scanner cheap enough to run on any cadence the host
  likes.

  ## One entry per SCANNER, not per leaf

  A source feeding several leaves — one crawl of one site whose rows land in two
  cells — is ONE unit of scheduled work. `scan_jobs/1` lists it per-cell, which
  is right for a control panel (a human triggers a leaf, and each leaf declares
  its own bound), and wrong for a scheduler: two leaves declaring the same module
  would emit two entries and crawl the same site twice an hour, each poll marking
  both leaves and the second achieving nothing.

  So entries are deduplicated by scanner. The `"cell"` carried is the first of
  its leaves alphabetically — any of them reaches the same scanner, and
  `refresh/3` marks every leaf the poll reports regardless of which one was
  named.

  Two leaves declaring DIFFERENT cadences for one scanner get one entry each, on
  the assumption that a host writing two different cadences meant them; the
  duplicate crawl is then declared rather than accidental.

  ## Adding your own arguments

  A crontab entry is built once at config time, so anything it carries is fixed
  for every firing — a per-run id has to be minted when the job fires, inside
  the worker. What a host CAN do here is add its own standing arguments, with
  `args:`:

      Source.crontab(plan, MyApp.ScanWorker, args: %{"queue" => "crawls"})

  merged UNDER the `"cell"` this computes, so a typo cannot silently retarget
  the job at a different leaf. A host that wants per-firing values wraps the
  worker rather than the crontab: mint the id in `perform/1`, then call
  `refresh/3` with `reason: "scan:\#{run_id}"` — `:reason` is a free string and
  rides through to the frontier rows, so the trace says which run dirtied a cell.
  """
  @spec crontab(graph(), module(), keyword()) :: [{String.t(), module(), keyword()}]
  def crontab(graph, worker \\ ReactiveDag.ScanWorker, opts \\ []) do
    extra = Keyword.get(opts, :args, %{})

    graph
    |> scan_jobs()
    |> Enum.reject(&is_nil(&1.every))
    # one poll per scanner per cadence: the same source on two leaves is one
    # crawl, and scheduling it twice is duplicated I/O whose second run marks
    # rows the first already handled
    |> Enum.group_by(&{&1.source, &1.every})
    |> Enum.map(fn {{_source, every}, jobs} ->
      cell = jobs |> Enum.map(& &1.cell) |> Enum.min()
      {every, worker, args: Map.merge(extra, %{"cell" => cell})}
    end)
    |> Enum.sort_by(fn {every, _w, args: %{"cell" => c}} -> {c, every} end)
  end

  @doc """
  The standing `args:` each scanner's leaf declared, as `%{module => keyword}`.

  `poll_all/2` merges these under the caller's opts. Exposed because a host
  driving one scanner directly wants the same default rather than a second copy
  of it.
  """
  @spec standing_args(graph()) :: %{module() => keyword()}
  def standing_args(graph) do
    # MERGED across the leaves sharing a scanner, not last-one-wins. A source
    # feeding several leaves is polled ONCE, so there is one set of args for
    # that poll; letting a second leaf's silence overwrite the first leaf's
    # declared bound turns a cheap poll into a full crawl (or the reverse)
    # depending on map ordering, which is not a thing anyone declared.
    for cell <- Map.values(graph.cells),
        mod = cell.meta[:scan],
        not is_nil(mod),
        reduce: %{} do
      acc -> Map.update(acc, mod, cell.meta[:scan_args] || [], &Keyword.merge(&1, cell.meta[:scan_args] || []))
    end
  end

  @doc "The distinct scanner modules a plan's leaves declare via `scan`."
  @spec scanners(graph()) :: [module()]
  def scanners(graph) do
    graph.cells
    |> Map.values()
    |> Enum.map(& &1.meta[:scan])
    |> Enum.reject(&is_nil/1)
    |> Enum.uniq()
  end

  # NB the rescue must not re-wrap: `{:error, reason}` from a well-behaved poll
  # and a raised exception both have to arrive as ONE `{:error, reason}`, or the
  # caller has to unwrap two shapes to print one message.
  defp safe_poll(mod, opts) do
    case mod.poll(opts) do
      {:ok, result} -> {:ok, result}
      {:error, reason} -> {:error, reason}
      other -> {:error, {:bad_return, other}}
    end
  rescue
    e -> {:error, Exception.message(e)}
  catch
    kind, value -> {:error, {kind, value}}
  end

  @doc """
  Verify every source's declared leaves resolve to real cells in `graph` — the
  authoritative scanner↔leaf check. Each driver's leaves are resolved via
  `cells_of/2` (`leaf_cells/1`, or the single-leaf `leaf_cell/0` fallback); this
  confirms every one is a real cell in the built plan. Needs the lowered graph
  (a host may expand generator leaves from live data), so it runs at
  assembly/boot time, not compile time.

  Returns `:ok`, or raises `ArgumentError` naming every `{source, dangling_leaf}`.
  """
  @spec verify!([module()], graph()) :: :ok
  def verify!(sources, graph) do
    verify_cells!(Enum.map(sources, &{&1, cells_of(&1, graph)}), graph)
  end

  @doc """
  The same dangling-leaf check as `verify!/2`, but over already-resolved
  `{source, [cell_id]}` pairs instead of resolving each module via `cells_of/2`.
  Use this when a host resolves fed cells itself (its own conventions beyond
  `leaf_cells/1` / `leaf_cell/0`).
  Returns `:ok`, or raises `ArgumentError` naming every `{source, dangling_leaf}`.
  """
  @spec verify_cells!([{module(), [String.t()]}], graph()) :: :ok
  def verify_cells!(source_cells, graph) do
    cell_ids = graph.cells |> Map.keys() |> MapSet.new()

    dangling =
      for {mod, cells} <- source_cells,
          leaf <- cells,
          not MapSet.member?(cell_ids, leaf),
          do: {mod, leaf}

    case dangling do
      [] ->
        :ok

      _ ->
        raise ArgumentError,
              "source(s) feed a leaf cell absent from the graph: " <>
                Enum.map_join(dangling, ", ", fn {m, l} -> "#{inspect(m)} -> #{l}" end)
    end
  end

  @doc """
  The scanner drivers feeding a lowered `graph`: the inline ones declared with
  `driver MyApp.Sources.FleetScan` on a leaf's `reactive` block (read from each
  leaf cell's `meta.driver`), unioned with any `extra` fan-out drivers a host
  passes (drivers that name their cells via `leaf_cells/1` because no single
  leaf owns them). This is the full scanner set — feed it to `verify!/2`, poll
  it in phase 1.
  """
  @spec drivers(graph(), [module()]) :: [module()]
  def drivers(graph, extra \\ []) do
    inline =
      graph.cells
      |> Map.values()
      |> Enum.flat_map(fn cell ->
        case cell.meta[:driver] do
          nil -> []
          driver -> [driver]
        end
      end)

    (inline ++ extra) |> Enum.uniq()
  end
end

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

  A source is a NODE. It declares `poll MyApp.Sources.FleetScan` in its
  `reactive` block, and everything that reads it is an ordinary edge:

      # the source — rows from outside the graph
      reactive do
        id :fleet
        leaf? true
        poll MyApp.Sources.FleetScan, every: "0 * * * *", args: [recent: true]
      end

      # a consumer — an ordinary node, reading it like any other input
      reactive do
        id :diggers
        reduce over: :fleet, group_by: :key, expand: &diggers_only/2
      end

  So the cells a source feeds are its children: `Source.cells_of/2` reads them
  off the plan. There is one declaration and one place it is read.

  It used to be two. A leaf declared `scan Mod`, the module declared
  `leaf_cells/1` back, and `verify_scan!/3` raised when the copies disagreed —
  an error class that existed only because the same fact was written twice, with
  a message asking you to work out which side was stale. `every:` and `args:`
  had the same problem one level up: spread across the leaves a source fed, they
  had to be reassembled, and a source feeding two leaves could silently lose the
  args one of them declared.

  ## The contract

  Two apps (a data pipeline and a compliance model) independently grew the same
  shape — `id` / `poll → changed-keys` — which is why it lives here rather than
  in either app.

      defmodule MyApp.Sources.FleetScan do
        @behaviour ReactiveDag.Source

        @impl true
        def id, do: :fleet_scan

        @impl true
        def poll(_opts) do
          # fetch the fleet, write the source node's rows, return changed keys
          {:ok, %{changed: ["host-1", "host-7"]}}
        rescue
          e -> {:error, Exception.message(e)}
        end
      end

  ## Fan-out

  One poll whose rows belong to several downstream nodes needs nothing special:
  the source holds what it found, and each consumer projects its own part with
  `expand:`, declining the keys that are not its own (`{:skip, key}` — see
  `ReactiveDag.Node`). One fetch, one cadence, one set of standing args, and the
  split is a declared property of the rows rather than a convention inside
  `poll/1`.
  """

  @typedoc "The lowered graph — a `ReactiveDag.Plan` (or any map with a `:cells` map keyed by id)."
  @type graph :: %{cells: %{optional(String.t()) => struct()}}

  @doc "Stable id of this source (matches the `source :id` binding where declared)."
  @callback id() :: atom()

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

  @optional_callbacks origin: 0

  @doc """
  The cells a source feeds, **derived from the graph**.

  A node declares `poll MyCrawler`; everything reading that node is an ordinary
  edge, so a source's outputs are its children. One declaration, read in one
  place, and nothing that can go stale.

  It used to be the reverse: the module declared `leaf_cells/1`, each fed leaf
  declared `scan`, and `verify_scan!/3` caught the two disagreeing. Every
  implementation in this repo and its host apps ignored the `graph` argument and
  returned a literal list, restating what the leaves had already said.

  Returns `[]` for a source no node polls — not an error: a plan built from a
  subset of resources legitimately excludes some.
  """
  @spec cells_of(module(), graph()) :: [String.t()]
  def cells_of(source, %{cells: cells}) do
    for {id, cell} <- cells, cell.meta[:scan] == source, do: id
  end

  @doc """
  Verify a `poll Mod` declaration on `cell_id`: the module must be a loadable
  `ReactiveDag.Source`.

  That is the whole check now. It used to also assert the module's own
  `leaf_cells/1` claimed this cell — a check that existed only because the
  pairing was written twice, and whose error message asked you to work out which
  copy was stale. The cells a source feeds are `plan.parents[id]`, so there is
  one declaration and nothing to disagree with.
  """
  @spec verify_poll!(module(), String.t()) :: :ok
  def verify_poll!(source, cell_id) do
    unless Code.ensure_loaded?(source) do
      raise ArgumentError,
            "reactive_dag: cell #{inspect(cell_id)} declares `poll #{inspect(source)}`, " <>
              "which is not a loadable module."
    end

    unless function_exported?(source, :poll, 1) do
      raise ArgumentError,
            "reactive_dag: cell #{inspect(cell_id)} declares `poll #{inspect(source)}`, " <>
              "which does not implement `ReactiveDag.Source` (no poll/1). A poll names " <>
              "the source that FETCHES this node's rows; use `compute`/`run` for a node " <>
              "that computes its own."
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

  ## Ordering

  Sources are polled SEQUENTIALLY, in cell order. A host whose sweep order
  carries meaning — observers before the nodes that read them — declares it:

      Source.poll_all(plan, order: [:observations, :verdicts])

  Anything unlisted follows, in cell order. Unlike `crontab/3`'s `order:`, this
  one is a real happens-before: these polls run one after another in this
  process, so a later source genuinely sees what an earlier one wrote.
  """
  @spec poll_all(graph(), keyword()) :: {:ok, map()} | {:error, [{module(), term()}]}
  def poll_all(graph, opts \\ []) do
    standing = standing_args(graph)
    {order, opts} = Keyword.pop(opts, :order)

    {oks, errors} =
      graph
      |> scanners(order: order)
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
  the job at a different leaf.

  ## Ordering

  Entries are alphabetical by cell — deterministic, so the list does not shift
  with map ordering, but arbitrary. A host whose scan order carries meaning says
  so:

      Source.crontab(plan, MyWorker, order: [:observations, :verdicts])

  Anything unlisted keeps the alphabetical tail. `order:` also takes a
  `(entry -> term)` function, for a rule rather than a list.

  This orders which entries are EMITTED. It is not a happens-before: crontab
  entries are independent jobs, and a host that needs one leg to finish before
  the next begins needs a queue with that property. The graph's edges do encode
  "a source before its consumers", but a cadence is not a dependency and reading
  one as the other would promise something this cannot keep. A host that wants per-firing values wraps the
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
    |> Enum.map(fn job -> {job.every, worker, args: Map.merge(extra, %{"cell" => job.cell})} end)
    |> order(graph, Keyword.get(opts, :order))
  end

  # DETERMINISTIC by default, and the default is alphabetical — which is stable
  # but arbitrary. A host whose scan order carries meaning (observers before the
  # nodes that read them; a sweep whose later legs need the earlier ones' rows)
  # says so with `order:`, and anything unlisted keeps the alphabetical tail.
  #
  # Not derived from the graph, though the edges do encode "source before its
  # consumers": crontab entries are independent jobs, and a host that needs one
  # to finish before the next starts needs a queue with that property, not a
  # list in a nicer sequence. The order here is which entries are EMITTED, and
  # saying more than that would be a promise this cannot keep.
  defp order(entries, _graph, nil), do: Enum.sort_by(entries, &sort_key/1)

  defp order(entries, _graph, declared) when is_list(declared) do
    declared = Enum.map(declared, &to_string/1)
    rank = declared |> Enum.with_index() |> Map.new()
    tail = length(declared)

    Enum.sort_by(entries, fn {_every, _w, args: a} = entry ->
      {Map.get(rank, a["cell"], tail), sort_key(entry)}
    end)
  end

  defp order(entries, _graph, fun) when is_function(fun, 1), do: Enum.sort_by(entries, fun)

  defp sort_key({every, _w, args: a}), do: {a["cell"], every}

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
      acc ->
        Map.update(
          acc,
          mod,
          cell.meta[:scan_args] || [],
          &Keyword.merge(&1, cell.meta[:scan_args] || [])
        )
    end
  end

  @doc "The distinct scanner modules a plan's leaves declare via `scan`."
  @spec scanners(graph()) :: [module()]
  def scanners(graph, opts \\ []) do
    # Sorted by CELL, not taken in map order. `poll_all/2` polls these
    # sequentially, so this is the order real work happens in — and an
    # undeclared order there is worse than in `crontab/3`, where the entries
    # are independent jobs anyway.
    graph.cells
    |> Enum.sort_by(fn {id, _cell} -> id end)
    |> Enum.map(fn {_id, cell} -> cell.meta[:scan] end)
    |> Enum.reject(&is_nil/1)
    |> Enum.uniq()
    |> sequence(graph, Keyword.get(opts, :order))
  end

  # A host whose sweep order carries meaning — observers before the nodes that
  # read them — declares it by CELL, the same vocabulary `crontab/3` takes.
  defp sequence(scanners, _graph, nil), do: scanners

  defp sequence(scanners, graph, declared) when is_list(declared) do
    by_cell =
      for {id, cell} <- graph.cells, mod = cell.meta[:scan], not is_nil(mod), into: %{} do
        {id, mod}
      end

    ranked =
      declared
      |> Enum.map(&to_string/1)
      |> Enum.map(&Map.get(by_cell, &1))
      |> Enum.reject(&is_nil/1)

    Enum.uniq(ranked ++ scanners)
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
end

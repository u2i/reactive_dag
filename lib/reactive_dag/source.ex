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
  that changed.

  Three shapes, in ascending order of how much they say:

      {:ok, ["k1", "k2"]}                     the changed keys, bare
      {:ok, %{changed: ["k1"]}}               ...plus `unreachable` / `detail`
      {:ok, %{changed: %{leaf => keys}}}      one poll, several cells

  The bare list is the common case and belongs to the cell being polled. The
  map form is what a scanner reaching several cells, or reporting an outage,
  needs. `{:error, reason}` when it couldn't run at all (no credential,
  API down) — contained, not raised, so one bad source doesn't abort a refresh.
  `arg` is source-specific (a since-timestamp, a manifest path, opts).

  A multi-upstream source that could observe SOME of its inputs reports the
  others under the optional `unreachable:` key (`{upstream_label, reason}`
  pairs) — the honest-gap discipline: a scan that couldn't look must never
  render as a scan that found nothing, so write what you observed, retire
  nothing you couldn't see, and surface the outage for the host to display.

  ## What the poll COST

  `detail:` is the scan-side counterpart to a drain step's meta: anything the
  scanner wants to report about the work, which the library carries without
  interpreting.

      {:ok, %{changed: keys, detail: %{tokens_in: 900, llm_calls: 12}}}

  This matters for a crawler that calls a model — classifying each new document,
  say. That spend never appears in a drain log, and not for want of recording:
  scans and drains are separate phases, so a poll has no drain step to attach
  to. `detail_total/2` and `detail_by/2` roll these up across a sweep, and a
  count may be flat or broken down per model exactly as on a step.
  """
  @callback poll(arg :: term()) ::
              {:ok, [String.t()]}
              | {:ok,
                 %{
                   :changed => [String.t()] | %{optional(String.t()) => [String.t()]},
                   optional(:unreachable) => [{String.t(), term()}],
                   optional(:detail) => map()
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
  Report progress from inside a `poll/1` — `[:reactive_dag, :scan, :progress]`.

      Source.progress(fetched, total, cell: "meeting_docs")

  A scanner is opaque to the library: it is handed options and returns a result,
  and everything between is the host's. So a crawl of 700 documents emits ONE
  `:source_stop`, and it fires when the crawl is already over. For anything a
  person is waiting on, that is the wrong end.

  Call this as each unit of work completes — per document fetched, per page
  walked. `done` and `total` are whatever the scanner counts; `total` may be nil
  when it is not yet known (a crawl still discovering pages), and a consumer
  then has a count without a denominator, which is still better than silence.

  ## Emit per unit, not per batch

  One event per document, not per twenty-five. Batching pushes an arbitrary N
  into every scanner, each host picks a different one, and a crawl smaller than N
  reports nothing at all. Coalescing is the CONSUMER's job and it is the one that
  can do it properly — `ReactiveDagDashboard` already flushes drain steps on a
  150ms timer, so 700 events become a handful of renders.

  The cost of an unhandled `:telemetry.execute/3` is a lookup on an ETS table; a
  host with no handler attached pays approximately nothing.

  ## Options

    * `:cell` — which cell is being polled. A sweep polls several sources, so a
      consumer showing per-row progress needs to know whose progress this is.
    * `:label` — what the number counts, for a UI that says "34/721 documents"
      rather than "34/721".
  """
  @spec progress(non_neg_integer(), non_neg_integer() | nil, keyword()) :: :ok
  def progress(done, total \\ nil, opts \\ []) do
    :telemetry.execute(
      [:reactive_dag, :scan, :progress],
      %{done: done, total: total},
      %{cell: opts[:cell], label: opts[:label], source: opts[:source]}
    )
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

  ## Telemetry

  `[:reactive_dag, :scan, :source_stop]` fires as each source finishes, with
  `%{source: module, result: result}` — including the failures, since a source
  that could not run is a thing a host wants recorded.

  Sources are polled one at a time, so this doubles as PROGRESS ACROSS a sweep: a
  sweep over eight sources can run for minutes, and without it the only
  observable moments are the beginning and the end of the whole run.

  Progress WITHIN one source is `progress/3` — a crawl of 700 documents is a
  single `:source_stop`, and `:source_stop` fires when it is already over.
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
        #
        # `resolve_args/1` runs AFTER the merge, so a caller passing a deferred
        # value gets the same treatment as a declared one. Merge order already
        # decides precedence — the caller's `year: 2019` beats a standing
        # `year: &Clock.year/0` because `Keyword.merge` replaced the pair, not
        # because of when resolution happens.
        t0 = System.monotonic_time(:microsecond)
        result = safe_poll(mod, resolve_args(Keyword.merge(Map.get(standing, mod, []), opts)))

        # PER SOURCE, as it happens. A sweep is one job that can run for
        # minutes, so `:start` and `:stop` on the whole run are the only two
        # moments a host sees — and neither says which source is going now, or
        # what the one that just finished did. This is the progress signal, and
        # the per-source record: "machines polled at 14:03, wrote 4 rows,
        # Huntress unreachable" (u2i/reactive_dag#133).
        :telemetry.execute(
          [:reactive_dag, :scan, :source_stop],
          %{duration_us: System.monotonic_time(:microsecond) - t0},
          %{source: mod, result: result}
        )

        {mod, result}
      end)
      |> Enum.split_with(fn {_mod, result} -> match?({:ok, _}, result) end)

    case errors do
      [] ->
        {:ok, Map.new(oks, fn {mod, {:ok, r}} -> {mod, r} end)}

      _ ->
        # `{:error, failures}` and not also the successes: hosts match on this
        # shape, and widening it is a breaking change nobody asked for. What
        # DID land is not lost — every source emitted `:source_stop` with its
        # own result as it finished, failures included.
        {:error, Enum.map(errors, fn {mod, {:error, reason}} -> {mod, reason} end)}
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
          mod -> safe_poll(mod, resolve_args(Keyword.merge(cell.meta[:scan_args] || [], opts)))
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
      {:ok, raw} ->
        # ONE normalisation, at the boundary. A bare list is a documented shape
        # and three separate `Map.get(result, …)` calls downstream each raised
        # on it — the reported crash was only the first of them, so fixing that
        # site alone would have moved the failure rather than removed it.
        result = normalise(raw)
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

  # Three shapes a poll can report its changed keys in, and the flat ones matter:
  #
  #   {:ok, ["k1"]}                      — the keys, bare
  #   {:ok, %{changed: ["k1"]}}          — the keys, plus `unreachable`/`detail`
  #   {:ok, %{changed: %{leaf => keys}}} — one poll, several leaves
  #
  # The bare list was documented and unreachable: the old code did
  # `Map.get(result, :changed, [])` FIRST, which raises `BadMapError` on a list
  # before the `is_list` clause it was guarding could match. Every scan of such
  # a source died on attempt 1, and through `ScanWorker` that reads as a button
  # that does nothing (u2i/reactive_dag#138).
  #
  # Worth supporting rather than rejecting: `refresh_source/3` already takes the
  # bare list, so refusing it here would have the two entry points disagree
  # about the same scanner.
  defp by_leaf(%{changed: keys}, cell_id) when is_list(keys), do: %{cell_id => keys}

  defp by_leaf(%{changed: %{} = by_leaf}, _cell_id),
    do: Map.new(by_leaf, fn {leaf, keys} -> {to_string(leaf), keys} end)

  defp by_leaf(%{}, _cell_id), do: %{}

  # A bare list IS the changed keys; anything else is a scanner returning a
  # shape the contract does not name, and saying which three are accepted beats
  # a `BadMapError` from three frames down.
  defp normalise(keys) when is_list(keys), do: %{changed: keys}
  defp normalise(%{} = result), do: result

  defp normalise(other) do
    raise ArgumentError,
          "reactive_dag: a scanner reported #{inspect(other)}. Return the changed keys as " <>
            "a list, `%{changed: keys}`, or `%{changed: %{leaf_id => keys}}` when one poll " <>
            "feeds several cells."
  end

  # A flat list belongs to the cell that was polled; a map names its own leaves.
  defp mark(graph, cell_id, result, reason, key_rule) do
    by_leaf = by_leaf(result, cell_id)

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

  ## One sweep, not N jobs

  The default emits ONE entry per distinct cadence — a sweep. The job polls
  every source of that cadence **sequentially, in graph order**, then drains
  once.

  That is the shape worth defaulting to, because N independent cron entries
  cannot order themselves: they fire concurrently whatever order the list is in,
  so a source that needs another to have run first has no way to say so. Inside
  one job it does — `depends_on` on a source is a sequencing edge, and
  `scanners/2` sorts by depth.

      # a source that must be polled after :observations
      reactive do
        id :verdicts
        leaf? true
        poll MyApp.Sources.Verdicts, every: "0 * * * *"
        depends_on [:observations]
      end

  `per_cell: true` opts back into one entry per source, for a host that wants
  independent jobs — separate queues, separate retry policies — and accepts that
  they are unordered. `order:` then applies to the emitted list, which is a
  reading convenience rather than a happens-before. A host that wants per-firing values wraps the
  worker rather than the crontab: mint the id in `perform/1`, then call
  `refresh/3` with `reason: "scan:\#{run_id}"` — `:reason` is a free string and
  rides through to the frontier rows, so the trace says which run dirtied a cell.
  """
  @spec crontab(graph(), module(), keyword()) :: [{String.t(), module(), keyword()}]
  def crontab(graph, worker \\ ReactiveDag.ScanWorker, opts \\ []) do
    extra = Keyword.get(opts, :args, %{})

    case Keyword.get(opts, :per_cell, false) do
      false -> sweep_entries(graph, worker, extra)
      true -> per_cell_entries(graph, worker, extra, Keyword.get(opts, :order))
    end
  end

  # ONE entry per distinct cadence, each a sweep. Sources run sequentially
  # inside the job, in graph order, followed by one drain.
  defp sweep_entries(graph, worker, extra) do
    graph
    |> scan_jobs()
    |> Enum.reject(&is_nil(&1.every))
    |> Enum.map(& &1.every)
    |> Enum.uniq()
    |> Enum.sort()
    |> Enum.map(fn every -> {every, worker, args: Map.merge(extra, %{"sweep" => true})} end)
  end

  defp per_cell_entries(graph, worker, extra, ordering) do
    graph
    |> scan_jobs()
    |> Enum.reject(&is_nil(&1.every))
    |> Enum.map(fn job -> {job.every, worker, args: Map.merge(extra, %{"cell" => job.cell})} end)
    |> order(graph, ordering)
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
    # BY DEPTH, then by cell. `poll_all/2` polls these one at a time, so this is
    # the order real work happens in, and the graph already knows it: a source
    # that must run after another says so with `depends_on`, which is the
    # vocabulary every other edge uses.
    #
    # Two independent sources are both depth 0 and sort alphabetically — stable,
    # and as meaningful as their relationship actually is. `order:` overrides
    # for the case the graph cannot know.
    graph.cells
    |> Enum.filter(fn {_id, cell} -> cell.meta[:scan] end)
    |> Enum.sort_by(fn {id, _cell} -> {Map.get(graph.depths, id, 0), id} end)
    |> Enum.map(fn {_id, cell} -> cell.meta[:scan] end)
    |> Enum.uniq()
    |> sequence(graph, Keyword.get(opts, :order))
  end

  # An explicit override, for an order the graph cannot derive — two sources
  # with no edge between them whose sequence still matters to the host.
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

  @doc """
  Resolve deferred values in a standing `args:` list by calling them.

  A zero-arity function as an arg VALUE is evaluated at poll time rather than
  read as data:

      poll MyApp.Crawler, args: [recent: true, year: &MyApp.Clock.year/0]

  This exists because `args:` is DSL data, frozen when the module compiles. A
  bound that depends on the clock — "the current year and the one before it" —
  cannot be written as a literal there: it would be correct on the day of the
  build and progressively wrong after, silently, until the next deploy.

  Only VALUES are resolved, and only when they are functions of arity 0. A list
  with no functions in it comes back unchanged, so this is free for the ordinary
  case, and a keyword list remains a keyword list — the type every call site and
  both introspection paths already expect.

  Applied to the MERGED opts, so a caller passing `year: &other_clock/0` is
  resolved exactly like a declared default. Precedence is `Keyword.merge`'s
  alone: an explicit `poll_all(plan, year: 2019)` replaced the standing pair
  before this ran, so there is no deferred value left to overwrite it.

  ## Where this does NOT happen

  `controls/1` and `scan_jobs/1` report `args:` verbatim, deferred values and
  all. Describing the graph must not run the host's code: a dashboard rendering
  a scan control would be calling a clock (or worse, whatever else a host
  deferred) on every page render, and a function value there is a truthful
  answer to "what did this leaf declare".

  So a host rendering an arg value should expect a function and show the fact,
  not the result. `poll_all/2` and `poll_cell/3` resolve; nothing else does.
  """
  @doc """
  Sum one `detail:` key across a sweep's results — the scan-side counterpart to
  `ReactiveDag.Drain.Report.total/2`.

      {:ok, results} = Source.poll_all(plan)
      Source.detail_total(results, :tokens_in)
      #=> 41_200

  ## Why a scan needs its own roll-up

  A drain returns a `%Report{}` and the library totals across its steps. A poll
  returns one result per source and there is no report, because a scan is not a
  cascade — sources are independent and there is no propagation to trace.

  That left a real gap. A crawler that classifies each new document with a model
  spends on every poll, and NONE of it reached the drain log: scans and drains
  are separate phases by design, so a scan's spend has no step to attach to. It
  was invisible not because nobody recorded it but because nothing aggregated
  it.

  ## What a scanner reports

  Whatever it likes, under `detail:` — the same "the library never interprets
  it" rule the drain's step meta follows:

      {:ok, %{changed: keys, detail: %{tokens_in: 900, llm_calls: 12}}}

  A count may be flat or broken down per bucket, exactly as on a drain step:

      detail: %{tokens_in: %{"claude-haiku-4-5" => 900}}

  Both total here; `detail_by/2` returns the breakdown. A source reporting no
  `detail:`, or one lacking the key, contributes nothing rather than raising —
  a sweep mixing LLM and plain crawlers still totals.

  Accepts what `poll_all/2` returns (`%{module => result}`), a list of results,
  or a single result, so a host can total one `poll_cell/3` the same way.
  """
  @spec detail_total(map() | [map()] | term(), atom()) :: number()
  def detail_total(results, key) do
    results
    |> detail_values(key)
    |> Enum.map(fn
      n when is_number(n) -> n
      m when is_map(m) -> m |> Map.values() |> Enum.filter(&is_number/1) |> Enum.sum()
      _ -> 0
    end)
    |> Enum.sum()
  end

  @doc """
  One `detail:` key across a sweep, summed **per bucket** — the breakdown behind
  `detail_total/2`, mirroring `ReactiveDag.Drain.Report.by/2`.

      Source.detail_by(results, :tokens_in)
      #=> %{"claude-haiku-4-5" => 900, "openai/gpt-5.6-luna" => 300}

  A source reporting the key as a bare number lands under `:unattributed`
  rather than being dropped, so the parts always sum to `detail_total/2`. A gap
  in attribution is worth seeing; a breakdown that silently disagrees with its
  own total is not.
  """
  @spec detail_by(map() | [map()] | term(), atom()) :: %{optional(String.t() | atom()) => number()}
  def detail_by(results, key) do
    results
    |> detail_values(key)
    |> Enum.reduce(%{}, fn
      n, acc when is_number(n) ->
        Map.update(acc, :unattributed, n, &(&1 + n))

      m, acc when is_map(m) ->
        for {bucket, n} <- m, is_number(n), reduce: acc do
          inner -> Map.update(inner, bucket, n, &(&1 + n))
        end

      _, acc ->
        acc
    end)
  end

  # The three shapes a host can hand these, flattened to "the values of `key`
  # in every result's detail". `poll_all/2` returns a map keyed by module; a
  # caller totalling one `poll_cell/3` has a bare result; a caller that already
  # collected some has a list.
  defp detail_values(results, key) do
    results
    |> case do
      %{} = map -> if scan_result?(map), do: [map], else: Map.values(map)
      list when is_list(list) -> list
      other -> [other]
    end
    |> Enum.map(fn
      {:ok, result} -> result
      result -> result
    end)
    |> Enum.map(fn
      %{} = result -> result |> Map.get(:detail) |> then(&if(is_map(&1), do: Map.get(&1, key)))
      _ -> nil
    end)
  end

  # A single poll result vs a `%{module => result}` map. `:changed` is the one
  # key every result shape carries — `poll_all/2`'s map is keyed by MODULE, and
  # a module is never `:changed`.
  defp scan_result?(map), do: Map.has_key?(map, :changed) or Map.has_key?(map, :detail)

  @spec resolve_args(keyword()) :: keyword()
  def resolve_args(args) when is_list(args) do
    # `is_function(v, 0)` and not `is_function(v)`: a 1-arity value is not a
    # deferred value with a missing argument, it is a value this does not know
    # how to resolve, and calling it would raise BadArity from inside a poll.
    # Passing it through means the scanner receives what the leaf declared and
    # can say so itself.
    Enum.map(args, fn
      {k, v} when is_function(v, 0) -> {k, v.()}
      pair -> pair
    end)
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

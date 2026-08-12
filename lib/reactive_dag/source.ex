defmodule ReactiveDag.Source do
  @moduledoc """
  A **scanner** — the fourth seam, alongside `ReactiveDag.RecomputeStrategy`,
  `ReactiveDag.KeyRule`, and `ReactiveDag.CoordinationWriter`.

  A source reads external state (a fleet API, a cloud estate, a repo, an LLM) and
  writes a **leaf cell** in a *poll* phase that is deliberately OUTSIDE the drain:

    1. **poll** — run each source's `poll/1`: fetch → write its leaf tuples →
       return the leaf keys that CHANGED (so the caller can mark parents dirty).
       Sources are independent; a failure is contained to its own leaf.
    2. **drain** — the engine recomputes everything downstream from the dirty
       frontier (`ReactiveDag.Drain`). No source runs here.

  This split is a design invariant, not an accident: the drain is pure set/graph
  computation over tuples already present — deterministic, re-runnable, and it
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
          # fetch the fleet, write the "machines" leaf's tuples, return changed keys
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
    {oks, errors} =
      graph
      |> scanners()
      |> Enum.map(fn mod -> {mod, safe_poll(mod, opts)} end)
      |> Enum.split_with(fn {_mod, result} -> match?({:ok, _}, result) end)

    case errors do
      [] -> {:ok, Map.new(oks, fn {mod, {:ok, r}} -> {mod, r} end)}
      _ -> {:error, Enum.map(errors, fn {mod, {:error, reason}} -> {mod, reason} end)}
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

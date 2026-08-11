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

    * **1:1 (the common case) — inline on the leaf.** `observed :machines, scan:
      MyApp.Sources.FleetScan` co-locates the leaf and its scanner in one
      declaration (they travel together). `ReactiveDag.Source.drivers/2` reads
      these off the graph.
    * **fan-out (rare) — on the driver.** A scanner that writes cells no single
      `observed` owns (e.g. many guarantee sub-cells) omits `scan:` and names its
      cells via its own `leaf_cells/1`; the host passes it as an `extra` driver.

  Either way `verify!/2` confirms every named leaf is a real cell in the built
  plan. `leaf_cells/1` is the general contract both paths satisfy (an inline
  `scan:` driver's leaf is just the `observed` it's attached to).

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
  that changed. `{:error, reason}` when it couldn't run (no credential, API down)
  — contained, not raised, so one bad source doesn't abort a refresh. `arg` is
  source-specific (a since-timestamp, a manifest path, opts).
  """
  @callback poll(arg :: term()) :: {:ok, %{changed: [String.t()]}} | {:error, term()}

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
  The scanner drivers feeding a lowered `graph`: the inline ones declared on
  `observed :x, scan: Driver` (read from each leaf cell's `meta.scan`), unioned
  with any `extra` fan-out drivers a host passes (drivers that name their cells via
  `leaf_cells/1` because no single leaf owns them). This is the full scanner set —
  feed it to `verify!/2`, poll it in phase 1.
  """
  @spec drivers(graph(), [module()]) :: [module()]
  def drivers(graph, extra \\ []) do
    inline =
      graph.cells
      |> Map.values()
      |> Enum.flat_map(fn cell ->
        case cell.meta[:scan] do
          nil -> []
          driver -> [driver]
        end
      end)

    (inline ++ extra) |> Enum.uniq()
  end
end

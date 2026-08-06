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
  (that's every scanner) stays in phase 1. A scanner is therefore a first-class
  *node in the graph spec* (so its leaf↔scanner binding is typed and validated),
  but its EXECUTION is out-of-band. See `ReactiveDag.Dsl.Spine` for the authoring
  side (`source :id, driver: Mod` + `observed …, fed_by: :id`).

  ## The contract

  Two apps (a data pipeline and a compliance model) independently grew the same
  three-callback shape — `id` / `leaf_cells` / `poll → changed-keys` — which is
  why it lives here rather than in either app.

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
  """
  @callback leaf_cells(graph()) :: [String.t()]

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

  @optional_callbacks origin: 0

  @doc """
  Verify every source's declared leaves resolve to real cells in `graph` — the
  runtime half of the leaf↔scanner binding check (the compile-time half, that
  `observed.fed_by` names a declared source, is in the spine transformer). Needs
  the lowered graph, so it can't be a compile-time check when a host expands
  generators from live data.

  Returns `:ok`, or raises `ArgumentError` naming every `{source, dangling_leaf}`.
  """
  @spec verify!([module()], graph()) :: :ok
  def verify!(sources, graph) do
    cell_ids = graph.cells |> Map.keys() |> MapSet.new()

    dangling =
      for mod <- sources,
          leaf <- mod.leaf_cells(graph),
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
end

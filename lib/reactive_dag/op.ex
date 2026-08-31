defmodule ReactiveDag.Op do
  @moduledoc """
  The behaviour a node's `compute` module implements — the recompute for ONE op,
  the per-cell unit of work.

  A `ReactiveDag.Node` records its compute module in `cell.meta.compute`, and
  `ReactiveDag.Node.Recompute` dispatches to it — after every combinator, so a
  node declaring both gets its combinator (which the verifier refuses). The op
  reads
  its inputs (the input cells' rows, and/or the host's own resources) and writes
  its output, returning the keys that ACTUALLY changed — only those propagate,
  keeping the cascade O(real changes).

  This is deliberately thin: the substrate says WHEN a cell recomputes and in
  what order; the op says HOW, in whatever storage/effect model the host uses
  (per-key Elixir calling an LLM, or a set-based SQL write). The library never
  inspects what an op does — only that it returns `{:ok, changed_keys}`, or
  `{:ok, changed_keys, meta}` when it also reports what the work cost.

  A LEAF has no op: an external source writes its rows and marks its parents
  dirty, so a leaf never reaches recompute.

  ## There is no coordination write

  This module used to carry `put/3`, `delete/2` and `tombstone/2`, routing to a
  configured `CoordinationWriter` that wrote a row per `(cell_id, key)` into a
  side table. Every node now writes its results into its own resource, so that
  table had nothing left to record that the resource does not already say — and
  an op's return value is the changed set, which is what the drain actually
  propagates. An op writes its rows and returns its keys; nothing else is asked
  of it.
  """

  alias ReactiveDag.Cell

  @type key :: String.t()

  @typedoc """
  What an op reports about the work, carried onto the drain's `%Report{}` step
  and never interpreted by the library.

  Tokens, cache hits, retries, rows scanned are all just keys.
  `ReactiveDag.Report.total/2` sums one across the run; `by/2` breaks it
  down when the value is a `%{bucket => number}` map rather than a bare number,
  which is how a graph running several models reports per-model cost.
  """
  @type meta :: map()

  @doc """
  Recompute `keys` of `cell` (or `["*"]` for a whole-cell recompute). Return
  `{:ok, changed_keys}` — the subset whose output changed. Returning all keys is
  always correct, just less efficient.

  `{:ok, changed_keys, meta}` additionally reports what the work cost. The
  library carries `meta` onto the step without reading it.

  ## Pick ONE arity and keep it

  Both are accepted, but an op that returns the 2-tuple *sometimes* and the
  3-tuple *other times* — say, only when it actually called a model — is a trap
  for its own direct callers. `{:ok, changed} = MyOp.recompute(...)` then works
  in every test and raises the first time a cache hit adds meta, which in a
  worker wrapped in `rescue` surfaces as a write failure over rows that were
  written correctly.

  The drain does not care which arity an op picks. Its callers do, so pick one.

  ## Failure

  Two channels, and they mean different things.

  **Raise** for "something is wrong with the graph" — a missing module, a
  malformed declaration, a bug. The drain has already claimed these keys, so a
  SWALLOWED failure would mark them clean over work that did not happen.

  **Return `{:error, reason}`** for a contained failure: this cell could not
  recompute, the rest of the cascade should carry on, and the next drain retries.
  The savepoint rolls the claim back, so the keys stay dirty. This must be a
  RETURNED value rather than an exception — an exception inside a nested
  transaction aborts the outer one, so only this shape can be isolated.

  ## The tenant

  A host running the same graph for several tenants implements `recompute/3`
  instead, and receives the plan's tenant in `opts[:tenant]`:

      @impl true
      def recompute(cell, keys, opts) do
        # ... write rows with `tenant: opts[:tenant]`, or pass `opts` to
        # `ReactiveDag.Node.Payload.upsert_row/5`, which handles it.
      end

  Both arities are optional and the library calls whichever the module exports,
  preferring `/3`. An op writing its own rows CANNOT get the tenant any other
  way: the library has no changeset of its own to set it on, which is the whole
  reason this arity exists. An op implementing only `/2` keeps working untouched
  and is correct for any host with one graph.
  """
  @callback recompute(cell :: Cell.t(), keys :: [key()]) ::
              {:ok, [key()]} | {:ok, [key()], meta()} | {:error, term()}

  @callback recompute(cell :: Cell.t(), keys :: [key()], opts :: keyword()) ::
              {:ok, [key()]} | {:ok, [key()], meta()} | {:error, term()}

  @optional_callbacks recompute: 2, recompute: 3

  @doc """
  Report progress from inside a recompute — `[:reactive_dag, :cascade, :progress]`.

      Op.progress(done, total, cell: cell.id, label: "meetings")

  The drain-side counterpart to `ReactiveDag.Source.progress/3`, and it exists for
  the same reason. An op is opaque to the library: it is handed keys and returns
  changed ones, and everything between is the host's. So a cell extracting 34
  meetings through an LLM emits ONE `:cascade, :step`, and it fires when the work is
  already over.

  `:cell_start` says a cell BEGAN, which is enough to name the slow cell. This is
  for saying how far through it is — the difference between "recomputing
  meeting_events" for four minutes and "meeting_events · 12/34 meetings".

  ## Emit per unit, not per batch

  One event per meeting, not per ten. Batching pushes an arbitrary N into every op,
  each host picks a different one, and a cell with fewer than N keys reports nothing
  at all. Coalescing is the CONSUMER's job — `ReactiveDagDashboard` already throttles
  progress to one render per 150ms.

  `total` may be nil when it is not yet known; the label then stands alone, which is
  how an op names a PHASE rather than a count (see `Source.progress/3`).

  The cost with no handler attached is an ETS lookup, so an op may call it freely.

  ## Options

    * `:cell` — which cell is recomputing. A drain runs many, so a consumer showing
      per-row progress needs to know whose progress this is.
    * `:label` — what the number counts, for a UI that says "12/34 meetings" rather
      than "12/34".
  """
  @spec progress(non_neg_integer() | nil, non_neg_integer() | nil, keyword()) :: :ok
  def progress(done, total \\ nil, opts \\ []) do
    :telemetry.execute(
      [:reactive_dag, :cascade, :progress],
      %{done: done, total: total},
      %{cell: opts[:cell], label: opts[:label]}
    )
  end
end

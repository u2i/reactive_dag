defmodule ReactiveDag.Op do
  @moduledoc """
  The behaviour a node's `compute` module implements — the recompute for ONE op,
  the per-cell unit of work.

  A `ReactiveDag.Node` records its compute module in `cell.meta.compute`; the
  generic `ReactiveDag.Node.Recompute` strategy dispatches to it. The op reads
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
  `ReactiveDag.Drain.Report.total/2` sums one across the run; `by/2` breaks it
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

  Raise. Do not return `{:error, reason}`: the drain has already claimed these
  keys, and a swallowed failure marks them clean over work that did not happen.
  """
  @callback recompute(cell :: Cell.t(), keys :: [key()]) ::
              {:ok, [key()]} | {:ok, [key()], meta()}
end

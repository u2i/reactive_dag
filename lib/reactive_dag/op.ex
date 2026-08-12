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
  inspects what an op does — only that it returns `{:ok, changed_keys}`.

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

  @doc """
  Recompute `keys` of `cell` (or `["*"]` for a whole-cell recompute). Return
  `{:ok, changed_keys}` — the subset whose output changed. Returning all keys is
  always correct, just less efficient.
  """
  @callback recompute(cell :: Cell.t(), keys :: [key()]) :: {:ok, [key()]}
end

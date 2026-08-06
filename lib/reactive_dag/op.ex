defmodule ReactiveDag.Op do
  @moduledoc """
  The behaviour a node's `compute` module implements — the recompute for ONE op,
  the per-cell unit of work.

  A `ReactiveDag.Node` records its compute module in `cell.meta.compute`; the
  generic `ReactiveDag.Node.Recompute` strategy dispatches to it. The op reads
  its inputs (from the coordination tuples of its input cells, and/or the host's
  typed payload resources) and writes its output (its payload rows + its own
  coordination tuples), returning the keys that ACTUALLY changed — only those
  propagate, keeping the cascade O(real changes).

  This is deliberately thin: the substrate says WHEN a cell recomputes and in
  what order; the op says HOW, in whatever storage/effect model the host uses
  (per-key Elixir calling an LLM, or a set-based SQL write). The library never
  inspects what an op does — only that it returns `{:ok, changed_keys}`.

  A LEAF has no op: an external source writes its tuples and marks its parents
  dirty, so a leaf never reaches recompute.
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

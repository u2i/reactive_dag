defmodule ReactiveDag.RecomputeStrategy do
  @moduledoc """
  The seam where the host app's OP ALGEBRA + RECOMPUTE MODEL plug in.

  The substrate drives *when* and *in what order* cells recompute; it never
  decides *how*. When the drain claims a cell's dirty keys, it calls the app's
  strategy, which recomputes the cell however it likes — per-key Elixir (call an
  LLM, parse a PDF), or one set-based SQL join over the dirty keys — and returns
  the keys that ACTUALLY changed. Only those propagate, keeping the cascade
  O(real changes).

  This is the single abstraction that spans the two known hosts:

    * cascade — `recompute/2` dispatches to the cell's `Cascade.Engine.Op`
      module (`cell.meta.compute`) and runs Elixir per key.
    * compliance portal — `recompute/2` dispatches by `cell.op` to a SQL
      template (`Recompute.reconcile/relation/…`) that writes `model_tuple`.

  A leaf never reaches here — its keys are written by an external source, which
  marks the leaf's parents dirty directly.
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

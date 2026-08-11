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

  A strategy with something worth REPORTING about the work may return
  `{:ok, changed_keys, meta}` instead: an arbitrary map that rides on the
  drain's `%Report{}` step. The library never interprets it — token and cost
  counts for an LLM node, cache hits, retries, rows scanned are all just keys —
  it only carries it, so `ReactiveDag.Insights` and a dashboard can show what
  the work actually cost.
  """
  @callback recompute(cell :: Cell.t(), keys :: [key()]) ::
              {:ok, [key()]} | {:ok, [key()], map()}
end

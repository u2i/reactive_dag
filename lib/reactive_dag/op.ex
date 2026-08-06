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

  # ── the coordination-write API ops call ────────────────────────────────────
  # An op writes its PAYLOAD however it likes (its typed resource — host domain),
  # then records each key's coordination verdict through these, which route to
  # the host-configured `ReactiveDag.CoordinationWriter`. This replaces ops
  # reaching into a host `Frontier.put_tuple` directly: the cell carries its id,
  # so there's no per-op `@cell` module attribute, and the extension-column write
  # stays host policy behind the writer seam.

  alias ReactiveDag.CoordinationWriter, as: W

  @doc "Mark `key` of `cell` present (opts carry host fields: source_ref, strength, …)."
  @spec put(Cell.t(), key(), keyword()) :: :ok
  def put(%Cell{} = cell, key, opts \\ []), do: W.writer().put(cell, key, opts)

  @doc "Tombstone `keys` of `cell` (retain-if-vanish, if the writer supports it; else delete)."
  @spec tombstone(Cell.t(), [key()]) :: :ok
  def tombstone(%Cell{} = cell, keys) do
    w = W.writer()
    if function_exported?(w, :tombstone, 2), do: w.tombstone(cell, keys), else: w.delete(cell, keys)
  end

  @doc "Hard-delete `keys` of `cell`."
  @spec delete(Cell.t(), [key()]) :: :ok
  def delete(%Cell{} = cell, keys), do: W.writer().delete(cell, keys)
end

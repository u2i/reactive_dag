defmodule ReactiveDag.Tuple.Writer do
  @moduledoc """
  The DEFAULT `ReactiveDag.CoordinationWriter` — spine-only, over the configured
  tuple table via `ReactiveDag.Tuple`. Suitable for a host with no extension
  columns on its coordination tuple. A host with extensions (cascade's
  `source_ref`/`tombstoned_at`, the portal's `strength`) configures its own
  writer that does the spine + extension write in one atomic upsert.

  `tombstone/2` here has no retain policy, so it falls back to `delete/2` (a
  spine with no `tombstoned_at` column can't retain).
  """
  @behaviour ReactiveDag.CoordinationWriter

  @impl true
  def put(cell_id, key, opts) do
    ReactiveDag.Tuple.put(cell_id, key, Keyword.take(opts, [:status, :stale_after, :observed_at]))
  end

  @impl true
  def delete(cell_id, keys), do: ReactiveDag.Tuple.delete(cell_id, keys)

  @impl true
  def tombstone(cell_id, keys), do: delete(cell_id, keys)
end

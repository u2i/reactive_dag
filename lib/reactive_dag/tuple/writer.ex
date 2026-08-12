defmodule ReactiveDag.Tuple.Writer do
  @moduledoc """
  The DEFAULT `ReactiveDag.CoordinationWriter` — spine-only, over the configured
  tuple table via `ReactiveDag.Tuple`. Suitable for a host with no extension
  columns on its coordination tuple. A host with extensions (cascade's
  `source_ref`/`tombstoned_at`) configures its own writer that does the spine +
  extension write in one atomic upsert.

  `put/3` returns `:ok`, not a changed-signal. It used to return one, comparing
  the tuple's `status` column against the incoming status — but the spine no
  longer stores a status, and nothing consumed the answer: every caller decides
  "did this change?" by comparing the node's own rows before it writes, which is
  a comparison against the actual result rather than a one-word projection of it.
  A writer MAY still return a boolean (the contract allows it, and a host writer
  guarding its upsert with `IS DISTINCT FROM` can report a real flip); `:ok` is
  read as "assume changed", which is correct and simply less scoped.

  `tombstone/2` here has no retain policy, so it falls back to `delete/2` (a
  spine with no `tombstoned_at` column can't retain).
  """
  @behaviour ReactiveDag.CoordinationWriter

  @impl true
  def put(cell_id, key, _opts), do: ReactiveDag.Tuple.put(cell_id, key)

  @impl true
  def delete(cell_id, keys), do: ReactiveDag.Tuple.delete(cell_id, keys)

  @impl true
  def tombstone(cell_id, keys), do: delete(cell_id, keys)
end

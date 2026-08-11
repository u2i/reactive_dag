defmodule ReactiveDag.Node.KeyRule do
  @moduledoc """
  A GENERIC `ReactiveDag.KeyRule` for graphs declared with `ReactiveDag.Node`.
  `Node` records each node's `key_rule` (`:identity | :all`) in `cell.meta`;
  this reads it:

    * `:all`      — any input change escalates to a whole-cell recompute (the
      drain turns this into `"*"`). For aggregate / cross-range (reduce) cells.
    * `:identity` — a changed input key maps to the same output key (pass through).
      For key-local (map) cells. The default.

  This is the uniform per-cell rule (cascade's shape). A host needing per-op,
  per-input-leg rules (the portal's product/relation escalation) still writes its
  own KeyRule; this generic one covers the `:identity | :all` case.
  """
  @behaviour ReactiveDag.KeyRule

  alias ReactiveDag.Cell

  @impl true
  def rule(%Cell{meta: %{key_rule: :all}}, _child, _changed), do: :all

  # `{:bucket, kind}` — the calendar ladder: each changed child key claims its
  # bucket by PURE string work (the key's leading segment parses as a date or a
  # finer bucket label; see ReactiveDag.Calendar.bucket_of_key/2). Deletion-safe
  # (a vanished key still names the bucket it left); a key that doesn't parse
  # degrades the whole propagation to :all — correctness over precision.
  def rule(%Cell{meta: %{key_rule: {:bucket, kind}}}, _child, changed) do
    labels = Enum.map(changed, &ReactiveDag.Calendar.bucket_of_key(kind, &1))

    if :error in labels, do: :all, else: {:keys, Enum.uniq(labels)}
  end

  def rule(_parent, _child, changed), do: {:keys, changed}
end

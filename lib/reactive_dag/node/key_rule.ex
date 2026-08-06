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
  def rule(_parent, _child, changed), do: {:keys, changed}
end

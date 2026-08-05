defmodule ReactiveDag.KeyRule do
  @moduledoc """
  How a change to a child propagates to a parent — the op-aware propagation
  seam. When `changed` keys of `child` feed `parent`, the rule decides which of
  the parent's keys become dirty:

    * `{:keys, mapped}` — a bounded, per-key fan-out (identity or a remap).
    * `:all` — the change fans out to the WHOLE parent cell (recompute all its
      keys). Needed when a single input change can affect arbitrary output keys
      (a fold's aggregate, a vanished key a relation must re-judge).

  This one callback generalizes both hosts' propagation:

    * cascade — a uniform `key_rule: :identity | :all` field per cell:
      `:identity` → `{:keys, changed}`, `:all` → `:all`.
    * compliance portal — a per-op, per-WHICH-input rule: e.g. `product` and
      `relation` return `:all` when their *fn* leg changed but pass keys through
      when their *members* leg changed. `rule/3` sees `parent`, the specific
      `child` (which input), and `changed`, so it can express exactly that.

  Default implementation (`identity/3`) covers the common identity-mapping case;
  a host provides its own module only for a richer algebra.
  """

  alias ReactiveDag.Cell

  @type key :: String.t()
  @type result :: :all | {:keys, [key()]}

  @doc """
  Given a `parent` cell, the specific `child` input id whose keys changed, and
  the `changed` keys, return how they propagate to the parent.
  """
  @callback rule(parent :: Cell.t(), child :: Cell.id(), changed :: [key()]) :: result()

  @doc "The trivial identity rule — pass the changed keys straight through."
  @spec identity(Cell.t(), Cell.id(), [key()]) :: result()
  def identity(_parent, _child, changed), do: {:keys, changed}
end

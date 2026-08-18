defmodule ReactiveDag.KeyRule do
  @moduledoc """
  The vocabulary of propagation: when `changed` keys of a child feed a parent,
  which of the parent's keys become dirty?

    * `{:keys, mapped}` — a bounded, per-key fan-out (identity, or a remap).
    * `:all` — the change fans out to the WHOLE parent cell. Needed when one
      input change can affect arbitrary output keys: a fold's aggregate, or a
      vanished key a relation must re-judge.

  Getting that distinction right is what keeps a cascade O(real changes).
  Escalating to `:all` is always CORRECT and sometimes wasteful; passing keys
  through when the grain actually changed is neither — it strands rows that
  needed repricing, which reads as data that quietly went stale.

  `ReactiveDag.Node.KeyRule` decides it from what the node declared
  (`key_rule: :identity | :all`, or a `recompute_by` unit), and the drain always
  uses that one. This module holds the shared types plus the trivial identity
  rule, which is the answer for a same-grain map and the fallback for a
  hand-assembled cell that declares nothing.
  """

  alias ReactiveDag.Cell

  @type key :: String.t()
  @type result :: :all | {:keys, [key()]}

  @doc "The trivial identity rule — pass the changed keys straight through."
  @spec identity(Cell.t(), Cell.id(), [key()]) :: result()
  def identity(_parent, _child, changed), do: {:keys, changed}
end

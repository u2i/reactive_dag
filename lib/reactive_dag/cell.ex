defmodule ReactiveDag.Cell do
  @moduledoc """
  One node in the DAG — the domain-neutral IR both host apps compile down to.

  A cell is either a **leaf** (`inputs: []`, `leaf?: true`; fed by an external
  source) or a **derived** node computed from `inputs`.

  `op` is an OPTIONAL free-atom label, NOT a fixed enum and NOT required. The
  substrate never interprets it; how a cell recomputes is decided by the app's
  `RecomputeStrategy`. Two patterns exist:

    * The `ReactiveDag.Node` surface dispatches on `meta` shape (`reduce`/`join`/
      `aggregate`/`compute`), so `op` there is pure documentation — an author may
      leave it `nil`.
    * A set-based `RecomputeStrategy` MAY dispatch on `op` (see `ReactiveDag.SetOp`,
      which maps `op` → a host SQL template). `op` is load-bearing only for such a
      strategy; it exists as a field so that seam has somewhere to read.

  `meta` is an open map for app-specific data the compiler wants to carry
  (compute module, SQL template name, key_rule tag, …). The substrate passes it
  through untouched.
  """

  @enforce_keys [:id]
  defstruct id: nil, op: nil, inputs: [], leaf?: false, meta: %{}

  @type id :: String.t()
  @type t :: %__MODULE__{
          id: id(),
          op: atom() | nil,
          inputs: [id()],
          leaf?: boolean(),
          meta: map()
        }

  # Access with a FLATTENED view: `cell[:op]` reads the core field; any other key
  # falls through to `meta[key]`. Lets a host that folds domain fields into meta
  # keep reading them with plain `cell[:field]` — the core/meta split is
  # invisible to consumers. Read-only (get_and_update/pop raise, as for a plan IR).
  @behaviour Access

  @core [:id, :op, :inputs, :leaf?, :meta]

  @impl Access
  def fetch(%__MODULE__{} = cell, key) when key in @core, do: Map.fetch(Map.from_struct(cell), key)
  def fetch(%__MODULE__{meta: meta}, key), do: Map.fetch(meta, key)

  @impl Access
  def get_and_update(_cell, _key, _fun),
    do: raise("ReactiveDag.Cell is a read-only plan IR; build a new struct instead")

  @impl Access
  def pop(_cell, _key),
    do: raise("ReactiveDag.Cell is a read-only plan IR; build a new struct instead")
end

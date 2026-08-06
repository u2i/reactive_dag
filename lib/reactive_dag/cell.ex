defmodule ReactiveDag.Cell do
  @moduledoc """
  One node in the DAG — the domain-neutral IR both host apps compile down to.

  A cell is either a **leaf** (`inputs: []`, `leaf?: true`; fed by an external
  source) or a **derived** node computed from `inputs` by an `op`. `op` is a free
  atom, NOT a fixed enum — the host app defines its own algebra (cascade:
  map/fold/join/…; the compliance portal: product/relation/reconcile/…). The
  substrate never interprets `op`; it hands the cell to the app's
  `RecomputeStrategy` and `KeyRule`.

  `meta` is an open map for app-specific data the compiler wants to carry
  (compute module, SQL template name, key_rule tag, …). The substrate passes it
  through untouched.
  """

  @enforce_keys [:id, :op]
  defstruct id: nil, op: nil, inputs: [], leaf?: false, meta: %{}

  @type id :: String.t()
  @type t :: %__MODULE__{
          id: id(),
          op: atom(),
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

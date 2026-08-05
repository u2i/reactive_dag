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
  (compute module, SQL template name, key_rule tag, grain, …). The substrate
  passes it through untouched.
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
end

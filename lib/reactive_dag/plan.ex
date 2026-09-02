defmodule ReactiveDag.Plan do
  @moduledoc """
  The compiled DAG plan — pure data the drain executes. Decoupled from any DSL:
  a host app lowers its declarations into a `Cell` list and `Graph.build/1`
  produces this. The drain only ever sees the Plan.

    * `cells`   — %{id => Cell}
    * `parents` — %{child_id => [parent_id]} — forward propagation index
      (inverse of each cell's inputs); a change to a child enqueues its parents.
    * `depths`  — %{id => longest path from a leaf}; a cascade walks cells in
      ascending depth, so a cell never recomputes while an input is still
      pending (topological order, no external scheduler).
    * `tenant`  — which GRAPH this plan is, `"*"` for a host running one.

  ## Tenant

  A host may run the same topology for several independent tenants. Each is its
  own plan with its own cells, and `tenant` is what the drain passes to the
  frontier so its claims, marks and cell selection see only that tenant's work
  (captured by the payload write that produced the change).

  It lives on the PLAN rather than on each cell because it is a property of the
  run, not of the declaration: the same resource lowers to the same cell in every
  tenant's plan, which is what makes "the same graph, N times" true rather than
  approximately true. A cell carries no tenant at all, so nothing about authoring
  changes.
  """

  @enforce_keys [:cells, :parents, :depths]
  defstruct cells: %{}, parents: %{}, depths: %{}, tenant: "*"

  @type t :: %__MODULE__{
          cells: %{ReactiveDag.Cell.id() => ReactiveDag.Cell.t()},
          parents: %{ReactiveDag.Cell.id() => [ReactiveDag.Cell.id()]},
          depths: %{ReactiveDag.Cell.id() => non_neg_integer()},
          tenant: String.t()
        }

  @doc """
  The frontier options for this plan — `[tenant: …]`.

  One place builds them, so a call site cannot forget the tenant and silently
  operate on `"*"`.
  """
  @spec frontier_opts(t()) :: keyword()
  def frontier_opts(%__MODULE__{tenant: tenant}), do: [tenant: tenant]
end

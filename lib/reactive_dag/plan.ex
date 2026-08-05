defmodule ReactiveDag.Plan do
  @moduledoc """
  The compiled DAG plan — pure data the drain executes. Decoupled from any DSL:
  a host app lowers its declarations into a `Cell` list and `Graph.build/1`
  produces this. The drain only ever sees the Plan.

    * `cells`   — %{id => Cell}
    * `parents` — %{child_id => [parent_id]} — forward propagation index
      (inverse of each cell's inputs); a change to a child enqueues its parents.
    * `depths`  — %{id => longest path from a leaf}; the drain processes cells
      in ascending depth, so a cell never recomputes while an input is still
      dirty (topological order, no external scheduler).
  """

  @enforce_keys [:cells, :parents, :depths]
  defstruct cells: %{}, parents: %{}, depths: %{}

  @type t :: %__MODULE__{
          cells: %{ReactiveDag.Cell.id() => ReactiveDag.Cell.t()},
          parents: %{ReactiveDag.Cell.id() => [ReactiveDag.Cell.id()]},
          depths: %{ReactiveDag.Cell.id() => non_neg_integer()}
        }
end

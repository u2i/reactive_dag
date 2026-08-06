defmodule ReactiveDag.Node do
  @moduledoc """
  An **Ash resource extension** that makes a resource a node in a reactive DAG.
  Instead of a central pipeline module declaring every cell, each node declares
  its own op + dependencies *on the resource itself* — the resource IS the node,
  and its rows are the node's payload (a tableless node just adds no attributes
  beyond its key):

      defmodule MyApp.Meeting do
        use Ash.Resource,
          domain: MyApp.Domain,
          data_layer: AshPostgres.DataLayer,
          extensions: [ReactiveDag.Node]

        reactive do
          id :meeting            # the cell id (defaults to the resource's short name)
          op :join
          compute MyApp.Ops.MeetingJoin
          key_rule :identity
          depends_on [:meeting_shell, :agenda_docs, :meeting_events, :discussions]
        end

        attributes do ... end    # the payload columns (omit for a tableless node)
      end

  A `depends_on` id is another node's `id` (by-name, the DAG's fan-out). Nested
  inline sub-ops (`ref`/`compose`) can be added later; the first cut is the flat
  `depends_on` form, which covers the common case.

  The whole graph is assembled by `ReactiveDag.Node.graph/1` over a list of node
  resources — each contributes one cell; the union is handed to
  `ReactiveDag.Graph.build/1`. The reactive substrate never reads the resource's
  ATTRIBUTES (those are host payload); it reads only this `reactive` block.
  """

  defmodule Dep do
    @moduledoc "A single by-name dependency edge (an input cell id)."
    defstruct [:to, :__identifier__, :__spark_metadata__]
  end

  @dep %Spark.Dsl.Entity{
    name: :dep,
    target: Dep,
    args: [:to],
    describe: "One dependency edge — the id of an input node.",
    schema: [to: [type: :atom, required: true, doc: "the input node's id"]]
  }

  @reactive %Spark.Dsl.Section{
    name: :reactive,
    describe: "Declares this resource as a reactive-DAG node: its op + dependencies.",
    entities: [@dep],
    schema: [
      id: [
        type: :atom,
        doc: "the cell id; defaults to the resource module's short name, snake_cased"
      ],
      op: [type: :atom, required: true, doc: "the op kind (free atom; the host interprets it)"],
      compute: [type: :atom, doc: "the recompute module for this node (nil for a leaf)"],
      key_rule: [
        type: {:one_of, [:identity, :all]},
        default: :identity,
        doc: "how a child key maps to this cell's key on propagation"
      ],
      leaf?: [type: :boolean, default: false, doc: "true for a source-fed leaf (no compute)"],
      depends_on: [
        type: {:list, :atom},
        default: [],
        doc: "input node ids (flat form; equivalent to a `dep` per id)"
      ]
    ]
  }

  use Spark.Dsl.Extension, sections: [@reactive]

  # ── introspection + graph assembly ────────────────────────────────────────

  alias Spark.Dsl.Extension, as: Ext

  @doc "The cell id for a node resource (explicit `id`, else the module's snake short-name)."
  @spec cell_id(module()) :: atom()
  def cell_id(resource) do
    Ext.get_opt(resource, [:reactive], :id, nil) || default_id(resource)
  end

  @doc """
  Assemble a `ReactiveDag.Plan` from a list of node resources. Each resource
  contributes one `ReactiveDag.Cell` (its `reactive` block); `depends_on` +
  every `dep` entity become the cell's inputs (by-name). The union is validated
  and depth-ordered by `ReactiveDag.Graph.build/1`.
  """
  @spec graph([module()]) :: ReactiveDag.Plan.t()
  def graph(resources) do
    resources |> Enum.map(&to_cell/1) |> ReactiveDag.Graph.build()
  end

  @doc "The `ReactiveDag.Cell` for one node resource (no graph math)."
  @spec to_cell(module()) :: ReactiveDag.Cell.t()
  def to_cell(resource) do
    id = cell_id(resource)
    dep_entities = Ext.get_entities(resource, [:reactive]) |> Enum.map(& &1.to)
    flat = Ext.get_opt(resource, [:reactive], :depends_on, [])
    inputs = (flat ++ dep_entities) |> Enum.uniq() |> Enum.map(&to_string/1)

    %ReactiveDag.Cell{
      id: to_string(id),
      op: Ext.get_opt(resource, [:reactive], :op, nil),
      inputs: inputs,
      leaf?: Ext.get_opt(resource, [:reactive], :leaf?, false),
      meta: %{
        resource: resource,
        compute: Ext.get_opt(resource, [:reactive], :compute, nil),
        key_rule: Ext.get_opt(resource, [:reactive], :key_rule, :identity)
      }
    }
  end

  defp default_id(resource) do
    resource
    |> Module.split()
    |> List.last()
    |> Macro.underscore()
    |> String.to_atom()
  end
end

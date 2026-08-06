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

  defmodule Ref do
    @moduledoc "A by-name reference leg to another named node (an input edge)."
    defstruct [:to, :__identifier__, :__spark_metadata__]
  end

  defmodule Compose do
    @moduledoc """
    An anonymous nested op-expression leg: composes inline as an intermediate
    cell (its `as` id, or a positional id derived from the parent). Its own legs
    are `ref`/`compose`, so the algebra reads as an expression tree.
    """
    defstruct [
      :op,
      :compute,
      :as,
      :key_rule,
      legs: [],
      __identifier__: nil,
      __spark_metadata__: nil
    ]
  end

  @dep %Spark.Dsl.Entity{
    name: :dep,
    target: Dep,
    args: [:to],
    describe: "One dependency edge — the id of an input node (flat form).",
    schema: [to: [type: :atom, required: true, doc: "the input node's id"]]
  }

  @ref %Spark.Dsl.Entity{
    name: :ref,
    target: Ref,
    args: [:to],
    describe: "A by-name reference leg to another named node.",
    schema: [to: [type: :atom, required: true, doc: "the referenced node's id"]]
  }

  @compose_base %Spark.Dsl.Entity{
    name: :compose,
    target: Compose,
    args: [:op],
    describe: "An anonymous nested op-expression leg; composes inline as an intermediate cell.",
    schema: [
      op: [type: :atom, required: true, doc: "the op kind for this intermediate cell"],
      compute: [type: :atom, doc: "the recompute module for this intermediate cell"],
      as: [type: :atom, doc: "an explicit id for this intermediate cell"],
      key_rule: [type: {:one_of, [:identity, :all]}, default: :identity]
    ]
  }
  # self-nest a fixed depth so a compose can hold compose legs (mirrors cascade).
  @compose Enum.reduce(1..8, @compose_base, fn _i, child ->
             %{@compose_base | entities: [legs: [@ref, child]]}
           end)

  @reactive %Spark.Dsl.Section{
    name: :reactive,
    describe: "Declares this resource as a reactive-DAG node: its op + dependencies.",
    # legs (ref/compose) are the nested form; dep is the flat form. Both allowed.
    entities: [@dep, @ref, @compose],
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
      source: [
        type: :atom,
        doc: "for a leaf: the source binding id its refresh dispatches on (host-defined)"
      ],
      driver: [
        type: :atom,
        doc: "for a leaf: the driver module that polls the outside world (host `Source` behaviour)"
      ],
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
  contributes its root cell PLUS an intermediate cell per nested `compose` leg
  (lowered through `ReactiveDag.Lowering.walk`). The union is validated and
  depth-ordered by `ReactiveDag.Graph.build/1`.
  """
  @spec graph([module()]) :: ReactiveDag.Plan.t()
  def graph(resources) do
    resources |> Enum.flat_map(&cells/1) |> ReactiveDag.Graph.build()
  end

  @doc """
  The `ReactiveDag.Cell`s a node resource lowers to (no graph math): its root
  cell + one per nested `compose`. Legs are lowered by-name via the shared
  `ReactiveDag.Lowering.walk` — a `ref`/`dep` resolves to an existing cell id
  (no new cell), a `compose` recurses into an intermediate cell.
  """
  @spec cells(module()) :: [ReactiveDag.Cell.t()]
  def cells(resource) do
    id = cell_id(resource) |> to_string()
    {^id, cells} = ReactiveDag.Lowering.walk(id, root_node(resource), walk_cbs())
    cells
  end

  @doc "The root cell id a node resource lowers to."
  @spec to_cell(module()) :: ReactiveDag.Cell.t()
  def to_cell(resource) do
    id = cell_id(resource) |> to_string()
    resource |> cells() |> Enum.find(&(&1.id == id))
  end

  # ── lowering: the reactive block → a node the walk callbacks understand ─────
  # The root node and every `compose` leg share one internal shape:
  #   {:op, id, op, compute, key_rule, leaf?, resource, legs}
  # where legs are the Ref/Dep/Compose entities. `resource` is nil for a compose
  # (an intermediate cell has no backing resource).
  defp root_node(resource) do
    legs =
      Ext.get_entities(resource, [:reactive])
      |> Enum.filter(&match?(%Ref{}, &1) or match?(%Compose{}, &1) or match?(%Dep{}, &1))

    flat_refs = Ext.get_opt(resource, [:reactive], :depends_on, []) |> Enum.map(&%Ref{to: &1})

    {:op, cell_id(resource) |> to_string(), Ext.get_opt(resource, [:reactive], :op, nil),
     Ext.get_opt(resource, [:reactive], :compute, nil),
     Ext.get_opt(resource, [:reactive], :key_rule, :identity),
     Ext.get_opt(resource, [:reactive], :leaf?, false), resource, legs ++ flat_refs,
     %{
       source: Ext.get_opt(resource, [:reactive], :source, nil),
       driver: Ext.get_opt(resource, [:reactive], :driver, nil)
     }}
  end

  defp walk_cbs do
    %{
      classify: fn
        %Ref{} -> :ref
        %Dep{} -> :ref
        %Compose{} -> :op
        {:op, _, _, _, _, _, _, _, _} -> :op
      end,
      legs: fn
        {:op, _, _, _, _, _, _, legs, _} -> legs
        %Compose{legs: legs} -> legs
      end,
      leg_id: fn parent, i, leg ->
        case leg do
          %Compose{as: as} when not is_nil(as) -> to_string(as)
          _ -> "#{parent}/#{i}"
        end
      end,
      ref_id: fn
        %Ref{to: to} -> to_string(to)
        %Dep{to: to} -> to_string(to)
      end,
      to_cell: &build_cell/3
    }
  end

  defp build_cell(id, {:op, _id, op, compute, key_rule, leaf?, resource, _legs, extra}, input_ids) do
    %ReactiveDag.Cell{
      id: id,
      op: op,
      inputs: input_ids,
      leaf?: leaf?,
      meta:
        Map.merge(%{resource: resource, compute: compute, key_rule: key_rule}, extra)
    }
  end

  defp build_cell(id, %Compose{op: op, compute: compute, key_rule: key_rule}, input_ids) do
    %ReactiveDag.Cell{
      id: id,
      op: op,
      inputs: input_ids,
      leaf?: false,
      meta: %{resource: nil, compute: compute, key_rule: key_rule}
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

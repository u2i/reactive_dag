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

  defmodule Reduce do
    @moduledoc """
    A declarative REDUCE (fold): read an input node's payload, group it, and
    reduce each group to one output row — the common map/fold shape, so the
    author writes the grouping + reduction, not the read/write/changed plumbing.
    Anything the combinator can't express (an LLM call, an external fetch, a
    bespoke join) uses the `compute:` module escape hatch instead.
    """
    defstruct [:over, :group_by, :key, :into, :read, :upsert, :__identifier__, :__spark_metadata__]
  end

  @reduce %Spark.Dsl.Entity{
    name: :reduce,
    target: Reduce,
    describe: "Declarative fold: read `over`, group by `group_by`, reduce each group with `into`.",
    schema: [
      over: [type: :atom, required: true, doc: "the input node id whose payload is read + grouped"],
      read: [
        type: {:fun, 1},
        required: true,
        doc: "`(over_id -> [item])` — read the input's payload items (host domain: Ash.read etc.)"
      ],
      group_by: [type: {:fun, 1}, required: true, doc: "`(item -> group_term)` — the grouping key"],
      key: [
        type: {:fun, 1},
        required: true,
        doc: "`(group_term -> cell_key_string)` — the output tuple key for a group"
      ],
      into: [
        type: {:fun, 2},
        required: true,
        doc: "`(group_term, [item] -> row)` — reduce a group to its output row/payload"
      ],
      upsert: [
        type: {:fun, 2},
        required: true,
        doc:
          "`(key, row -> boolean)` — write the row's payload (host typed resource) + return true iff it CHANGED. The library does the coordination `Op.put` for changed keys."
      ]
    ]
  }

  @reactive %Spark.Dsl.Section{
    name: :reactive,
    describe: "Declares this resource as a reactive-DAG node: its op + dependencies.",
    # legs (ref/compose) are the nested form; dep is the flat form. `reduce` is the
    # declarative fold combinator (else use `compute:` for an arbitrary recompute).
    entities: [@dep, @ref, @compose, @reduce],
    schema: [
      id: [
        type: :atom,
        doc: "the cell id; defaults to the resource module's short name, snake_cased"
      ],
      op: [type: :atom, required: true, doc: "the op kind (free atom; the host interprets it)"],
      compute: [type: :atom, doc: "the recompute module for this node (nil for a leaf / a `reduce`)"],
      key_rule: [
        type: {:one_of, [:identity, :all]},
        default: :identity,
        doc: "how a child key maps to this cell's key on propagation"
      ],
      leaf?: [type: :boolean, default: false, doc: "true for a source-fed leaf (no compute)"],
      source: [
        type: :atom,
        doc: "convenience: a leaf's source binding id (also merged into meta)"
      ],
      driver: [
        type: :atom,
        doc: "convenience: a leaf's driver module (also merged into meta)"
      ],
      meta: [
        type: :keyword_list,
        default: [],
        doc:
          "OPEN host binding — anything the host's recompute/refresh needs and the library never interprets (cascade: source/driver; portal leaf: source/check/org_kind/strength/attest_for/cadence). Merged into the cell's meta."
      ],
      for_each: [
        type: :atom,
        doc:
          "GENERATOR: expand this node per member of a named population (resolved by graph/2's member-fetcher). The template itself builds no cell; each member builds an instance `<id>.<member>`."
      ],
      over: [
        type: :atom,
        doc:
          "SECOND-ORDER: this node's population is computed from the graph itself (e.g. :findings | :register | :controls). Carried in meta for a host post-build hook; the library does not resolve it."
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

  Pass `:for_each` to expand GENERATOR nodes: a `(population_atom -> [member])`
  fun. A node with `for_each: :pop` builds no template cell — instead, for each
  member it builds an instance sub-tree rooted at `<id>.<member.id>`, with the
  member's `meta` merged onto every instance cell (the host's per-member stamp,
  e.g. a probe filter). A member is any map with an `:id` (+ optional `:meta`).
  Without a fetcher, a generator node is skipped (and logged by the caller).
  """
  @spec graph([module()], keyword()) :: ReactiveDag.Plan.t()
  def graph(resources, opts \\ []) do
    fetch = Keyword.get(opts, :for_each)

    resources
    |> Enum.flat_map(&cells(&1, fetch))
    |> ReactiveDag.Graph.build()
  end

  @doc """
  The `ReactiveDag.Cell`s a node resource lowers to (no graph math): its root
  cell + one per nested `compose`. Legs are lowered by-name via the shared
  `ReactiveDag.Lowering.walk` — a `ref`/`dep` resolves to an existing cell id
  (no new cell), a `compose` recurses into an intermediate cell.
  """
  @spec cells(module(), (atom() -> [map()]) | nil) :: [ReactiveDag.Cell.t()]
  def cells(resource, fetch \\ nil) do
    case Ext.get_opt(resource, [:reactive], :for_each, nil) do
      nil ->
        {_id, cells} = lower(resource, cell_id(resource) |> to_string(), %{})
        cells

      pop when is_function(fetch, 1) ->
        # GENERATOR: template builds no cell; expand one instance per member.
        base = cell_id(resource) |> to_string()

        fetch.(pop)
        |> Enum.flat_map(fn member ->
          {_id, cells} = lower(resource, "#{base}.#{member.id}", Map.get(member, :meta, %{}))
          cells
        end)

      _pop ->
        # a generator with no fetcher supplied — build nothing (caller decides).
        []
    end
  end

  # lower a resource's reactive block rooted at `root_id`, merging `stamp` (a
  # per-member metadata map, empty for a non-generator) onto every cell.
  defp lower(resource, root_id, stamp) do
    {id, cells} = ReactiveDag.Lowering.walk(root_id, root_node(resource, root_id), walk_cbs())
    {id, Enum.map(cells, &%{&1 | meta: Map.merge(&1.meta, stamp)})}
  end

  @doc "The root cell a NON-generator node resource lowers to."
  @spec to_cell(module()) :: ReactiveDag.Cell.t()
  def to_cell(resource) do
    id = cell_id(resource) |> to_string()
    resource |> cells() |> Enum.find(&(&1.id == id))
  end

  # ── lowering: the reactive block → a node the walk callbacks understand ─────
  # The root node and every `compose` leg share one internal shape:
  #   {:op, id, op, compute, key_rule, leaf?, resource, legs, extra}
  # where legs are the Ref/Dep/Compose entities and `extra` is the open host
  # binding merged into meta. `resource` is nil for a compose (an intermediate
  # cell has no backing resource). `root_id` lets a generator instance re-root.
  defp root_node(resource, root_id) do
    legs =
      Ext.get_entities(resource, [:reactive])
      |> Enum.filter(&(match?(%Ref{}, &1) or match?(%Compose{}, &1) or match?(%Dep{}, &1)))

    flat_refs = Ext.get_opt(resource, [:reactive], :depends_on, []) |> Enum.map(&%Ref{to: &1})

    # a `reduce over: :x` implies an input edge to :x (the node it folds).
    reduce_refs =
      case reduce(resource) do
        nil -> []
        r -> [%Ref{to: r.over}]
      end

    {:op, root_id, Ext.get_opt(resource, [:reactive], :op, nil),
     Ext.get_opt(resource, [:reactive], :compute, nil),
     Ext.get_opt(resource, [:reactive], :key_rule, :identity),
     Ext.get_opt(resource, [:reactive], :leaf?, false), resource,
     legs ++ flat_refs ++ reduce_refs, extra_meta(resource)}
  end

  # the single `reduce` entity of a node, or nil.
  defp reduce(resource) do
    Ext.get_entities(resource, [:reactive]) |> Enum.find(&match?(%Reduce{}, &1))
  end

  # the OPEN host binding folded into a cell's meta: the `meta:` keyword list, the
  # source/driver/over conveniences, and the `reduce` spec (which the generic
  # recompute strategy runs — see ReactiveDag.Node.Recompute).
  defp extra_meta(resource) do
    Ext.get_opt(resource, [:reactive], :meta, [])
    |> Map.new()
    |> Map.merge(
      %{
        source: Ext.get_opt(resource, [:reactive], :source, nil),
        driver: Ext.get_opt(resource, [:reactive], :driver, nil),
        over: Ext.get_opt(resource, [:reactive], :over, nil),
        reduce: reduce(resource)
      }
      |> Enum.reject(fn {_k, v} -> is_nil(v) end)
      |> Map.new()
    )
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

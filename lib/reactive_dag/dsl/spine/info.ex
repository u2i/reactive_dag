defmodule ReactiveDag.Dsl.Spine.Info do
  @moduledoc """
  Read + lowering API over a module authored with `ReactiveDag.Dsl.Spine`:
  introspect the `graph` block, lower it to `ReactiveDag.Cell`s, and build the
  `ReactiveDag.Plan` the drain consumes.

  Lowering reuses the shared `ReactiveDag.Lowering.walk` — the same recursion the
  `ReactiveDag.Node` resource extension uses — so a `ref` resolves to an existing
  cell id (no new cell), a `compose`/nested `node` becomes an intermediate cell,
  and an `observed` leaf is terminal. The host's `op` atoms + `meta:` ride into
  each cell's `meta` untouched.
  """

  alias ReactiveDag.Dsl.Spine.{Compose, Node, Observed, Ref, Source}
  alias Spark.Dsl.Extension, as: Ext

  @doc "The declared scanners' driver modules, in declaration order."
  @spec sources(module()) :: [module()]
  def sources(module) do
    module |> entities() |> Enum.filter(&match?(%Source{}, &1)) |> Enum.map(& &1.driver)
  end

  @doc "The declared `source` entities (id + driver)."
  @spec source_entities(module()) :: [Source.t()]
  def source_entities(module) do
    module |> entities() |> Enum.filter(&match?(%Source{}, &1))
  end

  @doc """
  Lower the module's `graph` block to a `ReactiveDag.Plan`. Structural validation
  (refs resolve, ids unique, acyclic) is enforced by `ReactiveDag.Graph.build/1`;
  a `ReactiveDag.Dsl.Spine.Transformer` runs the same lowering at compile time so
  those failures surface as `Spark.Error.DslError` rather than at first drain.
  """
  @spec plan(module()) :: ReactiveDag.Plan.t()
  def plan(module) do
    module |> cells() |> ReactiveDag.Graph.build()
  end

  @doc "Every `ReactiveDag.Cell` the module's `graph` block lowers to (no graph math)."
  @spec cells(module()) :: [ReactiveDag.Cell.t()]
  def cells(module) do
    module |> nodes() |> lower_all()
  end

  @doc false
  # Lower a list of graph entities (observed/node) to cells — shared by `cells/1`
  # (runtime) and the compile-time transformer, so both use identical lowering.
  @spec lower_all([struct()]) :: [ReactiveDag.Cell.t()]
  def lower_all(nodes) do
    Enum.flat_map(nodes, fn node ->
      {_root, cells} = ReactiveDag.Lowering.walk(to_string(node.id), node, walk_cbs())
      cells
    end)
  end

  @doc false
  # The graph-building entities (observed leaves + derived nodes) of a module.
  @spec graph_nodes(module()) :: [struct()]
  def graph_nodes(module), do: nodes(module)

  # the graph-building entities (observed leaves + derived nodes); sources are not
  # cells (they feed leaves, they aren't nodes themselves), refs/composes only
  # appear nested inside nodes.
  defp nodes(module) do
    module |> entities() |> Enum.filter(&(match?(%Observed{}, &1) or match?(%Node{}, &1)))
  end

  defp entities(module), do: Ext.get_entities(module, [:graph])

  # ── lowering callbacks over the spine's entity structs ──────────────────────
  defp walk_cbs do
    %{
      classify: fn
        %Ref{} -> :ref
        %Observed{} -> :leaf
        %Compose{leaf?: true} -> :leaf
        %Node{leaf?: true} -> :leaf
        %Compose{} -> :op
        %Node{} -> :op
      end,
      legs: fn
        %Node{legs: legs} -> legs
        %Compose{legs: legs} -> legs
        %Observed{} -> []
      end,
      leg_id: fn parent, i, leg ->
        case leg do
          %Compose{as: as} when not is_nil(as) -> to_string(as)
          _ -> "#{parent}/#{i}"
        end
      end,
      ref_id: fn %Ref{to: to} -> to_string(to) end,
      to_cell: &to_cell/3
    }
  end

  defp to_cell(id, %Observed{grain: grain, strength: strength, fed_by: fed_by, meta: meta}, _inputs) do
    %ReactiveDag.Cell{
      id: id,
      op: :leaf,
      inputs: [],
      leaf?: true,
      meta:
        %{grain: grain, strength: strength, fed_by: fed_by}
        |> Map.merge(Map.new(meta || []))
    }
  end

  defp to_cell(id, %Node{op: op, key_rule: kr, leaf?: leaf?, meta: meta}, inputs) do
    %ReactiveDag.Cell{
      id: id,
      op: op,
      inputs: inputs,
      leaf?: leaf? || false,
      meta: Map.merge(%{key_rule: kr}, Map.new(meta || []))
    }
  end

  defp to_cell(id, %Compose{op: op, key_rule: kr, leaf?: leaf?, meta: meta}, inputs) do
    %ReactiveDag.Cell{
      id: id,
      op: op,
      inputs: inputs,
      leaf?: leaf? || false,
      meta: Map.merge(%{key_rule: kr}, Map.new(meta || []))
    }
  end
end

defmodule ReactiveDag do
  @moduledoc """
  Top-level entry point for assembling a reactive DAG from **mixed authoring
  surfaces** into one `ReactiveDag.Plan`.

  A node can be authored two ways (see `ReactiveDag.Graph.Dsl` and
  `ReactiveDag.Node`), and both lower to the same `ReactiveDag.Cell` IR:

    * **graph-level** — a `graph do … end` block (`ReactiveDag.Dsl.Spine`); one
      module declares many cells.
    * **per-resource** — an Ash resource with the `ReactiveDag.Node` extension;
      the resource *is* one cell, its `reactive do … end` block the definition.

  `assemble/1` unifies them: it lowers every source, merges the cells **by id**,
  and builds one plan — so a graph can be authored mostly in a `graph` block while
  specific nodes are Ash resources (carrying a typed payload table + a
  `reduce`/`join` combinator), or vice-versa. A resource **overrides** a spine node
  of the same id (the resource is the more specific definition), which is exactly
  the "graft a resource cell over the DSL cell" pattern cascade hand-rolled — now
  a first-class call.

      ReactiveDag.assemble(
        spine:     [MyApp.Pipeline],                    # `graph do … end` module(s)
        resources: [MyApp.BudgetRollups, MyApp.FlowSeries],
        for_each:  &MyApp.Populations.fetch/1           # generator member-fetcher
      )
      # => %ReactiveDag.Plan{}

  Both `:spine` and `:resources` are optional lists (default `[]`), so `assemble/1`
  degrades to either single surface — it's the superset of
  `ReactiveDag.Dsl.Spine.Info.plan/1` and `ReactiveDag.Node.graph/2`.
  """

  alias ReactiveDag.{Cell, Graph}

  @typedoc "A `graph do … end` module (uses `ReactiveDag.Graph.Dsl`)."
  @type spine_module :: module()

  @typedoc "An Ash resource with the `ReactiveDag.Node` extension."
  @type node_resource :: module()

  @doc """
  Assemble a `ReactiveDag.Plan` from mixed sources, merging cells by id.

  Options:

    * `:spine` — `graph do … end` modules to lower via `ReactiveDag.Dsl.Spine.Info`.
    * `:resources` — `ReactiveDag.Node` resources to lower via `ReactiveDag.Node`.
    * `:for_each` — a `(population_atom -> [member])` fun for generator resources
      (`for_each:` in a `reactive` block); passed through to `ReactiveDag.Node`.

  On an id collision between a spine cell and a resource cell, the **resource
  wins** (it's the more specific authoring). A collision between two sources of the
  *same* kind (two spine modules, or two resources, declaring the same id) is a
  conflict — it raises, because neither is more specific than the other.
  """
  @spec assemble(keyword()) :: ReactiveDag.Plan.t()
  def assemble(opts \\ []) do
    spine_modules = Keyword.get(opts, :spine, [])
    resources = Keyword.get(opts, :resources, [])
    fetch = Keyword.get(opts, :for_each)

    spine_cells = spine_modules |> Enum.flat_map(&ReactiveDag.Dsl.Spine.Info.cells/1)
    resource_cells = resources |> Enum.flat_map(&ReactiveDag.Node.cells(&1, fetch))

    # within a surface, a duplicate id is a genuine conflict (neither is more
    # specific); across surfaces, the resource overrides the spine node.
    no_dupes!(spine_cells, "spine")
    no_dupes!(resource_cells, "resource")

    spine_cells
    |> merge_by_id(resource_cells)
    |> Graph.build()
  end

  # resource cells override spine cells of the same id; other spine cells survive.
  defp merge_by_id(spine_cells, resource_cells) do
    overridden = MapSet.new(resource_cells, & &1.id)
    Enum.reject(spine_cells, &MapSet.member?(overridden, &1.id)) ++ resource_cells
  end

  defp no_dupes!(cells, surface) do
    dupes =
      cells
      |> Enum.frequencies_by(& &1.id)
      |> Enum.filter(fn {_id, n} -> n > 1 end)
      |> Enum.map(&elem(&1, 0))

    case dupes do
      [] -> :ok
      ids -> raise ArgumentError, "duplicate #{surface} cell id(s): #{Enum.join(ids, ", ")}"
    end
  end

  @doc """
  The merged `ReactiveDag.Cell` list `assemble/1` would build a plan from — the
  same lowering + merge, without the graph math. Useful for inspection/tests.
  """
  @spec cells(keyword()) :: [Cell.t()]
  def cells(opts \\ []) do
    spine_cells = opts |> Keyword.get(:spine, []) |> Enum.flat_map(&ReactiveDag.Dsl.Spine.Info.cells/1)

    resource_cells =
      opts
      |> Keyword.get(:resources, [])
      |> Enum.flat_map(&ReactiveDag.Node.cells(&1, Keyword.get(opts, :for_each)))

    merge_by_id(spine_cells, resource_cells)
  end
end

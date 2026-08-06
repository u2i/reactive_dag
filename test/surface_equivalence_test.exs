defmodule ReactiveDag.SurfaceEquivalenceTest do
  @moduledoc """
  Proves the authoring-surface mapping (docs/authoring-surface-mapping.md): the
  SAME node authored as a `ReactiveDag.Node` resource (A) and as a
  `ReactiveDag.Dsl.Spine` graph node (B) lowers to EQUIVALENT `ReactiveDag.Cell`s.
  This is what makes A↔B translation mechanical — the cell is the shared pivot.
  """
  use ExUnit.Case, async: true

  # a shared op module + a shared reduce spec, referenced by both surfaces so the
  # only difference is the authoring syntax, not the computation.
  defmodule FakeOp do
    @behaviour ReactiveDag.Op
    @impl true
    def recompute(_cell, keys), do: {:ok, keys}
  end

  def read(:fiscal_lines), do: []
  def grp(r), do: r.fund
  def key(f), do: "#{f}"
  def into(_f, rows), do: %{n: length(rows)}
  def up(_k, _row), do: true

  # ── A: a derived node authored as a RESOURCE ────────────────────────────────
  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false
    resources do
      allow_unregistered? true
    end
  end

  defmodule RollupsResource do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Simple, extensions: [ReactiveDag.Node]

    reactive do
      id :rollups
      op :fold
      key_rule :all

      reduce over: :fiscal_lines,
             read: &ReactiveDag.SurfaceEquivalenceTest.read/1,
             group_by: &ReactiveDag.SurfaceEquivalenceTest.grp/1,
             key: &ReactiveDag.SurfaceEquivalenceTest.key/1,
             into: &ReactiveDag.SurfaceEquivalenceTest.into/2,
             upsert: &ReactiveDag.SurfaceEquivalenceTest.up/2
    end
  end

  # ── B: the SAME node authored in a graph, per the A→B recipe ────────────────
  defmodule Pipeline do
    use ReactiveDag.Graph.Dsl

    graph do
      observed :fiscal_lines, grain: :line

      node :rollups do
        op :fold
        key_rule :all

        reduce over: :fiscal_lines,
               read: &ReactiveDag.SurfaceEquivalenceTest.read/1,
               group_by: &ReactiveDag.SurfaceEquivalenceTest.grp/1,
               key: &ReactiveDag.SurfaceEquivalenceTest.key/1,
               into: &ReactiveDag.SurfaceEquivalenceTest.into/2,
               upsert: &ReactiveDag.SurfaceEquivalenceTest.up/2
      end
    end
  end

  test "A→B: a resource node and a graph node lower to equivalent cells" do
    a = ReactiveDag.Node.to_cell(RollupsResource)

    b =
      Pipeline
      |> ReactiveDag.Dsl.Spine.Info.cells()
      |> Enum.find(&(&1.id == "rollups"))

    # the graph fields match exactly
    assert a.id == b.id
    assert a.op == b.op
    assert a.inputs == b.inputs          # both: ["fiscal_lines"] via the over: edge
    assert a.leaf? == b.leaf?

    # the computation matches — the SAME %Reduce{} struct rides in meta.reduce
    assert a.meta.reduce == b.meta.reduce
    assert a.meta.key_rule == b.meta.key_rule

    # the ONE documented asymmetry: the resource cell carries its backing module,
    # the graph cell does not. Everything else is equal.
    assert a.meta.resource == RollupsResource
    assert b.meta[:resource] == nil
  end

  # ── the leaf case: scan: (graph) vs source:/driver (resource) ───────────────
  defmodule FleetScan do
    @behaviour ReactiveDag.Source
    @impl true
    def id, do: :fleet_scan
    @impl true
    def leaf_cells(_g), do: ["machines"]
    @impl true
    def poll(_), do: {:ok, %{changed: []}}
  end

  defmodule MachinesResource do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Simple, extensions: [ReactiveDag.Node]

    reactive do
      id :machines
      op :leaf
      leaf? true
      source :fleet_scan
      driver FleetScan
    end
  end

  defmodule LeafPipeline do
    use ReactiveDag.Graph.Dsl

    graph do
      observed :machines, grain: :machine, scan: FleetScan
    end
  end

  test "leaf: source:/driver (resource) and scan: (graph) both bind the same driver" do
    a = ReactiveDag.Node.to_cell(MachinesResource)
    b = LeafPipeline |> ReactiveDag.Dsl.Spine.Info.cells() |> Enum.find(&(&1.id == "machines"))

    # both are leaves feeding the same driver module
    assert a.leaf? and b.leaf?
    assert a.op == :leaf and b.op == :leaf
    assert a.meta.driver == FleetScan     # resource: split id + driver
    assert b.meta.scan == FleetScan       # graph: inlined module

    # the resource keeps the source: id; the graph doesn't need one — the single
    # documented shape-difference for leaves.
    assert a.meta.source == :fleet_scan
    refute Map.has_key?(b.meta, :source)
  end
end

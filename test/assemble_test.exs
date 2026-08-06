defmodule ReactiveDag.AssembleTest do
  @moduledoc """
  `ReactiveDag.assemble/1` — the unified model: lower a `graph do … end` spine AND
  `ReactiveDag.Node` resources, merge by id (resource overrides a same-id spine
  node), and build one Plan. Both surfaces already lower to `ReactiveDag.Cell`;
  this proves they compose into a single graph.
  """
  use ExUnit.Case, async: true

  # ── a spine graph ───────────────────────────────────────────────────────────
  defmodule Pipeline do
    use ReactiveDag.Graph.Dsl

    graph do
      observed :machines, grain: :machine

      # a spine node authored inline — will be OVERRIDDEN by a resource of the
      # same id in the mixed scenario.
      node :health do
        op :reduce
        meta grain: :machine, authored: :spine
        ref :machines
      end

      # a spine node with no resource counterpart — survives the merge.
      node :report do
        op :map
        meta authored: :spine
        ref :health
      end
    end
  end

  # ── a Node resource whose id COLLIDES with the spine's :health ──────────────
  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false
    resources do
      allow_unregistered? true
    end
  end

  defmodule FakeReduce do
    @behaviour ReactiveDag.Op
    @impl true
    def recompute(_cell, keys), do: {:ok, keys}
  end

  defmodule Health do
    # same cell id as the spine's `node :health`; the resource must WIN.
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Simple, extensions: [ReactiveDag.Node]

    reactive do
      id :health
      op :reduce
      compute FakeReduce
      meta authored: :resource
      dep :machines
    end
  end

  # a resource that adds a NEW cell (no spine counterpart).
  defmodule Audit do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Simple, extensions: [ReactiveDag.Node]

    reactive do
      id :audit
      op :map
      compute FakeReduce
      dep :health
    end
  end

  # a leaf resource so a resources-ONLY plan is self-contained (:health deps it).
  defmodule Machines do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Simple, extensions: [ReactiveDag.Node]

    reactive do
      id :machines
      op :leaf
      leaf? true
    end
  end

  # ── SCENARIO 1: spine-only degrades to Spine.Info.plan/1 ────────────────────
  test "spine only: assemble matches the spine's own plan" do
    plan = ReactiveDag.assemble(spine: [Pipeline])
    ids = plan.cells |> Map.keys() |> Enum.sort()
    assert ids == ["health", "machines", "report"]
    # the spine authored :health
    assert plan.cells["health"].meta.authored == :spine
  end

  # ── SCENARIO 2: resources-only degrades to Node.graph/2 ─────────────────────
  test "resources only: assemble builds a self-contained plan from resources alone" do
    plan = ReactiveDag.assemble(resources: [Machines, Health, Audit])
    assert Enum.sort(Map.keys(plan.cells)) == ["audit", "health", "machines"]
    assert plan.cells["health"].meta.authored == :resource
    assert plan.depths["machines"] < plan.depths["health"]
  end

  # ── SCENARIO 3: MIXED — resource overrides the same-id spine node ───────────
  test "mixed: a resource cell overrides the spine node of the same id" do
    plan = ReactiveDag.assemble(spine: [Pipeline], resources: [Health])

    # :health now comes from the RESOURCE, not the spine
    assert plan.cells["health"].meta.authored == :resource
    assert plan.cells["health"].meta.compute == FakeReduce

    # the other spine cells survive untouched
    assert plan.cells["machines"].leaf?
    assert plan.cells["report"].meta.authored == :spine
    assert Enum.sort(Map.keys(plan.cells)) == ["health", "machines", "report"]
  end

  # ── SCENARIO 4: MIXED — a resource ADDS a new cell alongside the spine ──────
  test "mixed: a resource with a fresh id adds a cell to the spine graph" do
    plan = ReactiveDag.assemble(spine: [Pipeline], resources: [Health, Audit])

    assert Enum.sort(Map.keys(plan.cells)) == ["audit", "health", "machines", "report"]
    # audit deps health (the resource-authored one)
    assert plan.cells["audit"].inputs == ["health"]
    # depths: machines < health < audit
    assert plan.depths["machines"] < plan.depths["health"]
    assert plan.depths["health"] < plan.depths["audit"]
  end

  # ── SCENARIO 5: a duplicate id WITHIN a surface is a conflict ───────────────
  test "conflict: two resources with the same id raise" do
    err =
      assert_raise ArgumentError, fn ->
        ReactiveDag.assemble(resources: [Health, Health])
      end

    assert Exception.message(err) =~ "duplicate resource cell id"
    assert Exception.message(err) =~ "health"
  end

  # ── SCENARIO 6: empty degrades cleanly ──────────────────────────────────────
  test "empty: no sources builds an empty plan" do
    plan = ReactiveDag.assemble([])
    assert plan.cells == %{}
  end
end

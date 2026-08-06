defmodule ReactiveDag.DslSpineTest do
  @moduledoc """
  The shared authoring grammar (`ReactiveDag.Dsl.Spine`): a `graph do … end` block
  lowers to cells; the leaf↔scanner binding is validated at compile time; a domain
  op-kind + meta survives lowering (the ADR-002 "domain via extension points"
  prototype gate).
  """
  use ExUnit.Case, async: true

  alias ReactiveDag.Dsl.Spine.Info

  # ── shared toy scanners ─────────────────────────────────────────────────────
  defmodule FleetScan do
    @behaviour ReactiveDag.Source
    @impl true
    def id, do: :fleet_scan
    @impl true
    def leaf_cells(_graph), do: ["machines"]
    @impl true
    def poll(_opts), do: {:ok, %{changed: []}}
  end

  # ── SCENARIO 1: basic authoring lowers to the right cells ───────────────────
  defmodule Basic do
    use ReactiveDag.Graph.Dsl

    graph do
      # the common 1:1 case — leaf + its scanner in ONE declaration.
      observed :machines, grain: :machine, strength: :measured, scan: FleetScan

      node :fleet_health do
        op :reduce
        meta grain: :machine
        ref :machines
      end
    end
  end

  test "SCENARIO basic: observed/node lower; observed is a leaf carrying its scanner" do
    cells = Basic |> Info.cells() |> Map.new(&{&1.id, &1})

    # the observed leaf — shape + the inline scanner binding
    assert cells["machines"].leaf?
    assert cells["machines"].op == :leaf
    assert cells["machines"].meta.grain == :machine
    assert cells["machines"].meta.strength == :measured
    assert cells["machines"].meta.scan == FleetScan

    # the derived node, its op-kind + meta preserved, input edge to the leaf
    assert cells["fleet_health"].op == :reduce
    assert cells["fleet_health"].meta.grain == :machine
    assert cells["fleet_health"].inputs == ["machines"]
  end

  test "SCENARIO basic: plan/1 builds a ReactiveDag.Plan with depths (leaf below its parent)" do
    plan = Info.plan(Basic)
    assert %ReactiveDag.Plan{} = plan
    assert plan.depths["machines"] < plan.depths["fleet_health"]
  end

  # ── SCENARIO 1b: inline scanners are discoverable (Info.scanners + Source.drivers) ─
  test "SCENARIO scanner: inline `scan:` drivers are discovered from the graph" do
    # from the DSL module
    assert Info.scanners(Basic) == [FleetScan]

    # from the lowered plan (reads meta.scan off leaf cells), unioned with extras
    plan = Info.plan(Basic)
    assert ReactiveDag.Source.drivers(plan) == [FleetScan]
    assert ReactiveDag.Source.drivers(plan, [OtherScan]) == [FleetScan, OtherScan]
  end

  defmodule OtherScan do
    @behaviour ReactiveDag.Source
    @impl true
    def id, do: :other
    @impl true
    def leaf_cells(_g), do: []
    @impl true
    def poll(_), do: {:ok, %{changed: []}}
  end

  # ── SCENARIO 2: an observed WITHOUT a scanner is a plain leaf (target-only) ──
  test "SCENARIO leaf: an observed with no scan: is a terminal leaf, scanner nil" do
    defmodule PlainLeaf do
      use ReactiveDag.Graph.Dsl

      graph do
        observed :targets, grain: :thing
      end
    end

    assert [%{id: "targets", leaf?: true, op: :leaf} = c] = Info.cells(PlainLeaf)
    assert c.meta.scan == nil
    assert Info.scanners(PlainLeaf) == []
  end

  # ── SCENARIO 3: structural checks (dangling ref, cycle) at compile ──────────
  test "SCENARIO structure: a node ref to a NON-EXISTENT cell fails at compile" do
    err =
      assert_raise Spark.Error.DslError, fn ->
        defmodule Dangling do
          use ReactiveDag.Graph.Dsl

          graph do
            node :derived do
              op :reduce
              ref :nonexistent
            end
          end
        end
      end

    assert Exception.message(err) =~ "invalid reactive graph"
  end

  test "SCENARIO structure: a cycle fails at compile" do
    assert_raise Spark.Error.DslError, ~r/invalid reactive graph/, fn ->
      defmodule Cyclic do
        use ReactiveDag.Graph.Dsl

        graph do
          node :a do
            op :reduce
            ref :b
          end

          node :b do
            op :reduce
            ref :a
          end
        end
      end
    end
  end

  # ── SCENARIO 4: compose nesting → intermediate cells ────────────────────────
  defmodule Nested do
    use ReactiveDag.Graph.Dsl

    graph do
      observed :machines
      node :health do
        op :reduce
        ref :machines
      end

      node :variance do
        op :join
        ref :machines

        compose :fold do
          as :rolling
          meta window: 3
          ref :health
        end
      end
    end
  end

  test "SCENARIO compose: a nested compose becomes an intermediate cell wired as an input" do
    cells = Nested |> Info.cells() |> Map.new(&{&1.id, &1})

    # the named compose intermediate exists with its op + meta
    assert cells["rolling"].op == :fold
    assert cells["rolling"].meta.window == 3
    assert cells["rolling"].inputs == ["health"]

    # the parent node's inputs = the direct ref + the compose intermediate
    assert Enum.sort(cells["variance"].inputs) == ["machines", "rolling"]
  end

  # ── SCENARIO 4b: a spine node carries its OWN executor (reduce / compute) ────
  # The compute-authoring parity with ReactiveDag.Node: a spine node can inline a
  # reduce/join combinator or a compute module, lowering to the SAME meta the
  # resource surface produces — so ReactiveDag.Node.Recompute runs it either way.
  defmodule FakeOp do
    @behaviour ReactiveDag.Op
    @impl true
    def recompute(_cell, keys), do: {:ok, keys}
  end

  defmodule WithExecutors do
    use ReactiveDag.Graph.Dsl

    graph do
      observed :fiscal_lines, grain: :line

      # a REDUCE authored on the spine node itself
      node :rollups do
        op :fold
        key_rule :all

        reduce over: :fiscal_lines,
               read: &__MODULE__.read/1,
               group_by: &__MODULE__.grp/1,
               key: &__MODULE__.key/1,
               into: &__MODULE__.into/2,
               upsert: &__MODULE__.up/2
      end

      # a COMPUTE escape hatch on a spine node
      node :extract do
        op :map
        compute FakeOp
        ref :fiscal_lines
      end
    end

    def read(:fiscal_lines), do: []
    def grp(r), do: r.fund
    def key(f), do: "#{f}"
    def into(_f, rows), do: %{n: length(rows)}
    def up(_k, _row), do: true
  end

  test "SCENARIO executor: a spine reduce lowers to meta.reduce + an implicit over edge" do
    cells = WithExecutors |> Info.cells() |> Map.new(&{&1.id, &1})

    r = cells["rollups"]
    assert r.op == :fold
    # `over: :fiscal_lines` becomes an input edge — no explicit ref needed
    assert r.inputs == ["fiscal_lines"]
    # the combinator rides in meta as the SAME struct the resource surface uses,
    # so ReactiveDag.Node.Recompute runs it identically
    assert %ReactiveDag.Node.Reduce{} = r.meta.reduce
  end

  test "SCENARIO executor: a spine compute lowers to meta.compute (the escape hatch)" do
    cells = WithExecutors |> Info.cells() |> Map.new(&{&1.id, &1})

    e = cells["extract"]
    assert e.op == :map
    assert e.meta.compute == FakeOp
    assert e.inputs == ["fiscal_lines"]
  end

  # ── SCENARIO 5: the prototype gate — a DOMAIN op-kind + rich meta survives ──
  # This is the ADR-002 claim: a host expresses its domain (here the portal's
  # `guarantee`) as an op-kind + meta on the shared `node`, and every domain field
  # rides through lowering UNTOUCHED — no bespoke entity needed for the common case.
  defmodule DomainGuarantee do
    use ReactiveDag.Graph.Dsl

    graph do
      observed :active_people, grain: :person

      # "guarantee" is a DOMAIN op-kind (the library never interprets it); the
      # compliance fields ride in meta. A reconcile-shaped set nests as a compose.
      node :g_hire_screened do
        op :guarantee
        key_rule :all

        meta claim: "every hire is screened BEFORE they start",
             addresses: [:CC1_4],
             shape: :queue,
             population: :events

        compose :relation do
          as :"g_hire_screened/set"
          meta grain: :person
          ref :active_people

          compose :leaf do
            as :"g_hire_screened/set/screen"
            leaf? true
            meta grain: :person, attest_kind: "background_screen", cadence: :none
          end
        end
      end
    end
  end

  test "SCENARIO gate: a domain op-kind + rich meta lowers with every field intact" do
    cells = DomainGuarantee |> Info.cells() |> Map.new(&{&1.id, &1})

    g = cells["g_hire_screened"]
    assert g.op == :guarantee
    assert g.meta.claim == "every hire is screened BEFORE they start"
    assert g.meta.addresses == [:CC1_4]
    assert g.meta.shape == :queue
    assert g.meta.population == :events
    assert g.inputs == ["g_hire_screened/set"]

    set = cells["g_hire_screened/set"]
    assert set.op == :relation
    assert set.meta.grain == :person
    assert Enum.sort(set.inputs) == ["active_people", "g_hire_screened/set/screen"]

    # the composed workflow leaf, terminal, with its domain binding
    leaf = cells["g_hire_screened/set/screen"]
    assert leaf.leaf?
    assert leaf.meta.attest_kind == "background_screen"
    assert leaf.meta.cadence == :none
  end

  test "SCENARIO gate: the whole domain graph still builds a valid Plan (depths ordered)" do
    plan = Info.plan(DomainGuarantee)
    # leaf below the set below the guarantee
    assert plan.depths["active_people"] < plan.depths["g_hire_screened/set"]
    assert plan.depths["g_hire_screened/set"] < plan.depths["g_hire_screened"]
  end

  # ── SCENARIO 6: the Source seam — verify!/2 ─────────────────────────────────
  defmodule GoodSource do
    @behaviour ReactiveDag.Source
    @impl true
    def id, do: :good
    @impl true
    def leaf_cells(_graph), do: ["machines"]
    @impl true
    def poll(_), do: {:ok, %{changed: []}}
  end

  defmodule DanglingSource do
    @behaviour ReactiveDag.Source
    @impl true
    def id, do: :dangling
    @impl true
    def leaf_cells(_graph), do: ["no_such_cell"]
    @impl true
    def poll(_), do: {:ok, %{changed: []}}
  end

  defmodule FanOutSource do
    # feeds many leaves, computed from the graph (here: every :leaf op cell)
    @behaviour ReactiveDag.Source
    @impl true
    def id, do: :fanout
    @impl true
    def leaf_cells(graph) do
      graph.cells |> Map.values() |> Enum.filter(& &1.leaf?) |> Enum.map(& &1.id)
    end
    @impl true
    def poll(_), do: {:ok, %{changed: []}}
  end

  test "SCENARIO verify: passes when every source's leaf_cells resolve" do
    plan = Info.plan(Basic)
    assert :ok = ReactiveDag.Source.verify!([GoodSource], plan)
  end

  test "SCENARIO verify: raises naming the source and the dangling leaf" do
    plan = Info.plan(Basic)

    err =
      assert_raise ArgumentError, fn ->
        ReactiveDag.Source.verify!([DanglingSource], plan)
      end

    assert Exception.message(err) =~ "DanglingSource"
    assert Exception.message(err) =~ "no_such_cell"
  end

  test "SCENARIO verify: a FAN-OUT source (leaf_cells computed from the graph) passes" do
    plan = Info.plan(Basic)
    # Basic has one leaf (machines); fan-out returns exactly the graph's leaves.
    assert :ok = ReactiveDag.Source.verify!([FanOutSource], plan)
  end
end

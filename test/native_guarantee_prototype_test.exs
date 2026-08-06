defmodule ReactiveDag.NativeGuaranteePrototypeTest do
  @moduledoc """
  PROTOTYPE: the portal's `guarantee` authored FULLY NATIVE — inside a
  `ReactiveDag.Node` resource's `reactive` block, using the existing nested
  op-expression machinery (`ref`/`compose`) — instead of a peer DSL lowered by a
  separate `Graph.build`.

  Tests the claim from the layering discussion: a guarantee is a COMPOSITIONAL
  expression (one authored unit → many cells), and the question is whether Node
  can carry that shape natively. If a single resource lowers to the same
  `g:<id>` → set-expr → leaf sub-graph the peer DSL produces, then "guarantees
  within resources" works — the compositional 1:many is inherent, not a bar to
  living in a resource.
  """
  use ExUnit.Case, async: true

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false
    resources do
      allow_unregistered?(true)
    end
  end

  # the two leaves the guarantee reconciles (named nodes its refs point at).
  defmodule Entitled do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Simple, extensions: [ReactiveDag.Node]
    reactive do
      id :entitled
      op :leaf
      leaf? true
    end
  end

  defmodule Observed do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Simple, extensions: [ReactiveDag.Node]
    reactive do
      id :observed
      op :leaf
      leaf? true
    end
  end

  # THE GUARANTEE, native: the resource IS the g:edr_live cell (op :guarantee),
  # and its SET is a nested `compose :reconcile` over the two leaf refs. One
  # resource → the guarantee cell + its composed set cell (1:many, in-resource).
  defmodule EdrLive do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Simple, extensions: [ReactiveDag.Node]

    reactive do
      id :"g:edr_live"
      op :guarantee
      key_rule :all

      compose :reconcile do
        as :"g:edr_live/set"
        ref :entitled
        ref :observed
      end
    end
  end

  @resources [Entitled, Observed, EdrLive]

  test "a guarantee authored IN a resource lowers to the g:<id> → set → leaf sub-graph" do
    cells = ReactiveDag.Node.cells(EdrLive)
    ids = cells |> Enum.map(& &1.id) |> Enum.sort()

    # one resource → TWO cells: the guarantee + its composed reconcile set.
    assert ids == ["g:edr_live", "g:edr_live/set"]

    g = Enum.find(cells, &(&1.id == "g:edr_live"))
    assert g.op == :guarantee
    assert g.inputs == ["g:edr_live/set"]

    set = Enum.find(cells, &(&1.id == "g:edr_live/set"))
    assert set.op == :reconcile
    # the set reconciles the two by-name leaf refs.
    assert Enum.sort(set.inputs) == ["entitled", "observed"]
  end

  test "the whole native model assembles into a valid depth-ordered plan" do
    plan = ReactiveDag.Node.graph(@resources)

    # leaves at 0; set deeper than its leaves; guarantee deepest.
    assert plan.depths["entitled"] == 0
    assert plan.depths["observed"] == 0
    assert plan.depths["g:edr_live/set"] > plan.depths["entitled"]
    assert plan.depths["g:edr_live"] > plan.depths["g:edr_live/set"]

    # edges: leaves → set → guarantee.
    assert "g:edr_live/set" in plan.parents["entitled"]
    assert "g:edr_live" in plan.parents["g:edr_live/set"]
  end

  test "this matches the peer-DSL's cell shape (same IR, different front-end)" do
    # the peer DSL's guarantee_cells produces: g:edr_live (op :guarantee) over
    # g:edr_live/set (op :reconcile) over its leaf legs. The native version above
    # produces the SAME cells — so the guarantee CAN live in a resource; the peer
    # DSL is a choice, not a requirement.
    cells = ReactiveDag.Node.cells(EdrLive)
    shapes = Map.new(cells, &{&1.id, &1.op})
    assert shapes["g:edr_live"] == :guarantee
    assert shapes["g:edr_live/set"] == :reconcile
  end
end

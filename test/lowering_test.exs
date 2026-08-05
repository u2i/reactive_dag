defmodule ReactiveDag.LoweringTest do
  @moduledoc "The generic nested-expression → flat-cell walk, via host callbacks."
  use ExUnit.Case, async: true

  alias ReactiveDag.{Cell, Graph, Lowering}

  # A tiny host DSL as plain maps: %{ref: id} | %{leaf: id} |
  # %{op: type, legs: [...]}. Ids: parent/i (portal-style).
  defp cb do
    %{
      classify: fn
        %{ref: _} -> :ref
        %{leaf: _} -> :leaf
        %{op: _} -> :op
      end,
      legs: fn %{op: _, legs: legs} -> legs end,
      leg_id: fn parent, i, _leg -> "#{parent}/#{i}" end,
      ref_id: fn %{ref: id} -> id end,
      to_cell: fn id, node, input_ids ->
        case node do
          %{leaf: _} -> %Cell{id: id, op: :leaf, leaf?: true}
          %{op: type} -> %Cell{id: id, op: type, inputs: input_ids}
        end
      end
    }
  end

  test "a ref leg resolves to an existing id and emits NO cell" do
    node = %{op: :join, legs: [%{ref: "shared_a"}, %{ref: "shared_b"}]}
    {root, cells} = Lowering.walk("root", node, cb())

    assert root == "root"
    # only the root cell — the two refs are edges, not new cells.
    assert Enum.map(cells, & &1.id) == ["root"]
    assert hd(cells).inputs == ["shared_a", "shared_b"]
  end

  test "a nested op leg becomes an intermediate cell wired below the parent" do
    node = %{op: :map, legs: [%{op: :fold, legs: [%{ref: "leaf_x"}]}]}
    {_root, cells} = Lowering.walk("root", node, cb())
    by_id = Map.new(cells, &{&1.id, &1})

    # intermediate emitted at root/0, feeding root; it refs leaf_x.
    assert by_id["root/0"].op == :fold
    assert by_id["root/0"].inputs == ["leaf_x"]
    assert by_id["root"].inputs == ["root/0"]
    # dependency order: child before parent.
    assert Enum.map(cells, & &1.id) == ["root/0", "root"]
  end

  test "a leaf leg emits a terminal leaf cell" do
    node = %{op: :map, legs: [%{leaf: "l"}]}
    {_root, cells} = Lowering.walk("root", node, cb())
    by_id = Map.new(cells, &{&1.id, &1})

    assert by_id["root/0"].leaf?
    assert by_id["root/0"].op == :leaf
    assert by_id["root/0"].inputs == []
  end

  test "the lowered cells build a valid plan (refs resolve to real cells)" do
    # root/0 is an intermediate; its ref leaf_x must exist as a real leaf cell.
    node = %{op: :map, legs: [%{op: :fold, legs: [%{ref: "leaf_x"}]}]}
    {root, cells} = Lowering.walk("root", node, cb())
    leaf = %Cell{id: "leaf_x", op: :leaf, leaf?: true}

    plan = Graph.build([leaf | cells])
    assert plan.depths["leaf_x"] == 0
    assert plan.depths["root/0"] == 1
    assert plan.depths[root] == 2
  end
end

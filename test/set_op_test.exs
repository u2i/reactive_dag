defmodule ReactiveDag.SetOpTest do
  @moduledoc """
  ReactiveDag.SetOp — the generic set-based RecomputeStrategy. Dispatches cell.op
  → a host template registry; owns leaf + dirty-key scoping. Tested with fake
  templates (no DB — the SQL is the host's; this proves the dispatch frame).
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.{Cell, SetOp}

  setup do
    prev = Application.get_env(:reactive_dag, :set_op_templates)

    templates = %{
      reconcile: fn cell, dirty ->
        send(self(), {:reconcile, cell.id, cell.inputs, dirty})
        {:ok, ["changed-#{cell.id}"]}
      end,
      product: fn cell, dirty ->
        send(self(), {:product, cell.id, dirty})
        {:ok, []}
      end
    }

    Application.put_env(:reactive_dag, :set_op_templates, templates)
    on_exit(fn -> Application.put_env(:reactive_dag, :set_op_templates, prev) end)
    :ok
  end

  test "dispatches cell.op to its template, passing the cell + scoped dirty keys" do
    cell = %Cell{id: "g:x/set", op: :reconcile, inputs: ["e", "h"]}
    assert {:ok, ["changed-g:x/set"]} = SetOp.recompute(cell, ["k1", "k2"])
    assert_received {:reconcile, "g:x/set", ["e", "h"], ["k1", "k2"]}
  end

  test "a whole-cell claim (\"*\") scopes dirty to nil (recompute the whole cell)" do
    cell = %Cell{id: "prod", op: :product, inputs: ["a", "b", "f"]}
    assert {:ok, []} = SetOp.recompute(cell, ["*"])
    assert_received {:product, "prod", nil}
  end

  test "an op with no template is a logged no-op (not a crash)" do
    cell = %Cell{id: "weird", op: :nonesuch, inputs: []}
    assert {:ok, []} = SetOp.recompute(cell, ["k"])
  end

  test "a leaf passes its dirty keys through without a template" do
    cell = %Cell{id: "leaf", op: :leaf, leaf?: true}
    assert {:ok, ["k1", "k2"]} = SetOp.recompute(cell, ["k1", "k2"])
  end
end

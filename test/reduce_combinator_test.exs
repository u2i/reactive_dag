defmodule ReactiveDag.ReduceCombinatorTest do
  @moduledoc """
  The declarative `reduce` combinator on the reactive block: the author writes
  only read/group_by/key/into/upsert; Node.Recompute runs the fold (group →
  reduce → upsert → Op.put the changed keys). Escape hatch (`compute: Module`)
  stays for arbitrary recomputes.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.Node.Recompute

  defmodule Writer do
    @behaviour ReactiveDag.CoordinationWriter
    @impl true
    def put(cell_id, key, _opts), do: send(self(), {:put, cell_id, key})
    @impl true
    def delete(cell_id, keys), do: send(self(), {:delete, cell_id, keys})
  end

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false
    resources do
      allow_unregistered?(true)
    end
  end

  # a fold node: total `value` per {fund, fy} over the :lines input. `read`
  # returns the (host) payload items; `upsert` records the row + reports changed.
  defmodule Rollups do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Simple, extensions: [ReactiveDag.Node]

    reactive do
      id :rollups
      op :fold
      key_rule :all

      reduce over: :lines,
             read: fn :lines -> Process.get(:test_lines, []) end,
             group_by: fn line -> {line.fund, line.fy} end,
             key: fn {fund, fy} -> "#{fund}|#{fy}" end,
             into: fn {fund, fy}, lines ->
               %{fund: fund, fy: fy, total: lines |> Enum.map(& &1.value) |> Enum.sum()}
             end,
             upsert: fn key, row ->
               send(self(), {:upsert, key, row})
               # "changed" iff the total is non-zero (a toy criterion).
               row.total != 0
             end
    end
  end

  setup do
    prev = Application.get_env(:reactive_dag, :coordination_writer)
    Application.put_env(:reactive_dag, :coordination_writer, Writer)
    on_exit(fn -> Application.put_env(:reactive_dag, :coordination_writer, prev) end)
    :ok
  end

  test "the reduce spec rides in the cell's meta with over as an input edge" do
    cell = ReactiveDag.Node.to_cell(Rollups)
    assert cell.op == :fold
    assert cell.inputs == ["lines"]
    assert %ReactiveDag.Node.Reduce{over: :lines} = cell.meta.reduce
  end

  test "recompute runs the fold: group → reduce → upsert → Op.put the changed keys" do
    Process.put(:test_lines, [
      %{fund: "A", fy: "25", value: 10.0},
      %{fund: "A", fy: "25", value: 5.0},
      %{fund: "B", fy: "25", value: 0.0}
    ])

    cell = ReactiveDag.Node.to_cell(Rollups)
    {:ok, changed} = Recompute.recompute(cell, ["*"])

    # A|25 summed to 15 (changed); B|25 summed to 0 (upsert reported unchanged).
    assert changed == ["A|25"]
    assert_received {:upsert, "A|25", %{total: total_a}}
    assert total_a == 15.0
    assert_received {:upsert, "B|25", %{total: total_b}}
    assert total_b == 0.0
    # only the CHANGED key got a coordination put.
    assert_received {:put, "rollups", "A|25"}
    refute_received {:put, "rollups", "B|25"}
  end

  test "the compute-module escape hatch still works alongside reduce" do
    # a node WITHOUT a reduce still dispatches to meta.compute.
    defmodule EchoOp do
      @behaviour ReactiveDag.Op
      @impl true
      def recompute(_cell, keys), do: {:ok, keys}
    end

    cell = %ReactiveDag.Cell{id: "x", op: :map, meta: %{compute: EchoOp}}
    assert {:ok, ["k"]} = Recompute.recompute(cell, ["k"])
  end
end

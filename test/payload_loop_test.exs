defmodule ReactiveDag.PayloadLoopTest do
  @moduledoc """
  The unified node shape: a resource that IS the node AND its own payload table.
  Its `reduce` omits `upsert:` — the library writes the row into the resource
  itself (`ReactiveDag.Node.Payload`) and reports change. This is the canonical
  way to author a stateful node; writing into a SEPARATE resource is the explicit
  deviation (a custom `upsert:`), not the default.
  """
  use ReactiveDag.PostgresCase

  alias ReactiveDag.Fixtures.PayloadLoopGraph
  alias ReactiveDag.Fixtures.PayloadLoopGraph.{DmrRow, FlowMonth}

  setup_all do
    migrate!(PayloadLoopGraph.Domain)
    :ok
  end

  setup do
    pid = Ecto.Adapters.SQL.Sandbox.start_owner!(ReactiveDag.TestRepo, shared: true)
    on_exit(fn -> Ecto.Adapters.SQL.Sandbox.stop_owner(pid) end)

    truncate(PayloadLoopGraph.tables())

    for {p, m, f, i} <- [{"north", "2024-01", 1.0, "a"}, {"north", "2024-01", 3.0, "b"}, {"south", "2024-01", 2.0, "c"}] do
      DmrRow |> Ash.Changeset.for_create(:create, %{id: i, plant: p, month: m, flow: f}) |> Ash.create!()
    end

    :ok
  end

  defp cell, do: ReactiveDag.Node.graph([DmrRow, FlowMonth]).cells["flow_month"]

  test "the lib writes the node's own payload (no upsert:) and reports changed keys" do
    cell = cell()

    # recompute with no host upsert callback — the lib closes the loop into FlowMonth
    {:ok, changed} = ReactiveDag.Node.Recompute.recompute(cell, ["*"])

    assert Enum.sort(changed) == ["north|2024-01", "south|2024-01"]

    # the ROWS actually landed in the node's own resource
    rows = FlowMonth |> Ash.read!() |> Map.new(&{&1.key, &1})
    # (1+3)/2
    assert rows["north|2024-01"].avg_flow == 2.0
    assert rows["south|2024-01"].avg_flow == 2.0
    assert rows["north|2024-01"].plant == "north"
  end

  test "a second identical recompute is a no-op (change detection): no keys reported changed" do
    cell = cell()

    {:ok, _first} = ReactiveDag.Node.Recompute.recompute(cell, ["*"])
    {:ok, second} = ReactiveDag.Node.Recompute.recompute(cell, ["*"])

    # nothing changed the second time → no keys, so the drain wouldn't re-dirty parents
    assert second == []
  end

  test "meta.resource is what the lib writes into (the field is USED, not just carried)" do
    # This used to also assert `cell.meta.reduce.upsert == nil` — "no override, so
    # the lib owns the write". There is no `upsert:` slot to override any more, so
    # the library owning the write is structural rather than a property of this
    # node: `meta.resource` is the only destination there is.
    assert cell().meta.resource == FlowMonth
  end
end

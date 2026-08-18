defmodule ReactiveDag.PayloadLoopTest do
  @moduledoc """
  The unified node shape: a resource that IS the node AND its own payload table.
  Its `reduce` omits `upsert:` — the library writes the row into the resource
  itself (`ReactiveDag.Node.Payload`) and reports change. This is the canonical
  way to author a stateful node; writing into a SEPARATE resource is the explicit
  deviation (a custom `upsert:`), not the default.
  """
  use ExUnit.Case, async: false

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false
    resources do
      allow_unregistered? true
    end
  end

  # the input the fold reads (raw per-day flow rows) — a leaf NODE, so the
  # rollup's declarative read can resolve its resource at assembly.
  defmodule DmrRow do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Ets, extensions: [ReactiveDag.Node]
    ets do private?(true) end
    attributes do
      attribute :id, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :plant, :string, public?: true
      attribute :month, :string, public?: true
      attribute :flow, :float, public?: true
    end
    actions do
      defaults [:read, :destroy]
      create :create do accept([:id, :plant, :month, :flow]) end
    end

    reactive do
      id :dmr_rows
      op :source
      leaf? true
    end
  end

  # THE UNIFIED NODE: an AshPostgres-shaped (here Ets) resource that is BOTH the
  # reactive node AND its own payload table. No `upsert:` — the lib closes the loop.
  defmodule FlowMonth do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Ets, extensions: [ReactiveDag.Node]
    ets do private?(true) end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :plant, :string, public?: true
      attribute :month, :string, public?: true
      attribute :avg_flow, :float, public?: true
    end

    identities do identity :by_key, [:key], pre_check_with: ReactiveDag.PayloadLoopTest.Domain end

    actions do
      defaults [:read, :destroy]
      create :upsert do upsert?(true); upsert_identity(:by_key); accept([:key, :plant, :month, :avg_flow]) end
    end

    reactive do
      id :flow_month
      op :fold
      key_rule :all

      # NB: no `read:` (the library reads :dmr_rows itself) and no `upsert:` —
      # into returns the row, the library writes it into THIS resource (keyed
      # by :key) and does the Op.put. group/key/into stay fn escapes here.
      reduce over: :dmr_rows,
             group_by: &ReactiveDag.PayloadLoopTest.group/1,
             key: &ReactiveDag.PayloadLoopTest.key/1,
             into: &ReactiveDag.PayloadLoopTest.into/2
    end
  end

  def group(r), do: {r.plant, r.month}
  def key({plant, month}), do: "#{plant}|#{month}"

  def into({plant, month}, rows) do
    avg = rows |> Enum.map(& &1.flow) |> then(&(Enum.sum(&1) / length(&1)))
    %{key: "#{plant}|#{month}", plant: plant, month: month, avg_flow: avg}
  end

  # a no-op coordination writer — this test exercises the PAYLOAD loop; the

  setup do

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
    assert rows["north|2024-01"].avg_flow == 2.0    # (1+3)/2
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

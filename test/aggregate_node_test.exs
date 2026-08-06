defmodule ReactiveDag.AggregateNodeTest do
  @moduledoc """
  The pure-Ash-query `aggregate` combinator: the datastore groups + aggregates a
  relationship (avg/count) in one Ash query — no raw rows cross into the BEAM. The
  node's resource IS the group grain; each parent row's aggregate values are its
  payload.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.Node.Recompute

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false
    resources do
      allow_unregistered? true
    end
  end

  defmodule NullWriter do
    @behaviour ReactiveDag.CoordinationWriter
    @impl true
    def put(_c, _k, _o), do: :ok
    @impl true
    def tombstone(_c, _k), do: :ok
    @impl true
    def delete(_c, _k), do: :ok
  end

  # the GROUP GRAIN resource: one row per (plant, month). It has_many raw readings.
  defmodule PlantMonth do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Ets, extensions: [ReactiveDag.Node]
    ets do private?(true) end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :avg_flow, :float, public?: true
      attribute :day_count, :integer, public?: true
    end

    relationships do
      has_many :readings, ReactiveDag.AggregateNodeTest.Reading do
        source_attribute :key
        destination_attribute :pm_key
      end
    end

    identities do identity :by_key, [:key], pre_check_with: ReactiveDag.AggregateNodeTest.Domain end

    actions do
      defaults [:read, :destroy]
      create :seed do accept([:key]) end
      create :upsert do upsert?(true); upsert_identity(:by_key); accept([:key, :avg_flow, :day_count]) end
    end

    # PURE-ASH aggregate: the datastore avgs/counts the :readings relationship.
    reactive do
      op :fold
      key_rule :all
      aggregate over: :readings,
                avg: [flow: :avg_flow],
                count: :day_count
    end
  end

  defmodule Reading do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Ets
    ets do private?(true) end
    attributes do
      attribute :id, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :pm_key, :string, public?: true
      attribute :flow, :float, public?: true
    end
    actions do defaults [:read, :destroy]; create :create do accept([:id, :pm_key, :flow]) end end
  end

  setup do
    prev = Application.get_env(:reactive_dag, :coordination_writer)
    Application.put_env(:reactive_dag, :coordination_writer, NullWriter)
    on_exit(fn -> Application.put_env(:reactive_dag, :coordination_writer, prev) end)

    # two groups; readings belong to them by pm_key.
    for k <- ["north|2024-01", "south|2024-01"],
        do: PlantMonth |> Ash.Changeset.for_create(:seed, %{key: k}) |> Ash.create!()

    for {id, pm, f} <- [
          {"a", "north|2024-01", 1.0}, {"b", "north|2024-01", 3.0},
          {"c", "south|2024-01", 2.0}
        ],
        do: Reading |> Ash.Changeset.for_create(:create, %{id: id, pm_key: pm, flow: f}) |> Ash.create!()

    :ok
  end

  test "the datastore aggregates the relationship; each group row gets its avg/count" do
    cell = ReactiveDag.Node.to_cell(PlantMonth)
    # the aggregate spec rode into meta (no reduce/join, no input edge from `over`)
    assert cell.meta.aggregate.over == :readings
    assert cell.inputs == []       # `over` is a relationship, NOT a DAG input edge

    {:ok, changed} = Recompute.recompute(cell, ["*"])
    assert Enum.sort(changed) == ["north|2024-01", "south|2024-01"]

    rows = PlantMonth |> Ash.read!() |> Map.new(&{&1.key, &1})
    assert rows["north|2024-01"].avg_flow == 2.0     # (1+3)/2, computed in the query
    assert rows["north|2024-01"].day_count == 2
    assert rows["south|2024-01"].avg_flow == 2.0
    assert rows["south|2024-01"].day_count == 1
  end

  test "a re-run with unchanged data reports no changed keys (change detection)" do
    cell = ReactiveDag.Node.to_cell(PlantMonth)
    {:ok, _} = Recompute.recompute(cell, ["*"])
    {:ok, second} = Recompute.recompute(cell, ["*"])
    assert second == []
  end
end

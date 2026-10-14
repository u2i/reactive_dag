defmodule ReactiveDag.AggregateNodeTest do
  @moduledoc """
  The pure-Ash-query `aggregate` combinator: the datastore groups + aggregates a
  relationship (avg/count) in one Ash query — no raw rows cross into the BEAM. The
  node's resource is the group's resource (one row per group); each parent row's aggregate values are its
  payload.
  """
  use ReactiveDag.PostgresCase

  alias ReactiveDag.Fixtures.AggregateGraph
  alias ReactiveDag.Fixtures.AggregateGraph.{FundFy, FundLine, PlantMonth, Reading}

  alias ReactiveDag.Node.Recompute

  setup_all do
    migrate!(AggregateGraph.Domain)
    :ok
  end

  setup do
    pid = Ecto.Adapters.SQL.Sandbox.start_owner!(ReactiveDag.TestRepo, shared: true)
    on_exit(fn -> Ecto.Adapters.SQL.Sandbox.stop_owner(pid) end)

    truncate(AggregateGraph.tables())
    # two groups; readings belong to them by pm_key.
    for k <- ["north|2024-01", "south|2024-01"],
        do: PlantMonth |> Ash.Changeset.for_create(:seed, %{key: k}) |> Ash.create!()

    for {id, pm, f} <- [
          {"a", "north|2024-01", 1.0},
          {"b", "north|2024-01", 3.0},
          {"c", "south|2024-01", 2.0}
        ],
        do: Reading |> Ash.Changeset.for_create(:create, %{id: id, pm_key: pm, flow: f}) |> Ash.create!()

    :ok
  end

  test "the datastore aggregates the relationship; each group row gets its avg/count" do
    cell = ReactiveDag.Node.to_cell(PlantMonth)
    # the aggregate spec rode into meta (no reduce/join, no input edge from `over`)
    assert cell.meta.aggregate.over == :readings
    # `over` is a relationship, NOT a DAG input edge
    assert cell.inputs == []

    {:ok, changed} = Recompute.recompute(cell, ["*"])
    assert Enum.sort(changed) == ["north|2024-01", "south|2024-01"]

    rows = PlantMonth |> Ash.read!() |> Map.new(&{&1.key, &1})
    # (1+3)/2, computed in the query
    assert rows["north|2024-01"].avg_flow == 2.0
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

  # ── parity with the in-BEAM fold ──────────────────────────────────────────
  #
  # `aggregate` and `reduce into:` are the SAME vocabulary at two rungs: same
  # kinds, same `[src: dest]` spelling, same nil semantics — and now the same
  # key rules. What differs is only WHO aggregates (Postgres vs the BEAM).

  test "an aggregate node is IDENTITY-KEYED by a composite primary key, like reduce" do
    for {f, y} <- [{"gf", "2025"}, {"water", "2026"}] do
      FundFy |> Ash.Changeset.for_create(:seed, %{fund: f, fy: y}) |> Ash.create!()
    end

    for {k, f, amt} <- [{"l1", "gf", 10.0}, {"l2", "gf", 5.0}, {"l3", "water", 7.0}] do
      FundLine |> Ash.Changeset.for_create(:create, %{key: k, fund: f, amount: amt}) |> Ash.create!()
    end

    cell = ReactiveDag.Node.to_cell(FundFy)

    # no payload_key: the row IS its identity (the same rule reduce follows)
    assert cell.meta[:payload_key] == nil
    assert cell.meta.identity_fields == [:fund, :fy]

    {:ok, changed} = Recompute.recompute(cell, ["*"])

    # the cell key is the identity SERIALIZED in primary-key order
    assert Enum.sort(changed) == ["gf|2025", "water|2026"]

    rows = FundFy |> Ash.read!() |> Map.new(&{{&1.fund, &1.fy}, &1})
    assert %{total: 15.0, n: 2} = rows[{"gf", "2025"}]
    assert rows[{"water", "2026"}].total == 7.0

    # change detection still per-identity
    {:ok, []} = Recompute.recompute(cell, ["*"])
  end

  test "an aggregate writing a non-existent attribute is a compile-time error" do
    alias ReactiveDag.Node.Verifiers.VerifyReactive

    defmodule BadDest do
      use Ash.Resource,
        domain: ReactiveDag.Fixtures.AggregateGraph.Domain,
        data_layer: Ash.DataLayer.Ets,
        extensions: [ReactiveDag.Node]

      ets do
        private?(true)
      end

      attributes do
        attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
        attribute :avg_flow, :float, public?: true
      end

      relationships do
        has_many :readings, ReactiveDag.Fixtures.AggregateGraph.Reading do
          source_attribute :key
          destination_attribute :pm_key
        end
      end

      actions do
        defaults [:read]
      end

      reactive do
        id(:bad_dest)
        key_rule(:all)
        # :no_such_column is not an attribute on this resource
        aggregate(over: :readings, avg: [flow: :avg_flow], count: :no_such_column)
      end
    end

    assert {:error, %Spark.Error.DslError{message: msg}} =
             VerifyReactive.verify(BadDest.spark_dsl_config())

    assert msg =~ ":no_such_column"
    assert msg =~ "no such attribute"
  end

  test "aggregate and reduce share ONE kinds list — they cannot drift" do
    # the DSL's aggregate kinds ARE the in-BEAM fold kinds
    assert ReactiveDag.Node.Recompute.Declarative.fold_kinds() ==
             [:count, :sum, :avg, :min, :max, :first]

    # every kind the fold accepts is a real option on the `aggregate` entity
    agg_opts =
      ReactiveDag.Node.sections()
      |> hd()
      |> Map.get(:entities)
      |> Enum.find(&(&1.name == :aggregate))
      |> Map.get(:schema)
      |> Keyword.keys()

    for kind <- ReactiveDag.Node.Recompute.Declarative.fold_kinds() do
      assert kind in agg_opts, "aggregate is missing the #{inspect(kind)} kind"
    end
  end
end

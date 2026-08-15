defmodule ReactiveDag.RowsAggregateTest do
  @moduledoc """
  Counting is pushed into the datastore (#76).

  `status_histogram/1` used to be `all() |> Enum.frequencies_by(& &1.status)` —
  a full table read, decoding every payload column, reduced to a handful of
  integers. For a node whose rows carry a JSON blob, decoding it to discard it
  *was* the read. The consumer measured 688ms → 181ms across a 33-cell graph.

  Ash has no arbitrary `GROUP BY … -> rows` (it is why `reduce` folds in the
  BEAM at all), so the histogram is a `DISTINCT` on the status column plus one
  `COUNT` per value: more round trips, no rows.

  The behaviour must not change, which is what most of these assert.

  Note the win is data-layer dependent, and this suite cannot demonstrate it:
  AshPostgres implements `run_aggregate_query` as SQL, while `Ash.DataLayer.Ets`
  implements it as `run_query` + reduce in memory (`ets.ex:336`). So on Ets these
  functions cost slightly MORE. The tests pin the contract; the win lives where
  the rows do.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.Node.Rows

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      allow_unregistered?(true)
    end
  end

  defmodule Docs do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Ets, extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :status, :string, public?: true
      attribute :payload, :map, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        accept([:key, :status, :payload])
      end
    end

    reactive do
      id(:docs)
      leaf?(true)
    end
  end

  # a rollup: no status column at all
  defmodule Totals do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Ets, extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :total, :float, public?: true
    end

    actions do
      defaults [:read, :destroy]
      create :upsert, upsert?: true, accept: [:key, :total]
    end

    reactive do
      id(:totals)
      leaf?(true)
    end
  end

  # composite PK — its cell key is a "|"-join built in the BEAM, which is the
  # one thing that cannot be pushed down
  defmodule Rollup do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Ets, extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute :fund, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :fy, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :status, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]
      create :upsert, upsert?: true, accept: [:fund, :fy, :status]
    end

    reactive do
      id(:rollup)
      leaf?(true)
    end
  end

  defmodule Elsewhere do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Simple,
      extensions: [ReactiveDag.Node]

    reactive do
      id(:elsewhere)
      leaf?(true)
    end
  end

  setup do
    for r <- [Docs, Totals, Rollup], row <- Ash.read!(r), do: Ash.destroy!(row)

    for {k, st} <- [
          {"a", "present"},
          {"b", "present"},
          {"c", "failing"},
          {"d", nil}
        ] do
      Docs
      |> Ash.Changeset.for_create(:upsert, %{key: k, status: st, payload: %{"blob" => k}})
      |> Ash.create!()
    end

    for {k, t} <- [{"x", 1.0}, {"y", 2.0}] do
      Totals |> Ash.Changeset.for_create(:upsert, %{key: k, total: t}) |> Ash.create!()
    end

    for {f, y, st} <- [{"gf", "2025", "present"}, {"water", "2025", "failing"}] do
      Rollup |> Ash.Changeset.for_create(:upsert, %{fund: f, fy: y, status: st}) |> Ash.create!()
    end

    :ok
  end

  defp cell(mod) do
    [c] = ReactiveDag.Node.cells(mod)
    c
  end

  describe "the answers are unchanged" do
    test "status_histogram counts each status, including nil" do
      assert Rows.status_histogram(cell(Docs)) == %{"present" => 2, "failing" => 1, nil => 1}
    end

    test "a node with no status column reports every row under nil" do
      assert Rows.status_histogram(cell(Totals)) == %{nil => 2}
    end

    test "an empty node has an empty histogram, not %{nil => 0}" do
      for row <- Ash.read!(Totals), do: Ash.destroy!(row)

      assert Rows.status_histogram(cell(Totals)) == %{}
    end

    test "key_count/1 agrees with the histogram's total" do
      assert Rows.key_count(cell(Docs)) == 4
      assert Rows.key_count(cell(Docs)) == Rows.status_histogram(cell(Docs)) |> Map.values() |> Enum.sum()
    end

    test "keys_by_status filters, sorts and caps" do
      assert Rows.keys_by_status(cell(Docs), ["present"]) == ["a", "b"]
      assert Rows.keys_by_status(cell(Docs), ["present"], limit: 1) == ["a"]
      assert Rows.keys_by_status(cell(Docs), ["failing"]) == ["c"]
    end

    test "keys_by_status can ask for the nil status" do
      assert Rows.keys_by_status(cell(Docs), [nil]) == ["d"]
    end

    test "...and for a mix of nil and real statuses" do
      assert Rows.keys_by_status(cell(Docs), [nil, "failing"]) == ["c", "d"]
    end

    test "asking for a status nothing has is empty, not an error" do
      assert Rows.keys_by_status(cell(Docs), ["exploded"]) == []
    end

    test "asking for no statuses at all is empty" do
      assert Rows.keys_by_status(cell(Docs), []) == []
    end

    test "an identity-keyed node still yields its serialized cell keys" do
      # the filter pushes down; the "|"-join is still built in the BEAM, because
      # no datastore knows this node's key grammar
      assert Rows.keys_by_status(cell(Rollup), ["failing"]) == ["water|2025"]
      assert Rows.status_histogram(cell(Rollup)) == %{"present" => 1, "failing" => 1}
    end

    test "a node that keeps no rows here reports empty" do
      assert Rows.key_count(cell(Elsewhere)) == 0
      assert Rows.status_histogram(cell(Elsewhere)) == %{}
      assert Rows.keys_by_status(cell(Elsewhere), ["failing"]) == []
    end
  end

  describe "the point of the change" do
    test "the reduction is expressed as a query, not as a fold over loaded rows" do
      # What can be asserted portably is the SHAPE: these go through
      # `Ash.count!` and a filtered read, not through `Rows.all/1`. Whether that
      # saves anything depends on the data layer —
      #
      #   * AshPostgres implements `run_aggregate_query` as SQL, so a COUNT never
      #     materialises a row. That is where the consumer measured 688ms → 181ms.
      #   * Ash.DataLayer.Ets implements it as `run_query` + reduce in memory
      #     (`ets.ex:336`), so on this suite it saves nothing and costs a little.
      #
      # So this test pins the contract, and the win lives where the rows do.
      for i <- 1..50 do
        Docs
        |> Ash.Changeset.for_create(:upsert, %{key: "big-#{i}", status: "present"})
        |> Ash.create!()
      end

      c = cell(Docs)

      assert Rows.key_count(c) == 54
      assert Rows.status_histogram(c)["present"] == 52
    end

    test "a sample fetches only what it needs" do
      for i <- 1..200 do
        Docs
        |> Ash.Changeset.for_create(:upsert, %{key: "f-#{i}", status: "failing"})
        |> Ash.create!()
      end

      # 201 failing rows exist; the limit is applied by the query, so the sample
      # decodes 5 rather than filtering 201 in the BEAM
      assert length(Rows.keys_by_status(cell(Docs), ["failing"], limit: 5)) == 5
    end
  end

  describe "Insights still agrees" do
    test "cell_status carries the same counts it always did" do
      status = ReactiveDag.Insights.cell_status(ReactiveDag.Node.graph([Docs]), "docs")

      assert status.key_count == 4
      assert status.statuses == %{"present" => 2, "failing" => 1, nil => 1}
      assert status.failing_sample == ["c"]
    end
  end
end

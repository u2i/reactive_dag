defmodule ReactiveDag.JoinExpandCombinatorTest do
  @moduledoc """
  The `join` combinator's fn escape hatches (computed side keys, fn key with a
  prefix, fn into) and the `expand:` slot — the group → many-rows shape, each row
  self-keyed.

  Both nodes write their OWN rows. They used to capture writes through an
  `upsert:` override, which is gone: a node owns its rows, so the assertions read
  the payload table the library wrote.
  """
  use ReactiveDag.PostgresCase

  alias ReactiveDag.Fixtures.JoinExpandGraph
  alias ReactiveDag.Fixtures.JoinExpandGraph.{Fiscal, KeylessExpand, Variance, YearBuckets}

  alias ReactiveDag.Node.Recompute

  setup_all do
    migrate!(JoinExpandGraph.Domain)
    :ok
  end

  setup do
    pid = Ecto.Adapters.SQL.Sandbox.start_owner!(ReactiveDag.TestRepo, shared: true)
    on_exit(fn -> Ecto.Adapters.SQL.Sandbox.stop_owner(pid) end)

    truncate(JoinExpandGraph.tables())
    Process.register(self(), ReactiveDag.JoinExpandCombinatorTest)

    for {k, kind, acct, amount, fy} <- [
          {"b1", "budget", "A1010", 100.0, 2025},
          {"b2", "budget", "A2020", 50.0, 2025},
          {"a1", "actual", "A1010", 90.0, 2025},
          {"a2", "actual", "A9090", 5.0, 2026}
        ] do
      Fiscal
      |> Ash.Changeset.for_create(:create, %{k: k, kind: kind, acct: acct, amount: amount, fy: fy})
      |> Ash.create!()
    end

    :ok
  end

  test "fn sides split one input; left join; computed columns; prefixed fn keys" do
    plan = ReactiveDag.Node.graph([Fiscal, Variance])
    {:ok, changed} = Recompute.recompute(plan.cells["variance"], ["*"])

    assert Enum.sort(changed) == ["va|A1010", "va|A2020"]

    rows = Variance |> Ash.read!() |> Map.new(&{&1.key, &1})

    assert %{budget: 100.0, actual: 90.0, variance: 10.0} = rows["va|A1010"]
    # a missing right is a gap, not an error
    assert %{budget: 50.0, actual: nil, variance: 50.0} = rows["va|A2020"]
    # right-only accounts don't emit on a left join
    refute Map.has_key?(rows, "va|A9090")
  end

  test "expand: one group → many self-keyed rows" do
    plan = ReactiveDag.Node.graph([Fiscal, YearBuckets])
    {:ok, changed} = Recompute.recompute(plan.cells["year_buckets"], ["*"])

    assert Enum.sort(changed) == ["2025|actual", "2025|budget", "2026|actual"]

    rows = YearBuckets |> Ash.read!() |> Map.new(&{&1.key, &1.n})
    assert rows["2025|budget"] == 2
    assert rows["2025|actual"] == 1
    assert rows["2026|actual"] == 1
  end

  test "an expand: row WITHOUT its own :key raises instructively" do
    plan = ReactiveDag.Node.graph([Fiscal, KeylessExpand])

    assert_raise RuntimeError, ~r/must carry its own :key/s, fn ->
      Recompute.recompute(plan.cells["keyless_expand"], ["*"])
    end
  end
end

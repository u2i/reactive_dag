defmodule ReactiveDag.DirtiesOnTest do
  @moduledoc """
  `dirties_on` (#39): an ordinary Ash write triggers the cascade.

  Before this, a leaf only became dirty by a host calling
  `Frontier.mark_dirty/3` by hand at every write site, or by a `Source` poll —
  and a missed call is silent staleness, the same failure class as #37 from the
  other end.

  The mark runs as an `after_action` change, so it is INSIDE the write's
  transaction: a rolled-back write leaves no dirty key, and a committed one
  always leaves one. A notifier could not promise that — Ash dispatches
  notifications after commit, so a crash in between would lose the mark.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.{Drain, Frontier}

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      allow_unregistered?(true)
    end
  end

  defmodule Expenses do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :category, :string, public?: true
      attribute :amount, :float, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :create do
        accept([:key, :category, :amount])
      end

      update :revise do
        accept([:amount])
      end

      update :recategorise do
        accept([:category])
      end
    end

    reactive do
      id(:expenses)
      op(:source)
      leaf?(true)
      # THE FEATURE: writes here mark this cell dirty, with no host wiring
      dirties_on([:create, :update, :destroy])
    end
  end

  # a composite-PK resource: the cell key is the identity serialization
  defmodule Rollups do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute :fund, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :fy, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :total, :float, public?: true
    end

    actions do
      # :destroy exists for test cleanup, but is NOT in `dirties_on` below —
      # which is what the "only declared action types mark" test relies on.
      defaults [:read, :destroy]

      create :create do
        accept([:fund, :fy, :total])
      end
    end

    reactive do
      id(:rollups)
      op(:source)
      leaf?(true)
      dirties_on([:create])
    end
  end

  # opt-in: no `dirties_on`, so writes mark nothing
  defmodule Quiet do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
    end

    actions do
      defaults [:read]

      create :create do
        accept([:key])
      end
    end

    reactive do
      id(:quiet)
      op(:source)
      leaf?(true)
    end
  end

  defmodule CategoryTotals do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :category, :string, public?: true
      attribute :total, :float, public?: true
      attribute :n, :integer, public?: true
    end

    identities do
      identity :by_key, [:key], pre_check_with: ReactiveDag.DirtiesOnTest.Domain
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        upsert_identity(:by_key)
        accept([:key, :category, :total, :n])
      end
    end

    reactive do
      id(:category_totals)
      op(:fold)

      recompute_by :category, to: :expenses, from: :category
      reduce into: [sum: [amount: :total], count: :n]
    end
  end

  defmodule FakeRepo do
    def start_link, do: Agent.start_link(fn -> %{} end, name: __MODULE__)

    # stores the PRIOR too, so claim can return it — the whole point of #60
    def query!("INSERT INTO " <> _, params) do
      params
      |> Enum.chunk_every(5)
      |> Enum.each(fn [cell, key, _r, _t, prior] ->
        # ON CONFLICT DO NOTHING: the FIRST snapshot wins
        Agent.update(__MODULE__, fn m -> Map.put_new(m, {cell, key}, prior) end)
      end)

      %{rows: []}
    end

    def query!("SELECT DISTINCT cell_id" <> _, _params) do
      ids = Agent.get(__MODULE__, & &1) |> Map.keys() |> Enum.map(&elem(&1, 0)) |> Enum.uniq()
      %{rows: Enum.map(ids, &[&1])}
    end

    def query!("DELETE FROM " <> _, [cell]) do
      rows =
        Agent.get_and_update(__MODULE__, fn m ->
          {mine, rest} = Enum.split_with(m, fn {{c, _}, _} -> c == cell end)
          {Enum.map(mine, fn {{_c, k}, prior} -> [k, prior] end), Map.new(rest)}
        end)

      %{rows: rows}
    end

    def query!("SELECT COUNT" <> _, _params), do: %{rows: [[Agent.get(__MODULE__, &map_size/1)]]}

    # what the frontier currently holds, for assertions
    def dirty, do: Agent.get(__MODULE__, &Map.keys/1) |> Enum.sort()
  end


  setup do
    start_supervised!(%{id: FakeRepo, start: {FakeRepo, :start_link, []}})
    prev_repo = Application.get_env(:reactive_dag, :repo)
    Application.put_env(:reactive_dag, :repo, FakeRepo)

    on_exit(fn ->
      Application.put_env(:reactive_dag, :repo, prev_repo)
    end)

    # the ETS tables are shared (not `private?`, since writes may come from
    # other processes), so start each test from a known-empty state — including
    # the frontier, which the destroys below would otherwise re-dirty.
    Expenses |> Ash.read!() |> Enum.each(&Ash.destroy!/1)
    CategoryTotals |> Ash.read!() |> Enum.each(&Ash.destroy!/1)
    Rollups |> Ash.read!() |> Enum.each(&Ash.destroy!/1)
    for cell <- ["expenses", "rollups", "quiet", "category_totals"], do: Frontier.claim(cell)
    :ok
  end

  test "a CREATE marks the written record's key — no host wiring" do
    Expenses
    |> Ash.Changeset.for_create(:create, %{key: "e1", category: "travel", amount: 10.0})
    |> Ash.create!()

    assert FakeRepo.dirty() == [{"expenses", "e1"}]
  end

  test "an UPDATE marks it too" do
    e =
      Expenses
      |> Ash.Changeset.for_create(:create, %{key: "e1", category: "travel", amount: 10.0})
      |> Ash.create!()

    Frontier.claim("expenses")
    assert FakeRepo.dirty() == []

    e |> Ash.Changeset.for_update(:revise, %{amount: 20.0}) |> Ash.update!()
    assert FakeRepo.dirty() == [{"expenses", "e1"}]
  end

  test "a DESTROY marks the vanished key — which is exactly what downstream needs" do
    e =
      Expenses
      |> Ash.Changeset.for_create(:create, %{key: "e1", category: "travel", amount: 10.0})
      |> Ash.create!()

    Frontier.claim("expenses")

    Ash.destroy!(e)

    # the row is gone, but its key is what reprices the group it left
    assert FakeRepo.dirty() == [{"expenses", "e1"}]
  end

  test "the key is the IDENTITY serialization for a composite primary key" do
    Rollups
    |> Ash.Changeset.for_create(:create, %{fund: "gf", fy: "2025", total: 1.0})
    |> Ash.create!()

    assert FakeRepo.dirty() == [{"rollups", "gf|2025"}]
  end

  test "OPT-IN: a resource without `dirties_on` marks nothing" do
    Quiet |> Ash.Changeset.for_create(:create, %{key: "q1"}) |> Ash.create!()
    assert FakeRepo.dirty() == []
  end

  test "only the DECLARED action types mark" do
    # Rollups declares `dirties_on [:create]` only, though it HAS a destroy
    r =
      Rollups
      |> Ash.Changeset.for_create(:create, %{fund: "w", fy: "2026", total: 1.0})
      |> Ash.create!()

    assert FakeRepo.dirty() == [{"rollups", "w|2026"}]
    Frontier.claim("rollups")

    # the destroy is not in the list, so it marks nothing
    Ash.destroy!(r)
    assert FakeRepo.dirty() == []
  end

  test "end to end: writing an expense makes the next drain recompute its category" do
    plan = ReactiveDag.Node.graph([Expenses, CategoryTotals])

    # NO Frontier.mark_dirty call anywhere — the write is the trigger
    Expenses
    |> Ash.Changeset.for_create(:create, %{key: "e1", category: "travel", amount: 100.0})
    |> Ash.create!()

    {:ok, report} =
      Drain.run(plan, recompute: ReactiveDag.Node.Recompute, key_rule: ReactiveDag.Node.KeyRule)

    steps = Map.new(report.steps, &{&1.cell, &1})
    assert steps["expenses"].claimed == ["e1"]
    assert steps["category_totals"].claimed == ["travel"]

    assert (CategoryTotals |> Ash.get!("travel")).total == 100.0
  end
  defp drain(plan),
    do: Drain.run(plan, recompute: ReactiveDag.Node.Recompute, key_rule: ReactiveDag.Node.KeyRule)

  describe "frontier snapshots (#60)" do
    # `dirties_on` records the row AS IT WAS at mark time, so a parent derives
    # its claim from what the row was — the only thing that survives a delete,
    # and the only thing that names where a moved row came from.
    test "the snapshot rides on the frontier row" do
      Expenses
      |> Ash.Changeset.for_create(:create, %{key: "e1", category: "travel", amount: 10.0})
      |> Ash.create!()

      assert [{"e1", prior}] = Frontier.claim_with_priors("expenses")

      # jsonb, so string keys — and every public attribute, not just the unit's
      assert prior["category"] == "travel"
      assert prior["amount"] == 10.0
      assert prior["key"] == "e1"
    end

    test "a DELETED row still names its unit — the claim stays precise" do
      plan = ReactiveDag.Node.graph([Expenses, CategoryTotals])

      Expenses
      |> Ash.Changeset.for_create(:create, %{key: "e1", category: "travel", amount: 100.0})
      |> Ash.create!()

      {:ok, _} = drain(plan)
      assert (CategoryTotals |> Ash.get!("travel")).total == 100.0

      Expenses |> Ash.get!("e1") |> Ash.destroy!()
      {:ok, report} = drain(plan)

      steps = Map.new(report.steps, &{&1.cell, &1})

      # a live lookup could not name the group of a row that is gone; the
      # snapshot can, so this is ["travel"] rather than the ["*"] degradation
      assert steps["category_totals"].claimed == ["travel"]
    end

    test "a MOVED row claims BOTH units — where it went AND where it came from" do
      plan = ReactiveDag.Node.graph([Expenses, CategoryTotals])

      Expenses
      |> Ash.Changeset.for_create(:create, %{key: "e1", category: "meals", amount: 40.0})
      |> Ash.create!()

      {:ok, _} = drain(plan)
      assert (CategoryTotals |> Ash.get!("meals")).total == 40.0

      # the live row says "travel"; only the snapshot says "meals", and meals is
      # the one that would otherwise silently keep counting a row it no longer has
      Expenses
      |> Ash.get!("e1")
      |> Ash.Changeset.for_update(:recategorise, %{category: "travel"})
      |> Ash.update!()

      {:ok, report} = drain(plan)
      steps = Map.new(report.steps, &{&1.cell, &1})

      assert Enum.sort(steps["category_totals"].claimed) == ["meals", "travel"]
      assert (CategoryTotals |> Ash.get!("travel")).total == 40.0
    end

    test "coalescing keeps the OLDEST snapshot — the unit the row started in" do
      plan = ReactiveDag.Node.graph([Expenses, CategoryTotals])

      Expenses
      |> Ash.Changeset.for_create(:create, %{key: "e1", category: "meals", amount: 40.0})
      |> Ash.create!()

      {:ok, _} = drain(plan)

      # two writes before the next drain: meals -> travel -> lodging
      e = Expenses |> Ash.get!("e1")
      e = e |> Ash.Changeset.for_update(:recategorise, %{category: "travel"}) |> Ash.update!()
      e |> Ash.Changeset.for_update(:recategorise, %{category: "lodging"}) |> Ash.update!()

      {:ok, report} = drain(plan)
      claimed = Map.new(report.steps, &{&1.cell, &1})["category_totals"].claimed

      # meals is the unit it was in when the drain last settled — the
      # intermediate "travel" never existed as far as any settled state knows
      assert "meals" in claimed
      assert "lodging" in claimed
    end
  end
end

defmodule ReactiveDag.RecomputeByTest do
  @moduledoc """
  `recompute_by` — THE declaration the engine cares about: **what unit does a
  change invalidate?**

      recompute_by :category, to: :expenses, from: :expense_cat

  Read: "recompute by category, from the input's `expense_cat`". A change to a
  row's `expense_cat` invalidates my `category` unit, so redo it whole. That one
  fact supplies the input edge, the grouping, the claim rule and the read scope
  — which is why it SUBSUMES `key_rule`: the same unit, previously stated twice
  and required to agree.

  Everything else a combinator declares (`group_by`, `into`, key derivation) is
  mapping data into shape once you already know what to recompute.

  Note it is the RECOMPUTE unit, not the output's grain. They coincide for a
  plain rollup and diverge the moment one unit emits many rows — percentiles
  per day recompute by day while their rows are keyed day+percentile.

  Consumed at COMPILE time: lowered to `over:` + `group_by:` + `key_rule`, and
  never traversed at recompute. A node reads its ONE input, materializes rows,
  and consumers query those rows rather than re-deriving them up the chain.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.{Drain, Frontier}
  alias ReactiveDag.Node.Recompute

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
      private?(true)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      # deliberately NOT named like the rollup's column — the pair maps it
      attribute :expense_cat, :string, public?: true
      attribute :amount, :float, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :create do
        accept([:key, :expense_cat, :amount])
      end

      update :revise do
        accept([:amount])
      end
    end

    reactive do
      id(:expenses)
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
      private?(true)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :category, :string, public?: true
      attribute :total, :float, public?: true
      attribute :n, :integer, public?: true
    end

    identities do
      identity :by_key, [:key], pre_check_with: ReactiveDag.RecomputeByTest.Domain
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

      # THE UNIT: a change to an expense's :expense_cat invalidates my
      # :category unit — redo it whole. Supplies the edge, the grouping and
      # the claim rule.
      recompute_by :category, to: :expenses, from: :expense_cat

      reduce into: [sum: [amount: :total], count: :n]
    end
  end

  defmodule FakeRepo do
    def start_link, do: Agent.start_link(fn -> MapSet.new() end, name: __MODULE__)

    def query!("INSERT INTO " <> _, params) do
      params
      |> Enum.chunk_every(7)
      |> Enum.each(fn [cell, _tenant, key, _r, _t, _held, vid] -> Agent.update(__MODULE__, &MapSet.put(&1, {cell, key})) end)

      %{rows: []}
    end

    def query!("SELECT DISTINCT cell_id" <> _, _params) do
      ids = Agent.get(__MODULE__, & &1) |> Enum.map(&elem(&1, 0)) |> Enum.uniq()
      %{rows: Enum.map(ids, &[&1])}
    end

    def query!("DELETE FROM " <> _, [cell | _tenant]) do
      keys =
        Agent.get_and_update(__MODULE__, fn set ->
          {mine, rest} = Enum.split_with(set, fn {c, _} -> c == cell end)
          {Enum.map(mine, &elem(&1, 1)), MapSet.new(rest)}
        end)

      %{rows: Enum.map(keys, &[&1, nil])}
    end

    def query!("SELECT COUNT" <> _, _params), do: %{rows: [[Agent.get(__MODULE__, &MapSet.size/1)]]}
  end


  setup do
    start_supervised!(%{id: FakeRepo, start: {FakeRepo, :start_link, []}})
    prev_repo = Application.get_env(:reactive_dag, :repo)
    Application.put_env(:reactive_dag, :repo, FakeRepo)

    on_exit(fn ->
      Application.put_env(:reactive_dag, :repo, prev_repo)
    end)

    for {k, cat, amt} <- [
          {"e1", "travel", 100.0},
          {"e2", "travel", 250.0},
          {"e3", "meals", 40.0}
        ] do
      Expenses
      |> Ash.Changeset.for_create(:create, %{key: k, expense_cat: cat, amount: amt})
      |> Ash.create!()
    end

    :ok
  end

  defp plan, do: ReactiveDag.Node.graph([Expenses, CategoryTotals])

  defp drain(plan),
    do: Drain.run(plan, recompute: ReactiveDag.Node.Recompute, key_rule: ReactiveDag.Node.KeyRule)

  test "the unit supplies the edge, the grouping AND the claim rule" do
    plan = plan()
    cell = plan.cells["category_totals"]

    # the DAG edge came from the unit
    assert cell.inputs == ["expenses"]

    # SUGAR: what reaches assembly/recompute is the ordinary `over:` + pair
    # form, with the claim rule derived. Nothing downstream knows `recompute_by`
    # existed — including `key_rule`, which is no longer written by hand.
    assert cell.meta.reduce.over == :expenses
    assert cell.meta.reduce.group_by == [{:category, :expense_cat}]
    assert cell.meta.key_rule == :group

    # ...so the group plan is the same one the explicit spelling produces
    assert cell.meta.over_source.group_key_plan == [{:attr, :expense_cat, true}]
  end

  test "it computes exactly as the explicit spelling does" do
    plan = plan()

    {:ok, changed} = Recompute.recompute(plan.cells["category_totals"], ["*"])
    assert Enum.sort(changed) == ["meals", "travel"]

    rows = CategoryTotals |> Ash.read!() |> Map.new(&{&1.key, &1})
    assert %{category: "travel", total: 350.0, n: 2} = rows["travel"]
    assert rows["meals"].total == 40.0
  end

  test "claims traverse the unit's pair in reverse" do
    plan = plan()
    {:ok, _} = Recompute.recompute(plan.cells["category_totals"], ["*"])

    # a changed expense claims ITS category — resolved through the unit's
    # `from:`, with neither group_by nor key_rule written by hand
    assert ReactiveDag.Node.KeyRule.rule(plan.cells["category_totals"], "expenses", ["e3"]) ==
             {:keys, ["meals"]}
  end

  test "end to end: touching one expense moves ONE category" do
    plan = plan()

    Frontier.mark_dirty("expenses", ["*"], "seed")
    {:ok, _} = drain(plan)

    Expenses
    |> Ash.get!("e2")
    |> Ash.Changeset.for_update(:revise, %{amount: 300.0})
    |> Ash.update!()

    Frontier.mark_dirty("expenses", ["e2"], "revised")
    {:ok, report} = drain(plan)

    steps = Map.new(report.steps, &{&1.cell, &1})
    assert steps["category_totals"].claimed == ["travel"]
    assert steps["category_totals"].changed == ["travel"]

    rows = CategoryTotals |> Ash.read!() |> Map.new(&{&1.key, &1})
    assert rows["travel"].total == 400.0
    assert rows["meals"].total == 40.0
  end

  test "`recompute_by :cell` is the whole-cell unit (what key_rule :all said)" do
    defmodule ByAmount do
      use Ash.Resource,
        domain: ReactiveDag.RecomputeByTest.Domain,
        data_layer: Ash.DataLayer.Ets,
        extensions: [ReactiveDag.Node]

      ets do
        private?(true)
      end

      attributes do
        attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
        attribute :n, :integer, public?: true
      end

      actions do
        defaults [:read]

        create :upsert do
          upsert?(true)
          accept([:key, :n])
        end
      end

      reactive do
        id(:by_amount)
        # whole-cell unit: any change re-prices everything, so the reduce
        # still says how rows are folded
        recompute_by :cell, to: :expenses

        reduce group_by: [:amount], into: [count: :n]
      end
    end

    plan = ReactiveDag.Node.graph([Expenses, ByAmount])
    cell = plan.cells["by_amount"]

    assert cell.inputs == ["expenses"]
    assert cell.meta.reduce.group_by == [:amount]
    assert cell.meta.key_rule == :all
  end

  describe "compile-time errors" do
    alias ReactiveDag.Node.Verifiers.VerifyReactive

    test "naming no input at all" do
      defmodule NoEdge do
        use Ash.Resource,
          domain: ReactiveDag.RecomputeByTest.Domain,
          data_layer: Ash.DataLayer.Simple,
          extensions: [ReactiveDag.Node]

        reactive do
          id(:no_edge)
          reduce group_by: [:category], into: [count: :n]
        end
      end

      assert {:error, %Spark.Error.DslError{message: msg}} =
               VerifyReactive.verify(NoEdge.spark_dsl_config())

      assert msg =~ "names no input"
    end

    test "naming the input twice" do
      defmodule TwiceDeclared do
        use Ash.Resource,
          domain: ReactiveDag.RecomputeByTest.Domain,
          data_layer: Ash.DataLayer.Simple,
          extensions: [ReactiveDag.Node]

        reactive do
          id(:twice_declared)

          recompute_by :category, to: :expenses, from: :expense_cat

          reduce over: :expenses, into: [count: :n]
        end
      end

      assert {:error, %Spark.Error.DslError{message: msg}} =
               VerifyReactive.verify(TwiceDeclared.spark_dsl_config())

      assert msg =~ "named TWICE"
    end

    test "a unit with no `from:` and no group_by to cover it" do
      defmodule HalfPair do
        use Ash.Resource,
          domain: ReactiveDag.RecomputeByTest.Domain,
          data_layer: Ash.DataLayer.Simple,
          extensions: [ReactiveDag.Node]

        reactive do
          id(:half_pair)

          recompute_by :category, to: :expenses

          reduce into: [count: :n]
        end
      end

      assert {:error, %Spark.Error.DslError{message: msg}} =
               VerifyReactive.verify(HalfPair.spark_dsl_config())

      assert msg =~ "declares no `from:`"
    end

    test "a plain over: still requires group_by" do
      defmodule NoGroup do
        use Ash.Resource,
          domain: ReactiveDag.RecomputeByTest.Domain,
          data_layer: Ash.DataLayer.Simple,
          extensions: [ReactiveDag.Node]

        reactive do
          id(:no_group)
          reduce over: :expenses, into: [count: :n]
        end
      end

      assert {:error, %Spark.Error.DslError{message: msg}} =
               VerifyReactive.verify(NoGroup.spark_dsl_config())

      assert msg =~ "must declare `group_by:`"
    end
  end
end

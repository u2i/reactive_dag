defmodule ReactiveDag.RelationshipEdgeTest do
  @moduledoc """
  `over_rel:` — an ASH RELATIONSHIP is the DAG edge. This is Ash's own
  relational vocabulary, not SQL's: the correspondence is declared ONCE on the
  resource (`has_many … source_attribute/destination_attribute`) and then
  *named*, exactly as loads, filters and aggregates name it. One declaration
  supplies all three facts the edge needs — which node, how rows group, and how
  `:group` claims traverse back.

  It is SUGAR: it lowers to `over:` + `group_by:` pairs, so the two spellings
  share one execution path (proved by asserting the lowered spec below).
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
      # deliberately NOT named like the rollup's column — the relationship maps it
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

    # THE EDGE, as an ordinary Ash relationship — nothing reactive_dag-specific
    relationships do
      has_many :expenses, ReactiveDag.RelationshipEdgeTest.Expenses do
        source_attribute :category
        destination_attribute :expense_cat
      end
    end

    identities do
      identity :by_key, [:key], pre_check_with: ReactiveDag.RelationshipEdgeTest.Domain
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

      # the relationship IS the edge: it names the input node AND the grouping
      reduce over_rel: :expenses,
             key_rule: :group,
             into: [sum: [amount: :total], count: :n]
    end
  end

  defmodule FakeRepo do
    def start_link, do: Agent.start_link(fn -> MapSet.new() end, name: __MODULE__)

    def query!("INSERT INTO " <> _, params) do
      params
      |> Enum.chunk_every(4)
      |> Enum.each(fn [cell, key, _r, _t] -> Agent.update(__MODULE__, &MapSet.put(&1, {cell, key})) end)

      %{rows: []}
    end

    def query!("SELECT DISTINCT cell_id" <> _, _params) do
      ids = Agent.get(__MODULE__, & &1) |> Enum.map(&elem(&1, 0)) |> Enum.uniq()
      %{rows: Enum.map(ids, &[&1])}
    end

    def query!("DELETE FROM " <> _, [cell]) do
      keys =
        Agent.get_and_update(__MODULE__, fn set ->
          {mine, rest} = Enum.split_with(set, fn {c, _} -> c == cell end)
          {Enum.map(mine, &elem(&1, 1)), MapSet.new(rest)}
        end)

      %{rows: Enum.map(keys, &[&1])}
    end

    def query!("SELECT COUNT" <> _, _params), do: %{rows: [[Agent.get(__MODULE__, &MapSet.size/1)]]}
  end

  defmodule NullWriter do
    @behaviour ReactiveDag.CoordinationWriter
    @impl true
    def put(_cell_id, _key, _opts), do: :ok
    @impl true
    def delete(_cell_id, _keys), do: :ok
  end

  setup do
    {:ok, _} = FakeRepo.start_link()
    prev_repo = Application.get_env(:reactive_dag, :repo)
    prev_writer = Application.get_env(:reactive_dag, :coordination_writer)
    Application.put_env(:reactive_dag, :repo, FakeRepo)
    Application.put_env(:reactive_dag, :coordination_writer, NullWriter)

    on_exit(fn ->
      Application.put_env(:reactive_dag, :repo, prev_repo)
      Application.put_env(:reactive_dag, :coordination_writer, prev_writer)
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

  test "the relationship supplies the edge AND the grouping — lowered to the pair form" do
    plan = plan()
    cell = plan.cells["category_totals"]

    # the DAG edge came from the relationship's destination
    assert cell.inputs == ["expenses"]

    # SUGAR: what reaches assembly/recompute is the ordinary `over:` + pair form
    assert cell.meta.reduce.over == :expenses
    assert cell.meta.reduce.group_by == [{:category, :expense_cat}]

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

  test ":group claims traverse the relationship in reverse" do
    plan = plan()
    {:ok, _} = Recompute.recompute(plan.cells["category_totals"], ["*"])

    # a changed expense claims ITS category — resolved through the
    # relationship's destination_attribute, with no group_by ever written
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

  test "an explicit group_by still wins over the relationship's own pair" do
    defmodule ByAmountBand do
      use Ash.Resource,
        domain: ReactiveDag.RelationshipEdgeTest.Domain,
        data_layer: Ash.DataLayer.Ets,
        extensions: [ReactiveDag.Node]

      ets do
        private?(true)
      end

      attributes do
        attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
        attribute :n, :integer, public?: true
      end

      relationships do
        has_many :expenses, ReactiveDag.RelationshipEdgeTest.Expenses do
          source_attribute :key
          destination_attribute :expense_cat
        end
      end

      actions do
        defaults [:read]

        create :upsert do
          upsert?(true)
          accept([:key, :n])
        end
      end

      reactive do
        id(:by_amount_band)
        # the relationship names the EDGE; grouping is declared explicitly
        reduce over_rel: :expenses,
               group_by: [:amount],
               into: [count: :n]
      end
    end

    plan = ReactiveDag.Node.graph([Expenses, ByAmountBand])
    cell = plan.cells["by_amount_band"]

    assert cell.inputs == ["expenses"]
    assert cell.meta.reduce.group_by == [:amount]
  end

  describe "compile-time errors" do
    alias ReactiveDag.Node.Verifiers.VerifyReactive

    test "declaring neither over: nor over_rel:" do
      defmodule NoEdge do
        use Ash.Resource,
          domain: ReactiveDag.RelationshipEdgeTest.Domain,
          data_layer: Ash.DataLayer.Simple,
          extensions: [ReactiveDag.Node]

        reactive do
          id(:no_edge)
          reduce group_by: [:category], into: [count: :n]
        end
      end

      assert {:error, %Spark.Error.DslError{message: msg}} =
               VerifyReactive.verify(NoEdge.spark_dsl_config())

      assert msg =~ "neither `over:` nor `over_rel:`"
    end

    test "declaring BOTH over: and over_rel:" do
      defmodule BothEdges do
        use Ash.Resource,
          domain: ReactiveDag.RelationshipEdgeTest.Domain,
          data_layer: Ash.DataLayer.Simple,
          extensions: [ReactiveDag.Node]

        attributes do
          attribute :category, :string, primary_key?: true, allow_nil?: false, public?: true
        end

        relationships do
          has_many :expenses, ReactiveDag.RelationshipEdgeTest.Expenses do
            source_attribute :category
            destination_attribute :expense_cat
          end
        end

        reactive do
          id(:both_edges)
          reduce over: :expenses, over_rel: :expenses, into: [count: :n]
        end
      end

      assert {:error, %Spark.Error.DslError{message: msg}} =
               VerifyReactive.verify(BothEdges.spark_dsl_config())

      assert msg =~ "BOTH"
    end

    test "over_rel: naming no such relationship" do
      defmodule NoSuchRel do
        use Ash.Resource,
          domain: ReactiveDag.RelationshipEdgeTest.Domain,
          data_layer: Ash.DataLayer.Simple,
          extensions: [ReactiveDag.Node]

        reactive do
          id(:no_such_rel)
          reduce over_rel: :nope, into: [count: :n]
        end
      end

      assert {:error, %Spark.Error.DslError{message: msg}} =
               VerifyReactive.verify(NoSuchRel.spark_dsl_config())

      assert msg =~ "names no such relationship"
    end

    test "a plain over: still requires group_by" do
      defmodule NoGroup do
        use Ash.Resource,
          domain: ReactiveDag.RelationshipEdgeTest.Domain,
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

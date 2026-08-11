defmodule ReactiveDag.OverEdgeTest do
  @moduledoc """
  The `over_grain` BLOCK — the combinator's input edge with its grain correspondence
  declared on it:

      over_grain :expenses do
        source_attribute :category         # this node's column
        destination_attribute :expense_cat # the input's field
      end

  The NOTATION is Ash's (`source_attribute`/`destination_attribute`, the
  correspondence written once and named), because Ash already solved how to say
  this. The SEMANTICS are deliberately not `has_many`'s: no cardinality claim,
  not loadable, not writable, no public API surface. It states one fact — how
  the input's grain maps to this node's — and that fact is consumed at COMPILE
  time, lowered to the combinator's `group_by` pair. Nothing traverses it at
  recompute: a node reads its ONE input, materializes rows, and consumers query
  those rows rather than re-deriving them back up the chain.
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
      identity :by_key, [:key], pre_check_with: ReactiveDag.OverEdgeTest.Domain
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

      # THE EDGE: which node, and how its grain maps to ours
      over_grain :expenses do
        source_attribute :category
        destination_attribute :expense_cat
      end

      reduce key_rule: :group,
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

  test "the block supplies the edge AND the grouping — lowered to the pair form" do
    plan = plan()
    cell = plan.cells["category_totals"]

    # the DAG edge came from the block
    assert cell.inputs == ["expenses"]

    # SUGAR: what reaches assembly/recompute is the ordinary `over:` + pair form.
    # Nothing downstream knows the block existed.
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

  test ":group claims traverse the pair in reverse" do
    plan = plan()
    {:ok, _} = Recompute.recompute(plan.cells["category_totals"], ["*"])

    # a changed expense claims ITS category — resolved through the block's
    # destination_attribute, with no group_by ever written by hand
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

  test "a bare `over_grain` block names only the edge; group_by stays explicit" do
    defmodule ByAmount do
      use Ash.Resource,
        domain: ReactiveDag.OverEdgeTest.Domain,
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
        # no grain pair: the block names the edge, the reduce names the grouping
        over_grain(:expenses)

        reduce group_by: [:amount], into: [count: :n]
      end
    end

    plan = ReactiveDag.Node.graph([Expenses, ByAmount])
    cell = plan.cells["by_amount"]

    assert cell.inputs == ["expenses"]
    assert cell.meta.reduce.group_by == [:amount]
  end

  describe "compile-time errors" do
    alias ReactiveDag.Node.Verifiers.VerifyReactive

    test "naming no input at all" do
      defmodule NoEdge do
        use Ash.Resource,
          domain: ReactiveDag.OverEdgeTest.Domain,
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

    test "declaring the edge twice" do
      defmodule TwiceDeclared do
        use Ash.Resource,
          domain: ReactiveDag.OverEdgeTest.Domain,
          data_layer: Ash.DataLayer.Simple,
          extensions: [ReactiveDag.Node]

        reactive do
          id(:twice_declared)

          over_grain :expenses do
            source_attribute(:category)
            destination_attribute(:expense_cat)
          end

          reduce over: :expenses, into: [count: :n]
        end
      end

      assert {:error, %Spark.Error.DslError{message: msg}} =
               VerifyReactive.verify(TwiceDeclared.spark_dsl_config())

      assert msg =~ "declared TWICE"
    end

    test "a half grain pair with no group_by to cover it" do
      defmodule HalfPair do
        use Ash.Resource,
          domain: ReactiveDag.OverEdgeTest.Domain,
          data_layer: Ash.DataLayer.Simple,
          extensions: [ReactiveDag.Node]

        reactive do
          id(:half_pair)

          over_grain :expenses do
            source_attribute(:category)
          end

          reduce into: [count: :n]
        end
      end

      assert {:error, %Spark.Error.DslError{message: msg}} =
               VerifyReactive.verify(HalfPair.spark_dsl_config())

      assert msg =~ "no complete grain pair"
    end

    test "a plain over: still requires group_by" do
      defmodule NoGroup do
        use Ash.Resource,
          domain: ReactiveDag.OverEdgeTest.Domain,
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

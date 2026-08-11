defmodule ReactiveDag.RelationshipJoinTest do
  @moduledoc """
  `left_rel:`/`right_rel:` — each join SIDE is an Ash relationship. This is the
  same move as `over_rel:`, applied to the join: the correspondence is declared
  once on the resource and referenced by name.

  It also lifts a real limitation. The `over:` join reads ONE node and splits it
  into two sides by a discriminator — so two DIFFERENT nodes could never be
  joined. With a relationship per side, each side carries its own resource, is
  read and scoped independently, and contributes its own input edge. The
  discriminator split survives as the special case: two relationships pointing
  at the same resource, their `where:` now living in the relationship's own
  `filter`, which is where Ash puts it.
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

  defmodule Budgets do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
      private?(true)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      # deliberately NOT named like the right side's column
      attribute :account_code, :string, public?: true
      attribute :amount, :float, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :create do
        accept([:key, :account_code, :amount])
      end

      update :revise do
        accept([:amount])
      end
    end

    reactive do
      id(:budgets)
      op(:source)
      leaf?(true)
    end
  end

  defmodule Actuals do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
      private?(true)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :acct, :string, public?: true
      attribute :amount, :float, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :create do
        accept([:key, :acct, :amount])
      end

      update :revise do
        accept([:amount])
      end
    end

    reactive do
      id(:actuals)
      op(:source)
      leaf?(true)
    end
  end

  # THE TWO-RELATION JOIN: two different nodes, one row per account.
  defmodule Variance do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
      private?(true)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :budget, :float, public?: true
      attribute :actual, :float, public?: true
    end

    identities do
      identity :by_key, [:key], pre_check_with: ReactiveDag.RelationshipJoinTest.Domain
    end

    relationships do
      has_many :budget_lines, ReactiveDag.RelationshipJoinTest.Budgets do
        source_attribute :key
        destination_attribute :account_code
      end

      has_many :actual_lines, ReactiveDag.RelationshipJoinTest.Actuals do
        source_attribute :key
        destination_attribute :acct
      end
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        upsert_identity(:by_key)
        accept([:key, :budget, :actual])
      end
    end

    reactive do
      id(:variance)
      op(:reconcile)

      # each side names a relationship; the sides are DIFFERENT nodes
      join left_rel: :budget_lines,
           right_rel: :actual_lines,
           outer: true,
           into: [left: [amount: :budget], right: [amount: :actual]]
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

    for {k, acct, amt} <- [{"b1", "5000", 100.0}, {"b2", "6000", 250.0}] do
      Budgets
      |> Ash.Changeset.for_create(:create, %{key: k, account_code: acct, amount: amt})
      |> Ash.create!()
    end

    for {k, acct, amt} <- [{"a1", "5000", 90.0}, {"a2", "7000", 40.0}] do
      Actuals |> Ash.Changeset.for_create(:create, %{key: k, acct: acct, amount: amt}) |> Ash.create!()
    end

    :ok
  end

  defp plan, do: ReactiveDag.Node.graph([Budgets, Actuals, Variance])

  defp drain(plan),
    do: Drain.run(plan, recompute: ReactiveDag.Node.Recompute, key_rule: ReactiveDag.Node.KeyRule)

  test "a two-relation join has TWO input edges and a source per side" do
    plan = plan()
    cell = plan.cells["variance"]

    assert Enum.sort(cell.inputs) == ["actuals", "budgets"]

    # each side resolved to its OWN resource
    assert cell.meta.side_sources.left.resource == Budgets
    assert cell.meta.side_sources.right.resource == Actuals

    # ...and each side lowered to the join-key spec the matcher already speaks
    assert cell.meta.join.left == [key: :account_code, where: []]
    assert cell.meta.join.right == [key: :acct, where: []]
  end

  test "it joins two DIFFERENT nodes — the thing `over:` could never do" do
    plan = plan()

    {:ok, changed} = Recompute.recompute(plan.cells["variance"], ["*"])
    assert Enum.sort(changed) == ["5000", "6000", "7000"]

    rows = Variance |> Ash.read!() |> Map.new(&{&1.key, &1})

    # matched on both sides
    assert %{budget: 100.0, actual: 90.0} = rows["5000"]
    # left-only: the actual is absent, and the gap is information
    assert %{budget: 250.0, actual: nil} = rows["6000"]
    # right-only, via outer: true — an actual with no budget
    assert %{budget: nil, actual: 40.0} = rows["7000"]
  end

  test "either side's change propagates through its own edge" do
    plan = plan()

    Frontier.mark_dirty("budgets", ["*"], "seed")
    Frontier.mark_dirty("actuals", ["*"], "seed")
    {:ok, _} = drain(plan)

    # touch the RIGHT side only
    Actuals |> Ash.get!("a1") |> Ash.Changeset.for_update(:revise, %{amount: 95.0}) |> Ash.update!()
    Frontier.mark_dirty("actuals", ["a1"], "revised")
    {:ok, report} = drain(plan)

    steps = Map.new(report.steps, &{&1.cell, &1})
    assert steps["variance"].triggered_by == "actuals"
    assert steps["variance"].changed == ["5000"]

    assert (Variance |> Ash.get!("5000")).actual == 95.0
  end

  test "the discriminator split: two filtered relationships over ONE node" do
    defmodule Lines do
      use Ash.Resource,
        domain: ReactiveDag.RelationshipJoinTest.Domain,
        data_layer: Ash.DataLayer.Ets,
        extensions: [ReactiveDag.Node]

      ets do
        private?(true)
      end

      attributes do
        attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
        attribute :acct, :string, public?: true
        attribute :kind, :string, public?: true
        attribute :amount, :float, public?: true
      end

      actions do
        defaults [:read]

        create :create do
          accept([:key, :acct, :kind, :amount])
        end
      end

      reactive do
        id(:lines)
        op(:source)
        leaf?(true)
      end
    end

    defmodule SplitVariance do
      use Ash.Resource,
        domain: ReactiveDag.RelationshipJoinTest.Domain,
        data_layer: Ash.DataLayer.Ets,
        extensions: [ReactiveDag.Node]

      ets do
        private?(true)
      end

      attributes do
        attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
        attribute :budget, :float, public?: true
        attribute :actual, :float, public?: true
      end

      # the `where:` discriminator now lives in the relationship's own filter
      relationships do
        has_many :budget_lines, ReactiveDag.RelationshipJoinTest.Lines do
          source_attribute :key
          destination_attribute :acct
          filter expr(kind == "budget")
        end

        has_many :actual_lines, ReactiveDag.RelationshipJoinTest.Lines do
          source_attribute :key
          destination_attribute :acct
          filter expr(kind == "actual")
        end
      end

      actions do
        defaults [:read]

        create :upsert do
          upsert?(true)
          accept([:key, :budget, :actual])
        end
      end

      reactive do
        id(:split_variance)

        join left_rel: :budget_lines,
             right_rel: :actual_lines,
             into: [left: [amount: :budget], right: [amount: :actual]]
      end
    end

    for {k, acct, kind, amt} <- [
          {"l1", "5000", "budget", 100.0},
          {"l2", "5000", "actual", 90.0},
          {"l3", "6000", "budget", 250.0}
        ] do
      Lines
      |> Ash.Changeset.for_create(:create, %{key: k, acct: acct, kind: kind, amount: amt})
      |> Ash.create!()
    end

    plan = ReactiveDag.Node.graph([Lines, SplitVariance])
    cell = plan.cells["split_variance"]

    # ONE input edge — both sides resolve to the same node
    assert cell.inputs == ["lines"]

    # the relationship's `filter` became the side's `where:`
    assert cell.meta.join.left == [key: :acct, where: [kind: "budget"]]
    assert cell.meta.join.right == [key: :acct, where: [kind: "actual"]]

    {:ok, changed} = Recompute.recompute(cell, ["*"])
    assert Enum.sort(changed) == ["5000", "6000"]

    rows = SplitVariance |> Ash.read!() |> Map.new(&{&1.key, &1})
    assert %{budget: 100.0, actual: 90.0} = rows["5000"]
    assert %{budget: 250.0, actual: nil} = rows["6000"]
  end

  describe "compile-time errors" do
    alias ReactiveDag.Node.Verifiers.VerifyReactive

    test "over: alongside side relationships" do
      defmodule OverAndRels do
        use Ash.Resource,
          domain: ReactiveDag.RelationshipJoinTest.Domain,
          data_layer: Ash.DataLayer.Simple,
          extensions: [ReactiveDag.Node]

        attributes do
          attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
        end

        relationships do
          has_many :budget_lines, ReactiveDag.RelationshipJoinTest.Budgets do
            source_attribute :key
            destination_attribute :account_code
          end

          has_many :actual_lines, ReactiveDag.RelationshipJoinTest.Actuals do
            source_attribute :key
            destination_attribute :acct
          end
        end

        reactive do
          id(:over_and_rels)

          join over: :budgets,
               left_rel: :budget_lines,
               right_rel: :actual_lines,
               into: [left: [amount: :budget]]
        end
      end

      assert {:error, %Spark.Error.DslError{message: msg}} =
               VerifyReactive.verify(OverAndRels.spark_dsl_config())

      assert msg =~ "no single `over:`"
    end

    test "only ONE side declared as a relationship" do
      defmodule OneSideOnly do
        use Ash.Resource,
          domain: ReactiveDag.RelationshipJoinTest.Domain,
          data_layer: Ash.DataLayer.Simple,
          extensions: [ReactiveDag.Node]

        attributes do
          attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
        end

        relationships do
          has_many :budget_lines, ReactiveDag.RelationshipJoinTest.Budgets do
            source_attribute :key
            destination_attribute :account_code
          end
        end

        reactive do
          id(:one_side_only)
          join left_rel: :budget_lines, into: [left: [amount: :budget]]
        end
      end

      assert {:error, %Spark.Error.DslError{message: msg}} =
               VerifyReactive.verify(OneSideOnly.spark_dsl_config())

      assert msg =~ "right_rel:"
    end

    test "a side naming no such relationship" do
      defmodule NoSuchSide do
        use Ash.Resource,
          domain: ReactiveDag.RelationshipJoinTest.Domain,
          data_layer: Ash.DataLayer.Simple,
          extensions: [ReactiveDag.Node]

        reactive do
          id(:no_such_side)
          join left_rel: :nope, right_rel: :also_nope, into: [left: [amount: :budget]]
        end
      end

      assert {:error, %Spark.Error.DslError{message: msg}} =
               VerifyReactive.verify(NoSuchSide.spark_dsl_config())

      assert msg =~ "names no such relationship"
    end

    test "a plain over: still requires both sides" do
      defmodule MissingSide do
        use Ash.Resource,
          domain: ReactiveDag.RelationshipJoinTest.Domain,
          data_layer: Ash.DataLayer.Simple,
          extensions: [ReactiveDag.Node]

        reactive do
          id(:missing_side)
          join over: :budgets, left: :account_code, into: [left: [amount: :budget]]
        end
      end

      assert {:error, %Spark.Error.DslError{message: msg}} =
               VerifyReactive.verify(MissingSide.spark_dsl_config())

      assert msg =~ "BOTH `left:` and `right:`"
    end
  end
end

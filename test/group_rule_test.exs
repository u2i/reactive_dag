defmodule ReactiveDag.GroupRuleTest do
  @moduledoc """
  `recompute_by`, the unit a change invalidates: a changed child row claims
  its GROUP — the mapping is the `group_by` fields the reduce already
  declares, evaluated by reading the changed rows. No key grammar, no host
  KeyRule module, no misleading block-level `:all`. The expense-category
  story end to end: touch one travel expense, and only the travel rollup
  moves — claim, read, fold, and propagation.
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
      private?(true)
    end

    attributes do
      # note: keys carry NO category grammar — :group looks the rows up
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
      identity :by_key, [:key], pre_check_with: ReactiveDag.GroupRuleTest.Domain
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

      # the whole granularity contract in ONE declaration: a change to an
      # expense's :category invalidates that category unit — claims resolved
      # through it, read scoped by it, rows grouped by it.
      recompute_by :category, to: :expenses, from: :category

      reduce into: [sum: [amount: :total], count: :n]
    end
  end

  # same-grain consumer: proves only the touched category propagates
  defmodule CategoryHealth do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :status, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        accept([:key, :status])
      end
    end

    reactive do
      id(:category_health)
      op(:check)

      reduce over: :category_totals,
             group_by: :key,
             into: fn _cat, [row | _] -> %{status: if(row.total < 1000.0, do: "present", else: "failing")} end
    end
  end

  defmodule FakeRepo do
    def start_link, do: Agent.start_link(fn -> MapSet.new() end, name: __MODULE__)

    def query!("INSERT INTO " <> _, params) do
      params
      |> Enum.chunk_every(5)
      |> Enum.each(fn [cell, key, _r, _t, _prior] -> Agent.update(__MODULE__, &MapSet.put(&1, {cell, key})) end)

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

      %{rows: Enum.map(keys, &[&1, nil])}
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
    start_supervised!(%{id: FakeRepo, start: {FakeRepo, :start_link, []}})
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
          {"e3", "meals", 40.0},
          {"e4", "lodging", 900.0}
        ] do
      Expenses |> Ash.Changeset.for_create(:create, %{key: k, category: cat, amount: amt}) |> Ash.create!()
    end

    :ok
  end

  defp plan, do: ReactiveDag.Node.graph([Expenses, CategoryTotals, CategoryHealth])

  defp drain(plan),
    do: Drain.run(plan, recompute: ReactiveDag.Node.Recompute, key_rule: ReactiveDag.Node.KeyRule)

  test "touching one expense moves ONE category — claim, read, fold, propagation" do
    plan = plan()

    # the op-level declaration landed in the cell meta, and assembly proved
    # the group is one plain string attribute — the read auto-scope target
    cell = plan.cells["category_totals"]
    assert cell.meta.key_rule == :group
    assert cell.meta.over_source.group_key_plan == [{:attr, :category, true}]

    Frontier.mark_dirty("expenses", ["*"], "seed")
    {:ok, _} = drain(plan)

    rows = CategoryTotals |> Ash.read!() |> Map.new(&{&1.key, &1})
    assert %{category: "travel", total: 350.0, n: 2} = rows["travel"]
    assert rows["meals"].total == 40.0

    # revise ONE travel expense
    Expenses
    |> Ash.get!("e2")
    |> Ash.Changeset.for_update(:revise, %{amount: 300.0})
    |> Ash.update!()

    Frontier.mark_dirty("expenses", ["e2"], "revised")
    {:ok, report} = drain(plan)

    steps = Map.new(report.steps, &{&1.cell, &1})

    # claimed per-CATEGORY (the :group lookup), not per-entry, not whole-cell
    assert steps["category_totals"].claimed == ["travel"]
    assert steps["category_totals"].changed == ["travel"]
    # and the same-grain consumer follows for exactly that category
    assert steps["category_health"].claimed == ["travel"]

    rows = CategoryTotals |> Ash.read!() |> Map.new(&{&1.key, &1})
    assert rows["travel"].total == 400.0
    assert rows["meals"].total == 40.0
    assert rows["lodging"].total == 900.0
  end

  test "a DELETED expense degrades the claim to whole-cell (the lookup can't name its group)" do
    plan = plan()
    Frontier.mark_dirty("expenses", ["*"], "seed")
    {:ok, _} = drain(plan)

    Expenses |> Ash.get!("e3") |> Ash.destroy!()
    Frontier.mark_dirty("expenses", ["e3"], "deleted")
    {:ok, report} = drain(plan)

    steps = Map.new(report.steps, &{&1.cell, &1})
    assert steps["category_totals"].claimed == ["*"]
  end

  test "recompute_by alongside a block-level key_rule is a compile-time error" do
    alias ReactiveDag.Node.Verifiers.VerifyReactive

    defmodule BothPlaces do
      use Ash.Resource,
        domain: ReactiveDag.GroupRuleTest.Domain,
        data_layer: Ash.DataLayer.Simple,
        extensions: [ReactiveDag.Node]

      reactive do
        id(:both_places)
        key_rule(:all)

        # the unit already says what a change invalidates — `key_rule` is the
        # same fact stated twice, and the two can disagree
        recompute_by :category, to: :expenses, from: :category

        reduce into: fn _c, _rows -> %{} end,
               upsert: fn _, _ -> true end
      end
    end

    assert {:error, %Spark.Error.DslError{message: msg}} =
             VerifyReactive.verify(BothPlaces.spark_dsl_config())

    assert msg =~ "already declares the claim unit"
  end
end

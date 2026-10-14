defmodule ReactiveDag.GroupRuleTest do
  @moduledoc """
  `recompute_by`, the unit a change invalidates: a changed child row claims
  its GROUP — the mapping is the `group_by` fields the reduce already
  declares, evaluated by reading the changed rows. No key grammar, no host
  KeyRule module, no misleading block-level `:all`. The expense-category
  story end to end: touch one travel expense, and only the travel rollup
  moves — claim, read, fold, and propagation.

  Runs on the shared `ExpenseGraph` fixture against a real database. The
  resources used to be declared here, and the suspension table used to be a
  hand-written `FakeRepo` that matched on `"INSERT INTO " <> _`. Both are
  gone: the graph is shared, and the library's own migration creates the real
  tables, so this exercises the SQL rather than a stand-in for it.
  """
  use ReactiveDag.PostgresCase

  alias ReactiveDag.Fixtures.ExpenseGraph
  alias ReactiveDag.Fixtures.ExpenseGraph.{CategoryTotals, Expenses}

  setup_all do
    migrate!(ExpenseGraph.Domain)
    :ok
  end

  setup do
    pid = Ecto.Adapters.SQL.Sandbox.start_owner!(ReactiveDag.TestRepo, shared: true)
    on_exit(fn -> Ecto.Adapters.SQL.Sandbox.stop_owner(pid) end)

    migrate_library_tables!()
    truncate(ExpenseGraph.tables())

    for {k, cat, amt} <- [
          {"e1", "travel", 100.0},
          {"e2", "travel", 250.0},
          {"e3", "meals", 40.0},
          {"e4", "lodging", 900.0}
        ] do
      Expenses
      |> Ash.Changeset.for_create(:create, %{key: k, category: cat, amount: amt})
      |> Ash.create!()
    end

    :ok
  end

  defp plan, do: ExpenseGraph.plan(:with_health)

  defp drain(plan), do: ReactiveDag.Test.Pending.cascade(plan)

  test "touching one expense moves ONE category — claim, read, fold, propagation" do
    plan = plan()

    # the op-level declaration landed in the cell meta, and assembly proved
    # the group is one plain string attribute — the read auto-scope target
    cell = plan.cells["category_totals"]
    assert cell.meta.key_rule == :group
    assert cell.meta.over_source.group_key_plan == [{:attr, :category, true}]

    ReactiveDag.Test.Pending.add("expenses", ["*"])
    {:ok, _} = drain(plan)

    rows = CategoryTotals |> Ash.read!() |> Map.new(&{&1.key, &1})
    assert %{category: "travel", total: 350.0, n: 2} = rows["travel"]
    assert rows["meals"].total == 40.0

    # revise ONE travel expense
    Expenses
    |> Ash.get!("e2")
    |> Ash.Changeset.for_update(:revise, %{amount: 300.0})
    |> Ash.update!()

    ReactiveDag.Test.Pending.add("expenses", ["e2"])
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
    ReactiveDag.Test.Pending.add("expenses", ["*"])
    {:ok, _} = drain(plan)

    Expenses |> Ash.get!("e3") |> Ash.destroy!()
    ReactiveDag.Test.Pending.add("expenses", ["e3"])
    {:ok, report} = drain(plan)

    steps = Map.new(report.steps, &{&1.cell, &1})
    assert steps["category_totals"].claimed == ["*"]
  end

  test "recompute_by alongside a block-level key_rule is a compile-time error" do
    alias ReactiveDag.Node.Verifiers.VerifyReactive

    # Stays INLINE and data-layer-less: the point is that it does not assemble,
    # so it has no rows and needs no table. Moving it to a fixture would mean
    # compiling a deliberately-invalid resource at fixture load.
    defmodule BothPlaces do
      use Ash.Resource,
        domain: ReactiveDag.Fixtures.ExpenseGraph.Domain,
        data_layer: Ash.DataLayer.Simple,
        extensions: [ReactiveDag.Node]

      reactive do
        id(:both_places)
        key_rule(:all)

        # the unit already says what a change invalidates — `key_rule` is the
        # same fact stated twice, and the two can disagree
        recompute_by(:category, to: :expenses, from: :category)

        reduce(into: fn _c, _rows -> %{} end)
      end
    end

    assert {:error, %Spark.Error.DslError{message: msg}} =
             VerifyReactive.verify(BothPlaces.spark_dsl_config())

    assert msg =~ "already declares the claim unit"
  end
end

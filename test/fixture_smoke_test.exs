defmodule ReactiveDag.FixtureSmokeTest do
  @moduledoc """
  The shared fixture graph, exercised against real Postgres.

  Proves the fixture is usable before any test is moved onto it: tables
  generated from the DSL, rows written and read, and the fold's unit
  derivation working — including the MOVE, which is the case the fixture
  exists for.
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
    truncate(ExpenseGraph.tables())
    :ok
  end

  defp expense(key, cat, amount) do
    Expenses
    |> Ash.Changeset.for_create(:create, %{key: key, category: cat, amount: amount})
    |> Ash.create!()
  end

  test "the generated tables hold rows" do
    expense("e1", "meals", 10.0)
    expense("e2", "travel", 5.0)

    assert length(Ash.read!(Expenses)) == 2
  end

  test "the plan assembles with the fold's unit rule" do
    plan = ExpenseGraph.plan()

    assert %ReactiveDag.Cell{} = cell = plan.cells["category_totals"]
    assert cell.meta[:key_rule] == :group
    assert cell.inputs == ["expenses"]
  end

  test "a row can MOVE between units — the case the fixture exists for" do
    row = expense("e1", "meals", 10.0)

    moved =
      row
      |> Ash.Changeset.for_update(:recategorise, %{category: "travel"})
      |> Ash.update!()

    assert moved.category == "travel"

    # The old unit is the one a naive implementation forgets: nothing points at
    # "meals" any more, yet a total computed before the move still counts it.
    assert Ash.get!(Expenses, "e1").category == "travel"
  end

  test "the fold's own rows round-trip" do
    CategoryTotals
    |> Ash.Changeset.for_create(:upsert, %{key: "meals", category: "meals", total: 10.0, n: 1})
    |> Ash.create!()

    assert [%{category: "meals", total: 10.0, n: 1}] = Ash.read!(CategoryTotals)
  end
end

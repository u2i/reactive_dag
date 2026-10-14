defmodule ReactiveDag.KeylessFoldTest do
  @moduledoc """
  A fold with NO stored key column.

  Its unit is named by `row_key` columns the row already carries — `{fund, year}`
  — so nothing needs a `"|"`-joined `rollup_key` beside them. That column was
  always a duplicate of two columns in its own row; what kept it was the LOOKUP:
  a payload write and a retire both had to find "the row for this unit", and the
  only handle they had was a key.

  With the unit's own values in hand (derived from the change, not from splitting
  a string) both find the row by its columns. This asserts the half that fails
  silently: a RETIRE, which returns false when it cannot find a row, so a stale
  derived row survives and reads exactly like a live one.
  """
  use ReactiveDag.PostgresCase

  alias ReactiveDag.Fixtures.FundYearGraph
  alias ReactiveDag.Fixtures.FundYearGraph.{Lines, Projected, KeylessRollup}

  alias ReactiveDag.{Cascade}

  defp plan, do: FundYearGraph.plan(:keyless)

  defp drain,
    do: ReactiveDag.Test.Pending.cascade(plan(), recompute: ReactiveDag.Node.Recompute, key_rule: ReactiveDag.Node.KeyRule)

  defp put_line(key, fund, year, amount) do
    Ash.create!(Lines, %{key: key, fund: fund, year: year, amount: amount}, action: :upsert)
    ReactiveDag.Test.Pending.add("lines", [key])
  end

  defp unit(fund, year),
    do: Ash.read!(KeylessRollup) |> Enum.find(&(&1.fund == fund and &1.year == year))

  setup_all do
    migrate!(FundYearGraph.Domain)
    :ok
  end

  setup do
    pid = Ecto.Adapters.SQL.Sandbox.start_owner!(ReactiveDag.TestRepo, shared: true)
    on_exit(fn -> Ecto.Adapters.SQL.Sandbox.stop_owner(pid) end)

    start_supervised!(ReactiveDag.Test.FakeSuspensionRepo)
    ReactiveDag.Test.FakeSuspensionRepo.install()

    for m <- [Lines, Projected, KeylessRollup], r <- Ash.read!(m), do: Ash.destroy!(r)
    ReactiveDag.Test.Pending.reset()
    :ok
  end

  test "the fold writes its rows with no key column at all" do
    put_line("a", "gf", "2025", 10)
    put_line("b", "water", "2025", 20)
    {:ok, _} = drain()

    assert unit("gf", "2025").total == 10
    assert unit("water", "2025").total == 20

    # …and nothing on the row is a joined key.
    refute Map.has_key?(unit("gf", "2025"), :rollup_key)
  end

  test "a MOVED row's abandoned unit is RETIRED — found by its columns" do
    put_line("a", "gf", "2025", 10)
    {:ok, _} = drain()
    assert unit("gf", "2025").total == 10

    # The only line in gf|2025 moves to water|2025, so gf|2025 has no input rows
    # left and its derived row must go. The retire has no key column to find it
    # by — only the unit's own values, derived from the change.
    put_line("a", "water", "2025", 10)
    {:ok, _} = drain()

    assert unit("water", "2025").total == 10

    refute unit("gf", "2025"),
           "the abandoned unit's row survived — a retire that cannot find its row " <>
             "returns false, so a stale derived row reads exactly like a live one"
  end

  test "an untouched unit is neither repriced nor retired" do
    put_line("a", "gf", "2025", 10)
    put_line("b", "water", "2026", 99)
    {:ok, _} = drain()

    put_line("a", "gf", "2026", 10)
    {:ok, report} = drain()

    claimed = Map.new(report.steps, &{&1.cell, &1})["rollup"].claimed
    refute "water|2026" in claimed
    assert unit("water", "2026").total == 99
  end
end

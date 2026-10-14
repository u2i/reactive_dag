defmodule ReactiveDag.ExactGroupScopeTest do
  @moduledoc """
  A composite fold reads the units it CLAIMED — not their cross-product.

  A `"|"`-joined label can only be split per column, so scoping a composite unit
  from labels filters each column independently: claims `"gf|2025"` and
  `"water|2026"` also admit `"gf|2026"`, a unit nothing touched. Sound (a
  superset read stays closed over unit boundaries) but wasteful, and the waste
  grows as the product of the claim set.

  `Diff.groups/2` derives the group's VALUES, so the claim names exact pairs.
  This asserts the read is exact by counting the rows the fold actually saw.
  """
  use ReactiveDag.PostgresCase

  alias ReactiveDag.Fixtures.FundYearGraph
  alias ReactiveDag.Fixtures.FundYearGraph.{Lines, Projected, KeyedRollup}

  alias ReactiveDag.{Cascade}

  defp plan, do: FundYearGraph.plan()

  defp drain,
    do: ReactiveDag.Test.Pending.cascade(plan(), recompute: ReactiveDag.Node.Recompute, key_rule: ReactiveDag.Node.KeyRule)

  defp put_line(key, fund, year, amount) do
    Ash.create!(Lines, %{key: key, fund: fund, year: year, amount: amount}, action: :upsert)
    ReactiveDag.Test.Pending.add("lines", [key])
  end

  setup_all do
    migrate!(FundYearGraph.Domain)
    :ok
  end

  setup do
    pid = Ecto.Adapters.SQL.Sandbox.start_owner!(ReactiveDag.TestRepo, shared: true)
    on_exit(fn -> Ecto.Adapters.SQL.Sandbox.stop_owner(pid) end)

    start_supervised!(ReactiveDag.Test.FakeSuspensionRepo)
    ReactiveDag.Test.FakeSuspensionRepo.install()

    for m <- [Lines, Projected, KeyedRollup], r <- Ash.read!(m), do: Ash.destroy!(r)
    ReactiveDag.Test.Pending.reset()
    :ok
  end

  test "two claims read their OWN units, not the cross-product" do
    # Four units across the 2×2 grid, so a cross-product read is distinguishable
    # from an exact one.
    put_line("a", "gf", "2025", 1)
    put_line("b", "water", "2026", 2)
    put_line("c", "gf", "2026", 4)
    put_line("d", "water", "2025", 8)
    {:ok, _} = drain()

    assert (KeyedRollup |> Ash.get!("gf|2026")).total == 4

    # Touch gf|2025 and water|2026 only. Splitting the two labels per column
    # yields fund in [gf, water] AND year in [2025, 2026] — the whole grid.
    put_line("a", "gf", "2025", 16)
    put_line("b", "water", "2026", 32)
    {:ok, report} = drain()

    claimed = Map.new(report.steps, &{&1.cell, &1})["rollup"].claimed
    assert Enum.sort(claimed) == ["gf|2025", "water|2026"]

    # The untouched diagonal keeps its value either way — this is about the READ,
    # not the write, so assert on what the fold was given.
    assert (KeyedRollup |> Ash.get!("gf|2026")).total == 4
    assert (KeyedRollup |> Ash.get!("water|2025")).total == 8
  end

  test "the QUEUE holds child keys, and the fold derives its own units" do
    # The model, end to end: a mark says WHICH CHILD ROW changed and references
    # the change; the fold derives its own units when it drains. No composite unit
    # name is ever stored, so nothing has to be split apart.
    #
    # The gap this closes: asserting `auto_scope/3` with hand-built groups passes
    # even when the drain derives none. A mutation making the derivation return
    # `%{}` survived until this existed.
    put_line("a", "gf", "2025", 1)
    put_line("b", "water", "2026", 2)
    put_line("c", "gf", "2026", 4)
    put_line("d", "water", "2025", 8)
    {:ok, _} = drain()

    put_line("a", "gf", "2025", 16)
    put_line("b", "water", "2026", 32)
    {:ok, report} = drain()

    steps = Map.new(report.steps, &{&1.cell, &1})

    # What the QUEUE carried: the child's own keys. `"gf|2025"` was never written
    # to it.
    assert Enum.sort(steps["projected"].claimed) == ["a", "b"]

    # What the FOLD derived from them: its own units, from the versions and its
    # own declared grain.
    assert Enum.sort(steps["rollup"].claimed) == ["gf|2025", "water|2026"]

    # The untouched diagonal is neither claimed nor disturbed.
    refute "gf|2026" in steps["rollup"].claimed
    assert (KeyedRollup |> Ash.get!("gf|2026")).total == 4
    assert (KeyedRollup |> Ash.get!("water|2025")).total == 8

    # …and the two that were claimed hold the new values.
    assert (KeyedRollup |> Ash.get!("gf|2025")).total == 16
    assert (KeyedRollup |> Ash.get!("water|2026")).total == 32
  end

  test "a PARTIAL derivation is refused — all changed keys, or none" do
    # The guard: a fold derives its units only when EVERY changed child key
    # carries a stamp. Deriving from some of them would claim the units those
    # rows belong to and silently strand the rest — a drain that reports
    # success having left work undone, which is the worst failure this queue
    # can have.
    #
    # Driven through the mechanism that EXISTS: a cascade origin carries
    # `versions`, a `key => recorded_at` map that suspensions use to diff
    # against history. Handing it a stamp for `a` but not `d` is exactly the
    # partial state, without needing the deleted `version_id` resolver that
    # used to simulate it.
    put_line("a", "gf", "2025", 1)
    put_line("d", "water", "2025", 8)
    {:ok, _} = drain()

    Ash.create!(Lines, %{key: "a", fund: "gf", year: "2025", amount: 16}, action: :upsert)
    Ash.create!(Lines, %{key: "d", fund: "water", year: "2025", amount: 64}, action: :upsert)

    {:ok, report} =
      ReactiveDag.Cascade.run(
        plan(),
        [%{cell: "lines", keys: ["a", "d"], versions: %{"a" => DateTime.utc_now()}}],
        recompute: ReactiveDag.Node.Recompute,
        key_rule: ReactiveDag.Node.KeyRule
      )

    claimed = Map.new(report.steps, &{&1.cell, &1})["rollup"].claimed

    # BOTH units repriced. Deriving from `a` only would claim `gf|2025` and
    # leave `water|2025` holding 8.
    assert (KeyedRollup |> Ash.get!("water|2025")).total == 64,
           "a key whose change carried no stamp must not be stranded"

    assert (KeyedRollup |> Ash.get!("gf|2025")).total == 16
    refute claimed == ["gf|2025"], "a partial derivation was used"
  end

  test "the scope names exact pairs" do
    put_line("a", "gf", "2025", 1)
    put_line("b", "water", "2026", 2)
    put_line("c", "gf", "2026", 4)
    {:ok, _} = drain()

    cell = plan().cells["rollup"]

    # The claim's group VALUES, as the drain derives them from a version.
    groups = %{"gf|2025" => [{"gf", "2025"}], "water|2026" => [{"water", "2026"}]}

    scope =
      ReactiveDag.Node.Recompute.auto_scope(cell, ["gf|2025", "water|2026"], groups)

    assert {:any_of, clauses} = scope,
           "with group values in hand the scope is exact, not a per-column hull"

    assert Enum.sort(clauses) == [
             [fund: "gf", year: "2025"],
             [fund: "water", year: "2026"]
           ]

    # WITHOUT the values it degrades to the hull — each column independently,
    # which is the cross-product this replaces.
    assert {:all_of, _} = ReactiveDag.Node.Recompute.auto_scope(cell, ["gf|2025", "water|2026"])
  end
end

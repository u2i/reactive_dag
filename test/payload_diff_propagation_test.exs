defmodule ReactiveDag.PayloadDiffPropagationTest do
  @moduledoc """
  A GRAPH-WRITTEN row propagates from its diff, not from a read-back.

  `dirties_on` covers the other producer — a host writing a row itself, which
  attaches the diff to the mark. This covers the one every derived cell uses: a
  payload write inside a recompute, whose diff `Payload.collecting_diffs/1`
  collects and the drain hands to the key rule.

  Written because a mutation exposed the gap: removing the payload-diff channel
  from the drain's propagation failed **nothing**. The external path had tests and
  the internal one did not, which is the wrong way round — a real host writes
  every one of its cells through the payload loop.

  The case that matters is a row MOVING between units. A live read names only
  where it landed, so the unit it left keeps counting a row it no longer holds
  unless the diff names both.
  """
  use ReactiveDag.PostgresCase

  alias ReactiveDag.Fixtures.PayloadDiffGraph
  alias ReactiveDag.Fixtures.PayloadDiffGraph.{Lines, Projected, Rollup}

  alias ReactiveDag.{Cascade}

  # The frontier's repo. Mirrors `dirties_on_test`'s: it must retain the DIFF a
  # mark carried, since that is half of what this file is about.

  # A `compute` op writing its own rows through the payload loop — which is what
  # every real host's extraction cells do.

  defp plan, do: ReactiveDag.Node.graph([Lines, Projected, Rollup])

  defp drain do
    ReactiveDag.Test.Pending.cascade(plan(), recompute: ReactiveDag.Node.Recompute, key_rule: ReactiveDag.Node.KeyRule)
  end

  defp put_line(key, fund, amount) do
    Ash.create!(Lines, %{key: key, fund: fund, amount: amount}, action: :upsert)
    ReactiveDag.Test.Pending.add("lines", [key])
  end

  setup_all do
    migrate!(PayloadDiffGraph.Domain)
    :ok
  end

  setup do
    pid = Ecto.Adapters.SQL.Sandbox.start_owner!(ReactiveDag.TestRepo, shared: true)
    on_exit(fn -> Ecto.Adapters.SQL.Sandbox.stop_owner(pid) end)

    truncate(PayloadDiffGraph.tables())
    start_supervised!(ReactiveDag.Test.FakeSuspensionRepo)
    ReactiveDag.Test.FakeSuspensionRepo.install()

    # The ETS tables are shared, so start from a known-empty state — and drain
    # the frontier the destroys above just dirtied.
    for m <- [Lines, Projected, Rollup], r <- Ash.read!(m), do: Ash.destroy!(r)
    ReactiveDag.Test.Pending.reset()
    :ok
  end

  test "the middle cell is graph-written, so its diff comes from the payload loop" do
    put_line("l1", "A", 10)
    {:ok, _} = drain()

    assert (Rollup |> Ash.get!("A")).total == 10
  end

  test "a MOVED row claims both units — from the payload write's own diff" do
    put_line("l1", "A", 10)
    {:ok, _} = drain()
    assert (Rollup |> Ash.get!("A")).total == 10

    # `fund` moves A → ES. `projected`'s payload write sees both sides; a live
    # read of the row would name only ES, leaving A counting a row it no longer
    # holds.
    put_line("l1", "ES", 10)
    {:ok, report} = drain()

    steps = Map.new(report.steps, &{&1.cell, &1})

    assert Enum.sort(steps["rollup"].claimed) == ["A", "ES"],
           "the unit it left AND the one it landed in — not `\"*\"`, and not just ES"

    assert (Rollup |> Ash.get!("ES")).total == 10

    # A was RETIRED, not zeroed — the fold produced no row for a unit with no
    # input rows left, and `retire_vanished` reconciled it away. Either outcome
    # proves the point (A was repriced rather than left holding 10); asserting a
    # zeroed row would have asserted the wrong mechanism.
    assert Ash.get(Rollup, "A") |> elem(0) == :error,
           "the unit the row left holds nothing, rather than still counting it"
  end

  test "the claim is per-unit, never the whole cell" do
    # Two funds, then move one row. Only the two affected units are claimed —
    # `"*"` would reprice GF as well, which is the over-claiming this replaces.
    put_line("l1", "A", 10)
    put_line("l2", "GF", 99)
    {:ok, _} = drain()

    put_line("l1", "ES", 10)
    {:ok, report} = drain()

    claimed = Map.new(report.steps, &{&1.cell, &1})["rollup"].claimed

    refute "*" in claimed
    refute "GF" in claimed, "an untouched unit is not repriced"
    assert (Rollup |> Ash.get!("GF")).total == 99
  end

  test "an unchanged rewrite propagates nothing" do
    put_line("l1", "A", 10)
    {:ok, _} = drain()

    # Same content. `projected`'s payload write reports `:unchanged`, so it
    # records no diff and reports no changed key — and the rollup is never
    # claimed at all.
    put_line("l1", "A", 10)
    {:ok, report} = drain()

    refute Map.has_key?(Map.new(report.steps, &{&1.cell, &1}), "rollup"),
           "nothing moved, so there was nothing to reprice"
  end
end

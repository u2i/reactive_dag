defmodule ReactiveDag.VerdictTest do
  @moduledoc """
  The generic read/rollup layer (ReactiveDag.Verdict) — the engine piece promoted
  out of the portal's hand-written Verdict. Pure rollup logic tested directly;
  for_cell is a thin wrapper over ReactiveDag.Tuple reads (proven by host suites).
  """
  use ExUnit.Case, async: true

  alias ReactiveDag.Verdict

  describe "rollup/2 — histogram + sample → verdict" do
    test "any failing → :findings, carrying counts + sample" do
      v = Verdict.rollup(%{"present" => 10, "failing" => 2}, ["bad1", "bad2"])
      assert v.status == :findings
      assert v.failing == 2
      assert v.sample == ["bad1", "bad2"]
    end

    test "no failing but pending/stale → :pending (stale counts as pending)" do
      assert Verdict.rollup(%{"present" => 3, "pending" => 1}, []).status == :pending
      assert Verdict.rollup(%{"present" => 3, "stale" => 1}, []).status == :pending
      assert Verdict.rollup(%{"present" => 3, "stale" => 1}, []).pending == 1
    end

    test "all present → :green" do
      assert Verdict.rollup(%{"present" => 5}, []).status == :green
    end

    test "empty (nothing evaluated) → :unknown, NOT green" do
      assert Verdict.rollup(%{}, []).status == :unknown
    end

    test "findings dominates pending" do
      assert Verdict.rollup(%{"failing" => 1, "pending" => 9}, ["x"]).status == :findings
    end
  end

  describe "roll_group/1 — group of verdicts → one status" do
    defp v(status), do: %{status: status, failing: 0, pending: 0, sample: []}

    test "findings if ANY child has findings" do
      assert Verdict.roll_group([v(:green), v(:findings), v(:green)]) == :findings
    end

    test "pending if any pending (no findings)" do
      assert Verdict.roll_group([v(:green), v(:pending)]) == :pending
    end

    test "green if any green (no findings/pending)" do
      assert Verdict.roll_group([v(:green), v(:unknown)]) == :green
    end

    test "unknown if all unknown" do
      assert Verdict.roll_group([v(:unknown), v(:unknown)]) == :unknown
    end
  end
end

defmodule ReactiveDag.FirstClassCoverageTest do
  @moduledoc """
  PROTOTYPE — first-class "missing-data-ness". The alternative to the companion
  cell: instead of a lossy violations-only verdict cell + a second coverage cell, a
  SINGLE guarantee cell RETAINS its evaluated-but-passing members as `covered` rows.
  Then absence means exactly ONE thing — never evaluated (unknown) — and the verdict
  is resolvable from that one cell.

  The whole thesis in one line: the lib's generic `ReactiveDag.Verdict.rollup/2`
  ALREADY implements the three-valued decision (`total == 0 -> :unknown`, else
  green/findings/pending). The ONLY thing that made the portal need a companion cell
  is that its `Recompute.guarantee` DROPS the covered rows — so the histogram's
  `total` counts violations only, and a passing app (0 violations) rolls up to a
  FALSE `:unknown`. Keep the covered rows and `total` reflects the evaluated
  population, so green vs unknown falls out with NO companion, NO cross-cell read.

  This needs NO substrate change (`status` is already an open host vocabulary) — only
  a host recompute that stops discarding coverage. Proven here directly against
  `rollup/2` with histograms that include vs omit the covered rows.
  """
  use ExUnit.Case, async: true

  alias ReactiveDag.Verdict

  describe "the bug the companion works around: a lossy (violations-only) histogram" do
    test "a passing app has an EMPTY violations-only histogram → false :unknown" do
      # violations-only: a covered app contributes NO rows, so its scoped histogram
      # is empty → rollup says :unknown even though it's actually green. THIS is why
      # the portal must consult the /set companion to recover the truth.
      assert Verdict.rollup(%{}, []).status == :unknown
    end
  end

  describe "first-class coverage: retain `covered` rows → correct verdict from one cell" do
    test "a passing app's histogram carries its covered rows → :green (not unknown)" do
      # scoped to a covered-only app: covered rows keep total > 0, no failing → green.
      assert Verdict.rollup(%{"covered" => 3}, []).status == :green
    end

    test "a failing app → :findings" do
      assert Verdict.rollup(%{"covered" => 2, "failing" => 1}, ["acme|repo:a"]).status == :findings
    end

    test "an app with only pending coverage → :pending" do
      assert Verdict.rollup(%{"covered" => 2, "pending" => 1}, []).status == :pending
    end

    test "a NEVER-evaluated app → :unknown (genuinely absent, no rows at all)" do
      assert Verdict.rollup(%{}, []).status == :unknown
    end

    test "the full three-valued split is intrinsic to ONE cell's histogram" do
      # given ONE guarantee cell holding failing + covered rows, each app's scoped
      # histogram alone resolves its verdict — no /set companion, no special-casing:
      covered_app = %{"covered" => 5}          # evaluated, all fine
      failing_app = %{"covered" => 4, "failing" => 1}
      never_app = %{}                          # never evaluated

      assert Verdict.rollup(covered_app, []).status == :green
      assert Verdict.rollup(failing_app, ["x"]).status == :findings
      assert Verdict.rollup(never_app, []).status == :unknown
    end
  end

  test "equivalence: `covered` and `present` are interchangeable to the rollup" do
    # the rollup only special-cases failing/pending/stale; any OTHER status (covered,
    # present, ok, …) is 'evaluated & fine'. So a host picks its own coverage label.
    assert Verdict.rollup(%{"covered" => 1}, []).status == :green
    assert Verdict.rollup(%{"present" => 1}, []).status == :green
    assert Verdict.rollup(%{"evaluated" => 1}, []).status == :green
  end
end

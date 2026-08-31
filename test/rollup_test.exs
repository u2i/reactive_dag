defmodule ReactiveDag.RollupTest do
  @moduledoc """
  The one fold both phases use.

  `Report.total/2` and `Source.detail_total/2` were two implementations of
  the same arithmetic over different containers — they had to agree, and nothing
  made them. They now both delegate here.

  `report_breakdown_test.exs` and `scan_spend_test.exs` still exercise each
  caller's own container handling, which is the part that genuinely differs.
  This covers the shared arithmetic once.
  """
  use ExUnit.Case, async: true

  alias ReactiveDag.Rollup

  describe "total/2" do
    test "sums a flat count" do
      assert Rollup.total([%{t: 100}, %{t: 5}], :t) == 105
    end

    test "sums a per-bucket breakdown to one number" do
      assert Rollup.total([%{t: %{"a" => 100, "b" => 5}}], :t) == 105
    end

    test "mixes shapes without refusing to answer" do
      assert Rollup.total([%{t: %{"a" => 100}}, %{t: 5}], :t) == 105
    end

    test "maps lacking the key contribute nothing" do
      assert Rollup.total([%{t: 7}, %{}, %{other: 9}], :t) == 7
    end

    test "a key nothing reported is zero, not a raise" do
      assert Rollup.total([%{t: 7}], :missing) == 0
    end

    test "an empty enumerable is zero" do
      assert Rollup.total([], :t) == 0
    end

    test "non-map entries are skipped rather than raising" do
      # The callers hand this whatever their container held; a step with no meta
      # at all is ordinary, not an error.
      assert Rollup.total([%{t: 5}, nil, :whatever], :t) == 5
    end

    test "non-numeric bucket values are ignored" do
      assert Rollup.total([%{t: %{"a" => 10, "bad" => "lots"}}], :t) == 10
    end
  end

  describe "by/2" do
    test "sums per bucket" do
      assert Rollup.by([%{t: %{"a" => 100, "b" => 5}}, %{t: %{"a" => 1}}], :t) ==
               %{"a" => 101, "b" => 5}
    end

    test "a flat number lands under :unattributed rather than vanishing" do
      assert Rollup.by([%{t: %{"a" => 100}}, %{t: 5}], :t) == %{"a" => 100, unattributed: 5}
    end

    test "a key nothing reported is an empty map" do
      assert Rollup.by([%{t: 1}], :missing) == %{}
    end

    test "non-numeric bucket values are ignored" do
      assert Rollup.by([%{t: %{"a" => 10, "bad" => "lots"}}], :t) == %{"a" => 10}
    end
  end

  describe "the invariant" do
    # What keeps a breakdown honest: a cost line built from `by/2` and a summary
    # built from `total/2` must never disagree, whatever mix of shapes arrives.
    test "by/2 always sums to total/2" do
      for metas <- [
            [%{t: %{"a" => 100, "b" => 5}}, %{t: %{"a" => 1}}],
            [%{t: 100}, %{t: 5}],
            [%{t: %{"a" => 100}}, %{t: 5}, %{}, %{other: 9}],
            [%{t: %{"a" => 10, "bad" => "lots"}}],
            [%{t: 5}, nil, :whatever],
            []
          ] do
        parts = metas |> Rollup.by(:t) |> Map.values() |> Enum.sum()

        assert parts == Rollup.total(metas, :t), "disagree for #{inspect(metas)}"
      end
    end
  end

  describe "both callers agree" do
    # The point of the extraction: the same numbers, whichever phase produced
    # them and whichever entry point a host reaches for.
    test "a report and a sweep carrying identical meta total identically" do
      meta = %{tokens_in: %{"haiku" => 900, "luna" => 100}}

      report = %ReactiveDag.Report{
        passes: 1,
        duration_us: 1,
        steps: [
          %{
            cell: "c",
            pass: 1,
            claimed: ["k"],
            changed: ["k"],
            triggered_by: nil,
            duration_us: 1,
            op: :map,
            meta: meta
          }
        ]
      }

      sweep = %{SomeCrawler => %{changed: ["k"], detail: meta}}

      assert ReactiveDag.Report.total(report, :tokens_in) ==
               ReactiveDag.Source.detail_total(sweep, :tokens_in)

      assert ReactiveDag.Report.by(report, :tokens_in) ==
               ReactiveDag.Source.detail_by(sweep, :tokens_in)
    end
  end
end

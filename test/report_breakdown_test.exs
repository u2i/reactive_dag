defmodule ReactiveDag.ReportBreakdownTest do
  @moduledoc """
  `Report.by/2` — one meta key summed per bucket, and `total/2` over the same
  broken-down shape.

  ## Why a single number was not enough

  A drain's token count was summed flat across every step, so a graph running
  several models reported one undifferentiated number. Models differ in price by
  an order of magnitude, which makes that number unusable for the two questions
  anyone actually asks: what did this cost, and which model is driving it.

  The library still does not interpret meta — a bucket is a model name only
  because a host chose to key by one. What it does is stop flattening the
  breakdown a host reports.
  """
  use ExUnit.Case, async: true

  alias ReactiveDag.Drain.Report

  defp report(metas) do
    steps =
      for {meta, i} <- Enum.with_index(metas) do
        %{
          cell: "c#{i}",
          pass: 0,
          claimed: ["k"],
          changed: ["k"],
          triggered_by: nil,
          duration_us: 1,
          op: nil,
          depth: 0,
          meta: meta
        }
      end

    %Report{steps: steps, passes: 1, duration_us: 1}
  end

  describe "by/2" do
    test "sums a broken-down key per bucket across steps" do
      r =
        report([
          %{tokens_in: %{"haiku" => 100, "sonnet" => 10}},
          %{tokens_in: %{"haiku" => 5}}
        ])

      assert Report.by(r, :tokens_in) == %{"haiku" => 105, "sonnet" => 10}
    end

    test "a key no step reported is an empty map, not a raise" do
      assert Report.by(report([%{tokens_in: %{"haiku" => 1}}]), :cost_usd) == %{}
    end

    test "steps that lack the key contribute nothing" do
      r = report([%{tokens_in: %{"haiku" => 7}}, %{}, %{other: 1}])

      assert Report.by(r, :tokens_in) == %{"haiku" => 7}
    end

    test "a flat number is collected as :unattributed rather than dropped" do
      # A node reporting tokens without saying which model produced them is a
      # gap worth SEEING. Dropping it would make the breakdown silently
      # disagree with total/2.
      r = report([%{tokens_in: %{"haiku" => 100}}, %{tokens_in: 90}])

      assert Report.by(r, :tokens_in) == %{"haiku" => 100, unattributed: 90}
    end

    test "non-numeric bucket values are ignored" do
      r = report([%{tokens_in: %{"haiku" => 10, "bad" => "lots"}}])

      assert Report.by(r, :tokens_in) == %{"haiku" => 10}
    end
  end

  describe "total/2 over the same shapes" do
    test "sums a broken-down key to one number" do
      r =
        report([
          %{tokens_in: %{"haiku" => 100, "sonnet" => 10}},
          %{tokens_in: %{"haiku" => 5}}
        ])

      assert Report.total(r, :tokens_in) == 115
    end

    test "still sums flat numbers — the existing shape is untouched" do
      assert Report.total(report([%{tokens_in: 100}, %{tokens_in: 5}]), :tokens_in) == 105
    end

    test "mixed shapes across steps total correctly" do
      # A graph where one node reports per-model tokens and another reports a
      # bare count must not refuse to show a number.
      r = report([%{tokens_in: %{"haiku" => 100}}, %{tokens_in: 5}])

      assert Report.total(r, :tokens_in) == 105
    end
  end

  describe "the invariant" do
    # This is what keeps a breakdown honest: a cost line built from `by/2` and a
    # summary built from `total/2` must never disagree, whatever mix of shapes
    # the graph's nodes happen to report.
    test "by/2 always sums to total/2" do
      for metas <- [
            [%{tokens_in: %{"haiku" => 100, "sonnet" => 10}}, %{tokens_in: %{"haiku" => 5}}],
            [%{tokens_in: 100}, %{tokens_in: 5}],
            [%{tokens_in: %{"haiku" => 100}}, %{tokens_in: 5}, %{}, %{other: 9}],
            [%{tokens_in: %{"haiku" => 10, "bad" => "lots"}}],
            []
          ] do
        r = report(metas)
        breakdown = r |> Report.by(:tokens_in) |> Map.values() |> Enum.sum()

        assert breakdown == Report.total(r, :tokens_in),
               "by/2 and total/2 disagree for #{inspect(metas)}"
      end
    end
  end
end

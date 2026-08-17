defmodule ReactiveDag.ScanRunTest do
  @moduledoc """
  The poll and the drain it triggered, as one value.

  A scan is two phases that always happen together and were reported
  separately — a host wanting "what happened when this ran" collected both
  halves from one payload and had to know which answered which question.

  The case that forced it: a run's COST lives in both phases. A crawler that
  classifies documents with one model, feeding nodes that summarise them with
  another, spends in `detail` and in the report's steps, and neither number
  alone is the bill.
  """
  use ExUnit.Case, async: true

  alias ReactiveDag.{Drain.Report, ScanRun}

  defp step(meta) do
    %{
      cell: "summaries",
      pass: 1,
      claimed: ["k"],
      changed: ["k"],
      triggered_by: "docs",
      duration_us: 1,
      op: :map,
      meta: meta
    }
  end

  defp report(metas),
    do: %Report{steps: Enum.map(metas, &step/1), passes: 1, duration_us: 1}

  describe "total/2 and by/2 span both phases" do
    test "sums the poll's detail and the drain's step meta together" do
      # The crawl classified documents; the drain summarised them. Both spent,
      # and the bill is the sum — which no single existing call could give.
      run = %ScanRun{
        cell: "meeting_docs",
        detail: %{tokens_in: 900},
        report: report([%{tokens_in: 3000}])
      }

      assert ScanRun.total(run, :tokens_in) == 3900
    end

    test "keeps the two phases' models apart in the breakdown" do
      # The normal case: a classifier and a summariser are chosen separately, so
      # a run routinely spends on two models. "Which is driving this" is
      # unanswerable without the breakdown.
      run = %ScanRun{
        detail: %{tokens_in: %{"openai/gpt-5.6-luna" => 900}},
        report: report([%{tokens_in: %{"claude-haiku-4-5" => 3000}}])
      }

      assert ScanRun.by(run, :tokens_in) == %{
               "openai/gpt-5.6-luna" => 900,
               "claude-haiku-4-5" => 3000
             }
    end

    test "mixes a flat poll count with a per-model drain breakdown" do
      run = %ScanRun{
        detail: %{tokens_in: 60},
        report: report([%{tokens_in: %{"haiku" => 100}}])
      }

      assert ScanRun.total(run, :tokens_in) == 160
      assert ScanRun.by(run, :tokens_in) == %{"haiku" => 100, unattributed: 60}
    end

    test "a poll that spent nothing still totals the drain" do
      run = %ScanRun{detail: %{}, report: report([%{tokens_in: 100}])}

      assert ScanRun.total(run, :tokens_in) == 100
    end

    test "a run with no drain totals just the poll" do
      run = %ScanRun{detail: %{tokens_in: 900}, report: nil}

      assert ScanRun.total(run, :tokens_in) == 900
    end

    test "a key neither phase reported is zero" do
      run = %ScanRun{detail: %{tokens_in: 900}, report: report([%{tokens_in: 100}])}

      assert ScanRun.total(run, :cost_usd) == 0
      assert ScanRun.by(run, :cost_usd) == %{}
    end

    test "by/2 always sums to total/2 across both phases" do
      for {detail, metas} <- [
            {%{t: 60}, [%{t: %{"a" => 100}}]},
            {%{t: %{"a" => 1}}, [%{t: %{"a" => 2}}, %{}]},
            {%{}, []},
            {%{t: %{"a" => 1, "bad" => "lots"}}, [%{t: 5}]}
          ] do
        run = %ScanRun{detail: detail, report: report(metas)}
        parts = run |> ScanRun.by(:t) |> Map.values() |> Enum.sum()

        assert parts == ScanRun.total(run, :t),
               "disagree for #{inspect(detail)} / #{inspect(metas)}"
      end
    end
  end

  describe "the predicates" do
    test "changed? is about the POLL, not the drain" do
      # A poll can change keys whose recompute produced nothing downstream, and
      # a drain can run over marks another source left. They are different
      # questions and were previously both inferred from counts.
      assert ScanRun.changed?(%ScanRun{changed: ["a"]})
      refute ScanRun.changed?(%ScanRun{changed: []})
    end

    test "drained? distinguishes 'no drain ran' from 'a drain did nothing'" do
      # An unscannable source completes WITHOUT draining. Rendering "0 passes"
      # for it would report a drain that never happened.
      refute ScanRun.drained?(%ScanRun{report: nil})
      assert ScanRun.drained?(%ScanRun{report: report([])})
    end

    test "complete? is the honest-gap predicate" do
      # A scan that could not look must never render as a scan that found
      # nothing.
      assert ScanRun.complete?(%ScanRun{unreachable: []})
      refute ScanRun.complete?(%ScanRun{unreachable: [{"archive", :timeout}]})
    end
  end
end

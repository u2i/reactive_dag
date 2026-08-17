defmodule ReactiveDag.ScanSpendTest do
  @moduledoc """
  `detail_total/2` and `detail_by/2` — what a POLL cost, rolled up.

  ## The gap these close

  A drain returns a `%Report{}` and the library totals across its steps. A poll
  returns one result per source and there is no report, because a scan is not a
  cascade: sources are independent and there is nothing to propagate.

  So a crawler that classifies each new document with a model spent on every
  poll and none of it appeared anywhere. Not for want of recording — scans and
  drains are separate phases by design, so a poll has no drain step to attach
  to. Nothing aggregated it, which is a different problem and this is its fix.
  """
  use ExUnit.Case, async: true

  alias ReactiveDag.Source

  defp result(detail), do: %{changed: ["k"], detail: detail}

  describe "detail_total/2" do
    test "sums a flat count across sources" do
      results = %{
        SourceA => result(%{tokens_in: 900}),
        SourceB => result(%{tokens_in: 300})
      }

      assert Source.detail_total(results, :tokens_in) == 1200
    end

    test "sums a per-model breakdown to one number" do
      results = %{
        SourceA => result(%{tokens_in: %{"haiku" => 900, "luna" => 100}})
      }

      assert Source.detail_total(results, :tokens_in) == 1000
    end

    test "a sweep mixing LLM and plain crawlers still totals" do
      # The common case: one source calls a model, the rest just fetch. A
      # source with no `detail:` at all must contribute nothing rather than
      # raise, or adding one LLM crawler breaks every existing sweep.
      results = %{
        SourceA => result(%{tokens_in: 900}),
        SourceB => %{changed: ["k"]},
        SourceC => result(%{other_key: 5})
      }

      assert Source.detail_total(results, :tokens_in) == 900
    end

    test "a key nothing reported is zero, not a raise" do
      assert Source.detail_total(%{SourceA => result(%{tokens_in: 1})}, :cost_usd) == 0
    end

    test "an empty sweep is zero" do
      assert Source.detail_total(%{}, :tokens_in) == 0
    end
  end

  describe "detail_by/2" do
    test "sums per model across sources" do
      results = %{
        SourceA => result(%{tokens_in: %{"haiku" => 900, "luna" => 100}}),
        SourceB => result(%{tokens_in: %{"haiku" => 50}})
      }

      assert Source.detail_by(results, :tokens_in) == %{"haiku" => 950, "luna" => 100}
    end

    test "a flat count lands under :unattributed rather than vanishing" do
      results = %{
        SourceA => result(%{tokens_in: %{"haiku" => 900}}),
        SourceB => result(%{tokens_in: 60})
      }

      assert Source.detail_by(results, :tokens_in) == %{"haiku" => 900, unattributed: 60}
    end

    test "a key nothing reported is an empty map" do
      assert Source.detail_by(%{SourceA => result(%{tokens_in: 1})}, :cost_usd) == %{}
    end
  end

  describe "the shapes a host can hand these" do
    # `poll_all/2` returns `%{module => result}`; `poll_cell/3` returns one
    # result. Both have to work, or a host totals its sweep one way and its
    # single-cell refresh another.
    test "a single poll_cell/3 result" do
      assert Source.detail_total(result(%{tokens_in: 42}), :tokens_in) == 42
    end

    test "a bare list of results" do
      assert Source.detail_total([result(%{tokens_in: 1}), result(%{tokens_in: 2})], :tokens_in) ==
               3
    end

    test "results still wrapped in {:ok, _}" do
      results = %{SourceA => {:ok, result(%{tokens_in: 7})}}

      assert Source.detail_total(results, :tokens_in) == 7
    end

    test "a single result is not mistaken for a module-keyed map" do
      # `%{changed: …, detail: …}` IS a map, so the module-keyed branch would
      # have taken its VALUES and looked for `detail` inside a key list.
      single = %{changed: ["a", "b"], detail: %{tokens_in: 5}}

      assert Source.detail_total(single, :tokens_in) == 5
      assert Source.detail_by(single, :tokens_in) == %{unattributed: 5}
    end
  end

  describe "the invariant" do
    test "detail_by/2 always sums to detail_total/2" do
      for results <- [
            %{A => result(%{t: %{"h" => 9, "l" => 1}}), B => result(%{t: 5})},
            %{A => result(%{t: 5}), B => %{changed: []}},
            %{A => result(%{t: %{"h" => 1, "bad" => "lots"}})},
            %{}
          ] do
        parts = results |> Source.detail_by(:t) |> Map.values() |> Enum.sum()

        assert parts == Source.detail_total(results, :t),
               "parts and total disagree for #{inspect(results)}"
      end
    end
  end
end

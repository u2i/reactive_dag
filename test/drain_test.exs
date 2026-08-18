defmodule ReactiveDag.DrainTest do
  @moduledoc """
  The drain loop against an IN-MEMORY frontier (a fake repo speaking the four
  SQL shapes `ReactiveDag.Frontier` issues — no Postgres). Covers the loop's
  own behavior: depth order, propagation into the report, and the stale-row
  case a live dirty table can produce but a unit-built plan never does.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias ReactiveDag.{Cell, Drain, Frontier, Graph}

  # The dirty table as an Agent: a MapSet of {cell_id, key}. Implements exactly
  # the four statements Frontier issues, matched by SQL prefix.
  defmodule FakeRepo do
    def start_link, do: Agent.start_link(fn -> MapSet.new() end, name: __MODULE__)

    def query!("INSERT INTO " <> _, params) do
      params
      |> Enum.chunk_every(5)
      |> Enum.each(fn [cell, key, _reason, _at, _prior] ->
        Agent.update(__MODULE__, &MapSet.put(&1, {cell, key}))
      end)

      %{rows: []}
    end

    def query!("SELECT DISTINCT cell_id" <> _, _params) do
      ids = Agent.get(__MODULE__, & &1) |> Enum.map(&elem(&1, 0)) |> Enum.uniq()
      %{rows: Enum.map(ids, &[&1])}
    end

    def query!("DELETE FROM " <> _, [cell]) do
      keys =
        Agent.get_and_update(__MODULE__, fn set ->
          {mine, rest} = Enum.split_with(set, fn {c, _k} -> c == cell end)
          {Enum.map(mine, &elem(&1, 1)), MapSet.new(rest)}
        end)

      %{rows: Enum.map(keys, &[&1, nil])}
    end

    def query!("SELECT COUNT" <> _, _params) do
      %{rows: [[Agent.get(__MODULE__, &MapSet.size/1)]]}
    end

    # Enough of a transaction to test the property that matters: a snapshot,
    # restored if the block raises. That is what the drain relies on to keep a
    # claim when a recompute fails.
    # Enough of a transaction to test the two properties the drain relies on: a
    # snapshot restored when the block RAISES, and one restored when it calls
    # `rollback/1` — which is how a savepoint contains a reported failure.
    def transaction(fun, _opts \\ []) do
      snapshot = Agent.get(__MODULE__, & &1)

      try do
        {:ok, fun.()}
      catch
        :throw, {:rollback, reason} ->
          Agent.update(__MODULE__, fn _ -> snapshot end)
          {:error, reason}
      rescue
        e ->
          Agent.update(__MODULE__, fn _ -> snapshot end)
          reraise e, __STACKTRACE__
      end
    end

    def rollback(reason), do: throw({:rollback, reason})
  end

  setup do
    start_supervised!(%{id: FakeRepo, start: {FakeRepo, :start_link, []}})
    prev = Application.get_env(:reactive_dag, :repo)
    Application.put_env(:reactive_dag, :repo, FakeRepo)
    on_exit(fn -> Application.put_env(:reactive_dag, :repo, prev) end)
    :ok
  end

  # leaf a → derived b (no compute in meta: Node.Recompute passes keys through)
  defp plan do
    Graph.build([
      %Cell{id: "a", leaf?: true},
      %Cell{id: "b", inputs: ["a"], meta: %{key_rule: :identity}}
    ])
  end

  test "drains leaf → parent in depth order, recording the causal trace" do
    Frontier.mark_dirty("a", ["k1"], "seed")

    {:ok, report} =
      Drain.run(plan(), recompute: ReactiveDag.Node.Recompute, key_rule: ReactiveDag.Node.KeyRule)

    assert Enum.map(report.steps, & &1.cell) == ["a", "b"]
    assert [%{triggered_by: nil}, %{triggered_by: "a", claimed: ["k1"]}] = report.steps
    assert Frontier.empty?()
  end

  # a recompute that re-dirties its own input — the runaway shape
  defmodule SelfDirtying do
    @behaviour ReactiveDag.RecomputeStrategy
    @impl true
    def recompute(%{id: "b"} = _cell, keys) do
      ReactiveDag.Frontier.mark_dirty("a", ["k1"], "loop")
      {:ok, keys}
    end

    def recompute(_cell, keys), do: {:ok, keys}
  end

  test "the runaway guard raises RunawayError CARRYING the partial report" do
    # regression: it used to raise a bare string, discarding the very trace
    # that shows which cells keep re-dirtying each other.
    Frontier.mark_dirty("a", ["k1"], "seed")

    err =
      assert_raise Drain.RunawayError, ~r/exceeded 40 passes.*"b"/s, fn ->
        Drain.run(plan(), recompute: SelfDirtying, max_passes: 40)
      end

    assert %ReactiveDag.Drain.Report{} = err.report
    assert err.report.passes == 40
    # the trace shows the loop: a and b alternating
    assert Enum.map(err.report.steps, & &1.cell) |> Enum.uniq() |> Enum.sort() == ["a", "b"]
  end

  test "a stale frontier row (cell absent from the plan) is claimed, logged, and skipped" do
    # regression: this used to KeyError out of Map.fetch! AFTER the claim had
    # deleted the dirty keys — crashing the drain and destroying the work item.
    Frontier.mark_dirty("a", ["k1"], "seed")
    Frontier.mark_dirty("ghost", ["gk"], "a source writing to a renamed cell")

    log =
      capture_log(fn ->
        {:ok, report} =
          Drain.run(plan(),
            recompute: ReactiveDag.Node.Recompute,
            key_rule: ReactiveDag.Node.KeyRule
          )

        # the real cells still drained; the stale id produced no step
        assert Enum.map(report.steps, & &1.cell) == ["a", "b"]
      end)

    assert log =~ "ghost"
    assert log =~ ~s(["gk"])
    # the stale rows were consumed, not left to re-select forever
    assert Frontier.empty?()
  end
  # ── strategy-reported meta ──────────────────────────────────────────────────
  #
  # A recompute may report what the work COST — token/cost counts for an LLM
  # node, cache hits, retries, rows scanned. The library carries the map without
  # interpreting it, so Insights and a dashboard can show it.

  defmodule CountingStrategy do
    @behaviour ReactiveDag.RecomputeStrategy

    @impl true
    def recompute(%ReactiveDag.Cell{leaf?: true}, keys), do: {:ok, keys}

    def recompute(_cell, keys),
      do: {:ok, keys, %{tokens_in: 100 * length(keys), tokens_out: 7, model: "stub"}}
  end

  defmodule SilentStrategy do
    @behaviour ReactiveDag.RecomputeStrategy
    @impl true
    def recompute(_cell, keys), do: {:ok, keys}
  end

  describe "a recompute that REPORTS a failure is contained" do
    # A fallible unit — a poll that could not reach its upstream — must not take
    # the drain down with it. `Source.poll_all/2` gives a sweep exactly this
    # containment; a cell that reports `{:error, _}` gets it where the work
    # actually happens.
    #
    # It must RETURN the failure, not raise: an exception inside a nested
    # transaction aborts the outer one, so only a value can be isolated by a
    # savepoint.
    defmodule FlakyStrategy do
      @behaviour ReactiveDag.RecomputeStrategy
      @impl true
      def recompute(%ReactiveDag.Cell{leaf?: true}, keys), do: {:ok, keys}
      def recompute(%ReactiveDag.Cell{id: "b"}, _keys), do: {:error, :upstream_down}
      def recompute(_cell, keys), do: {:ok, keys}
    end

    test "the drain finishes, and the failed cell's keys stay dirty" do
      Frontier.mark_dirty("a", ["k1"], "seed")

      # No raise: the drain completes and reports.
      assert {:ok, %ReactiveDag.Drain.Report{}} =
               Drain.run(plan(), recompute: FlakyStrategy, key_rule: ReactiveDag.Node.KeyRule)

      # 'b' rolled back, so its claim is intact for the next drain.
      assert Frontier.claim("b") == ["k1"]
    end

    test "it is not re-selected forever — the drain terminates" do
      # The hazard the skip set exists for: a rolled-back claim leaves the keys
      # dirty, so `next_cell` would hand back the same cell on every pass and
      # the drain would spin to the runaway guard.
      Frontier.mark_dirty("a", ["k1"], "seed")

      assert {:ok, report} =
               Drain.run(plan(),
                 recompute: FlakyStrategy,
                 key_rule: ReactiveDag.Node.KeyRule,
                 max_passes: 20
               )

      # A handful of passes, not twenty.
      assert report.passes < 10
    end
  end

  describe "a recompute that raises does not lose its claim" do
    # A claim is a DELETE, so without re-marking a transient failure — a
    # deadlock, a timeout, an upstream 503 — silently drops work: the keys are
    # gone from the frontier and nothing knows they are stale.
    #
    # The drain still fails loudly. Swallowing the error would mark keys clean
    # over work that did not happen, which is the one thing this substrate must
    # never do. The next drain retries instead of never knowing.
    defmodule BoomStrategy do
      @behaviour ReactiveDag.RecomputeStrategy
      @impl true
      def recompute(%ReactiveDag.Cell{leaf?: true}, keys), do: {:ok, keys}
      def recompute(_cell, _keys), do: raise("upstream 503")
    end

    test "the keys are back on the frontier, and the drain still raises" do
      Frontier.mark_dirty("a", ["k1", "k2"], "seed")

      assert_raise RuntimeError, "upstream 503", fn ->
        Drain.run(plan(), recompute: BoomStrategy, key_rule: ReactiveDag.Node.KeyRule)
      end

      # 'a' is a leaf and recomputed fine, propagating to 'b' — which blew up.
      # Its claim must be back, or those keys are stale forever.
      assert Frontier.claim("b") |> Enum.sort() == ["k1", "k2"]
    end

    test "a retry after the failure succeeds, so no work was lost" do
      Frontier.mark_dirty("a", ["k1"], "seed")

      assert_raise RuntimeError, fn ->
        Drain.run(plan(), recompute: BoomStrategy, key_rule: ReactiveDag.Node.KeyRule)
      end

      # The whole point: the next drain picks up what the failed one dropped.
      assert {:ok, report} =
               Drain.run(plan(), recompute: SilentStrategy, key_rule: ReactiveDag.Node.KeyRule)

      assert "b" in ReactiveDag.Drain.Report.cells(report)
    end
  end

  test "a strategy may report meta; it rides on the step untouched" do
    Frontier.mark_dirty("a", ["k1", "k2"], "seed")

    {:ok, report} =
      Drain.run(plan(), recompute: CountingStrategy, key_rule: ReactiveDag.Node.KeyRule)

    steps = Map.new(report.steps, &{&1.cell, &1})

    # the derived cell reported; the library stored the map verbatim
    assert steps["b"].meta == %{tokens_in: 200, tokens_out: 7, model: "stub"}

    # a leaf returns the 2-tuple, so its meta is simply empty
    assert steps["a"].meta == %{}
  end

  test "the 2-tuple contract still works — meta defaults to empty" do
    Frontier.mark_dirty("a", ["k1"], "seed")

    {:ok, report} =
      Drain.run(plan(), recompute: SilentStrategy, key_rule: ReactiveDag.Node.KeyRule)

    assert Enum.all?(report.steps, &(&1.meta == %{}))
  end

  test "Report.total/2 rolls one meta key up across steps" do
    Frontier.mark_dirty("a", ["k1", "k2"], "seed")

    {:ok, report} =
      Drain.run(plan(), recompute: CountingStrategy, key_rule: ReactiveDag.Node.KeyRule)

    # only the derived step reported tokens; the leaf contributes nothing
    assert ReactiveDag.Drain.Report.total(report, :tokens_in) == 200

    # a key no step reported sums to zero rather than raising
    assert ReactiveDag.Drain.Report.total(report, :cost_usd) == 0
  end

  # A strategy returning something the drain cannot use is a host bug, and the
  # message has to make it findable. A bare CaseClauseError says which VALUE was
  # unmatched but not which CELL produced it — and in a drain over a dozen cells
  # that is the whole question.
  describe "a strategy that returns an unusable shape" do
    defmodule BareListStrategy do
      @behaviour ReactiveDag.RecomputeStrategy
      @impl true
      def recompute(%ReactiveDag.Cell{leaf?: true}, keys), do: {:ok, keys}
      def recompute(_cell, keys), do: keys
    end

    defmodule ErrorTupleStrategy do
      @behaviour ReactiveDag.RecomputeStrategy
      @impl true
      def recompute(%ReactiveDag.Cell{leaf?: true}, keys), do: {:ok, keys}
      def recompute(_cell, _keys), do: {:error, :upstream_down}
    end

    defmodule BadMetaStrategy do
      @behaviour ReactiveDag.RecomputeStrategy
      @impl true
      def recompute(%ReactiveDag.Cell{leaf?: true}, keys), do: {:ok, keys}
      def recompute(_cell, keys), do: {:ok, keys, "tokens: lots"}
    end

    test "names the cell and both accepted shapes" do
      Frontier.mark_dirty("a", ["k1"], "seed")

      err =
        assert_raise ArgumentError, fn ->
          Drain.run(plan(), recompute: BareListStrategy, key_rule: ReactiveDag.Node.KeyRule)
        end

      assert err.message =~ ~s("b"), "must name the cell that produced it"
      assert err.message =~ "{:ok, changed_keys}"
      assert err.message =~ "{:ok, changed_keys, meta_map}"
    end

    test "an error tuple is a CONTAINED failure, not a rejected shape" do
      # This used to raise. `{:error, reason}` is now how a fallible cell — a
      # poll that could not reach its upstream — reports without taking the
      # drain down: the savepoint rolls it back, its keys stay dirty, and the
      # rest of the cascade runs. Accepting it as a quiet SUCCESS would still be
      # the unforgivable bug; it is not accepted as one.
      Frontier.mark_dirty("a", ["k1"], "seed")

      assert {:ok, _report} =
               Drain.run(plan(), recompute: ErrorTupleStrategy, key_rule: ReactiveDag.Node.KeyRule)

      # Not marked clean over work that did not happen.
      assert Frontier.claim("b") == ["k1"]
    end

    test "a 3-tuple whose meta is not a map is rejected" do
      Frontier.mark_dirty("a", ["k1"], "seed")

      assert_raise ArgumentError, ~r/meta_map/, fn ->
        Drain.run(plan(), recompute: BadMetaStrategy, key_rule: ReactiveDag.Node.KeyRule)
      end
    end
  end
end

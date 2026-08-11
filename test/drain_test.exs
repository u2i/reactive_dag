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
      |> Enum.chunk_every(4)
      |> Enum.each(fn [cell, key, _reason, _at] ->
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

      %{rows: Enum.map(keys, &[&1])}
    end

    def query!("SELECT COUNT" <> _, _params) do
      %{rows: [[Agent.get(__MODULE__, &MapSet.size/1)]]}
    end
  end

  setup do
    {:ok, _} = FakeRepo.start_link()
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
end

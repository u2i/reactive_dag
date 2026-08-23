defmodule ReactiveDag.DrainForceTest do
  @moduledoc """
  `force:` — a cell's claimed keys propagate whether or not its recompute
  reported them changed.

  The situation it is for: a host re-runs a cell out-of-band (to defeat a memo the
  library cannot see, or after clearing a payload by hand) and then wants the
  graph to catch up. The work is already done, so the op has nothing to report,
  and `changed == []` stops the cascade at that cell — leaving everything
  downstream unrecomputed against a payload that DID move.

  Cascade worked around this by hand-marking the re-read cell's immediate parents,
  which reaches one level and reads like duplicated propagation
  (u2i/muni_watch#22).
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.{Cell, Drain, Frontier, Graph}

  # The dirty table as an Agent — the four statements Frontier issues, by prefix.
  defmodule FakeRepo do
    def start_link, do: Agent.start_link(fn -> MapSet.new() end, name: __MODULE__)

    def query!("INSERT INTO " <> _, params) do
      params
      |> Enum.chunk_every(7)
      |> Enum.each(fn [cell, _tenant, key, _reason, _at, _held, _vid] ->
        Agent.update(__MODULE__, &MapSet.put(&1, {cell, key}))
      end)

      %{rows: []}
    end

    def query!("SELECT DISTINCT cell_id" <> _, _params) do
      ids = Agent.get(__MODULE__, & &1) |> Enum.map(&elem(&1, 0)) |> Enum.uniq()
      %{rows: Enum.map(ids, &[&1])}
    end

    def query!("DELETE FROM " <> _, [cell | _tenant]) do
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

  # root → mid → top. Three levels, so "only the forced cell is forced" is
  # observable: forcing `root` must reach `mid` and stop there if `mid` is honest
  # about not having changed.
  # Each cell DECLARES what recomputes it (`meta.compute`) — there is no strategy
  # to pass to the drain. `root_op` is the memoised one under test; the rest
  # recompute honestly unless a test says otherwise.
  defp plan(root_op, rest_op \\ __MODULE__.Works) do
    Graph.build([
      %Cell{id: "root", inputs: [], meta: %{key_rule: :identity, compute: root_op}},
      %Cell{id: "mid", inputs: ["root"], meta: %{key_rule: :identity, compute: rest_op}},
      %Cell{id: "top", inputs: ["mid"], meta: %{key_rule: :identity, compute: rest_op}}
    ])
  end

  # Every cell reports NOTHING changed — the memoised op's answer, and the
  # situation force exists for.
  defmodule Memoised do
    @behaviour ReactiveDag.Op
    @impl true
    def recompute(_cell, _keys), do: {:ok, []}
  end

  # `root` skips (memoised), but `mid` genuinely recomputes when it is reached.
  # `root` skips (memoised) while `mid`/`top` genuinely recompute — so the plan
  # stamps a different op per cell, which is how a real graph says it.
  defmodule Works do
    @behaviour ReactiveDag.Op
    @impl true
    def recompute(_cell, keys), do: {:ok, keys}
  end

  test "without force, a memoised recompute stops the cascade at its own cell" do
    # The baseline, and the bug a host hits: root was re-run out-of-band, so it
    # has nothing to report, and nothing downstream hears about it.
    Frontier.mark_dirty("root", ["k1"], "seed")

    {:ok, report} = Drain.run(plan(Memoised, Memoised))

    assert Enum.map(report.steps, & &1.cell) == ["root"]
  end

  test "force propagates the CLAIMED keys, so the next cell recomputes" do
    Frontier.mark_dirty("root", ["k1"], "seed")

    {:ok, report} =
      Drain.run(plan(Memoised, Memoised), force: "root")

    assert Enum.map(report.steps, & &1.cell) == ["root", "mid"]

    mid = Enum.find(report.steps, &(&1.cell == "mid"))
    assert mid.claimed == ["k1"], "the claimed key propagated, not the empty changed set"
  end

  test "only the FORCED cell is forced — an honest `unchanged` still stops the cascade" do
    # This is the semantics that keeps change detection meaningful past the first
    # hop. `mid` is reached because root was forced; `mid` reports nothing
    # changed on its own account, so `top` is never claimed.
    Frontier.mark_dirty("root", ["k1"], "seed")

    {:ok, report} =
      Drain.run(plan(Memoised, Memoised), force: "root")

    refute "top" in Enum.map(report.steps, & &1.cell),
           "forcing must not cascade transitively — that would recompute the whole graph"
  end

  test "a forced cell whose parent DOES change carries on down the graph" do
    Frontier.mark_dirty("root", ["k1"], "seed")

    {:ok, report} =
      Drain.run(plan(Memoised), force: "root")

    assert Enum.map(report.steps, & &1.cell) == ["root", "mid", "top"],
           "mid recomputed for real, so its own verdict carries the cascade to top"
  end

  test "force takes a list" do
    Frontier.mark_dirty("root", ["k1"], "seed")

    {:ok, report} =
      Drain.run(plan(Memoised, Memoised), force: ["root", "mid"])

    assert Enum.map(report.steps, & &1.cell) == ["root", "mid", "top"]
  end

  test "force: :all forces every cell" do
    Frontier.mark_dirty("root", ["k1"], "seed")

    {:ok, report} =
      Drain.run(plan(Memoised, Memoised), force: :all)

    assert Enum.map(report.steps, & &1.cell) == ["root", "mid", "top"]
  end

  test "an unforced cell named in force is untouched by it" do
    # forcing a cell that is never claimed changes nothing
    Frontier.mark_dirty("root", ["k1"], "seed")

    {:ok, report} =
      Drain.run(plan(Memoised, Memoised), force: "top")

    assert Enum.map(report.steps, & &1.cell) == ["root"]
  end

  # An op may report a key it was not claimed for — a whole-cell pass finding a
  # new one. Forcing must not drop that.
  defmodule ReportsExtra do
    @behaviour ReactiveDag.Op
    @impl true
    def recompute(_cell, _keys), do: {:ok, ["k2"]}
  end

  test "force propagates claimed keys UNION whatever the op reported" do
    Frontier.mark_dirty("root", ["k1"], "seed")

    {:ok, report} =
      Drain.run(plan(ReportsExtra), force: "root")

    mid = Enum.find(report.steps, &(&1.cell == "mid"))
    assert Enum.sort(mid.claimed) == ["k1", "k2"]
  end
end

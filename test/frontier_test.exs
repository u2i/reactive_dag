defmodule ReactiveDag.FrontierTest do
  @moduledoc """
  The frontier's Elixir-side logic against an in-memory dirty table (the same
  fake-repo approach as drain_test.exs — the SQL itself is a host-suite
  concern). Covers depth ordering with the unknown-id fallback, coalescing,
  claim-as-delete, and the table-name identifier guard.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.Frontier

  defmodule FakeRepo do
    def start_link, do: Agent.start_link(fn -> MapSet.new() end, name: __MODULE__)

    def query!("INSERT INTO " <> _, params) do
      params
      |> Enum.chunk_every(6)
      |> Enum.each(fn [cell, _tenant, key, _reason, _at, _prior] ->
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
  end

  setup do
    start_supervised!(%{id: FakeRepo, start: {FakeRepo, :start_link, []}})
    prev = Application.get_env(:reactive_dag, :repo)
    Application.put_env(:reactive_dag, :repo, FakeRepo)
    on_exit(fn -> Application.put_env(:reactive_dag, :repo, prev) end)
    :ok
  end

  test "mark_dirty coalesces and empty key lists are a no-op" do
    assert :ok = Frontier.mark_dirty("a", [], "noop")
    assert Frontier.empty?()

    Frontier.mark_dirty("a", ["k1", "k1", "k2"], "seed")
    assert Enum.sort(Frontier.claim("a")) == ["k1", "k2"]
    assert Frontier.empty?()
  end

  test "next_cell picks the smallest depth; unknown ids sort LAST (depth fallback)" do
    Frontier.mark_dirty("deep", ["x"], "seed")
    Frontier.mark_dirty("shallow", ["y"], "seed")
    Frontier.mark_dirty("ghost", ["z"], "stale")

    depths = %{"shallow" => 0, "deep" => 3}
    assert Frontier.next_cell(depths) == "shallow"

    Frontier.claim("shallow")
    assert Frontier.next_cell(depths) == "deep"

    Frontier.claim("deep")
    # only the unknown id remains — still returned (the drain logs + skips it)
    assert Frontier.next_cell(depths) == "ghost"
  end

  test "claim is per-cell: other cells' keys stay" do
    Frontier.mark_dirty("a", ["k1"], "seed")
    Frontier.mark_dirty("b", ["k2"], "seed")

    assert Frontier.claim("a") == ["k1"]
    refute Frontier.empty?()
    assert Frontier.claim("b") == ["k2"]
    assert Frontier.empty?()
  end

  test "an invalid configured table name fails loudly, not as SQL syntax" do
    prev = Application.get_env(:reactive_dag, :dirty_table)
    Application.put_env(:reactive_dag, :dirty_table, "my_dirty; DROP TABLE x")

    on_exit(fn ->
      if prev,
        do: Application.put_env(:reactive_dag, :dirty_table, prev),
        else: Application.delete_env(:reactive_dag, :dirty_table)
    end)

    assert_raise ArgumentError, ~r/not a valid table identifier/, fn ->
      Frontier.mark_dirty("a", ["k"], "seed")
    end
  end

  describe "with_lock/2 — one drain at a time, across a cluster" do
    # The per-cell claim is atomic, so no key is processed twice. But two
    # concurrent drains can pick the same CELL and split its keys, recomputing
    # it twice for a disjoint slice each. `Drain` has always said "run one drain
    # at a time per graph"; this is how a host running more than one node
    # actually gets that.
    defmodule LockRepo do
      def start_link(granted), do: Agent.start_link(fn -> {granted, []} end, name: __MODULE__)
      def calls, do: Agent.get(__MODULE__, &elem(&1, 1)) |> Enum.reverse()

      def query!("SELECT pg_try_advisory_lock" <> _, [key]) do
        granted = Agent.get_and_update(__MODULE__, fn {g, c} -> {g, {g, [{:lock, key} | c]}} end)
        %{rows: [[granted]]}
      end

      def query!("SELECT pg_advisory_unlock" <> _, [key]) do
        Agent.update(__MODULE__, fn {g, c} -> {g, [{:unlock, key} | c]} end)
        %{rows: [[true]]}
      end
    end

    setup context do
      start_supervised!(%{id: LockRepo, start: {LockRepo, :start_link, [context[:granted]]}})
      prev = Application.get_env(:reactive_dag, :repo)
      Application.put_env(:reactive_dag, :repo, LockRepo)
      on_exit(fn -> Application.put_env(:reactive_dag, :repo, prev) end)
      :ok
    end

    @tag granted: true
    test "runs the function and releases the lock" do
      assert {:ok, :did_it} = Frontier.with_lock(fn -> :did_it end)

      assert [{:lock, key}, {:unlock, key}] = LockRepo.calls()
    end

    @tag granted: true
    test "releases even when the function raises" do
      # a drain that blows up must not leave the graph locked for every other
      # node until someone notices
      assert_raise RuntimeError, fn -> Frontier.with_lock(fn -> raise "boom" end) end

      assert [{:lock, _}, {:unlock, _}] = LockRepo.calls()
    end

    @tag granted: false
    test "reports :busy without running the function" do
      assert :busy = Frontier.with_lock(fn -> flunk("must not run") end)
    end

    @tag granted: false
    test "and does not unlock a lock it never held" do
      Frontier.with_lock(fn -> :nope end)

      refute Enum.any?(LockRepo.calls(), &match?({:unlock, _}, &1))
    end

    @tag granted: true
    test "the key is derived from the dirty table, so two graphs do not block each other" do
      Frontier.with_lock(fn -> :a end)
      Frontier.with_lock(fn -> :b end, scope: "other_graph_dirty")

      [{:lock, k1}, {:unlock, _}, {:lock, k2}, {:unlock, _}] = LockRepo.calls()
      refute k1 == k2
    end
  end
end

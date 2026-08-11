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
end

defmodule ReactiveDag.FrontierPartitionTest do
  @moduledoc """
  `next_cell/2` must only ever return a cell that is IN the plan it was asked
  about.

  The frontier is one table shared by every plan in the application. `next_cell/2`
  reads it whole (`SELECT DISTINCT cell_id`) and picks the minimum by the depths
  it was given — and a cell absent from those depths used to rank at 1_000_000
  rather than be excluded. With only a foreign cell dirty, 1_000_000 was the
  minimum, so it was returned.

  The drain then claims it (`DELETE … RETURNING`, which succeeds — the row is
  real) and recomputes it against a plan that does not contain it. So a host
  running two plans over one dirty table had each drain able to consume the
  other's work.

  This matters for one plan too: a cell REMOVED from a graph, whose keys are
  still dirty from before the deploy, is a foreign cell by the same definition.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.Frontier

  defmodule FakeRepo do
    def start_link, do: Agent.start_link(fn -> MapSet.new() end, name: __MODULE__)

    def query!("INSERT INTO " <> _, params) do
      params
      |> Enum.chunk_every(5)
      |> Enum.each(fn [cell, key, _r, _t, _prior] ->
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
          {mine, rest} = Enum.split_with(set, fn {c, _} -> c == cell end)
          {Enum.map(mine, &elem(&1, 1)), MapSet.new(rest)}
        end)

      %{rows: Enum.map(keys, &[&1, nil])}
    end

    def query!("SELECT COUNT" <> _, _params),
      do: %{rows: [[Agent.get(__MODULE__, &MapSet.size/1)]]}
  end

  setup do
    {:ok, _} = FakeRepo.start_link()
    prev = Application.get_env(:reactive_dag, :repo)
    Application.put_env(:reactive_dag, :repo, FakeRepo)
    on_exit(fn -> Application.put_env(:reactive_dag, :repo, prev) end)
    :ok
  end

  # Plan A knows only its own cell. Plan B's cell is dirty in the same table.
  @plan_a %{"a_cell" => 0}

  test "a cell outside the plan is never returned, even as the only dirty cell" do
    Frontier.mark_dirty("b_cell", ["k"], "other plan")

    assert Frontier.next_cell(@plan_a, []) == nil,
           "a plan must not be handed a cell it cannot recompute"
  end

  test "its own cell is still returned when both are dirty" do
    Frontier.mark_dirty("a_cell", ["k1"], "mine")
    Frontier.mark_dirty("b_cell", ["k2"], "other plan")

    assert Frontier.next_cell(@plan_a, []) == "a_cell"
  end

  test "and the foreign cell is left ALONE, not consumed" do
    # The whole point: B's work must still be there for B's drain. A claim is a
    # DELETE, so a drain that merely LOOKED at the wrong cell would have eaten it.
    Frontier.mark_dirty("a_cell", ["k1"], "mine")
    Frontier.mark_dirty("b_cell", ["k2"], "other plan")

    assert Frontier.next_cell(@plan_a, []) == "a_cell"
    assert Frontier.claim("a_cell") == ["k1"]

    # A is drained; B's key is untouched.
    assert Frontier.next_cell(@plan_a, []) == nil
    assert Frontier.next_cell(%{"b_cell" => 0}, []) == "b_cell"
    assert Frontier.claim("b_cell") == ["k2"]
  end

  test "depth order still decides among the plan's OWN cells" do
    depths = %{"shallow" => 0, "deep" => 3, "middle" => 1}

    Frontier.mark_dirty("deep", ["k"], "seed")
    Frontier.mark_dirty("middle", ["k"], "seed")
    Frontier.mark_dirty("shallow", ["k"], "seed")

    assert Frontier.next_cell(depths, []) == "shallow"
    assert Frontier.next_cell(depths, ["shallow"]) == "middle"
    assert Frontier.next_cell(depths, ["shallow", "middle"]) == "deep"
  end

  test "`except` and the plan filter compose" do
    depths = %{"a" => 0, "b" => 1}

    Frontier.mark_dirty("a", ["k"], "seed")
    Frontier.mark_dirty("b", ["k"], "seed")
    Frontier.mark_dirty("foreign", ["k"], "seed")

    assert Frontier.next_cell(depths, ["a"]) == "b"

    # every own cell excluded, a foreign one dirty — still nil, not the foreign
    assert Frontier.next_cell(depths, ["a", "b"]) == nil
  end

  test "an empty frontier is still nil" do
    assert Frontier.next_cell(@plan_a, []) == nil
  end
end

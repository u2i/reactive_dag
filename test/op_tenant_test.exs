defmodule ReactiveDag.OpTenantTest do
  @moduledoc """
  A `compute Mod` op gets the plan's tenant by implementing `recompute/3`.

  This is the one place the library CANNOT pass a tenant implicitly: a `compute`
  op does its own writes, so there is no changeset of the library's to set it on.
  Found by piloting a real migration — a host's OSC-actuals node is a `compute`
  node, and it had no way to learn which municipality it was writing for.

  Both arities are optional and the library calls whichever the module exports,
  preferring `/3`. An op implementing only `/2` is correct for any host with one
  graph, and must keep working untouched — which is most ops.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.{Cascade, Cell, Node.Recompute}

  defmodule TwoArity do
    @behaviour ReactiveDag.Op

    @impl true
    def recompute(_cell, keys) do
      send(self(), {:called, :two_arity, keys})
      {:ok, keys}
    end
  end

  defmodule ThreeArity do
    @behaviour ReactiveDag.Op

    @impl true
    def recompute(_cell, keys, opts) do
      send(self(), {:called, :three_arity, keys, Keyword.get(opts, :tenant)})
      {:ok, keys}
    end
  end

  defmodule BothArities do
    @behaviour ReactiveDag.Op

    @impl true
    def recompute(_cell, keys), do: send(self(), {:called, :two}) && {:ok, keys}

    @impl true
    def recompute(_cell, keys, _opts), do: send(self(), {:called, :three}) && {:ok, keys}
  end

  defp cell(op), do: %Cell{id: "c", meta: %{compute: op}}

  test "an op implementing only `/2` is called as before" do
    assert {:ok, ["k"]} = Recompute.recompute(cell(TwoArity), ["k"], tenant: "a")

    assert_receive {:called, :two_arity, ["k"]}
  end

  test "an op implementing `/3` receives the plan's tenant" do
    assert {:ok, ["k"]} = Recompute.recompute(cell(ThreeArity), ["k"], tenant: "tenant_a")

    assert_receive {:called, :three_arity, ["k"], "tenant_a"}
  end

  test "`/3` is preferred when a module exports both" do
    # A module offering both is ambiguous, and the tenant-aware one is the safe
    # resolution: calling `/2` would silently drop the tenant.
    Recompute.recompute(cell(BothArities), ["k"], tenant: "a")

    assert_receive {:called, :three}
    refute_receive {:called, :two}
  end

  test "`/3` gets an empty tenant when the plan has none" do
    # An untenanted plan still calls `/3` if that is what the op exports — the
    # tenant is simply absent, rather than the op being called with the wrong
    # arity.
    Recompute.recompute(cell(ThreeArity), ["k"])

    assert_receive {:called, :three_arity, ["k"], nil}
  end

  test "the tenant reaches an op through a real drain" do
    # The end-to-end path: plan tenant -> Drain -> Recompute -> op. Asserting the
    # unit alone would pass while the drain dropped it, which is what happened to
    # the `:all` propagation branch when this was first threaded.
    plan = %ReactiveDag.Plan{
      cells: %{"c" => cell(ThreeArity)},
      parents: %{},
      depths: %{"c" => 0},
      tenant: "via_drain"
    }

    assert {:ok, _report} = drain_with(plan, ["k1"])
    assert_receive {:called, :three_arity, _keys, "via_drain"}
  end

  defmodule FakeRepo do
    def start_link, do: Agent.start_link(fn -> [] end, name: __MODULE__)

    def query!("INSERT INTO " <> _, params) do
      params
      |> Enum.chunk_every(7)
      |> Enum.each(fn [cell, tenant, key, _r, _t, _held, vid] ->
        Agent.update(__MODULE__, &(&1 ++ [{tenant, cell, key}]))
      end)

      %{rows: []}
    end

    def query!("SELECT DISTINCT cell_id" <> _, [tenant]) do
      ids =
        Agent.get(__MODULE__, & &1)
        |> Enum.filter(&(elem(&1, 0) == tenant))
        |> Enum.map(&elem(&1, 1))
        |> Enum.uniq()

      %{rows: Enum.map(ids, &[&1])}
    end

    def query!("DELETE FROM " <> _, [cell, tenant]) do
      keys =
        Agent.get_and_update(__MODULE__, fn rows ->
          {mine, rest} = Enum.split_with(rows, fn {t, c, _} -> t == tenant and c == cell end)
          {Enum.map(mine, &elem(&1, 2)), rest}
        end)

      %{rows: Enum.map(keys, &[&1, nil])}
    end

    def query!("SELECT COUNT" <> _, [tenant]) do
      %{rows: [[Enum.count(Agent.get(__MODULE__, & &1), &(elem(&1, 0) == tenant))]]}
    end

    def query!("SELECT pg_" <> _, _), do: %{rows: [[true]]}
  end

  defp drain_with(plan, keys) do
    # `start_supervised!`, not `start_link`: the agent is registered under a global
    # name and linked to the test process, so it exits ASYNCHRONOUSLY when a test
    # ends — and the next test can call `start_link` before the name is released,
    # failing setup with `{:error, {:already_started, …}}`. ExUnit's supervisor waits
    # for the exit, so the name is free by the time the next test starts.
    start_supervised!(%{id: FakeRepo, start: {FakeRepo, :start_link, []}})
    prev = Application.get_env(:reactive_dag, :repo)
    Application.put_env(:reactive_dag, :repo, FakeRepo)
    on_exit(fn -> Application.put_env(:reactive_dag, :repo, prev) end)

    ReactiveDag.Test.Pending.add("c", keys)
    ReactiveDag.Test.Pending.cascade(plan)
  end
end

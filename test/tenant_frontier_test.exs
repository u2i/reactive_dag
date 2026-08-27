defmodule ReactiveDag.TenantFrontierTest do
  @moduledoc """
  The frontier carries a TENANT, so one dirty table serves N independent graphs.

  Why it has to be the frontier and not the cell id: one table serves every plan
  in the application, so without a tenant "a cell this plan does not know" and "a
  cell nobody owns" are the SAME observation — and they need opposite handling.

    * FOREIGN (another tenant's row) — leave it alone. It has an owner.
    * ORPHANED (this tenant, a cell the plan no longer declares) — claim, log,
      drop. Claiming rather than skipping is what stops the row being
      re-selected on every pass forever (`ReactiveDag.Drain` does this).

  Filtering `next_cell/2` by the plan's cells instead was tried and is wrong: it
  makes both cases behave like FOREIGN, so an orphaned row is never cleared and
  stays dirty for good.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.Frontier

  defmodule FakeRepo do
    def start_link, do: Agent.start_link(fn -> [] end, name: __MODULE__)

    # rows are {tenant, cell, key}
    def query!("INSERT INTO " <> _, params) do
      params
      |> Enum.chunk_every(7)
      |> Enum.each(fn [cell, tenant, key, _r, _t, _held, vid] ->
        Agent.update(__MODULE__, fn rows ->
          if {tenant, cell, key} in rows, do: rows, else: rows ++ [{tenant, cell, key}]
        end)
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
          {mine, rest} =
            Enum.split_with(rows, fn {t, c, _} -> t == tenant and c == cell end)

          {Enum.map(mine, &elem(&1, 2)), rest}
        end)

      %{rows: Enum.map(keys, &[&1, nil])}
    end

    def query!("SELECT COUNT" <> _, [tenant]) do
      n = Agent.get(__MODULE__, & &1) |> Enum.count(&(elem(&1, 0) == tenant))
      %{rows: [[n]]}
    end
  end

  setup do
    # `start_supervised!`, not `start_link`: the agent is registered under a global
    # name and linked to the test process, so it exits ASYNCHRONOUSLY when a test
    # ends — and the next test can call `start_link` before the name is released,
    # failing setup with `{:error, {:already_started, …}}`. ExUnit's supervisor waits
    # for the exit, so the name is free by the time the next test starts.
    start_supervised!(%{id: FakeRepo, start: {FakeRepo, :start_link, []}})
    prev = Application.get_env(:reactive_dag, :repo)
    Application.put_env(:reactive_dag, :repo, FakeRepo)
    on_exit(fn -> Application.put_env(:reactive_dag, :repo, prev) end)
    :ok
  end

  defp a, do: [tenant: "tenant_a"]
  defp b, do: [tenant: "tenant_b"]

  describe "the default tenant" do
    test "an omitted tenant is `\"*\"`, and a nil tenant is the same row" do
      # `"*"` not NULL: the coalescing unique index backs mark_dirty's ON
      # CONFLICT, and Postgres treats NULLs as DISTINCT in a unique index — a
      # nullable column would grow a queue row per mark instead of coalescing.
      assert Frontier.tenant([]) == "*"
      assert Frontier.tenant(tenant: nil) == "*"
      assert Frontier.tenant(tenant: :atom_ok) == "atom_ok"
      assert Frontier.tenant(tenant: "x") == "x"
    end

    test "untenanted calls behave exactly as before" do
      Frontier.mark_dirty("cell", ["k1", "k2"], "seed")

      assert Frontier.next_cell(%{"cell" => 0}) == "cell"
      assert Enum.sort(Frontier.claim("cell")) == ["k1", "k2"]
      assert Frontier.empty?()
    end

    test "marking the same key twice coalesces" do
      Frontier.mark_dirty("cell", ["k"], "first")
      Frontier.mark_dirty("cell", ["k"], "second")

      assert Frontier.claim("cell") == ["k"], "one row, not two"
    end
  end

  describe "isolation between tenants" do
    test "a drain sees only its own tenant's dirty cells" do
      Frontier.mark_dirty("docs", ["k"], "seed", a())
      Frontier.mark_dirty("other", ["k"], "seed", b())

      assert Frontier.next_cell(%{"docs" => 0}, [], a()) == "docs"

      # B's cell is invisible to A even though A's plan doesn't contain it
      assert Frontier.next_cell(%{"docs" => 0, "other" => 0}, [], a()) == "docs"
    end

    test "the SAME cell id in two tenants is two independent units of work" do
      # The point of the whole design: every tenant runs the same topology, so
      # cell ids repeat and must not collide.
      Frontier.mark_dirty("shell", ["k_a"], "seed", a())
      Frontier.mark_dirty("shell", ["k_b"], "seed", b())

      assert Frontier.claim("shell", a()) == ["k_a"]

      assert Frontier.claim("shell", b()) == ["k_b"],
             "B's work survived A's claim of the same cell id"
    end

    test "a claim does not consume another tenant's keys" do
      Frontier.mark_dirty("shell", ["k_a"], "seed", a())
      Frontier.mark_dirty("shell", ["k_b"], "seed", b())

      Frontier.claim("shell", a())

      refute Frontier.empty?(b()), "B still has work"
      assert Frontier.empty?(a())
    end

    test "`empty?` is per tenant" do
      Frontier.mark_dirty("docs", ["k"], "seed", b())

      assert Frontier.empty?(a()), "A has nothing to do"
      refute Frontier.empty?(b())
    end
  end

  describe "a cell the plan does not declare" do
    test "an ORPHAN in this tenant is still returned, so it can be cleared" do
      # A renamed or removed cell, or a source writing an old leaf id. The drain
      # claims it, logs it and drops it; skipping it instead would leave the row
      # dirty forever and re-select it on every pass.
      Frontier.mark_dirty("ghost", ["k"], "stale", a())

      assert Frontier.next_cell(%{"live" => 0}, [], a()) == "ghost",
             "an unknown cell of OURS is work to clear, not work to ignore"

      assert Frontier.claim("ghost", a()) == ["k"]
      assert Frontier.empty?(a())
    end

    test "...while another tenant's cell is never returned" do
      Frontier.mark_dirty("ghost", ["k"], "stale", b())

      assert Frontier.next_cell(%{"live" => 0}, [], a()) == nil,
             "not ours to look at, and not ours to clear"

      refute Frontier.empty?(b()), "and B's row is untouched"
    end
  end

  describe "ordering is unchanged" do
    test "depth decides, and `except` still skips" do
      depths = %{"shallow" => 0, "middle" => 1, "deep" => 3}

      for id <- ["deep", "middle", "shallow"],
          do: Frontier.mark_dirty(id, ["k"], "seed", a())

      assert Frontier.next_cell(depths, [], a()) == "shallow"
      assert Frontier.next_cell(depths, ["shallow"], a()) == "middle"
      assert Frontier.next_cell(depths, ["shallow", "middle"], a()) == "deep"
    end

    test "an unknown cell still sorts LAST among ours" do
      Frontier.mark_dirty("ghost", ["k"], "stale", a())
      Frontier.mark_dirty("known", ["k"], "seed", a())

      assert Frontier.next_cell(%{"known" => 5}, [], a()) == "known",
             "a real cell at depth 5 still beats an unknown one"
    end
  end
end

defmodule ReactiveDag.StaleTest do
  @moduledoc """
  Staleness, asked of the data rather than of the engine.

  A cascade propagates; this finds what propagation MISSED. The two are not
  alternatives — a dropped enqueue, a crash mid-walk, a gate that was off all
  leave a row stale with nothing recording the fact, and the engine has no way
  to know. The comparison does.
  """
  use ReactiveDag.PostgresCase

  alias ReactiveDag.Fixtures.StaleGraph
  alias ReactiveDag.Fixtures.StaleGraph.{Derived, Source}

  alias ReactiveDag.Stale

  defmodule Double do
    @behaviour ReactiveDag.Op
    @impl true
    def recompute(_cell, _keys, _opts), do: {:ok, []}
  end

  defp plan, do: ReactiveDag.Node.graph([Source, Derived])

  defp write(resource, attrs) do
    resource |> Ash.Changeset.for_create(:upsert, attrs) |> Ash.create!()
  end

  setup_all do
    migrate!(StaleGraph.Domain)
    :ok
  end

  setup do
    pid = Ecto.Adapters.SQL.Sandbox.start_owner!(ReactiveDag.TestRepo, shared: true)
    on_exit(fn -> Ecto.Adapters.SQL.Sandbox.stop_owner(pid) end)

    truncate(StaleGraph.tables())
    for r <- [Source, Derived], row <- Ash.read!(r), do: Ash.destroy!(row)
    :ok
  end

  describe "edges come from the plan" do
    test "every declared input of a temporal cell is an edge" do
      assert [{"source", "derived", :identity}] = Stale.edges(plan())
    end
  end

  describe "a child derived BEFORE its input moved is stale" do
    test "and one derived after is not" do
      write(Source, %{key: "a", amount: 1.0})
      write(Source, %{key: "b", amount: 1.0})
      Process.sleep(5)

      # both derived now — neither is behind
      write(Derived, %{key: "a", doubled: 2.0})
      write(Derived, %{key: "b", doubled: 2.0})

      assert {:ok, []} = Stale.on_edge(plan(), "source", "derived")

      # `a`'s input moves AFTER it was derived, and nothing propagates
      Process.sleep(5)
      write(Source, %{key: "a", amount: 9.0})

      assert {:ok, ["a"]} = Stale.on_edge(plan(), "source", "derived"),
             "the comparison finds what no propagation recorded"
    end

    test "a key with no parent row is not stale — it is orphaned, which is a different question" do
      write(Derived, %{key: "ghost", doubled: 0.0})

      assert {:ok, []} = Stale.on_edge(plan(), "source", "derived")
    end
  end

  describe "the answer is never a bare list when it could not be computed" do
    test "an unknown cell says so" do
      assert {:error, :no_such_cell} = Stale.on_edge(plan(), "nope", "derived")
    end
  end
end

defmodule ReactiveDag.VerdictNodeTest do
  @moduledoc """
  A VERDICT is a row like any other.

  There used to be a second node shape for this — `verdict? true`, with no
  table, writing a status straight into the coordination tuple through a
  dedicated `status:` slot. It saved a migration when the answer was one word,
  and cost a ceiling: the tuple's schema is fixed, so the moment a verdict
  wanted company (a headroom, a breached_at) the shape had nothing to offer and
  you abandoned it entirely.

  Now a verdict node is an ordinary payload node whose row happens to carry a
  `:status` column — written by `into:` like any other column, queryable with
  ordinary Ash reads, and free to carry whatever else the domain needs.
  """
  use ReactiveDag.PostgresCase

  alias ReactiveDag.Fixtures.VerdictGraph
  alias ReactiveDag.Fixtures.VerdictGraph.{Health, Totals}

  require Ash.Query

  alias ReactiveDag.Node.Recompute

  # the observed stores — a leaf node the verdict reads declaratively.
  # `verdict? true` saves you a table when the result is one word. The moment
  # it wants company — a headroom, a breached_at — that shape has nothing to
  # offer, because the coordination tuple has nowhere to put them. A node with
  # a :status column is an ordinary payload node whose row happens to be a
  # verdict, so it carries whatever it likes.

  setup_all do
    migrate!(VerdictGraph.Domain)
    :ok
  end

  setup do
    pid = Ecto.Adapters.SQL.Sandbox.start_owner!(ReactiveDag.TestRepo, shared: true)
    on_exit(fn -> Ecto.Adapters.SQL.Sandbox.stop_owner(pid) end)

    truncate(VerdictGraph.tables())
    :ok
  end

  setup do
    Totals |> Ash.read!() |> Enum.each(&Ash.destroy!/1)
    Health |> Ash.read!() |> Enum.each(&Ash.destroy!/1)

    for {k, t} <- [{"a", 10.0}, {"b", 5000.0}] do
      Totals |> Ash.Changeset.for_create(:create, %{key: k, total: t}) |> Ash.create!()
    end

    :ok
  end

  test "a verdict is an ordinary column — no verdict? needed" do
    plan = ReactiveDag.Node.graph([Totals, Health])

    {:ok, changed} = Recompute.recompute(plan.cells["health"], ["*"])
    assert Enum.sort(changed) == ["a", "b"]

    rows = Health |> Ash.read!() |> Map.new(&{&1.key, &1.status})
    assert rows == %{"a" => "present", "b" => "failing"}
  end

  test "the graph-wide question is an ordinary Ash read" do
    plan = ReactiveDag.Node.graph([Totals, Health])
    {:ok, _} = Recompute.recompute(plan.cells["health"], ["*"])

    failing = Health |> Ash.Query.filter(status == "failing") |> Ash.read!()

    assert Enum.map(failing, & &1.key) == ["b"]
  end

  test "the node is NOT a verdict node — it has a resource and a payload row" do
    cell = ReactiveDag.Node.graph([Totals, Health]).cells["health"]

    refute cell.meta[:verdict]
    assert cell.meta[:resource] == Health
  end
end

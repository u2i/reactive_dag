defmodule ReactiveDag.InsightsTest do
  @moduledoc """
  `ReactiveDag.Insights` — the engine viewed from outside (issue #41).

  Everything here is a READ over things the library already knows: the plan
  carries structure and depths, the coordination tuple carries per-key status
  and freshness, and `Drain.Report` is already a causal trace. Insights just
  assembles them into the shape a dashboard, a mix task, or an alerting check
  asks for — deliberately with no UI dependency of any kind.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.{Drain, Frontier, Insights}

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      allow_unregistered?(true)
    end
  end

  defmodule Expenses do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
      private?(true)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :category, :string, public?: true
      attribute :amount, :float, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :create do
        accept([:key, :category, :amount])
      end
    end

    reactive do
      id(:expenses)
      op(:source)
      leaf?(true)
    end
  end

  defmodule CategoryTotals do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
      private?(true)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :category, :string, public?: true
      attribute :total, :float, public?: true
      attribute :n, :integer, public?: true
    end

    identities do
      identity :by_key, [:key], pre_check_with: ReactiveDag.InsightsTest.Domain
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        upsert_identity(:by_key)
        accept([:key, :category, :total, :n])
      end
    end

    reactive do
      id(:category_totals)
      op(:fold)

      recompute_by :category, to: :expenses, from: :category
      reduce into: [sum: [amount: :total], count: :n]
    end
  end

  defmodule CategoryHealth do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Simple,
      extensions: [ReactiveDag.Node]

    reactive do
      id(:category_health)
      op(:check)
      verdict?(true)

      reduce over: :category_totals,
             group_by: :key,
             status: fn _cat, [row | _] -> if row.total < 1000.0, do: "present", else: "failing" end
    end
  end

  defmodule FakeRepo do
    def start_link, do: Agent.start_link(fn -> MapSet.new() end, name: __MODULE__)

    def query!("INSERT INTO " <> _, params) do
      params
      |> Enum.chunk_every(4)
      |> Enum.each(fn [cell, key, _r, _t] -> Agent.update(__MODULE__, &MapSet.put(&1, {cell, key})) end)

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

      %{rows: Enum.map(keys, &[&1])}
    end

    def query!("SELECT COUNT" <> _, _params), do: %{rows: [[Agent.get(__MODULE__, &MapSet.size/1)]]}
  end

  defmodule NullWriter do
    @behaviour ReactiveDag.CoordinationWriter
    @impl true
    def put(_cell_id, _key, _opts), do: :ok
    @impl true
    def delete(_cell_id, _keys), do: :ok
  end

  setup do
    {:ok, _} = FakeRepo.start_link()
    prev_repo = Application.get_env(:reactive_dag, :repo)
    prev_writer = Application.get_env(:reactive_dag, :coordination_writer)
    Application.put_env(:reactive_dag, :repo, FakeRepo)
    Application.put_env(:reactive_dag, :coordination_writer, NullWriter)
    Insights.forget_reports()

    on_exit(fn ->
      Application.put_env(:reactive_dag, :repo, prev_repo)
      Application.put_env(:reactive_dag, :coordination_writer, prev_writer)
      Insights.forget_reports()
    end)

    for {k, cat, amt} <- [{"e1", "travel", 100.0}, {"e2", "meals", 40.0}] do
      Expenses
      |> Ash.Changeset.for_create(:create, %{key: k, category: cat, amount: amt})
      |> Ash.create!()
    end

    :ok
  end

  defp plan, do: ReactiveDag.Node.graph([Expenses, CategoryTotals, CategoryHealth])

  describe "structure" do
    test "levels/1 groups cells by depth, in execution order" do
      levels = Insights.levels(plan())

      assert [{0, leaves}, {1, mid}, {2, top}] = levels
      assert Enum.map(leaves, & &1.id) == ["expenses"]
      assert Enum.map(mid, & &1.id) == ["category_totals"]
      assert Enum.map(top, & &1.id) == ["category_health"]
    end

    test "edges/1 points in the direction change FLOWS (input → consumer)" do
      edges = Insights.edges(plan())

      assert {"expenses", "category_totals"} in edges
      assert {"category_totals", "category_health"} in edges
    end
  end

  describe "per-cell state" do
    test "cell_status/2 carries the declaration AND the live coordination state" do
      p = plan()
      Frontier.mark_dirty("expenses", ["*"], "seed")
      {:ok, _} = Drain.run(p, recompute: ReactiveDag.Node.Recompute, key_rule: ReactiveDag.Node.KeyRule)

      status = Insights.cell_status(p, "category_totals")

      # the declaration
      assert status.id == "category_totals"
      assert status.depth == 1
      assert status.inputs == ["expenses"]
      assert status.op == :fold
      refute status.leaf?
      refute status.verdict?

      # a verdict node is flagged as one (it stores no payload row)
      assert Insights.cell_status(p, "category_health").verdict?
      assert Insights.cell_status(p, "expenses").leaf?
    end

    test "cell_status/2 returns nil for a cell this plan doesn't have" do
      assert Insights.cell_status(plan(), "nope") == nil
    end

    test "summary/1 covers every cell, ordered by depth then id" do
      ids = plan() |> Insights.summary() |> Enum.map(& &1.id)
      assert ids == ["expenses", "category_totals", "category_health"]
    end

    test "pending/1 reports cells the NEXT drain would work on, without consuming them" do
      p = plan()
      assert Insights.pending(p) == []

      Frontier.mark_dirty("expenses", ["e1"], "seed")
      assert Insights.pending(p) == ["expenses"]

      # a READ: peeking twice leaves the frontier intact for the drain
      assert Insights.pending(p) == ["expenses"]
      refute Frontier.empty?()
    end

    test "pending/1 ignores dirty cells this plan doesn't know" do
      Frontier.mark_dirty("some_other_graph", ["k"], "seed")
      assert Insights.pending(plan()) == []
    end

    test "status reads degrade rather than crash when the tuple table is unavailable" do
      # a dashboard should render STRUCTURE even where coordination isn't configured
      Application.put_env(:reactive_dag, :repo, __MODULE__.NoSuchRepo)

      status = Insights.cell_status(plan(), "category_totals")

      assert status.statuses == %{}
      assert status.key_count == 0
      assert status.last_observed_at == nil
      assert status.failing_sample == []
      # ...and the declaration is still there, which is the point
      assert status.inputs == ["expenses"]
    end
  end

  describe "drain reports" do
    test "record/1 retains reports; recent/1 reads them newest-first" do
      assert Insights.last_report() == nil

      p = plan()
      Frontier.mark_dirty("expenses", ["*"], "seed")

      {:ok, first} =
        Drain.run(p, recompute: ReactiveDag.Node.Recompute, key_rule: ReactiveDag.Node.KeyRule)

      assert ^first = Insights.record(first)

      Frontier.mark_dirty("expenses", ["e1"], "again")

      {:ok, second} =
        Drain.run(p, recompute: ReactiveDag.Node.Recompute, key_rule: ReactiveDag.Node.KeyRule)

      Insights.record(second)

      assert [newest, older] = Insights.recent()
      assert newest.report == second
      assert older.report == first
      assert Insights.last_report().report == second
      assert %DateTime{} = newest.at
    end

    test "the window is bounded — oldest reports fall off" do
      prev = Application.get_env(:reactive_dag, :insights_keep)
      Application.put_env(:reactive_dag, :insights_keep, 3)
      on_exit(fn -> Application.put_env(:reactive_dag, :insights_keep, prev) end)

      for i <- 1..6 do
        Insights.record(%Drain.Report{passes: i, duration_us: i, steps: []})
      end

      retained = Insights.recent() |> Enum.map(& &1.report.passes)
      assert retained == [6, 5, 4]
    end

    test "recent/1 takes a limit" do
      for i <- 1..4, do: Insights.record(%Drain.Report{passes: i, steps: []})
      assert Insights.recent(2) |> Enum.map(& &1.report.passes) == [4, 3]
    end

    test "the retained report IS the causal trace — what ran, and why" do
      p = plan()
      Frontier.mark_dirty("expenses", ["*"], "seed")

      {:ok, report} =
        Drain.run(p, recompute: ReactiveDag.Node.Recompute, key_rule: ReactiveDag.Node.KeyRule)

      Insights.record(report)
      %{report: r} = Insights.last_report()

      # the question a dashboard exists to answer: why did this recompute?
      assert Drain.Report.causes(r)["category_totals"] == "expenses"
      assert Drain.Report.causes(r)["category_health"] == "category_totals"
      assert "category_totals" in Drain.Report.cells(r)
    end
  end
end

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

  alias ReactiveDag.{Drain, Frontier, Insights, ScanRun}

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
      attribute(:key, :string, primary_key?: true, allow_nil?: false, public?: true)
      attribute(:category, :string, public?: true)
      attribute(:amount, :float, public?: true)
    end

    actions do
      defaults([:read, :destroy])

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
      attribute(:key, :string, primary_key?: true, allow_nil?: false, public?: true)
      attribute(:category, :string, public?: true)
      attribute(:total, :float, public?: true)
      attribute(:n, :integer, public?: true)
    end

    identities do
      identity(:by_key, [:key], pre_check_with: ReactiveDag.InsightsTest.Domain)
    end

    actions do
      defaults([:read, :destroy])

      create :upsert do
        upsert?(true)
        upsert_identity(:by_key)
        accept([:key, :category, :total, :n])
      end
    end

    reactive do
      id(:category_totals)
      op(:fold)

      recompute_by(:category, to: :expenses, from: :category)
      reduce(into: [sum: [amount: :total], count: :n])
    end
  end

  defmodule CategoryHealth do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute(:key, :string, primary_key?: true, allow_nil?: false, public?: true)
      attribute(:status, :string, public?: true)
    end

    actions do
      defaults([:read, :destroy])

      create :upsert do
        upsert?(true)
        accept([:key, :status])
      end
    end

    reactive do
      id(:category_health)
      op(:check)

      reduce(
        over: :category_totals,
        group_by: :key,
        into: fn _cat, [row | _] ->
          %{status: if(row.total < 1000.0, do: "present", else: "failing")}
        end
      )
    end
  end

  defmodule NoopOp do
    @behaviour ReactiveDag.Op
    @impl true
    def recompute(_cell, _keys), do: {:ok, []}
  end

  # A node that owns a table, over a `compose` LEG that does not. The leg is the
  # tableless cell that still exists: its work is real and its keys flow, but it
  # is an intermediate — `meta.resource` is nil, so there is no table to read and
  # "nothing lives here" is the honest answer rather than a failure to look.
  #
  # The `:elsewhere` state used to be reached through the write-elsewhere shape
  # (`Simple`, no attributes, a compute module). A derived NODE may no longer be
  # tableless, so it is reached through the one shape that legitimately has no
  # resource — which is what `Insights.rows_kind/1` has always keyed on.
  defmodule Publish do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
      private?(true)
    end

    attributes do
      attribute(:key, :string, primary_key?: true, allow_nil?: false, public?: true)
    end

    actions do
      defaults([:read, :destroy])

      create :upsert do
        upsert?(true)
        upsert_identity(:by_key)
        accept([:key])
      end
    end

    identities do
      identity(:by_key, [:key])
    end

    reactive do
      id(:publish)
      compute(ReactiveDag.InsightsTest.NoopOp)

      compose :fold do
        as(:published_rows)
        compute(ReactiveDag.InsightsTest.NoopOp)
        ref(:expenses)
      end
    end
  end

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
    start_supervised!(%{id: FakeRepo, start: {FakeRepo, :start_link, []}})
    prev_repo = Application.get_env(:reactive_dag, :repo)
    Application.put_env(:reactive_dag, :repo, FakeRepo)
    Insights.forget_runs()

    on_exit(fn ->
      Application.put_env(:reactive_dag, :repo, prev_repo)
      Insights.forget_runs()
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

      {:ok, _} =
        Drain.run(p, recompute: ReactiveDag.Node.Recompute, key_rule: ReactiveDag.Node.KeyRule)

      status = Insights.cell_status(p, "category_totals")

      # the declaration
      assert status.id == "category_totals"
      assert status.depth == 1
      assert status.inputs == ["expenses"]
      assert status.op == :fold
      refute status.leaf?

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

    test "status reads degrade rather than crash when a node's rows are unreadable" do
      # a dashboard should render STRUCTURE even where a read fails — a resource
      # whose data layer isn't up, a policy that forbids the read, a dropped table.
      defmodule Unreadable do
        use Ash.Resource,
          domain: ReactiveDag.InsightsTest.Domain,
          data_layer: Ash.DataLayer.Simple,
          extensions: [ReactiveDag.Node]

        attributes do
          attribute(:key, :string, primary_key?: true, allow_nil?: false, public?: true)
        end

        # no :read action, so Ash.read! raises
        actions do
        end

        reactive do
          id(:unreadable)
          leaf?(true)
        end
      end

      plan = ReactiveDag.Node.graph([Unreadable])
      status = Insights.cell_status(plan, "unreadable")

      assert status.statuses == %{}
      assert status.key_count == 0
      assert status.failing_sample == []
      # ...and the declaration is still there, which is the point
      assert status.id == "unreadable"
      assert status.leaf?

      assert status.rows == :unreadable, "the read FAILED, and says so"
    end
  end

  describe "why a key count is zero" do
    # Three different states collapsed into one number. A consumer showing the
    # same symbol for all three raises an alarm for two non-problems — and one
    # of them is the publish root the app actually reads.
    test "a node with rows reports :stored" do
      status = Insights.cell_status(plan(), "expenses")

      assert status.rows == :stored
      assert status.key_count > 0
    end

    test "an EMPTY table is still :stored — zero is a real answer" do
      for row <- Ash.read!(Expenses), do: Ash.destroy!(row)

      status = Insights.cell_status(plan(), "expenses")

      assert status.rows == :stored
      assert status.key_count == 0
    end

    test "a cell that keeps no rows of its own reports :elsewhere" do
      plan = ReactiveDag.Node.graph([Expenses, Publish])
      status = Insights.cell_status(plan, "published_rows")

      assert status.rows == :elsewhere
      assert status.key_count == 0
      assert status.statuses == %{}

      # …and the node that DOES own a table reads as :stored, so the two states
      # are told apart by where the rows live rather than by node shape.
      assert Insights.cell_status(plan, "publish").rows == :stored
    end

    test "and it is not confused with a failed read" do
      plan = ReactiveDag.Node.graph([Expenses, Publish])

      refute Insights.cell_status(plan, "published_rows").rows == :unreadable
    end
  end

  describe "retained runs" do
    test "record/1 retains runs; recent/1 reads them newest-first" do
      assert Insights.last_run() == nil

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
      assert newest.run.report == second
      assert older.run.report == first
      assert Insights.last_run().run.report == second
      assert %DateTime{} = newest.at
    end

    test "the window is bounded — oldest runs fall off" do
      prev = Application.get_env(:reactive_dag, :insights_keep)
      Application.put_env(:reactive_dag, :insights_keep, 3)
      on_exit(fn -> Application.put_env(:reactive_dag, :insights_keep, prev) end)

      for i <- 1..6 do
        Insights.record(%Drain.Report{passes: i, duration_us: i, steps: []})
      end

      retained = Insights.recent() |> Enum.map(& &1.run.report.passes)
      assert retained == [6, 5, 4]
    end

    test "the window trims SCAN runs too — one buffer, one bound" do
      prev = Application.get_env(:reactive_dag, :insights_keep)
      Application.put_env(:reactive_dag, :insights_keep, 2)
      on_exit(fn -> Application.put_env(:reactive_dag, :insights_keep, prev) end)

      for i <- 1..5 do
        Insights.record(%ScanRun{cell: "docs", duration_us: i})
      end

      assert Insights.recent() |> Enum.map(& &1.run.duration_us) == [5, 4]
    end

    test "recent/1 takes a limit" do
      for i <- 1..4, do: Insights.record(%Drain.Report{passes: i, steps: []})
      assert Insights.recent(2) |> Enum.map(& &1.run.report.passes) == [4, 3]
    end

    test "the retained report IS the causal trace — what ran, and why" do
      p = plan()
      Frontier.mark_dirty("expenses", ["*"], "seed")

      {:ok, report} =
        Drain.run(p, recompute: ReactiveDag.Node.Recompute, key_rule: ReactiveDag.Node.KeyRule)

      Insights.record(report)
      %{run: run} = Insights.last_run()
      r = run.report

      # the question a dashboard exists to answer: why did this recompute?
      assert Drain.Report.causes(r)["category_totals"] == "expenses"
      assert Drain.Report.causes(r)["category_health"] == "category_totals"
      assert "category_totals" in Drain.Report.cells(r)
    end
  end

  describe "a scan is retained WHOLE" do
    # The bug this shape exists for: a two-minute scan logged as
    # `0 cells · 0 changed · 6.1ms` because the host unwrapped the run to its
    # report, and the report only knows about the drain. Everything the POLL did
    # — its wall time, what it found, what it cost, what it could NOT reach —
    # has to survive the round trip through the buffer.
    setup do
      run = %ScanRun{
        cell: "meeting_docs",
        changed: ["d1", "d2"],
        unreachable: [{"calendar", :timeout}, {"drive", :econnrefused}],
        detail: %{tokens_in: 4200, llm_calls: 7, cache_hits: 31},
        duration_us: 134_000_000,
        report: %Drain.Report{passes: 1, duration_us: 6_100, steps: []}
      }

      Insights.record(run)
      %{recorded: Insights.last_run()}
    end

    test "the POLL's duration survives — not the drain's", %{recorded: entry} do
      # the whole point: 134s is the number the dashboard was missing, and the
      # drain's own 6.1ms is what it was showing instead.
      assert entry.run.duration_us == 134_000_000
      assert entry.run.report.duration_us == 6_100
    end

    test "the poll's changed keys survive", %{recorded: entry} do
      assert entry.run.changed == ["d1", "d2"]
    end

    test "unreachable survives — a scan that could not look says so", %{recorded: entry} do
      assert entry.run.unreachable == [{"calendar", :timeout}, {"drive", :econnrefused}]
      refute ScanRun.complete?(entry.run)
    end

    test "the poll's cost detail survives", %{recorded: entry} do
      assert entry.run.detail == %{tokens_in: 4200, llm_calls: 7, cache_hits: 31}
    end

    test "and cost still totals across BOTH phases from the retained run", %{recorded: entry} do
      # `ScanRun.total/2` reaches the poll's detail AND the drain's steps. Under
      # the old shape the poll's spend was simply not in the buffer.
      assert ScanRun.total(entry.run, :tokens_in) == 4200
    end
  end

  describe "a scan and a bare drain are told apart" do
    test "polled? distinguishes them, without inspecting the run's insides" do
      Insights.record(%ScanRun{
        cell: "meeting_docs",
        unreachable: [{"drive", :timeout}],
        duration_us: 134_000_000,
        report: %Drain.Report{passes: 1, duration_us: 6_100, steps: []}
      })

      Insights.record(%Drain.Report{passes: 2, duration_us: 900, steps: []})

      assert [bare, scan] = Insights.recent()

      refute bare.polled?
      assert scan.polled?
    end

    test "a bare report is normalised into a run — recent/1 returns ONE shape" do
      Insights.record(%ScanRun{cell: "docs", duration_us: 5})
      Insights.record(%Drain.Report{passes: 2, duration_us: 900, steps: []})

      # A consumer pattern-matching two shapes gets it wrong, so there is only
      # one: every entry carries a %ScanRun{}, whichever way it was recorded.
      for entry <- Insights.recent() do
        assert %ScanRun{} = entry.run
      end
    end

    test "a bare drain's duration is the DRAIN's — the two rows mean the same thing" do
      Insights.record(%Drain.Report{passes: 2, duration_us: 900, steps: []})

      entry = Insights.last_run()

      # A log's duration column has to mean one thing across both kinds of row,
      # so a bare drain reports the drain's wall time rather than a zero the
      # `%ScanRun{}` default would have given it.
      assert entry.run.duration_us == 900
    end

    test "a bare drain claims no poll findings it never made" do
      Insights.record(%Drain.Report{passes: 2, duration_us: 900, steps: []})

      run = Insights.last_run().run

      assert run.cell == nil
      assert run.changed == []
      assert run.unreachable == []
      assert run.detail == %{}
      # …and empty `unreachable` here is honest rather than a claim of
      # completeness: there was no poll to be incomplete.
      assert ScanRun.drained?(run)
    end

    test "a scan that never drained is retained, and says so" do
      # An unscannable source is a COMPLETED scan that found nothing. The row is
      # still worth having, and "0 passes" must not be rendered for a drain that
      # never happened.
      Insights.record(%ScanRun{
        cell: "quickbooks",
        not_scannable: {:not_scannable, :no_credential},
        duration_us: 1_200
      })

      run = Insights.last_run().run

      assert run.report == nil
      refute ScanRun.drained?(run)
      assert Insights.last_run().polled?
    end
  end
end

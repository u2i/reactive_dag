defmodule ReactiveDag.DrainTelemetryTest do
  @moduledoc """
  The drain's `:telemetry` events — the observability seam.

  This replaces an earlier `:on_step` closure. A callback threaded through every
  `run/2` call site only serves one consumer: whoever owns that call site.
  Telemetry lets a dashboard, a metrics backend and a log attach independently
  without changing how the drain is invoked, which is what makes live observation
  a host concern rather than a library one.

  The property that matters most is on `:step`: it carries the changed **keys**,
  not just a count. A consumer with the keys can read only what moved; a
  consumer with a count has to re-read the graph, which defeats the point of
  being told at all.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.{Drain, Frontier}

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      allow_unregistered?(true)
    end
  end

  defmodule Expenses do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Ets, extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :category, :string, public?: true
      attribute :amount, :float, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        accept([:key, :category, :amount])
      end
    end

    reactive do
      id(:expenses)
      leaf?(true)
    end
  end

  defmodule CategoryTotals do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Ets, extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :total, :float, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        accept([:key, :total])
      end
    end

    reactive do
      id(:category_totals)
      op(:fold)

      reduce over: :expenses,
             group_by: :category,
             into: fn _cat, rows -> %{total: rows |> Enum.map(& &1.amount) |> Enum.sum()} end
    end
  end

  # an in-memory frontier — this suite has no Postgres
  defmodule FakeRepo do
    def start_link, do: Agent.start_link(fn -> MapSet.new() end, name: __MODULE__)

    def query!("INSERT INTO " <> _, params) do
      params
      |> Enum.chunk_every(7)
      |> Enum.each(fn [cell, _tenant, key, _r, _t, _prior, _held] ->
        Agent.update(__MODULE__, &MapSet.put(&1, {cell, key}))
      end)

      %{rows: []}
    end

    def query!("SELECT DISTINCT cell_id" <> _, _params) do
      ids = Agent.get(__MODULE__, & &1) |> Enum.map(&elem(&1, 0)) |> Enum.uniq()
      %{rows: Enum.map(ids, &[&1])}
    end

    def query!("DELETE FROM " <> _, [cell | _tenant]) do
      claimed =
        Agent.get_and_update(__MODULE__, fn set ->
          {mine, rest} = Enum.split_with(set, fn {c, _} -> c == cell end)
          {mine, MapSet.new(rest)}
        end)

      %{rows: Enum.map(claimed, fn {_c, k} -> [k, nil] end)}
    end

    def query!("SELECT COUNT" <> _, _), do: %{rows: [[Agent.get(__MODULE__, &MapSet.size/1)]]}
  end

  setup do
    start_supervised!(%{id: FakeRepo, start: {FakeRepo, :start_link, []}})
    prev = Application.get_env(:reactive_dag, :repo)
    Application.put_env(:reactive_dag, :repo, FakeRepo)
    on_exit(fn -> Application.put_env(:reactive_dag, :repo, prev) end)

    for r <- [Expenses, CategoryTotals], row <- Ash.read!(r), do: Ash.destroy!(row)

    for {k, cat, amt} <- [{"e1", "travel", 100.0}, {"e2", "meals", 40.0}] do
      Expenses |> Ash.Changeset.for_create(:upsert, %{key: k, category: cat, amount: amt}) |> Ash.create!()
    end

    :ok
  end

  defp attach(events) do
    test_pid = self()
    handler = "test-#{System.unique_integer([:positive])}"

    :telemetry.attach_many(
      handler,
      events,
      fn event, measurements, metadata, _ ->
        send(test_pid, {:telemetry, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)
  end

  defp plan, do: ReactiveDag.Node.graph([Expenses, CategoryTotals])

  defp drain do
    Drain.run(plan())
  end

  describe "start / stop" do
    test "a drain brackets itself, and stop carries the report" do
      attach([[:reactive_dag, :drain, :start], [:reactive_dag, :drain, :stop]])
      Frontier.mark_dirty("expenses", ["*"], "seed")

      {:ok, report} = drain()

      assert_received {:telemetry, [:reactive_dag, :drain, :start], start_m, start_meta}
      assert is_integer(start_m.system_time)
      assert start_meta.cells == 2

      assert_received {:telemetry, [:reactive_dag, :drain, :stop], stop_m, stop_meta}
      assert stop_meta.report == report
      assert stop_m.passes == report.passes
      assert stop_m.steps == length(report.steps)
      assert stop_m.duration_us >= 0
    end

    test "stop names the cells touched — enough to refresh without re-reading everything" do
      attach([[:reactive_dag, :drain, :stop]])
      Frontier.mark_dirty("expenses", ["*"], "seed")

      {:ok, _} = drain()

      assert_received {:telemetry, [:reactive_dag, :drain, :stop], _m, meta}
      assert "category_totals" in meta.cells_touched
    end

    test "an EMPTY drain still brackets — a consumer must not wait forever on stop" do
      attach([[:reactive_dag, :drain, :start], [:reactive_dag, :drain, :stop]])

      {:ok, report} = drain()

      assert report.steps == []
      assert_received {:telemetry, [:reactive_dag, :drain, :start], _, _}
      assert_received {:telemetry, [:reactive_dag, :drain, :stop], m, _}
      assert m.steps == 0
    end
  end

  describe "step" do
    test "carries the changed KEYS, not just a count" do
      # the whole reason a consumer can be incremental
      attach([[:reactive_dag, :drain, :step]])
      Frontier.mark_dirty("expenses", ["*"], "seed")

      {:ok, _} = drain()

      steps = collect_steps()
      totals = Enum.find(steps, fn {_m, meta} -> meta.cell == "category_totals" end)

      assert {measurements, metadata} = totals
      assert Enum.sort(metadata.changed_keys) == ["meals", "travel"]
      assert measurements.changed == 2
      assert measurements.duration_us >= 0
    end

    test "names what triggered the cell — the causal edge, live" do
      attach([[:reactive_dag, :drain, :step]])
      Frontier.mark_dirty("expenses", ["*"], "seed")

      {:ok, _} = drain()

      {_m, meta} =
        collect_steps() |> Enum.find(fn {_m, meta} -> meta.cell == "category_totals" end)

      assert meta.triggered_by == "expenses"
    end

    test "one event per recomputed cell, and the report agrees" do
      attach([[:reactive_dag, :drain, :step]])
      Frontier.mark_dirty("expenses", ["*"], "seed")

      {:ok, report} = drain()

      assert length(collect_steps()) == length(report.steps)
    end

    test "fires DURING the drain, not after — that is what makes it live" do
      # if events only arrived once run/2 returned, a progress UI would learn
      # nothing until the work was already over. Asserted from inside the
      # handler: it sees a drain still in progress.
      test_pid = self()
      handler = "during-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        handler,
        [:reactive_dag, :drain, :step],
        fn _e, _m, meta, _ -> send(test_pid, {:mid_drain, meta.cell, self()}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler) end)
      Frontier.mark_dirty("expenses", ["*"], "seed")

      drain_pid = self()
      {:ok, _} = drain()

      # the handler ran in the DRAIN's process, synchronously, before run/2
      # returned — which is exactly why it must stay cheap
      assert_received {:mid_drain, "category_totals", ^drain_pid}
    end
  end

  describe "exception" do
    test "a runaway drain emits :exception with the PARTIAL report" do
      # a monitor should see the runaway, not just the crash
      attach([[:reactive_dag, :drain, :exception]])
      Frontier.mark_dirty("expenses", ["*"], "seed")

      assert_raise Drain.RunawayError, fn ->
        Drain.run(plan(),
          max_passes: 1
        )
      end

      assert_received {:telemetry, [:reactive_dag, :drain, :exception], m, meta}
      assert meta.kind == :error
      assert %Drain.RunawayError{} = meta.reason
      assert %ReactiveDag.Drain.Report{} = meta.report
      assert m.duration_us >= 0
    end

    test "a raising recompute emits :exception and re-raises" do
      defmodule Boom do
        @behaviour ReactiveDag.Op
        @impl true
        def recompute(_cell, _keys), do: raise("boom")
      end

      attach([[:reactive_dag, :drain, :exception]])
      Frontier.mark_dirty("category_totals", ["*"], "seed")

      # The cell DECLARES what recomputes it, so a raising op is stamped on the
      # cell rather than passed to the drain. `Node.Recompute` matches the
      # combinator FIRST, so the `reduce` has to come off for `compute` to be
      # reached — which is the dispatch order a real node relies on.
      boom_plan =
        update_in(plan().cells["category_totals"].meta, fn meta ->
          meta |> Map.delete(:reduce) |> Map.put(:compute, Boom)
        end)

      assert_raise RuntimeError, "boom", fn ->
        Drain.run(boom_plan)
      end

      assert_received {:telemetry, [:reactive_dag, :drain, :exception], _m, meta}
      assert meta.kind == :error
      # nothing partial to report from a strategy that blew up mid-pass
      assert meta.report == nil
    end

    test "no :stop is emitted when the drain raised" do
      attach([[:reactive_dag, :drain, :stop], [:reactive_dag, :drain, :exception]])
      Frontier.mark_dirty("expenses", ["*"], "seed")

      assert_raise Drain.RunawayError, fn ->
        Drain.run(plan(),
          max_passes: 1
        )
      end

      assert_received {:telemetry, [:reactive_dag, :drain, :exception], _, _}
      refute_received {:telemetry, [:reactive_dag, :drain, :stop], _, _}
    end
  end

  defp collect_steps(acc \\ []) do
    receive do
      {:telemetry, [:reactive_dag, :drain, :step], m, meta} -> collect_steps([{m, meta} | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end
  describe "a step is a durable record, not just a live signal" do
    # A host writing a processing log reads these rows back long after the plan
    # that produced them has moved on — so anything needing a plan lookup to
    # recover is effectively unrecoverable (u2i/reactive_dag#114).
    test "carries the cell's op and depth, which a step cannot otherwise recover" do
      Frontier.mark_dirty("expenses", ["e1"], "test")

      {:ok, report} = drain()

      rollup = Enum.find(report.steps, &(&1.cell == "category_totals"))

      assert rollup.depth == 1, "one hop from the leaf"
      assert Map.has_key?(rollup, :op)
    end

    test "the telemetry step carries them too, so a live consumer sees the same shape" do
      test_pid = self()

      :telemetry.attach(
        "step-shape",
        [:reactive_dag, :drain, :step],
        fn _e, _m, meta, _ -> send(test_pid, {:step, meta.cell, meta.step}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach("step-shape") end)

      Frontier.mark_dirty("expenses", ["e1"], "test")
      {:ok, _} = drain()

      assert_received {:step, "category_totals", step}
      assert step.depth == 1
    end
  end

end

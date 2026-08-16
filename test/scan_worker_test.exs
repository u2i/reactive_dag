defmodule ReactiveDag.ScanWorkerTest do
  @moduledoc """
  `Source.refresh/3` and `ReactiveDag.ScanWorker` — the poll half of the loop.

  `poll_cell/3` returned what a scanner said and stopped, leaving every host to
  hand-write the same steps: normalise the return shape, mark the frontier,
  propagate to parents, drain. That loop is documented in the guide and was
  provided nowhere, which is why each host grew a worker to hold it.

  The worker is a thin shell over `refresh/3` plus `Drain.run/2` — deliberately
  thin, so a host that needs its own audit or run-id bookkeeping wraps it by
  calling the same two functions rather than extending a framework.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.{Frontier, Source}

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      allow_unregistered?(true)
    end
  end

  defmodule Crawler do
    @behaviour Source

    def start_link, do: Agent.start_link(fn -> %{polls: [], result: nil} end, name: __MODULE__)
    def polls, do: Agent.get(__MODULE__, & &1.polls) |> Enum.reverse()
    def returns(result), do: Agent.update(__MODULE__, &%{&1 | result: result})
    def raises, do: Agent.update(__MODULE__, &%{&1 | result: :raise})

    @impl true
    def id, do: :crawler
    @impl true
    def leaf_cells(_g), do: ["docs", "notices"]
    @impl true
    def poll(opts) do
      Agent.update(__MODULE__, &%{&1 | polls: [opts | &1.polls]})

      case Agent.get(__MODULE__, & &1.result) do
        :raise -> raise "upstream is down"
        result -> {:ok, result || %{changed: []}}
      end
    end
  end

  # a source owning exactly ONE leaf: a flat key list is unambiguous, so the map
  # form must not be forced on the common case
  defmodule Solo do
    @behaviour Source

    @impl true
    def id, do: :solo
    @impl true
    def leaf_cells(_g), do: ["solo_docs"]
    @impl true
    def poll(_opts), do: {:ok, %{changed: ["s1"]}}
  end

  defmodule SoloDocs do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute(:key, :string, primary_key?: true, allow_nil?: false, public?: true)
    end

    actions do
      defaults([:read, :destroy])
      create(:upsert, upsert?: true, accept: [:key])
    end

    reactive do
      id(:solo_docs)
      leaf?(true)
      scan(ReactiveDag.ScanWorkerTest.Solo)
    end
  end

  defmodule Docs do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute(:key, :string, primary_key?: true, allow_nil?: false, public?: true)
      attribute(:category, :string, public?: true)
    end

    actions do
      defaults([:read, :destroy])
      create(:upsert, upsert?: true, accept: [:key, :category])
    end

    reactive do
      id(:docs)
      leaf?(true)
      scan(ReactiveDag.ScanWorkerTest.Crawler, args: [recent: true], every: "0 * * * *")
    end
  end

  # A SECOND leaf fed by the same scanner — one crawl of one upstream that lands
  # rows in two places. This is the shape a host means by "one poll is one unit
  # of work": the source is the unit, and the cells are where its output goes.
  defmodule Notices do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute(:key, :string, primary_key?: true, allow_nil?: false, public?: true)
      attribute(:category, :string, public?: true)
    end

    actions do
      defaults([:read, :destroy])
      create(:upsert, upsert?: true, accept: [:key, :category])
    end

    reactive do
      id(:notices)
      leaf?(true)
      scan(ReactiveDag.ScanWorkerTest.Crawler)
    end
  end

  defmodule Totals do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute(:key, :string, primary_key?: true, allow_nil?: false, public?: true)
      attribute(:category, :string, public?: true)
      attribute(:n, :integer, public?: true)
    end

    actions do
      defaults([:read, :destroy])
      create(:upsert, upsert?: true, accept: [:key, :category, :n])
    end

    reactive do
      id(:totals)
      recompute_by(:category, to: :docs, from: :category)
      reduce(group_by: :category, into: [count: :n])
    end
  end

  defmodule FakeRepo do
    def start_link, do: Agent.start_link(fn -> MapSet.new() end, name: __MODULE__)
    def marks, do: Agent.get(__MODULE__, &MapSet.to_list/1) |> Enum.sort()

    def query!("INSERT INTO " <> _, p) do
      p
      |> Enum.chunk_every(5)
      |> Enum.each(fn [c, k, r, _, _] -> Agent.update(__MODULE__, &MapSet.put(&1, {c, k, r})) end)

      %{rows: []}
    end

    def query!("SELECT DISTINCT cell_id" <> _, _),
      do: %{rows: Agent.get(__MODULE__, & &1) |> Enum.map(&[elem(&1, 0)]) |> Enum.uniq()}

    def query!("DELETE FROM " <> _, [cell]) do
      c =
        Agent.get_and_update(__MODULE__, fn s ->
          {m, r} = Enum.split_with(s, fn {x, _, _} -> x == cell end)
          {m, MapSet.new(r)}
        end)

      %{rows: Enum.map(c, fn {_, k, _} -> [k, nil] end)}
    end

    def query!("SELECT COUNT" <> _, _), do: %{rows: [[Agent.get(__MODULE__, &MapSet.size/1)]]}
  end

  setup do
    start_supervised!(%{id: Crawler, start: {Crawler, :start_link, []}})
    start_supervised!(%{id: FakeRepo, start: {FakeRepo, :start_link, []}})

    prev = Application.get_env(:reactive_dag, :repo)
    Application.put_env(:reactive_dag, :repo, FakeRepo)
    on_exit(fn -> Application.put_env(:reactive_dag, :repo, prev) end)

    for r <- [Docs, Notices, Totals], row <- Ash.read!(r), do: Ash.destroy!(row)
    :ok
  end

  defp plan, do: ReactiveDag.Node.graph([Docs, Notices, SoloDocs, Totals])

  describe "one poll, several leaves" do
    # A source feeding two leaves is ONE crawl whose rows land in two places.
    # `scan_jobs/1` lists it per-cell — that is what a scheduler and a control
    # panel need — but a host that treats the poll as its unit of work does not
    # have to poll twice: a scanner returns `changed:` keyed BY LEAF and a single
    # refresh marks them all, under one reason.
    test "a map-shaped result marks each leaf it names" do
      Crawler.returns(%{changed: %{"docs" => ["d1"], "notices" => ["n1"]}})

      {:ok, result} = Source.refresh(plan(), "docs", reason: "scan:run-42")

      assert result.marked == %{"docs" => ["d1"], "notices" => ["n1"]}
      assert Enum.sort(result.changed) == ["d1", "n1"]
    end

    test "and the reason rides through to every one of them" do
      # this is what makes a run id work without any library support for run ids:
      # `:reason` is a free string, so the frontier rows say which run dirtied
      # them, across every leaf the one poll touched
      Crawler.returns(%{changed: %{"docs" => ["d1"], "notices" => ["n1"]}})

      {:ok, _} = Source.refresh(plan(), "docs", reason: "scan:run-42")

      marks = FakeRepo.marks()

      assert {"docs", "d1", "scan:run-42"} in marks
      assert {"notices", "n1", "scan:run-42"} in marks
    end

    test "a leaf the poll did not name is not marked" do
      Crawler.returns(%{changed: %{"docs" => ["d1"]}})

      {:ok, result} = Source.refresh(plan(), "docs")

      refute Map.has_key?(result.marked, "notices")
    end

    test "a flat list still belongs to the cell that was polled" do
      # the single-leaf form is unchanged: no host has to adopt the map shape
      Crawler.returns(%{changed: ["d1"]})

      {:ok, result} = Source.refresh(plan(), "docs")

      assert result.marked == %{"docs" => ["d1"]}
    end
  end

  describe "refresh/3 marks what the poll changed" do
    test "a flat key list belongs to the polled cell" do
      Crawler.returns(%{changed: ["a", "b"]})

      {:ok, result} = Source.refresh(plan(), "docs")

      assert result.changed == ["a", "b"]
      assert result.marked == %{"docs" => ["a", "b"]}
    end

    test "the frontier actually has the keys, labelled with a reason" do
      Crawler.returns(%{changed: ["a"]})

      {:ok, _} = Source.refresh(plan(), "docs", reason: "nightly")

      assert {"docs", "a", "nightly"} in FakeRepo.marks()
    end

    test "parents are marked too, so the cascade reaches them" do
      Crawler.returns(%{changed: ["a"]})

      {:ok, _} = Source.refresh(plan(), "docs")

      assert Enum.any?(FakeRepo.marks(), fn {cell, _, _} -> cell == "totals" end),
             "marking a leaf without its parents would strand the change"
    end

    test "a fan-out source's %{leaf => keys} is normalised, not rejected" do
      # both shapes are in the Source contract, so both are handled here rather
      # than in each host
      Crawler.returns(%{changed: %{"docs" => ["a", "b"]}})

      {:ok, result} = Source.refresh(plan(), "docs")

      assert result.marked == %{"docs" => ["a", "b"]}
    end

    test "a poll that changed nothing marks nothing" do
      Crawler.returns(%{changed: []})

      {:ok, result} = Source.refresh(plan(), "docs")

      assert result.changed == []
      assert result.marked == %{}
      assert FakeRepo.marks() == []
    end

    test "the leaf's declared args still apply, and the caller still wins" do
      Crawler.returns(%{changed: []})

      {:ok, _} = Source.refresh(plan(), "docs")
      {:ok, _} = Source.refresh(plan(), "docs", recent: false)

      assert [first, second] = Crawler.polls()
      assert first[:recent] == true
      assert second[:recent] == false
    end

    test "an unreachable upstream is reported, and marks nothing for it" do
      Crawler.returns(%{changed: [], unreachable: [{"api", :timeout}]})

      {:ok, result} = Source.refresh(plan(), "docs")

      assert result.unreachable == [{"api", :timeout}]
      assert FakeRepo.marks() == [], "an outage propagates nothing, by construction"
    end

    test "a cell with no scanner is reported rather than raising" do
      assert Source.refresh(plan(), "totals") == {:error, :no_scanner}
    end
  end

  describe "the worker" do
    test "polls, marks and drains in one job" do
      Docs |> Ash.Changeset.for_create(:upsert, %{key: "a", category: "x"}) |> Ash.create!()
      Crawler.returns(%{changed: ["a"]})

      assert :ok =
               perform_job(%{
                 "cell" => "docs",
                 "plan_mfa" => ["ReactiveDag.ScanWorkerTest", "plan_for_worker", []]
               })

      # the drain ran: the derived cell was recomputed from the marked leaf
      assert [%{key: "x", n: 1}] = Ash.read!(Totals)
    end

    test "job opts override the leaf's declared args" do
      Crawler.returns(%{changed: []})

      perform_job(%{
        "cell" => "docs",
        "plan_mfa" => ["ReactiveDag.ScanWorkerTest", "plan_for_worker", []],
        "opts" => %{"recent" => false}
      })

      assert [opts] = Crawler.polls()
      assert opts[:recent] == false
    end

    test "a cell with no scanner does not fail the job" do
      # nothing to retry: it will have no scanner next attempt either
      assert :ok =
               perform_job(%{
                 "cell" => "totals",
                 "plan_mfa" => ["ReactiveDag.ScanWorkerTest", "plan_for_worker", []]
               })
    end

    test "it emits telemetry a host can attach to" do
      test_pid = self()

      :telemetry.attach(
        "scan-test",
        [:reactive_dag, :scan, :stop],
        fn _e, m, meta, _ -> send(test_pid, {:scan_stop, m, meta}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach("scan-test") end)

      Crawler.returns(%{changed: ["a"]})

      perform_job(%{
        "cell" => "docs",
        "plan_mfa" => ["ReactiveDag.ScanWorkerTest", "plan_for_worker", []]
      })

      assert_received {:scan_stop, m, meta}
      assert m.changed == 1
      assert meta.cell == "docs"
    end

    test "a missing plan raises with the fix, rather than a match error" do
      prev = Application.get_env(:reactive_dag, :plan_mfa)
      Application.delete_env(:reactive_dag, :plan_mfa)
      on_exit(fn -> if prev, do: Application.put_env(:reactive_dag, :plan_mfa, prev) end)

      err = assert_raise RuntimeError, fn -> perform_job(%{"cell" => "docs"}) end

      assert Exception.message(err) =~ "plan_mfa"
      assert Exception.message(err) =~ "config :reactive_dag"
    end
  end

  @doc false
  def plan_for_worker, do: plan()

  defp perform_job(args) do
    ReactiveDag.ScanWorker.perform(%Oban.Job{args: args})
  end

  describe "telemetry a host can actually attach to" do
    # The moduledoc offers handlers as the alternative to forking this worker.
    # That offer only holds if a handler can tell WHICH RUN a scan belonged to:
    # a scan is usually one leg of a run whose id the enqueuer chose, and only
    # the job carries it (u2i/reactive_dag#118).
    setup do
      test_pid = self()

      :telemetry.attach_many(
        "scan-span",
        [
          [:reactive_dag, :scan, :start],
          [:reactive_dag, :scan, :stop],
          [:reactive_dag, :scan, :exception]
        ],
        fn event, m, meta, _ -> send(test_pid, {:tel, List.last(event), m, meta}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach("scan-span") end)
      :ok
    end

    defp run(extra \\ %{}) do
      ReactiveDag.ScanWorker.perform(%Oban.Job{
        args:
          Map.merge(
            %{
              "cell" => "docs",
              "plan_mfa" => ["ReactiveDag.ScanWorkerTest", "plan_for_worker", []]
            },
            extra
          )
      })
    end

    test "stop carries the job's args, so a run id survives the loop" do
      run(%{"run_id" => "run-42"})

      assert_received {:tel, :stop, _m, meta}
      assert meta.args["run_id"] == "run-42"
      assert meta.cell == "docs"
    end

    test "a scan announces that it STARTED, not only that it finished" do
      # a full crawl takes minutes; without this the only observable moment is
      # the end, and a page watching it looks idle while the machine is busy
      run(%{"run_id" => "run-42"})

      assert_received {:tel, :start, m, meta}
      assert meta.args["run_id"] == "run-42"
      assert meta.cell == "docs"
      assert m.system_time
    end

    test "start arrives BEFORE stop" do
      run()

      assert_received {:tel, :start, _, _}
      assert_received {:tel, :stop, _, _}
    end

    test "a scanner that raises is an ERROR return, not a telemetry exception" do
      # worth pinning, because it is the opposite of what the event name
      # suggests: `Source.safe_poll/2` rescues, so a crawler blowing up becomes
      # `{:error, reason}` — an Oban retry — and never reaches the worker's own
      # rescue. `:exception` fires only for failures OUTSIDE the poll.
      #
      # So a host tracking a failed leg needs the return value as well as the
      # telemetry; `:stop` does not fire here either.
      Crawler.raises()

      assert {:error, reason} = run(%{"run_id" => "run-42"})
      assert reason =~ "upstream is down"

      refute_received {:tel, :stop, _, _}
      refute_received {:tel, :exception, _, _}

      # ...but the start DID fire, so a page showing the leg in-flight has
      # something to clear
      assert_received {:tel, :start, _m, meta}
      assert meta.args["run_id"] == "run-42"
    end
  end

  describe "a source-keyed job — the fan-out unit" do
    # `Crawler` feeds `docs` and `notices` from one poll. A cell-keyed job polls
    # per leaf, so scheduling both fetches the same upstream twice for one
    # source's work (u2i/reactive_dag#124).
    defp source_job(extra \\ %{}) do
      ReactiveDag.ScanWorker.perform(%Oban.Job{
        args:
          Map.merge(
            %{
              "source" => "ReactiveDag.ScanWorkerTest.Crawler",
              "plan_mfa" => ["ReactiveDag.ScanWorkerTest", "plan_for_worker", []]
            },
            extra
          )
      })
    end

    test "polls the source ONCE for all its leaves" do
      Crawler.returns(%{changed: %{"docs" => ["d1"], "notices" => ["n1"]}})

      assert :ok = source_job()

      assert length(Crawler.polls()) == 1, "one crawl, not one per leaf"
    end

    test "and marks every leaf it reports" do
      # via refresh_source/3 rather than the worker: the worker drains, and the
      # drain claims-as-deletes, so the marks are gone by the time it returns
      Crawler.returns(%{changed: %{"docs" => ["d1"], "notices" => ["n1"]}})

      {:ok, result} = Source.refresh_source(plan(), Crawler, reason: "scan:run-42")

      assert result.marked == %{"docs" => ["d1"], "notices" => ["n1"]}
      assert {"docs", "d1", "scan:run-42"} in FakeRepo.marks()
      assert {"notices", "n1", "scan:run-42"} in FakeRepo.marks()
    end

    test "the leaf's declared standing args still apply" do
      Crawler.returns(%{changed: %{}})

      source_job()

      assert [opts] = Crawler.polls()
      assert opts[:recent] == true
    end

    test "a flat key list from a FAN-OUT source is refused, not guessed" do
      # the keys could belong to either leaf; picking one would mark the wrong
      # subtree and nothing downstream would say so
      Crawler.returns(%{changed: ["d1"]})

      assert {:error, msg} = source_job()
      assert msg =~ "docs"
      assert msg =~ "notices"
      assert msg =~ "map form"
    end

    test "telemetry names the source, and carries the args" do
      test_pid = self()

      :telemetry.attach(
        "source-scan",
        [:reactive_dag, :scan, :stop],
        fn _e, _m, meta, _ -> send(test_pid, {:stop, meta}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach("source-scan") end)

      Crawler.returns(%{changed: %{"docs" => ["d1"]}})
      source_job(%{"run_id" => "run-9"})

      assert_received {:stop, meta}
      assert meta.cell =~ "Crawler"
      assert meta.args["run_id"] == "run-9"
    end

    test "a cell-keyed job still works, unchanged" do
      Crawler.returns(%{changed: ["d1"]})

      assert :ok =
               ReactiveDag.ScanWorker.perform(%Oban.Job{
                 args: %{
                   "cell" => "docs",
                   "plan_mfa" => ["ReactiveDag.ScanWorkerTest", "plan_for_worker", []]
                 }
               })

      # a flat list is unambiguous here: the cell that was polled owns it
      assert length(Crawler.polls()) == 1
    end

    test "a flat list is fine when the source owns ONE leaf" do
      # nothing to disambiguate, so the map form is not forced on the common case
      {:ok, result} = Source.refresh_source(plan(), Solo, reason: "scan")

      assert result.marked == %{"solo_docs" => ["s1"]}
    end
  end
end

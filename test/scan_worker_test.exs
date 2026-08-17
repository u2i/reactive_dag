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
        # a scanner reporting it CANNOT scan, rather than a poll that found
        # nothing — the distinction #122 is about
        {:error, _} = error -> error
        result -> {:ok, result || %{changed: []}}
      end
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
      poll(ReactiveDag.ScanWorkerTest.Crawler, args: [recent: true], every: "0 * * * *")
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

      # a CONSUMER of the crawl, not a second declaration of it. One scanner is
      # one node now; two nodes naming the same module means the upstream is
      # polled twice, and `verify_one_node_per_source!/1` rejects it.
      reduce(
        over: :docs,
        group_by: :key,
        expand: fn key, rows ->
          for r <- rows, r.category == "notice", do: %{key: key, category: r.category}
        end
      )
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

    # the sweep takes a cluster-wide advisory lock; granted in tests
    def query!("SELECT pg_try_advisory_lock" <> _, _), do: %{rows: [[true]]}
    def query!("SELECT pg_advisory_unlock" <> _, _), do: %{rows: [[true]]}
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

  defp plan, do: ReactiveDag.Node.graph([Docs, Notices, Totals])

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

    test "a cell with no scanner is CANCELLED, not failed and not silently ok" do
      # nothing to retry — it will have no scanner next attempt either — but
      # `:ok` claimed a scan happened. `{:cancel, _}` is Oban's word for
      # "complete, and do not try again", which is exactly this.
      assert {:cancel, :no_scanner} =
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

    test "what the poll reported under `detail:` reaches the handler" do
      # The only route a scan's OWN cost has to a live consumer: `report`
      # covers the drain, and a poll produces no step. Without this a host can
      # roll spend up after a sweep (`Source.detail_total/2`) but cannot show it
      # as the scan finishes.
      test_pid = self()

      :telemetry.attach(
        "scan-detail-test",
        [:reactive_dag, :scan, :stop],
        fn _e, _m, meta, _ -> send(test_pid, {:scan_stop, meta}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach("scan-detail-test") end)

      Crawler.returns(%{
        changed: ["a"],
        detail: %{tokens_in: %{"claude-haiku-4-5" => 900}, llm_calls: 3}
      })

      perform_job(%{
        "cell" => "docs",
        "plan_mfa" => ["ReactiveDag.ScanWorkerTest", "plan_for_worker", []]
      })

      assert_received {:scan_stop, meta}
      assert meta.detail == %{tokens_in: %{"claude-haiku-4-5" => 900}, llm_calls: 3}
    end

    test "a poll reporting no detail sends an empty map, not nil" do
      # A handler should not have to guard: `%{}` and "this poll spends nothing"
      # are the same statement, and nil would make every consumer write a clause.
      test_pid = self()

      :telemetry.attach(
        "scan-nodetail-test",
        [:reactive_dag, :scan, :stop],
        fn _e, _m, meta, _ -> send(test_pid, {:scan_stop, meta}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach("scan-nodetail-test") end)

      Crawler.returns(%{changed: ["a"]})

      perform_job(%{
        "cell" => "docs",
        "plan_mfa" => ["ReactiveDag.ScanWorkerTest", "plan_for_worker", []]
      })

      assert_received {:scan_stop, meta}
      assert meta.detail == %{}
    end

    test "the poll and its drain arrive as one `%ScanRun{}`" do
      test_pid = self()

      :telemetry.attach(
        "scan-run-test",
        [:reactive_dag, :scan, :stop],
        fn _e, _m, meta, _ -> send(test_pid, {:scan_stop, meta}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach("scan-run-test") end)

      Crawler.returns(%{changed: ["a"], detail: %{tokens_in: 900}})

      perform_job(%{
        "cell" => "docs",
        "plan_mfa" => ["ReactiveDag.ScanWorkerTest", "plan_for_worker", []]
      })

      assert_received {:scan_stop, meta}
      assert %ReactiveDag.ScanRun{} = run = meta.run

      assert run.cell == "docs"
      assert run.changed == ["a"]
      assert run.detail == %{tokens_in: 900}
      assert %ReactiveDag.Drain.Report{} = run.report

      # The point of the pairing: one call for what the RUN cost, rather than
      # the caller adding the poll's spend to the drain's.
      assert ReactiveDag.ScanRun.total(run, :tokens_in) >= 900
      assert ReactiveDag.ScanRun.drained?(run)
      assert ReactiveDag.ScanRun.complete?(run)
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

  describe "the sweep job — single thread, graph order" do
    # The intended shape: ONE job, every source in an order that makes sense,
    # one drain at the end. A source that must run after another says so with
    # `depends_on`, and because these polls run sequentially in this process it
    # genuinely sees what the earlier one wrote.
    test "polls every source and drains once" do
      Crawler.returns(%{changed: %{"docs" => ["d1"]}})

      assert :ok =
               ReactiveDag.ScanWorker.perform(%Oban.Job{
                 args: %{
                   "sweep" => true,
                   "plan_mfa" => ["ReactiveDag.ScanWorkerTest", "plan_for_worker", []]
                 }
               })

      assert length(Crawler.polls()) == 1, "one poll per source, not per fed cell"
    end

    test "another node holding the lock is a stand-down, not a failure" do
      # the frontier is a SET, not a queue: whatever this sweep would have
      # claimed is still there for whoever holds the lock. Returning an error
      # would make Oban retry work that is already happening.
      defmodule BusyRepo do
        def query!("SELECT pg_try_advisory_lock" <> _, _), do: %{rows: [[false]]}
        def query!("SELECT DISTINCT cell_id" <> _, _), do: %{rows: []}
        def query!("SELECT COUNT" <> _, _), do: %{rows: [[0]]}
      end

      Application.put_env(:reactive_dag, :repo, BusyRepo)
      Crawler.returns(%{changed: %{"docs" => ["d1"]}})

      assert :ok =
               ReactiveDag.ScanWorker.perform(%Oban.Job{
                 args: %{
                   "sweep" => true,
                   "plan_mfa" => ["ReactiveDag.ScanWorkerTest", "plan_for_worker", []]
                 }
               })

      assert Crawler.polls() == [], "did not poll: the other node is doing it"
    end

    test "the lock covers the POLL, not just the drain" do
      # two nodes polling the same upstreams at the same minute is duplicated
      # external I/O, which is the expensive half
      defmodule BusyRepo2 do
        def query!("SELECT pg_try_advisory_lock" <> _, _), do: %{rows: [[false]]}
        def query!("SELECT DISTINCT cell_id" <> _, _), do: %{rows: []}
        def query!("SELECT COUNT" <> _, _), do: %{rows: [[0]]}
      end

      Application.put_env(:reactive_dag, :repo, BusyRepo2)

      ReactiveDag.ScanWorker.perform(%Oban.Job{
        args: %{
          "sweep" => true,
          "plan_mfa" => ["ReactiveDag.ScanWorkerTest", "plan_for_worker", []]
        }
      })

      assert Crawler.polls() == []
    end

    test "telemetry names the sweep rather than a cell" do
      test_pid = self()

      :telemetry.attach_many(
        "sweep-span",
        [[:reactive_dag, :scan, :start], [:reactive_dag, :scan, :stop]],
        fn e, _m, meta, _ -> send(test_pid, {List.last(e), meta}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach("sweep-span") end)

      ReactiveDag.ScanWorker.perform(%Oban.Job{
        args: %{
          "sweep" => true,
          "run_id" => "run-3",
          "plan_mfa" => ["ReactiveDag.ScanWorkerTest", "plan_for_worker", []]
        }
      })

      assert_received {:start, start_meta}
      assert_received {:stop, stop_meta}

      assert start_meta.cell == :sweep
      assert start_meta.args["run_id"] == "run-3"
      assert is_list(stop_meta.sources)
    end
  end

  describe "not scannable — the third outcome" do
    # A scan can succeed, fail in a way retrying might fix, or be structurally
    # unscannable: no credential configured, an integration not enabled for this
    # tenant. Retrying cannot conjure a missing credential, and burning every
    # attempt to land in `discarded` reads as "something is broken" when the
    # honest answer is "this was never going to work" (u2i/reactive_dag#122).
    test "a scanner saying so is cancelled, not retried" do
      Crawler.returns({:error, :not_scannable})

      assert {:cancel, :not_scannable} =
               perform_job(%{
                 "cell" => "docs",
                 "plan_mfa" => ["ReactiveDag.ScanWorkerTest", "plan_for_worker", []]
               })
    end

    test "and it can say WHY" do
      Crawler.returns({:error, {:not_scannable, :no_credential}})

      assert {:cancel, {:not_scannable, :no_credential}} =
               perform_job(%{
                 "cell" => "docs",
                 "plan_mfa" => ["ReactiveDag.ScanWorkerTest", "plan_for_worker", []]
               })
    end

    test "an ORDINARY error still retries — the distinction is the point" do
      Crawler.returns({:error, :timeout})

      assert {:error, :timeout} =
               perform_job(%{
                 "cell" => "docs",
                 "plan_mfa" => ["ReactiveDag.ScanWorkerTest", "plan_for_worker", []]
               })
    end

    test "stop still fires: an unscannable source is a COMPLETED scan" do
      # a host recording scan results wants the row — "we looked, and there was
      # nothing to look with" is an outcome, not an absence
      test_pid = self()

      :telemetry.attach(
        "unscannable-stop",
        [:reactive_dag, :scan, :stop],
        fn _e, m, meta, _ -> send(test_pid, {:stop, m, meta}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach("unscannable-stop") end)

      Crawler.returns({:error, {:not_scannable, :no_credential}})

      perform_job(%{
        "cell" => "docs",
        "run_id" => "run-5",
        "plan_mfa" => ["ReactiveDag.ScanWorkerTest", "plan_for_worker", []]
      })

      assert_received {:stop, m, meta}
      assert m.changed == 0
      assert meta.not_scannable == {:not_scannable, :no_credential}
      assert meta.args["run_id"] == "run-5", "still attributable to its run"

      # `detail` used to be OMITTED on this path only, so a handler got nil from
      # an unscannable source and a map from every other, for no reason a caller
      # could infer.
      assert meta.detail == %{}

      # A completed scan that never drained. `drained?` says so, rather than a
      # host inferring it from a zero pass count — which means something else.
      assert %ReactiveDag.ScanRun{} = run = meta.run
      assert run.not_scannable == {:not_scannable, :no_credential}
      refute ReactiveDag.ScanRun.drained?(run)
      refute ReactiveDag.ScanRun.changed?(run)
      assert ReactiveDag.ScanRun.total(run, :tokens_in) == 0
    end
  end

  describe "a sweep keeps its per-source detail" do
    # `poll_all/2` returns `%{module => result}` — each source's own `changed`,
    # `unreachable`, and whatever else it reported. The sweep used to compute a
    # total from it and drop the rest, so a host's activity log went from
    # "machines polled at 14:03, wrote 4 rows, Huntress unreachable" to "a sweep
    # ran, eight sources participated" (u2i/reactive_dag#133).
    defp sweep(extra \\ %{}) do
      ReactiveDag.ScanWorker.perform(%Oban.Job{
        args:
          Map.merge(
            %{
              "sweep" => true,
              "plan_mfa" => ["ReactiveDag.ScanWorkerTest", "plan_for_worker", []]
            },
            extra
          )
      })
    end

    test "stop carries each source's own result, not just its name" do
      test_pid = self()

      :telemetry.attach(
        "sweep-results",
        [:reactive_dag, :scan, :stop],
        fn _e, _m, meta, _ -> send(test_pid, {:stop, meta}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach("sweep-results") end)

      Crawler.returns(%{changed: %{"docs" => ["d1"]}, unreachable: [{"archive", :timeout}]})

      sweep()

      assert_received {:stop, meta}
      assert %{} = meta.results

      result = meta.results[Crawler]
      assert result.changed == %{"docs" => ["d1"]}
      assert result.unreachable == [{"archive", :timeout}], "the outage survives the aggregate"
    end

    test "and the aggregate is still there for a host that only wants the total" do
      test_pid = self()

      :telemetry.attach(
        "sweep-total",
        [:reactive_dag, :scan, :stop],
        fn _e, m, meta, _ -> send(test_pid, {:stop, m, meta}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach("sweep-total") end)

      Crawler.returns(%{changed: %{"docs" => ["d1"]}})
      sweep()

      assert_received {:stop, m, meta}
      assert m.changed == 1
      assert Crawler in meta.sources
    end
  end

  describe "source_stop — progress through a long sweep" do
    test "fires once per source, as it finishes" do
      test_pid = self()

      :telemetry.attach(
        "source-stop",
        [:reactive_dag, :scan, :source_stop],
        fn _e, m, meta, _ -> send(test_pid, {:src, meta.source, meta.result, m}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach("source-stop") end)

      Crawler.returns(%{changed: %{"docs" => ["d1"]}})

      ReactiveDag.ScanWorker.perform(%Oban.Job{
        args: %{
          "sweep" => true,
          "plan_mfa" => ["ReactiveDag.ScanWorkerTest", "plan_for_worker", []]
        }
      })

      assert_received {:src, Crawler, {:ok, result}, m}
      assert result.changed == %{"docs" => ["d1"]}
      assert is_integer(m.duration_us), "how long THIS source took, not the sweep"
    end

    test "a FAILING source still reports — that is the thing worth recording" do
      test_pid = self()

      :telemetry.attach(
        "source-stop-fail",
        [:reactive_dag, :scan, :source_stop],
        fn _e, _m, meta, _ -> send(test_pid, {:src, meta.source, meta.result}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach("source-stop-fail") end)

      Crawler.raises()

      ReactiveDag.ScanWorker.perform(%Oban.Job{
        args: %{
          "sweep" => true,
          "plan_mfa" => ["ReactiveDag.ScanWorkerTest", "plan_for_worker", []]
        }
      })

      assert_received {:src, Crawler, {:error, reason}}
      assert reason =~ "upstream is down"
    end

    test "it comes from poll_all/2, so a direct caller sees it too" do
      test_pid = self()

      :telemetry.attach(
        "source-stop-direct",
        [:reactive_dag, :scan, :source_stop],
        fn _e, _m, meta, _ -> send(test_pid, {:src, meta.source}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach("source-stop-direct") end)

      Source.poll_all(plan())

      assert_received {:src, Crawler}
    end
  end

  describe "the shapes a poll may report changed keys in" do
    # A bare list was documented in `mark/5`'s own comment and unreachable in
    # its code: `Map.get(result, :changed, [])` ran FIRST and raises
    # `BadMapError` on a list, before the `is_list` clause guarding it could
    # match. Every scan of such a source died on attempt 1, and through
    # `ScanWorker` that reads as a button doing nothing (u2i/reactive_dag#138).
    test "a bare list belongs to the cell that was polled" do
      Crawler.returns(["d1", "d2"])

      {:ok, result} = Source.refresh(plan(), "docs")

      assert result.marked == %{"docs" => ["d1", "d2"]}
    end

    test "and reaches the frontier, not just the return value" do
      Crawler.returns(["d1"])

      {:ok, _} = Source.refresh(plan(), "docs", reason: "flat")

      assert {"docs", "d1", "flat"} in FakeRepo.marks()
    end

    test "an empty bare list marks nothing, rather than crashing" do
      # the reported stack trace was on `[]` — a poll that found nothing
      Crawler.returns([])

      assert {:ok, result} = Source.refresh(plan(), "docs")
      assert result.marked == %{}
    end

    test "`%{changed: keys}` still works" do
      Crawler.returns(%{changed: ["d1"]})

      {:ok, result} = Source.refresh(plan(), "docs")

      assert result.marked == %{"docs" => ["d1"]}
    end

    test "and the by-leaf map still works" do
      Crawler.returns(%{changed: %{"docs" => ["d1"], "notices" => ["n1"]}})

      {:ok, result} = Source.refresh(plan(), "docs")

      assert result.marked == %{"docs" => ["d1"], "notices" => ["n1"]}
    end

    test "anything else names the three accepted shapes" do
      Crawler.returns("nope")

      err = assert_raise ArgumentError, fn -> Source.refresh(plan(), "docs") end
      msg = Exception.message(err)

      assert msg =~ "\"nope\""
      assert msg =~ "%{changed: keys}"
      assert msg =~ "leaf_id => keys"
    end
  end
end

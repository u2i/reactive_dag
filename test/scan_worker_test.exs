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

    @impl true
    def id, do: :crawler
    @impl true
    def leaf_cells(_g), do: ["docs"]
    @impl true
    def poll(opts) do
      Agent.update(__MODULE__, &%{&1 | polls: [opts | &1.polls]})
      {:ok, Agent.get(__MODULE__, & &1.result) || %{changed: []}}
    end
  end

  defmodule Docs do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Ets, extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :category, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]
      create :upsert, upsert?: true, accept: [:key, :category]
    end

    reactive do
      id(:docs)
      leaf?(true)
      scan(ReactiveDag.ScanWorkerTest.Crawler, args: [recent: true], every: "0 * * * *")
    end
  end

  defmodule Totals do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Ets, extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :category, :string, public?: true
      attribute :n, :integer, public?: true
    end

    actions do
      defaults [:read, :destroy]
      create :upsert, upsert?: true, accept: [:key, :category, :n]
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

    for r <- [Docs, Totals], row <- Ash.read!(r), do: Ash.destroy!(row)
    :ok
  end

  defp plan, do: ReactiveDag.Node.graph([Docs, Totals])

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
end

defmodule ReactiveDag.CascadeTest do
  @moduledoc """
  A change, propagated to completion in one transaction — stopping only where
  it must.

  The properties under test are the ones the queue could not offer:

    * a cascade runs the whole reachable subtree from an explicit origin,
      shallowest first, without asking the database what needs doing;
    * a **diamond recomputes its apex once** — two inputs changing in one
      cascade merge before the shared parent runs, where the queue managed this
      only if two marks happened to land before a claim;
    * a suspendable node **stops its branch and leaves a row**, while every
      other branch keeps running and commits;
    * a failing cell **stops its own branch only**;
    * a change that cannot be attributed suspends with `"*"`, which is what
      makes a whole-cell resumption an honest statement rather than a guess.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.Cascade

  # An in-memory stand-in for the suspension table. The walk's behaviour is what
  # is under test here; the SQL has its own file, executed against real Postgres.
  defmodule FakeRepo do
    use Agent

    def child_spec(_), do: %{id: __MODULE__, start: {__MODULE__, :start_link, [[]]}}
    def start_link(_ \\ []), do: Agent.start_link(fn -> [] end, name: __MODULE__)

    def install do
      prev = Application.get_env(:reactive_dag, :repo)
      Application.put_env(:reactive_dag, :repo, __MODULE__)

      ExUnit.Callbacks.on_exit(fn ->
        if prev,
          do: Application.put_env(:reactive_dag, :repo, prev),
          else: Application.delete_env(:reactive_dag, :repo)
      end)

      :ok
    end

    def recorded, do: Agent.get(__MODULE__, &Enum.reverse/1)

    def query!("INSERT INTO " <> _, [id, tenant, waiting, resource, row_uuid, version_id, reason]) do
      Agent.update(__MODULE__, &[
        %{
          id: id,
          tenant: tenant,
          waiting: waiting,
          resource: resource,
          row_uuid: row_uuid,
          version_id: version_id,
          reason: reason
        }
        | &1
      ])

      %{rows: [], num_rows: 1}
    end

    def query!("SELECT" <> _, _), do: %{rows: [], num_rows: 0}
    def query!("DELETE" <> _, _), do: %{rows: [], num_rows: 0}

    # `Suspension.transaction/1` and `savepoint/1` only wrap when the repo
    # exports `transaction/2` — so a fake without it silently takes the
    # no-transaction path, and a test asserting transaction boundaries would
    # pass for the wrong reason. This flags the process while inside, which is
    # what lets a recompute observe whether it is running in one.
    def transaction(fun, _opts \\ []) do
      outer = Process.get(:rd_in_txn, false)
      Process.put(:rd_in_txn, true)

      try do
        {:ok, fun.()}
      catch
        :throw, {:rd_rollback, reason} -> {:error, reason}
      after
        Process.put(:rd_in_txn, outer)
      end
    end

    def rollback(reason), do: throw({:rd_rollback, reason})
  end

  setup do
    start_supervised!(FakeRepo)
    FakeRepo.install()
    :ok
  end

  # ---- a hand-built plan, so the walk is tested without a DSL in the way ----

  defp cell(id, inputs, opts \\ []) do
    %ReactiveDag.Cell{
      id: id,
      op: Keyword.get(opts, :op, :test),
      inputs: inputs,
      leaf?: inputs == [],
      meta: Keyword.get(opts, :meta, %{})
    }
  end

  # Records every recompute, so the test can assert HOW MANY times a cell ran —
  # which is the only way to see the diamond property.
  defmodule Ran do
    def child_spec(_), do: %{id: __MODULE__, start: {__MODULE__, :start_link, [[]]}}
    def start_link(_ \\ []), do: Agent.start_link(fn -> [] end, name: __MODULE__)
    def note(id, keys), do: Agent.update(__MODULE__, &[{id, Enum.sort(keys)} | &1])
    def all, do: Agent.get(__MODULE__, &Enum.reverse/1)
    def count(id), do: all() |> Enum.count(fn {c, _} -> c == id end)
    def keys(id), do: all() |> Enum.find_value([], fn {c, k} -> if c == id, do: k end)
  end

  defmodule PassThrough do
    @behaviour ReactiveDag.Op

    @impl true
    def recompute(cell, keys) do
      ReactiveDag.CascadeTest.Ran.note(cell.id, keys)
      {:ok, keys}
    end
  end

  defmodule Fails do
    @behaviour ReactiveDag.Op

    @impl true
    def recompute(cell, keys) do
      ReactiveDag.CascadeTest.Ran.note(cell.id, keys)
      {:error, :upstream_unavailable}
    end
  end

  defp plan_of(cells) do
    ReactiveDag.Graph.build(cells)
  end

  defp compute(id, inputs, mod \\ PassThrough, extra \\ %{}) do
    cell(id, inputs, meta: Map.merge(%{compute: mod}, extra))
  end

  setup do
    # `start_supervised!`, not `Agent.start_link` with an `already_started`
    # fallback. The fallback REUSED a pid that may be mid-exit — the previous
    # test's agent, dying asynchronously — so it traded a visible setup error
    # for an intermittent failure somewhere later. ExUnit's supervisor waits for
    # the exit, so the name is free by the time this runs.
    start_supervised!(Ran)
    :ok
  end

  describe "the walk" do
    test "runs the whole reachable subtree from one origin" do
      plan =
        plan_of([
          cell("leaf", []),
          compute("mid", ["leaf"]),
          compute("top", ["mid"])
        ])

      {:ok, report} = Cascade.run(plan, [%{cell: "leaf", keys: ["k1"]}])

      assert ["leaf", "mid", "top"] == Enum.map(report.steps, & &1.cell),
             "an explicit origin, then everything downstream — no queue consulted"

      assert report.suspended == []
    end

    test "runs shallowest first" do
      plan =
        plan_of([
          cell("leaf", []),
          compute("deep", ["mid"]),
          compute("mid", ["leaf"])
        ])

      {:ok, report} = Cascade.run(plan, [%{cell: "leaf", keys: ["k1"]}])

      cells = Enum.map(report.steps, & &1.cell)
      assert Enum.find_index(cells, &(&1 == "mid")) < Enum.find_index(cells, &(&1 == "deep"))
    end

    test "records what caused each step" do
      plan = plan_of([cell("leaf", []), compute("mid", ["leaf"])])

      {:ok, report} = Cascade.run(plan, [%{cell: "leaf", keys: ["k1"]}])

      assert [%{cell: "leaf", triggered_by: nil}, %{cell: "mid", triggered_by: "leaf"}] =
               report.steps
    end

    test "a diamond recomputes its apex ONCE" do
      # Two inputs of one cell change in the same cascade. Under the queue this
      # coalesced only if both marks landed before the claim; in memory the
      # merge happens before the cell runs, by construction.
      plan =
        plan_of([
          cell("leaf", []),
          compute("left", ["leaf"]),
          compute("right", ["leaf"]),
          compute("apex", ["left", "right"])
        ])

      {:ok, report} = Cascade.run(plan, [%{cell: "leaf", keys: ["k1"]}])

      assert Ran.count("apex") == 1,
             "the apex ran #{Ran.count("apex")} times; two inputs changing in one " <>
               "cascade must merge into one unit of work"

      assert Enum.count(report.steps, &(&1.cell == "apex")) == 1
    end

    test "the apex sees the union of what BOTH sides changed" do
      # Running once is not enough — it must run once with everything. Each side
      # renames its key, so the apex's two arrivals carry disjoint work and an
      # overwrite would silently drop one side's rows while still looking like
      # a single tidy recompute.
      defmodule RenameLeft do
        @behaviour ReactiveDag.Op
        @impl true
        def recompute(cell, keys) do
          ReactiveDag.CascadeTest.Ran.note(cell.id, keys)
          {:ok, Enum.map(keys, &"L-#{&1}")}
        end
      end

      defmodule RenameRight do
        @behaviour ReactiveDag.Op
        @impl true
        def recompute(cell, keys) do
          ReactiveDag.CascadeTest.Ran.note(cell.id, keys)
          {:ok, Enum.map(keys, &"R-#{&1}")}
        end
      end

      plan =
        plan_of([
          cell("leaf", []),
          compute("left", ["leaf"], RenameLeft),
          compute("right", ["leaf"], RenameRight),
          compute("apex", ["left", "right"])
        ])

      {:ok, _} = Cascade.run(plan, [%{cell: "leaf", keys: ["k1"]}])

      assert Ran.count("apex") == 1
      assert Ran.keys("apex") == ["L-k1", "R-k1"],
             "the apex must recompute BOTH sides' keys in its single run; " <>
               "keeping only the last arrival loses the other side's work"
    end
  end

  describe "suspension" do
    test "a suspendable cell stops its branch and records a row" do
      plan =
        plan_of([
          cell("leaf", []),
          compute("slow", ["leaf"], PassThrough, %{suspends: %{expensive: []}}),
          compute("after_slow", ["slow"])
        ])

      {:ok, report} =
        Cascade.run(plan, [%{cell: "leaf", keys: ["k1"], versions: %{"k1" => "v-1"}}])

      assert Ran.count("slow") == 0, "the expensive cell must NOT run inline"
      assert Ran.count("after_slow") == 0, "and nothing below it runs either"

      assert [%{waiting: "slow", resource: "leaf", version_id: "v-1", reason: "expensive"}] =
               FakeRepo.recorded()

      assert [%{waiting: "slow", reason: :expensive}] = report.suspended
    end

    test "other branches keep running and commit" do
      plan =
        plan_of([
          cell("leaf", []),
          compute("slow", ["leaf"], PassThrough, %{suspends: %{expensive: []}}),
          compute("fast_a", ["leaf"]),
          compute("fast_b", ["leaf"])
        ])

      {:ok, report} = Cascade.run(plan, [%{cell: "leaf", keys: ["k1"]}])

      assert Ran.count("fast_a") == 1
      assert Ran.count("fast_b") == 1
      assert Ran.count("slow") == 0

      assert length(report.suspended) == 1,
             "a suspension truncates ONE branch — the rest of the graph is unaffected"
    end

    test "gated suspends with :approval rather than :expensive" do
      plan =
        plan_of([
          cell("leaf", []),
          compute("reviewed", ["leaf"], PassThrough, %{suspends: %{approval: []}})
        ])

      {:ok, _} = Cascade.run(plan, [%{cell: "leaf", keys: ["k1"]}])

      assert [%{reason: "approval"}] = FakeRepo.recorded()
    end

    test "one suspension per changed row" do
      plan =
        plan_of([
          cell("leaf", []),
          compute("slow", ["leaf"], PassThrough, %{suspends: %{expensive: []}})
        ])

      {:ok, _} =
        Cascade.run(plan, [
          %{cell: "leaf", keys: ["a", "b", "c"], versions: %{"a" => "v-a", "b" => "v-b"}}
        ])

      recorded = FakeRepo.recorded()
      assert length(recorded) == 3, "the row is what a resumption narrows by"

      assert ["v-a", "v-b", "*"] == Enum.map(recorded, & &1.version_id),
             "a key with no version suspends with the sentinel, not a nil"
    end

    test "an unattributable whole-cell change suspends with *" do
      plan =
        plan_of([
          cell("leaf", []),
          compute("slow", ["leaf"], PassThrough, %{suspends: %{expensive: []}})
        ])

      {:ok, _} = Cascade.run(plan, [%{cell: "leaf", keys: ["*"]}])

      assert [%{row_uuid: "*", version_id: "*"}] = FakeRepo.recorded(),
             "not `something happened somewhere`, but `this could not be narrowed`"
    end

    test "the suspension names what CHANGED, not what stopped, as its resource" do
      plan =
        plan_of([
          cell("leaf", []),
          compute("mid", ["leaf"]),
          compute("slow", ["mid"], PassThrough, %{suspends: %{expensive: []}})
        ])

      {:ok, _} = Cascade.run(plan, [%{cell: "leaf", keys: ["k1"]}])

      assert [%{waiting: "slow", resource: "mid"}] = FakeRepo.recorded(),
             "`waiting` is the cell that stopped; `resource` is the cell whose " <>
               "change reached it. Reading one off the other would name the " <>
               "wrong resource in every suspension"
    end
  end

  describe "failure" do
    test "a failing cell stops its own branch only" do
      plan =
        plan_of([
          cell("leaf", []),
          compute("bad", ["leaf"], Fails),
          compute("below_bad", ["bad"]),
          compute("good", ["leaf"])
        ])

      {:ok, report} = Cascade.run(plan, [%{cell: "leaf", keys: ["k1"]}])

      assert Ran.count("good") == 1, "an unrelated branch commits"
      assert Ran.count("below_bad") == 0, "nothing below the failure runs"

      refute Enum.any?(report.steps, &(&1.cell == "bad")),
             "a failed cell contributes no step — it changed nothing"
    end

    test "a runaway cascade raises with the partial trace" do
      # `mid` re-dirties itself through a cycle the plan builder would reject,
      # so instead: a very low budget over a legitimate graph.
      plan = plan_of([cell("leaf", []), compute("mid", ["leaf"]), compute("top", ["mid"])])

      assert_raise Cascade.RunawayError, ~r/exceeded/, fn ->
        Cascade.run(plan, [%{cell: "leaf", keys: ["k1"]}], max_steps: 1)
      end
    end
  end

  describe "the report" do
    test "carries steps, suspensions and timing" do
      plan =
        plan_of([
          cell("leaf", []),
          compute("fast", ["leaf"]),
          compute("slow", ["leaf"], PassThrough, %{suspends: %{expensive: []}})
        ])

      {:ok, report} = Cascade.run(plan, [%{cell: "leaf", keys: ["k1"]}])

      assert length(report.steps) == 2
      assert length(report.suspended) == 1
      assert report.duration_us >= 0
      assert "leaf" in ReactiveDag.Report.cells(report)
    end
  end

  describe "resuming a suspended cell" do
    test "runs the suspended cell and cascades onward from it" do
      plan =
        plan_of([
          cell("leaf", []),
          compute("slow", ["leaf"], PassThrough, %{suspends: %{expensive: []}}),
          compute("after_slow", ["slow"])
        ])

      {:ok, report} =
        Cascade.run(plan, [%{cell: "slow", keys: ["k1"]}], resuming: "slow")

      assert Ran.count("slow") == 1,
             "`resuming:` is what lets a suspendable cell run at all — without " <>
               "it the cascade suspends it again on sight and the point never clears"

      assert Ran.count("after_slow") == 1, "and the branch below it continues"
      assert report.suspended == []
      assert ["slow", "after_slow"] == Enum.map(report.steps, & &1.cell)
    end

    test "the resumed cell runs with NO transaction open" do
      # The property the whole redesign exists for. The drain held a
      # transaction across a nine-minute extraction until the database closed
      # the connection; here the slow work happens with nothing open.
      defmodule TxnSpy do
        @behaviour ReactiveDag.Op

        @impl true
        def recompute(cell, keys) do
          ReactiveDag.CascadeTest.Ran.note(cell.id, keys)
          send(self(), {:ran_inside_txn?, cell.id, Process.get(:rd_in_txn, false)})
          {:ok, keys}
        end
      end

      plan =
        plan_of([
          cell("leaf", []),
          compute("slow", ["leaf"], TxnSpy, %{suspends: %{expensive: []}}),
          compute("after_slow", ["slow"], TxnSpy)
        ])

      {:ok, _} = Cascade.run(plan, [%{cell: "slow", keys: ["k1"]}], resuming: "slow")

      assert_received {:ran_inside_txn?, "slow", false}
      assert_received {:ran_inside_txn?, "after_slow", true},
                      "the fast subtree below the resumed cell IS transactional — " <>
                        "only the expensive step is lifted out"
    end

    test "a failing resumption reports, so its suspensions are kept" do
      plan =
        plan_of([
          cell("leaf", []),
          compute("slow", ["leaf"], Fails, %{suspends: %{expensive: []}})
        ])

      assert {:error, {:resumption_failed, "slow", :upstream_unavailable}} =
               Cascade.run(plan, [%{cell: "slow", keys: ["k1"]}], resuming: "slow")

      # Slow work keeps its retry because its suspension committed before the
      # work began. Fast work has no such record and relies on re-observation.
    end

    test "resuming a cell that changed nothing cascades nowhere" do
      defmodule NoChange do
        @behaviour ReactiveDag.Op

        @impl true
        def recompute(cell, keys) do
          ReactiveDag.CascadeTest.Ran.note(cell.id, keys)
          {:ok, []}
        end
      end

      plan =
        plan_of([
          cell("leaf", []),
          compute("slow", ["leaf"], NoChange, %{suspends: %{expensive: []}}),
          compute("after_slow", ["slow"])
        ])

      {:ok, report} = Cascade.run(plan, [%{cell: "slow", keys: ["k1"]}], resuming: "slow")

      assert Ran.count("slow") == 1
      assert Ran.count("after_slow") == 0,
             "the ordinary outcome when a resumption`s input was already superseded"

      assert report.suspended == []
    end
  end

  describe "scheduling the resumption" do
    test "every distinct stopping point is scheduled, once" do
      # A suspension nobody resumes is a cascade that stopped forever. This
      # lives in `Cascade.run/3` rather than in a worker because a poll and a
      # reprocess also run cascades directly — leaving it to the caller meant
      # one of three paths scheduled anything and the other two wrote rows that
      # would never be read.
      test_pid = self()

      plan =
        plan_of([
          cell("leaf", []),
          compute("slow", ["leaf"], PassThrough, %{suspends: %{expensive: []}})
        ])

      {:ok, _} =
        Cascade.run(plan, [%{cell: "leaf", keys: ["a", "b"]}],
          resumption_scheduler: fn point, _opts ->
            send(test_pid, {:scheduled, point.waiting, point.row_uuid})
            {:ok, :fake}
          end
        )

      assert_received {:scheduled, "slow", "a"}
      assert_received {:scheduled, "slow", "b"}
    end

    test "several suspensions at ONE point schedule one job" do
      # A stopping point is `(tenant, waiting, resource, row_uuid)`, so two
      # DIFFERENT rows reaching one slow cell are two points and two jobs — that
      # is correct, and covered above.
      #
      # The dedup matters when one point is reached more than once in a single
      # cascade: two inputs of the same slow cell both carrying the same changed
      # row. The pending job reads every suspension there when it runs, so a
      # second job would find nothing and exit.
      test_pid = self()

      plan =
        plan_of([
          cell("leaf", []),
          compute("via_a", ["leaf"]),
          compute("via_b", ["leaf"]),
          compute("slow", ["via_a", "via_b"], PassThrough, %{suspends: %{expensive: []}})
        ])

      {:ok, report} =
        Cascade.run(plan, [%{cell: "leaf", keys: ["k1"]}],
          resumption_scheduler: fn point, _ ->
            send(test_pid, {:scheduled, point.waiting, point.row_uuid})
            {:ok, :fake}
          end
        )

      # One suspension, because the walk merges both arrivals before `slow`
      # runs — the same property that makes a diamond recompute its apex once.
      assert length(report.suspended) == 1

      assert_received {:scheduled, "slow", "k1"}
      refute_received {:scheduled, "slow", _}
    end

    test "a scheduler that raises does not lose the cascade" do
      # The suspensions are already durable, so a queue being down costs a delay
      # someone must notice — not the work.
      plan =
        plan_of([
          cell("leaf", []),
          compute("slow", ["leaf"], PassThrough, %{suspends: %{expensive: []}})
        ])

      assert {:ok, report} =
               Cascade.run(plan, [%{cell: "leaf", keys: ["a"]}],
                 resumption_scheduler: fn _point, _ -> raise "queue is down" end
               )

      assert [%{waiting: "slow"}] = report.suspended
      assert length(FakeRepo.recorded()) == 1, "the suspension is still recorded"
    end

    test "a cascade that suspends nothing schedules nothing" do
      test_pid = self()
      plan = plan_of([cell("leaf", []), compute("fast", ["leaf"])])

      {:ok, _} =
        Cascade.run(plan, [%{cell: "leaf", keys: ["a"]}],
          resumption_scheduler: fn _p, _o -> send(test_pid, :scheduled) end
        )

      refute_received :scheduled
    end
  end
end

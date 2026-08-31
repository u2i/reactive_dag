defmodule ReactiveDag.ReprocessWorkerTest do
  @moduledoc """
  `ReprocessWorker` — re-derive without any input having changed.

  The frontier's sentence is "an input moved, redo this". For a changed prompt or
  a fixed fold that is false: the inputs are identical and the *function* is not.
  The mark is mechanically the same; the reason is not, and a job is where that
  belongs.

  Two things these pin that are easy to get wrong:

    * a slice reprocesses ONLY its slice — the point of selecting one is that
      FY24 is not re-derived when you asked about FY25;
    * a `per_key` node's fingerprint still skips, because it answers "did the
      input move?" and the input did not. The job reports claimed-vs-changed so
      that no-op is visible rather than mistaken for success.
  """
  use ExUnit.Case, async: false

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      allow_unregistered?(true)
    end
  end

  defmodule Lines do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute(:key, :string, primary_key?: true, allow_nil?: false, public?: true)
      attribute(:fiscal_year, :string, public?: true)
      attribute(:amount, :float, public?: true)
    end

    actions do
      defaults([:read, :destroy])
      create(:upsert, upsert?: true, accept: [:key, :fiscal_year, :amount])
    end

    reactive do
      id(:lines)
      leaf?(true)
      slice(:fiscal_year, values: ["FY24", "FY25"])
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
      attribute(:total, :float, public?: true)
    end

    actions do
      defaults([:read, :destroy])
      create(:upsert, upsert?: true, accept: [:key, :total])
    end

    reactive do
      id(:totals)
      recompute_by(:fiscal_year, to: :lines, from: :fiscal_year)

      reduce(
        group_by: :fiscal_year,
        into: fn fy, rows -> %{key: fy, total: rows |> Enum.map(& &1.amount) |> Enum.sum()} end
      )
    end
  end

  defmodule FakeRepo do
    def start_link do
      # the claimable set, plus a permanent ledger the claim does not consume
      {:ok, _} = Agent.start_link(fn -> [] end, name: :reprocess_mark_log)
      Agent.start_link(fn -> MapSet.new() end, name: __MODULE__)
    end

    def marks,
      do: Agent.get(__MODULE__, &MapSet.to_list/1) |> Enum.map(&{elem(&1, 1), elem(&1, 2), elem(&1, 3)}) |> Enum.sort()

    @doc "Marks as `{tenant, cell, key, reason}` — the tenant is the point."
    def tenanted_marks, do: Agent.get(__MODULE__, &MapSet.to_list/1) |> Enum.sort()

    @doc "Every mark made, including ones the drain has since claimed."
    def mark_log, do: Agent.get(:reprocess_mark_log, & &1) |> Enum.sort()

    def query!("INSERT INTO " <> _, p) do
      p
      |> Enum.chunk_every(7)
      |> Enum.each(fn [c, tenant, k, r, _, _held, _vid] ->
        Agent.update(__MODULE__, &MapSet.put(&1, {tenant, c, k, r}))
        # A LEDGER of every mark ever made, which the claim does not consume.
        # Asserting on the claimable set cannot see a mark the drain has already
        # taken — and the drain runs inside the worker.
        Agent.update(:reprocess_mark_log, &[{tenant, c, k, r} | &1])
      end)

      %{rows: []}
    end

    def query!("SELECT DISTINCT cell_id" <> _, _),
      do: %{rows: Agent.get(__MODULE__, & &1) |> Enum.map(&[elem(&1, 1)]) |> Enum.uniq()}

    # Rows are `{tenant, cell, key, reason}`, and the claim is scoped to BOTH —
    # a fake that ignored the tenant would let a tenant-scoping bug pass.
    def query!("DELETE FROM " <> _, [cell, tenant]) do
      c =
        Agent.get_and_update(__MODULE__, fn s ->
          {m, r} = Enum.split_with(s, fn {t, x, _, _} -> t == tenant and x == cell end)
          {m, MapSet.new(r)}
        end)

      %{rows: Enum.map(c, fn {_, _, k, _} -> [k, nil] end)}
    end

    def query!("SELECT COUNT" <> _, _), do: %{rows: [[Agent.get(__MODULE__, &MapSet.size/1)]]}
  end

  setup do
    # A poll or a write now ENQUEUES a cascade rather than leaving a mark,
    # so without this the library reaches for Oban and these tests fail on
    # a missing instance rather than on anything they are about.
    ReactiveDag.Test.Pending.capture_enqueues()

    start_supervised!(%{id: FakeRepo, start: {FakeRepo, :start_link, []}})
    prev = Application.get_env(:reactive_dag, :repo)
    Application.put_env(:reactive_dag, :repo, FakeRepo)
    on_exit(fn -> Application.put_env(:reactive_dag, :repo, prev) end)

    for r <- [Lines, Totals], row <- Ash.read!(r), do: Ash.destroy!(row)

    for {k, y, a} <- [{"a", "FY24", 1.0}, {"b", "FY24", 2.0}, {"c", "FY25", 5.0}] do
      Lines
      |> Ash.Changeset.for_create(:upsert, %{key: k, fiscal_year: y, amount: a})
      |> Ash.create!()
    end

    :ok
  end

  @doc false
  def plan, do: ReactiveDag.Node.graph([Lines, Totals])

  @doc "The same plan, as one tenant's — for the tenant-scoping test."
  def tenanted_plan, do: ReactiveDag.Node.graph([Lines, Totals], tenant: "tenant_a")

  defp run(args) do
    ReactiveDag.ReprocessWorker.perform(%Oban.Job{
      args: Map.put(args, "plan_mfa", ["ReactiveDag.ReprocessWorkerTest", "plan", []])
    })
  end

  defp totals, do: Totals |> Ash.read!() |> Enum.map(&{&1.key, &1.total}) |> Enum.sort()

  describe "the plan's tenant" do
    test "a reprocess runs in the plan's tenant, not untenanted" do
      # THE SILENT FAILURE, still the same one. Work scoped to the wrong tenant
      # reads nothing — and a run that finds nothing reports SUCCESS. So the
      # button appears to work, the job succeeds, and nothing recomputes.
      #
      # What carries the tenant has moved: there is no mark to inspect, so this
      # asserts on the OPTIONS the cascade ran with, which is what every read
      # and write below it is scoped by.
      test_pid = self()

      :telemetry.attach(
        "reprocess-tenant",
        [:reactive_dag, :cascade, :start],
        fn _e, _m, meta, _ -> send(test_pid, {:cascade_start, meta}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach("reprocess-tenant") end)

      ReactiveDag.ReprocessWorker.perform(%Oban.Job{
        args: %{
          "cell" => "lines",
          "plan_mfa" => ["ReactiveDag.ReprocessWorkerTest", "tenanted_plan", []]
        }
      })

      assert_received {:cascade_start, _meta}

      # The plan's tenant reaches the rows: a tenanted recompute writes tenanted
      # rows, and reading them back without the tenant would find nothing.
      assert Totals |> Ash.read!(tenant: "tenant_a") |> Enum.any?(),
             "the reprocess wrote nothing under tenant_a — the tenant was lost " <>
               "somewhere between the plan and the write"
    end
  end

  describe "selecting what to redo" do
    test "a slice reprocesses ONLY its slice" do
      assert :ok = run(%{"cell" => "lines", "where" => %{"fiscal_year" => "FY25"}})

      # FY25 was derived; FY24 was never claimed, which is the point of slicing
      assert totals() == [{"FY25", 5.0}]
    end

    test "explicit keys win over any filter" do
      assert :ok = run(%{"cell" => "lines", "keys" => ["a", "b"]})

      assert totals() == [{"FY24", 3.0}]
    end

    test "neither means the whole cell, and everything below it" do
      assert :ok = run(%{"cell" => "lines"})

      assert totals() == [{"FY24", 3.0}, {"FY25", 5.0}]
    end

    test "a slice matching nothing marks nothing" do
      assert :ok = run(%{"cell" => "lines", "where" => %{"fiscal_year" => "FY99"}})

      assert totals() == []
    end
  end

  describe "it is a reprocess, not a scan" do
    test "the reason reaches the trace, so it says WHY" do
      # `:reason` used to be written to the queue row, where a trace could read
      # it back later. There is no queue row now — a suspension records a
      # VERSION, not a free-text label — so the reason lives only in telemetry.
      #
      # That is a real narrowing: the reason is now observable while the job
      # runs and not afterwards. Asserted here so the loss is visible rather
      # than discovered.
      test_pid = self()

      :telemetry.attach(
        "reprocess-why",
        [:reactive_dag, :reprocess, :start],
        fn _e, _m, meta, _ -> send(test_pid, {:why, meta.cell, meta.reason}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach("reprocess-why") end)

      run(%{"cell" => "lines", "where" => %{"fiscal_year" => "FY25"}, "reason" => "prompt v3"})

      assert_received {:why, "lines", "prompt v3"}
    end

    test "the reason defaults to something honest" do
      test_pid = self()

      :telemetry.attach(
        "reprocess-reason",
        [:reactive_dag, :reprocess, :stop],
        fn _e, _m, meta, _ -> send(test_pid, {:reason, meta.reason}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach("reprocess-reason") end)

      run(%{"cell" => "lines", "keys" => ["a"]})

      assert_received {:reason, "reprocess"}
    end

    test "the cascade reaches the parents, not just the reprocessed cell" do
      # A reprocess originates AT the cell, and the cascade carries the change
      # to its parents — the hand-walk that used to be needed here is gone.
      test_pid = self()

      :telemetry.attach(
        "reprocess-parents",
        [:reactive_dag, :cascade, :step],
        fn _e, _m, meta, _ -> send(test_pid, {:step, meta.cell}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach("reprocess-parents") end)

      run(%{"cell" => "lines", "where" => %{"fiscal_year" => "FY25"}})

      assert_received {:step, "lines"}
      assert_received {:step, "totals"}
    end

    test "a cell absent from the plan is reported, not retried" do
      assert :ok = run(%{"cell" => "nope"})
    end
  end

  describe "telemetry" do
    test "reports claimed against changed, so a no-op is visible" do
      test_pid = self()

      :telemetry.attach(
        "reprocess-stop",
        [:reactive_dag, :reprocess, :stop],
        fn _e, m, meta, _ -> send(test_pid, {:stop, m, meta}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach("reprocess-stop") end)

      run(%{"cell" => "lines", "where" => %{"fiscal_year" => "FY25"}, "reason" => "prompt v3"})

      assert_received {:stop, m, meta}
      assert m.claimed == 1
      assert m.changed > 0
      assert meta.cell == "lines"
      assert meta.reason == "prompt v3"
    end

    test "a whole-cell claim reports no count, because there isn't one" do
      test_pid = self()

      :telemetry.attach(
        "reprocess-all",
        [:reactive_dag, :reprocess, :stop],
        fn _e, m, _meta, _ -> send(test_pid, {:stop, m}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach("reprocess-all") end)

      run(%{"cell" => "lines"})

      assert_received {:stop, m}
      refute m.claimed, "`*` is not a number of keys"
    end
  end

  describe "the same span shape as a scan" do
    test "start and stop both carry the job's args" do
      test_pid = self()

      :telemetry.attach_many(
        "reprocess-span",
        [[:reactive_dag, :reprocess, :start], [:reactive_dag, :reprocess, :stop]],
        fn e, _m, meta, _ -> send(test_pid, {:tel, List.last(e), meta}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach("reprocess-span") end)

      run(%{"cell" => "lines", "keys" => ["a"], "run_id" => "run-7"})

      assert_received {:tel, :start, start_meta}
      assert_received {:tel, :stop, stop_meta}

      assert start_meta.args["run_id"] == "run-7"
      assert stop_meta.args["run_id"] == "run-7"
    end
  end
end

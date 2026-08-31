defmodule ReactiveDag.TenantScanningTest do
  @moduledoc """
  Scanning under a tenanted plan: a poll marks its OWN tenant's frontier, and a
  tenant's cron entries are distinct jobs from another tenant's.

  The marking half is the one that fails silently. A tenanted scan that marked
  under `"*"` would report rows found — the crawl worked, the counts are real —
  and nothing would ever recompute, because that tenant's drain does not look
  there. "Found 40 documents, nothing changed" reads like a no-op scan rather
  than a lost one.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.{Cascade, Source}

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      allow_unregistered?(true)
    end
  end

  # The scanner reports whatever the test asked for via process state, so one
  # module can stand in for every tenant's upstream.
  defmodule Crawler do
    @behaviour ReactiveDag.Source

    @impl true
    def id, do: :crawler

    @impl true
    def poll(opts) do
      send(self(), {:polled, opts})
      {:ok, Process.get(:crawler_keys, [])}
    end
  end

  defmodule Docs do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
      private?(true)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        accept([:key])
      end
    end

    reactive do
      id(:docs)
      op(:source)
      leaf?(true)
      poll(Crawler, every: "0 12 * * *")
    end
  end

  defmodule FakeRepo do
    def start_link, do: Agent.start_link(fn -> [] end, name: __MODULE__)

    def rows, do: Agent.get(__MODULE__, & &1)

    def query!("INSERT INTO " <> _, params) do
      params
      |> Enum.chunk_every(7)
      |> Enum.each(fn [cell, tenant, key, _r, _t, _held, vid] ->
        Agent.update(__MODULE__, fn rows ->
          if {tenant, cell, key} in rows, do: rows, else: rows ++ [{tenant, cell, key}]
        end)
      end)

      %{rows: []}
    end

    def query!("SELECT DISTINCT cell_id" <> _, [tenant]) do
      ids =
        rows() |> Enum.filter(&(elem(&1, 0) == tenant)) |> Enum.map(&elem(&1, 1)) |> Enum.uniq()

      %{rows: Enum.map(ids, &[&1])}
    end

    def query!("DELETE FROM " <> _, [cell, tenant]) do
      keys =
        Agent.get_and_update(__MODULE__, fn rows ->
          {mine, rest} = Enum.split_with(rows, fn {t, c, _} -> t == tenant and c == cell end)
          {Enum.map(mine, &elem(&1, 2)), rest}
        end)

      %{rows: Enum.map(keys, &[&1, nil])}
    end

    def query!("SELECT COUNT" <> _, [tenant]) do
      %{rows: [[Enum.count(rows(), &(elem(&1, 0) == tenant))]]}
    end

    # the advisory lock — always granted; the KEY is what the test inspects.
    # `self()` is the test process: `with_lock/2` runs in the caller.
    def query!("SELECT pg_try_advisory_lock" <> _, [key]) do
      send(self(), {:lock, key})
      %{rows: [[true]]}
    end

    def query!("SELECT pg_advisory_unlock" <> _, _), do: %{rows: [[true]]}
  end

  setup do
    # A poll or a write now ENQUEUES a cascade rather than leaving a mark,
    # so without this the library reaches for Oban and these tests fail on
    # a missing instance rather than on anything they are about.
    ReactiveDag.Test.Pending.capture_enqueues()

    # `start_supervised!`, not `start_link`: the agent is registered under a global
    # name and linked to the test process, so it exits ASYNCHRONOUSLY when a test
    # ends — and the next test can call `start_link` before the name is released,
    # failing setup with `{:error, {:already_started, …}}`. ExUnit's supervisor waits
    # for the exit, so the name is free by the time the next test starts.
    start_supervised!(%{id: FakeRepo, start: {FakeRepo, :start_link, []}})
    prev = Application.get_env(:reactive_dag, :repo)
    Application.put_env(:reactive_dag, :repo, FakeRepo)
    on_exit(fn -> Application.put_env(:reactive_dag, :repo, prev) end)
    Ash.bulk_destroy!(Docs, :destroy, %{})
    :ok
  end

  defp plan(tenant), do: ReactiveDag.Node.graph([Docs], tenant: tenant)

  describe "a poll under a tenanted plan" do
    # A poll no longer leaves a mark in a tenanted table that a claim reads
    # back. It enqueues a cascade, and the tenant rides on THAT — so these
    # assert the same property one step earlier: the tenant the plan was built
    # with is the tenant the cascade is enqueued under. Getting this wrong is
    # the failure my notes call "empty is how tenancy fails" — the cascade runs
    # scoped to the wrong tenant, reads nothing, and reports success.
    test "enqueues the cascade in ITS OWN tenant" do
      Process.put(:crawler_keys, ["d1", "d2"])

      {:ok, _} = Source.refresh(plan("tenant_a"), "docs")

      assert [{"docs", keys, opts}] = ReactiveDag.Test.Pending.enqueued()
      assert Enum.sort(keys) == ["d1", "d2"]
      assert Keyword.get(opts, :tenant) == "tenant_a"
    end

    test "two tenants' findings do not mix" do
      Process.put(:crawler_keys, ["a1"])
      {:ok, _} = Source.refresh(plan("tenant_a"), "docs")

      Process.put(:crawler_keys, ["b1"])
      {:ok, _} = Source.refresh(plan("tenant_b"), "docs")

      assert [{"docs", ["a1"], a_opts}, {"docs", ["b1"], b_opts}] =
               ReactiveDag.Test.Pending.enqueued()

      assert Keyword.get(a_opts, :tenant) == "tenant_a"
      assert Keyword.get(b_opts, :tenant) == "tenant_b"
    end

    test "an untenanted plan still enqueues under `\"*\"`" do
      # NOTE, because the two enqueue paths disagree: a SCAN carries the plan's
      # tenant verbatim, and an untenanted plan's is the `"*"` sentinel — so
      # `"*"` arrives here. A `dirties_on` WRITE takes its tenant off the
      # changeset instead, where an untenanted write simply has none, and
      # enqueues `nil` (see dirties_on_test). Both mean "not scoped to one
      # tenant" and `CascadeWorker` handles each, but they are not the same
      # value, and a reader comparing the two tests should know it is not a typo.
      Process.put(:crawler_keys, ["d1"])

      {:ok, _} = Source.refresh(ReactiveDag.Node.graph([Docs]), "docs")

      assert [{"docs", ["d1"], opts}] = ReactiveDag.Test.Pending.enqueued()
      assert Keyword.get(opts, :tenant) == "*"
    end
  end

  describe "crontab" do
    test "a tenanted plan's entries carry the tenant" do
      assert [{"0 12 * * *", ReactiveDag.ScanWorker, args: args}] =
               Source.crontab(plan("tenant_a"))

      assert args == %{"sweep" => true, "tenant" => "tenant_a"}
    end

    test "two tenants produce DISTINCT args, so Oban does not dedupe them" do
      # `ScanWorker`'s uniqueness is on `:args`. Identical args would collapse
      # two tenants' sweeps into one job — one tenant simply never crawls.
      [{_, _, args: a}] = Source.crontab(plan("tenant_a"))
      [{_, _, args: b}] = Source.crontab(plan("tenant_b"))

      refute a == b
    end

    test "an untenanted plan's entries are unchanged — no `tenant` key at all" do
      assert [{"0 12 * * *", ReactiveDag.ScanWorker, args: %{"sweep" => true}}] =
               Source.crontab(ReactiveDag.Node.graph([Docs]))
    end

    test "`per_cell: true` carries the tenant too" do
      assert [{_, _, args: args}] = Source.crontab(plan("tenant_a"), ReactiveDag.ScanWorker, per_cell: true)

      assert args["tenant"] == "tenant_a"
      assert args["cell"] == "docs"
    end

    test "a host's own `args:` still merge, and win on conflict" do
      [{_, _, args: args}] =
        Source.crontab(plan("tenant_a"), ReactiveDag.ScanWorker, args: %{"run" => "r1"})

      assert args["tenant"] == "tenant_a"
      assert args["run"] == "r1"
    end
  end

  describe "the sweep lock" do
    test "an untenanted plan locks on the dirty table, as before" do
      ReactiveDag.Lock.with_lock(fn -> :ok end)
      assert_receive {:lock, unscoped}

      ReactiveDag.Lock.with_lock(fn -> :ok end, scope: nil)
      assert_receive {:lock, explicit_nil}

      assert unscoped == explicit_nil,
             "an explicit nil scope must not move the lock — Keyword.get's default " <>
               "only applies when the key is ABSENT"
    end

    test "two tenants take DIFFERENT locks, so they do not queue behind each other" do
      ReactiveDag.Lock.with_lock(fn -> :ok end, scope: {:tenant, "tenant_a"})
      assert_receive {:lock, a}

      ReactiveDag.Lock.with_lock(fn -> :ok end, scope: {:tenant, "tenant_b"})
      assert_receive {:lock, b}

      ReactiveDag.Lock.with_lock(fn -> :ok end)
      assert_receive {:lock, global}

      assert a != b
      assert a != global
    end
  end
end

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

  alias ReactiveDag.{Frontier, Source}

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
      |> Enum.each(fn [cell, tenant, key, _r, _t, _p, _held] ->
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
    {:ok, _} = FakeRepo.start_link()
    prev = Application.get_env(:reactive_dag, :repo)
    Application.put_env(:reactive_dag, :repo, FakeRepo)
    on_exit(fn -> Application.put_env(:reactive_dag, :repo, prev) end)
    Ash.bulk_destroy!(Docs, :destroy, %{})
    :ok
  end

  defp plan(tenant), do: ReactiveDag.Node.graph([Docs], tenant: tenant)

  describe "a poll under a tenanted plan" do
    test "marks the frontier in ITS OWN tenant" do
      Process.put(:crawler_keys, ["d1", "d2"])

      {:ok, _} = Source.refresh(plan("tenant_a"), "docs")

      # visible to A...
      assert Frontier.next_cell(%{"docs" => 0}, [], tenant: "tenant_a") == "docs"
      assert Enum.sort(Frontier.claim("docs", tenant: "tenant_a")) == ["d1", "d2"]

      # ...and to nobody else
      assert Frontier.empty?(tenant: "tenant_b")
      assert Frontier.empty?()
    end

    test "two tenants' findings do not mix" do
      Process.put(:crawler_keys, ["a1"])
      {:ok, _} = Source.refresh(plan("tenant_a"), "docs")

      Process.put(:crawler_keys, ["b1"])
      {:ok, _} = Source.refresh(plan("tenant_b"), "docs")

      assert Frontier.claim("docs", tenant: "tenant_a") == ["a1"]
      assert Frontier.claim("docs", tenant: "tenant_b") == ["b1"]
    end

    test "an untenanted plan still marks `\"*\"`" do
      Process.put(:crawler_keys, ["d1"])

      {:ok, _} = Source.refresh(ReactiveDag.Node.graph([Docs]), "docs")

      assert Frontier.claim("docs") == ["d1"]
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
      Frontier.with_lock(fn -> :ok end)
      assert_receive {:lock, unscoped}

      Frontier.with_lock(fn -> :ok end, scope: nil)
      assert_receive {:lock, explicit_nil}

      assert unscoped == explicit_nil,
             "an explicit nil scope must not move the lock — Keyword.get's default " <>
               "only applies when the key is ABSENT"
    end

    test "two tenants take DIFFERENT locks, so they do not queue behind each other" do
      Frontier.with_lock(fn -> :ok end, scope: {:tenant, "tenant_a"})
      assert_receive {:lock, a}

      Frontier.with_lock(fn -> :ok end, scope: {:tenant, "tenant_b"})
      assert_receive {:lock, b}

      Frontier.with_lock(fn -> :ok end)
      assert_receive {:lock, global}

      assert a != b
      assert a != global
    end
  end
end

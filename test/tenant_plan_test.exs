defmodule ReactiveDag.TenantPlanTest do
  @moduledoc """
  `graph(resources, tenant: t)` builds the SAME topology as its own plan, and a
  drain over it touches only that tenant's work.

  The point of the design: cell ids are as authored in every tenant's plan —
  there is one `lines` and one `rollup`, not `lines.tenant_a` — because the
  tenant lives on the plan and reaches the frontier from there. So a resource is
  authored once and nothing about the DSL changes.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.{Drain, Frontier, Plan}

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
      private?(true)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :fund, :string, public?: true
      attribute :amount, :float, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        accept([:key, :fund, :amount])
      end
    end

    reactive do
      id(:lines)
      op(:source)
      leaf?(true)
    end
  end

  defmodule Rollup do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
      private?(true)
    end

    attributes do
      attribute :fund, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :total, :float, public?: true
      # a constant, so the node above can group every row into one bucket
      attribute :scope, :string, default: "all", public?: true
    end

    identities do
      identity :by_fund, [:fund], pre_check_with: ReactiveDag.TenantPlanTest.Domain
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        upsert_identity(:by_fund)
        accept([:fund, :total, :scope])
      end
    end

    reactive do
      id(:rollup)
      op(:fold)
      payload_key(:fund)
      reduce(over: :lines, group_by: :fund, into: [sum: [amount: :total]])
    end
  end

  # `key_rule :all` makes propagation TO this node take the `:all` branch — a
  # different call site from the keyed one, and it needs the tenant just as much.
  defmodule Total do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
      private?(true)
    end

    attributes do
      attribute :scope, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :total, :float, public?: true
    end

    identities do
      identity :by_scope, [:scope], pre_check_with: ReactiveDag.TenantPlanTest.Domain
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        upsert_identity(:by_scope)
        accept([:scope, :total])
      end
    end

    reactive do
      id(:total)
      op(:fold)
      key_rule(:all)
      payload_key(:scope)
      reduce(over: :rollup, group_by: :scope, into: [sum: [total: :total]])
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
          # ON CONFLICT (tenant, cell_id, key) DO NOTHING — the FIRST mark wins,
          # so a re-mark must not replace the prior the first one captured.
          if Enum.any?(rows, fn {t, c, k, _} -> {t, c, k} == {tenant, cell, key} end),
            do: rows,
            else: rows ++ [{tenant, cell, key, vid}]
        end)
      end)

      %{rows: []}
    end

    def query!("SELECT DISTINCT cell_id" <> _, [tenant]) do
      ids =
        rows()
        |> Enum.filter(&(elem(&1, 0) == tenant))
        |> Enum.map(&elem(&1, 1))
        |> Enum.uniq()

      %{rows: Enum.map(ids, &[&1])}
    end

    def query!("DELETE FROM " <> _, [cell, tenant]) do
      taken =
        Agent.get_and_update(__MODULE__, fn rows ->
          {mine, rest} = Enum.split_with(rows, fn {t, c, _, _} -> t == tenant and c == cell end)
          {mine, rest}
        end)

      %{rows: Enum.map(taken, fn {_t, _c, k, p} -> [k, p] end)}
    end

    def query!("SELECT COUNT" <> _, [tenant]) do
      %{rows: [[Enum.count(rows(), &(elem(&1, 0) == tenant))]]}
    end
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

    on_exit(fn ->
      Application.put_env(:reactive_dag, :repo, prev_repo)
      Application.put_env(:reactive_dag, :coordination_writer, prev_writer)
    end)

    for r <- [Lines, Rollup, Total], do: Ash.bulk_destroy!(r, :destroy, %{})
    :ok
  end

  defp plan(tenant), do: ReactiveDag.Node.graph([Lines, Rollup, Total], tenant: tenant)

  describe "the plan" do
    test "carries its tenant, and its cell ids are as authored" do
      p = plan("tenant_a")

      assert p.tenant == "tenant_a"
      assert Enum.sort(Map.keys(p.cells)) == ["lines", "rollup", "total"]
    end

    test "every tenant's plan is the same topology" do
      a = plan("tenant_a")
      b = plan("tenant_b")

      assert Map.keys(a.cells) == Map.keys(b.cells)
      assert a.depths == b.depths
      assert a.parents == b.parents
      refute a.tenant == b.tenant
    end

    test "an untenanted plan is `\"*\"`, so existing hosts are unchanged" do
      assert ReactiveDag.Node.graph([Lines, Rollup, Total]).tenant == "*"
    end

    test "`frontier_opts/1` is the one place the tenant becomes an option" do
      assert Plan.frontier_opts(plan("t")) == [tenant: "t"]
    end
  end

  describe "a drain over a tenant plan" do
    test "claims only its own tenant's keys" do
      Lines
      |> Ash.Changeset.for_create(:upsert, %{key: "l1", fund: "gf", amount: 10.0})
      |> Ash.create!()

      # both tenants dirty the same cell id
      Frontier.mark_dirty("lines", ["l1"], "seed", tenant: "tenant_a")
      Frontier.mark_dirty("lines", ["l1"], "seed", tenant: "tenant_b")

      {:ok, report} = Drain.run(plan("tenant_a"))

      assert Enum.any?(report.steps, &(&1.cell == "lines"))

      # B's mark survived A's whole drain
      refute Frontier.empty?(tenant: "tenant_b")
      assert Frontier.empty?(tenant: "tenant_a")
    end

    test "propagation marks the parent in the SAME tenant" do
      Lines
      |> Ash.Changeset.for_create(:upsert, %{key: "l1", fund: "gf", amount: 10.0})
      |> Ash.create!()

      Frontier.mark_dirty("lines", ["l1"], "seed", tenant: "tenant_a")
      {:ok, report} = Drain.run(plan("tenant_a"))

      # the cascade reached the rollup, so the propagated mark was readable by
      # this tenant's next pass — under the wrong tenant it would have been
      # marked and never seen
      assert Enum.any?(report.steps, &(&1.cell == "rollup")),
             "the fold ran, so its mark landed in this tenant"

      assert (Rollup |> Ash.read!() |> hd()).total == 10.0
    end

    test "an `:all` propagation marks the parent in the SAME tenant" do
      # The whole-cell propagation branch — a separate call site from the keyed
      # one. If it drops the tenant, `total` is marked under `"*"`, this drain
      # never sees it, and the cascade stops one cell short.
      Lines
      |> Ash.Changeset.for_create(:upsert, %{key: "l1", fund: "gf", amount: 10.0})
      |> Ash.create!()

      # A WHOLE-CELL claim ("*") is what makes propagation take the `:all`
      # branch — see `Drain`: a whole recompute can delete keys, so per-key
      # propagation would strand a vanished one.
      Frontier.mark_dirty("lines", ["*"], "seed", tenant: "tenant_a")
      {:ok, report} = Drain.run(plan("tenant_a"))

      assert Enum.any?(report.steps, &(&1.cell == "rollup")),
             "the `:all` propagation landed in this tenant"

      assert Enum.any?(report.steps, &(&1.cell == "total")),
             "...and carried on down the cascade"

      assert (Total |> Ash.read!() |> hd()).total == 10.0
      assert Frontier.empty?(tenant: "tenant_a")
    end

    test "a tenant with nothing dirty drains to empty without touching another's" do
      Frontier.mark_dirty("lines", ["l1"], "seed", tenant: "tenant_b")

      {:ok, report} = Drain.run(plan("tenant_a"))

      assert report.steps == []
      refute Frontier.empty?(tenant: "tenant_b")
    end
  end
end

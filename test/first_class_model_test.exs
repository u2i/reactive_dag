defmodule ReactiveDag.FirstClassModelTest do
  @moduledoc """
  A representative MULTI-node first-class model: several guarantees that
  MATERIALIZE their computed result sets as typed Ash data (the store-per-node
  shape), assembled via ReactiveDag.Node.graph, driven bottom-up in depth order,
  each writing its rows. Proves the whole shape end-to-end and MEASURES the cost
  the product blow-up imposes.

  Scope note: this drives the graph in DEPTH ORDER directly (not the DB-backed
  ReactiveDag.Drain loop — the library has no test repo; the drain is proven by
  the host suites). What's exercised here is what THIS question is about:
  per-node materialization, cross-node reads, the product grid cost, Ash reads.
  Data layer = ETS (in-memory, writable).
  """
  use ExUnit.Case, async: false
  require Ash.Query

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false
    resources do
      allow_unregistered?(true)
    end
  end

  # coordination writes go to a recording fake (the substrate still runs).
  defmodule Writer do
    @behaviour ReactiveDag.CoordinationWriter
    @impl true
    def put(_c, _k, _o), do: :ok
    @impl true
    def delete(_c, _k), do: :ok
  end

  # ── leaves: source-fed, seeded by the test (no materialization of their own) ──
  for {mod, id} <- [{People, :people}, {Systems, :systems}, {Coverage, :coverage},
                    {Entitled, :entitled}, {Observed, :observed}, {Events, :events}] do
    defmodule mod do
      use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Simple, extensions: [ReactiveDag.Node]
      @id id
      reactive do
        id @id
        op :leaf
        leaf? true
      end
    end
  end

  # ── each first-class guarantee's OWN typed result table (the materialized data) ─
  defmodule RowsReconcile do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Ets
    ets do private?(true) end
    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :status, :atom, public?: true
      attribute :drift, :string, public?: true
    end
    identities do identity :by_key, [:key], pre_check_with: ReactiveDag.FirstClassModelTest.Domain end
    actions do
      defaults [:read, :destroy]
      create :upsert do upsert?(true); upsert_identity(:by_key); accept([:key, :status, :drift]) end
    end
  end

  defmodule RowsProduct do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Ets
    ets do private?(true) end
    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :status, :atom, public?: true
    end
    identities do identity :by_key, [:key], pre_check_with: ReactiveDag.FirstClassModelTest.Domain end
    actions do
      defaults [:read, :destroy]
      create :upsert do upsert?(true); upsert_identity(:by_key); accept([:key, :status]) end
    end
  end

  defmodule RowsFold do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Ets
    ets do private?(true) end
    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :status, :atom, public?: true
      attribute :subject, :string, public?: true
    end
    identities do identity :by_key, [:key], pre_check_with: ReactiveDag.FirstClassModelTest.Domain end
    actions do
      defaults [:read, :destroy]
      create :upsert do upsert?(true); upsert_identity(:by_key); accept([:key, :status, :subject]) end
    end
  end

  # ── the FIRST-CLASS guarantees: Node (reactive) + materialize into their table ─

  # 1. reconcile: entitled ⟗ observed → RowsReconcile
  defmodule StoreEncrypted do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Simple, extensions: [ReactiveDag.Node]
    reactive do
      id :"g:store_encrypted"; op :reconcile; key_rule :all
      depends_on [:entitled, :observed]
      compute ReactiveDag.FirstClassModelTest.ReconcileOp
    end
  end

  # 2. product: people × systems minus coverage → RowsProduct (the grid-cost case)
  defmodule StrongAuth do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Simple, extensions: [ReactiveDag.Node]
    reactive do
      id :"g:strong_auth"; op :product; key_rule :all
      depends_on [:people, :systems, :coverage]
      compute ReactiveDag.FirstClassModelTest.ProductOp
    end
  end

  # 3. fold: events → latest-per-subject → RowsFold
  defmodule ActiveEmployment do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Simple, extensions: [ReactiveDag.Node]
    reactive do
      id :"g:active_employment"; op :fold; key_rule :all
      depends_on [:events]
      compute ReactiveDag.FirstClassModelTest.FoldOp
    end
  end

  # ── the recompute ops: materialize result rows into the typed tables ──────────
  defmodule ReconcileOp do
    @behaviour ReactiveDag.Op
    @impl true
    def recompute(cell, _keys) do
      e = Process.get(:entitled, MapSet.new()); o = Process.get(:observed, MapSet.new())
      for k <- MapSet.union(e, o) do
        {ein, oin} = {MapSet.member?(e, k), MapSet.member?(o, k)}
        status = if ein and oin, do: :present, else: :failing
        drift = cond do ein and not oin -> "unobserved"; oin and not ein -> "rogue"; true -> nil end
        {:ok, _} = RowsReconcile |> Ash.Changeset.for_create(:upsert, %{key: k, status: status, drift: drift}) |> Ash.create()
        ReactiveDag.Op.put(cell, k)
        k
      end
      |> then(&{:ok, &1})
    end
  end

  defmodule ProductOp do
    @behaviour ReactiveDag.Op
    @impl true
    def recompute(cell, _keys) do
      a = Process.get(:people, []); b = Process.get(:systems, [])
      cov = Process.get(:coverage, MapSet.new())
      # MATERIALIZE THE WHOLE GRID (the cost): a row per (person,system) pair.
      for pa <- a, pb <- b do
        k = "#{pa}|#{pb}"
        status = if MapSet.member?(cov, k), do: :present, else: :failing
        {:ok, _} = RowsProduct |> Ash.Changeset.for_create(:upsert, %{key: k, status: status}) |> Ash.create()
        ReactiveDag.Op.put(cell, k)
        k
      end
      |> then(&{:ok, &1})
    end
  end

  defmodule FoldOp do
    @behaviour ReactiveDag.Op
    @impl true
    def recompute(cell, _keys) do
      # events: [{subject, status}] latest wins; present iff latest == "started".
      Process.get(:events, [])
      |> Enum.group_by(&elem(&1, 0))
      |> Enum.map(fn {subject, evs} ->
        latest = evs |> List.last() |> elem(1)
        status = if latest == "started", do: :present, else: :failing
        {:ok, _} = RowsFold |> Ash.Changeset.for_create(:upsert, %{key: subject, subject: subject, status: status}) |> Ash.create()
        ReactiveDag.Op.put(cell, subject)
        subject
      end)
      |> then(&{:ok, &1})
    end
  end

  @nodes [People, Systems, Coverage, Entitled, Observed, Events,
          StoreEncrypted, StrongAuth, ActiveEmployment]

  setup do
    prev = Application.get_env(:reactive_dag, :coordination_writer)
    Application.put_env(:reactive_dag, :coordination_writer, Writer)
    on_exit(fn -> Application.put_env(:reactive_dag, :coordination_writer, prev) end)
    :ok
  end

  # drive non-leaf nodes in depth order (the drain's job, done inline — see moduledoc).
  defp run do
    plan = ReactiveDag.Node.graph(@nodes)
    plan.depths
    |> Map.keys()
    |> Enum.sort_by(&plan.depths[&1])
    |> Enum.each(fn id ->
      cell = plan.cells[id]
      unless cell.leaf?, do: {:ok, _} = cell.meta.compute.recompute(cell, ["*"])
    end)

    plan
  end

  test "the whole model assembles: 6 leaves + 3 first-class guarantees, valid plan" do
    plan = ReactiveDag.Node.graph(@nodes)
    assert plan.depths["g:store_encrypted"] > plan.depths["entitled"]
    assert plan.depths["g:strong_auth"] > plan.depths["people"]
    assert plan.depths["g:active_employment"] > plan.depths["events"]
  end

  test "each guarantee materializes its result set as queryable typed data" do
    Process.put(:entitled, MapSet.new(["alice", "bob"]))
    Process.put(:observed, MapSet.new(["alice"]))
    Process.put(:people, ["p1", "p2"])
    Process.put(:systems, ["s1", "s2", "s3"])
    Process.put(:coverage, MapSet.new(["p1|s1", "p1|s2", "p1|s3", "p2|s1"]))
    Process.put(:events, [{"e1", "started"}, {"e2", "started"}, {"e2", "ended"}])

    run()

    # reconcile detail — drill-down, no recompute:
    {:ok, bob} = Ash.get(RowsReconcile, "bob")
    assert bob.status == :failing and bob.drift == "unobserved"

    # product — the WHOLE grid is materialized (2×3 = 6 rows), even the greens:
    prod = Ash.read!(RowsProduct)
    assert length(prod) == 6
    failing = RowsProduct |> Ash.Query.filter(status == :failing) |> Ash.read!()
    assert Enum.map(failing, & &1.key) |> Enum.sort() == ["p2|s2", "p2|s3"]

    # fold — latest-per-subject: e1 started (present), e2 ended (failing):
    assert {:ok, %{status: :present}} = Ash.get(RowsFold, "e1")
    assert {:ok, %{status: :failing}} = Ash.get(RowsFold, "e2")
  end

  test "MEASURE the product cost: storage grows with the POPULATION, not the findings" do
    # fully covered → verdict-only would store 0 failing rows; first-class stores
    # the ENTIRE grid regardless.
    Process.put(:people, Enum.map(1..20, &"p#{&1}"))
    Process.put(:systems, Enum.map(1..15, &"s#{&1}"))
    all_pairs = for p <- Process.get(:people), s <- Process.get(:systems), do: "#{p}|#{s}"
    Process.put(:coverage, MapSet.new(all_pairs))   # 100% covered → 0 findings

    run()

    stored = Ash.read!(RowsProduct) |> length()
    failing = RowsProduct |> Ash.Query.filter(status == :failing) |> Ash.read!() |> length()
    # THE TRADE, quantified: 300 rows stored, 0 of them findings.
    assert stored == 20 * 15
    assert failing == 0
  end
end

defmodule ReactiveDag.RollupNodeTest do
  @moduledoc """
  The storage/read-layer reconciliation: per-guarantee FIRST-CLASS tables upstream
  (each node materializes its own detail), PLUS a ROLLUP node — itself a node in
  the DAG, `depends_on` many guarantee tables — whose recompute reads those N
  tables and materializes ONE consolidated verdict table the read layer queries.

  So "first-class per node" (normalized source of truth, rich detail) and "one
  uniform read" coexist: the rollup is just another pipeline node, using the same
  depends_on/recompute/materialize machinery. The read layer reads the rollup's
  ONE table; the per-guarantee tables are the upstream detail. No special verdict
  spine, no ad-hoc source_table joins — it's all nodes.
  """
  use ExUnit.Case, async: false
  require Ash.Query

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false
    resources do allow_unregistered?(true) end
  end

  defmodule Writer do
    @behaviour ReactiveDag.CoordinationWriter
    @impl true
    def put(_c, _k, _o), do: :ok
    @impl true
    def delete(_c, _k), do: :ok
  end

  # ── two first-class guarantee detail tables (the per-node source of truth) ────
  defmodule RowsA do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Ets
    ets do private?(true) end
    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :status, :atom, public?: true
      attribute :drift, :string, public?: true
    end
    identities do identity :by_key, [:key], pre_check_with: ReactiveDag.RollupNodeTest.Domain end
    actions do
      defaults [:read, :destroy]
      create :upsert do upsert?(true); upsert_identity(:by_key); accept([:key, :status, :drift]) end
    end
  end

  defmodule RowsB do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Ets
    ets do private?(true) end
    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :status, :atom, public?: true
      attribute :drift, :string, public?: true
    end
    identities do identity :by_key, [:key], pre_check_with: ReactiveDag.RollupNodeTest.Domain end
    actions do
      defaults [:read, :destroy]
      create :upsert do upsert?(true); upsert_identity(:by_key); accept([:key, :status, :drift]) end
    end
  end

  # ── the ONE consolidated verdict table the read layer queries (rollup output) ─
  defmodule Verdicts do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Ets
    ets do private?(true) end
    attributes do
      attribute :cell_key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :guarantee, :string, public?: true
      attribute :key, :string, public?: true
      attribute :status, :atom, public?: true
    end
    identities do identity :by_ck, [:cell_key], pre_check_with: ReactiveDag.RollupNodeTest.Domain end
    actions do
      defaults [:read, :destroy]
      create :upsert do upsert?(true); upsert_identity(:by_ck); accept([:cell_key, :guarantee, :key, :status]) end
    end
  end

  # ── the two first-class guarantee NODES (each materializes its own table) ─────
  defmodule Entitled do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Simple, extensions: [ReactiveDag.Node]
    reactive do id(:entitled); op(:leaf); leaf?(true) end
  end
  defmodule Observed do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Simple, extensions: [ReactiveDag.Node]
    reactive do id(:observed); op(:leaf); leaf?(true) end
  end

  defmodule GA do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Simple, extensions: [ReactiveDag.Node]
    reactive do
      id :"g:a"; op :reconcile; key_rule :all
      depends_on [:entitled]
      compute ReactiveDag.RollupNodeTest.OpA
    end
  end
  defmodule GB do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Simple, extensions: [ReactiveDag.Node]
    reactive do
      id :"g:b"; op :reconcile; key_rule :all
      depends_on [:observed]
      compute ReactiveDag.RollupNodeTest.OpB
    end
  end

  # ── THE ROLLUP NODE: depends_on the two guarantees, reads their tables, ───────
  # materializes the ONE consolidated Verdicts table. Just another pipeline node.
  defmodule VerdictRollup do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Simple, extensions: [ReactiveDag.Node]
    reactive do
      id :"rollup:verdicts"; op :union; key_rule :all
      depends_on [:"g:a", :"g:b"]
      compute ReactiveDag.RollupNodeTest.RollupOp
    end
  end

  # ── the ops ───────────────────────────────────────────────────────────────────
  defmodule OpA do
    @behaviour ReactiveDag.Op
    @impl true
    def recompute(cell, _), do: ReactiveDag.RollupNodeTest.mat(ReactiveDag.RollupNodeTest.RowsA, Process.get(:a, %{}), cell)
  end
  defmodule OpB do
    @behaviour ReactiveDag.Op
    @impl true
    def recompute(cell, _), do: ReactiveDag.RollupNodeTest.mat(ReactiveDag.RollupNodeTest.RowsB, Process.get(:b, %{}), cell)
  end

  # materialize a guarantee's {key => status} into its own first-class table.
  def mat(table, keyed, cell) do
    for {k, status} <- keyed do
      drift = if status == :failing, do: "gap", else: nil
      {:ok, _} = table |> Ash.Changeset.for_create(:upsert, %{key: k, status: status, drift: drift}) |> Ash.create()
      ReactiveDag.Op.put(cell, k)
      k
    end
    |> then(&{:ok, &1})
  end

  defmodule RollupOp do
    @behaviour ReactiveDag.Op
    @impl true
    def recompute(cell, _) do
      # READ the N upstream first-class tables → consolidate into ONE table.
      sources = [{"g:a", RowsA}, {"g:b", RowsB}]

      changed =
        Enum.flat_map(sources, fn {gname, table} ->
          for row <- Ash.read!(table) do
            ck = "#{gname}|#{row.key}"
            {:ok, _} =
              Verdicts
              |> Ash.Changeset.for_create(:upsert, %{cell_key: ck, guarantee: gname, key: row.key, status: row.status})
              |> Ash.create()
            ReactiveDag.Op.put(cell, ck)
            ck
          end
        end)

      {:ok, changed}
    end
  end

  @nodes [Entitled, Observed, GA, GB, VerdictRollup]

  setup do
    prev = Application.get_env(:reactive_dag, :coordination_writer)
    Application.put_env(:reactive_dag, :coordination_writer, Writer)
    on_exit(fn -> Application.put_env(:reactive_dag, :coordination_writer, prev) end)
    :ok
  end

  defp run do
    plan = ReactiveDag.Node.graph(@nodes)
    plan.depths |> Map.keys() |> Enum.sort_by(&plan.depths[&1])
    |> Enum.each(fn id ->
      c = plan.cells[id]
      unless c.leaf?, do: {:ok, _} = c.meta.compute.recompute(c, ["*"])
    end)
    plan
  end

  test "the rollup node depends on the guarantee nodes (deeper in the DAG)" do
    plan = ReactiveDag.Node.graph(@nodes)
    assert plan.depths["rollup:verdicts"] > plan.depths["g:a"]
    assert plan.depths["rollup:verdicts"] > plan.depths["g:b"]
    assert "rollup:verdicts" in plan.parents["g:a"]
    assert "rollup:verdicts" in plan.parents["g:b"]
  end

  test "per-guarantee tables hold DETAIL; the rollup consolidates into ONE read table" do
    Process.put(:a, %{"alice" => :present, "bob" => :failing})
    Process.put(:b, %{"sys1" => :failing})

    run()

    # per-node first-class detail (drill-down, upstream source of truth):
    assert {:ok, %{status: :failing, drift: "gap"}} = Ash.get(RowsA, "bob")

    # the ONE consolidated table the read layer queries — all guarantees, uniform:
    all = Ash.read!(Verdicts)
    assert length(all) == 3   # a:alice, a:bob, b:sys1

    # the generic "verdict" query the LiveViews want — over ONE table, produced
    # reactively by the rollup node (not a special spine):
    failing = Verdicts |> Ash.Query.filter(status == :failing) |> Ash.read!()
    assert Enum.map(failing, & &1.cell_key) |> Enum.sort() == ["g:a|bob", "g:b|sys1"]

    # per-guarantee rollup slice (what for_cell would read):
    a_rows = Verdicts |> Ash.Query.filter(guarantee == "g:a") |> Ash.read!()
    assert length(a_rows) == 2
  end
end

defmodule ReactiveDag.IdentityKeysTest do
  @moduledoc """
  Ash-identity keys: a payload node declares a COMPOSITE primary key and drops
  the `:key` column entirely — the row upserts by its identity (Ash's own
  machinery) and the cell key is the identity's serialization in primary-key
  order. `group_by` pairs (`parent_column: :child_field`) are the
  relational-join spelling of the DAG edge. Nothing here invents key plumbing:
  the resource's identity IS the key.
  """
  use ReactiveDag.PostgresCase

  alias ReactiveDag.Fixtures.IdentityKeysGraph
  alias ReactiveDag.Fixtures.IdentityKeysGraph.{Lines, Rollups}

  alias ReactiveDag.Node.Recompute

  setup_all do
    migrate!(IdentityKeysGraph.Domain)
    :ok
  end

  setup do
    pid = Ecto.Adapters.SQL.Sandbox.start_owner!(ReactiveDag.TestRepo, shared: true)
    on_exit(fn -> Ecto.Adapters.SQL.Sandbox.stop_owner(pid) end)

    truncate(IdentityKeysGraph.tables())

    for {k, fund, fy, amount} <- [
          {"l1", "gf", "2025", 10.0},
          {"l2", "gf", "2025", 5.0},
          {"l3", "water", "2026", 7.0}
        ] do
      Lines
      |> Ash.Changeset.for_create(:create, %{key: k, fund_code: fund, fy: fy, amount: amount})
      |> Ash.create!()
    end

    :ok
  end

  defp plan, do: ReactiveDag.Node.graph([Lines, Rollups])

  test "payload_key derives from the primary key; composite PKs are identity-keyed" do
    plan = plan()

    # tier 1: the LEAF's payload key derived from its PK, undeclared
    assert plan.cells["lines"].meta.payload_key == :key

    # tier 2: the rollup is identity-keyed — no payload_key, identity stamped
    rollup = plan.cells["rollups"]
    assert rollup.meta[:payload_key] == nil
    assert rollup.meta.identity_fields == [:fund, :fy]
  end

  test "no :key column anywhere: rows upsert by identity, cell keys ARE the identity" do
    plan = plan()

    {:ok, changed} = Recompute.recompute(plan.cells["rollups"], ["*"])
    assert Enum.sort(changed) == ["gf|2025", "water|2026"]

    rows = Rollups |> Ash.read!() |> Map.new(&{{&1.fund, &1.fy}, &1})
    assert %{total: 15.0, n: 2} = rows[{"gf", "2025"}]
    assert rows[{"water", "2026"}].total == 7.0

    # change detection still per-identity: a second run reports nothing
    {:ok, []} = Recompute.recompute(plan.cells["rollups"], ["*"])

    # ...and a real change reports exactly its identity's key
    Lines |> Ash.get!("l2") |> Ash.Changeset.for_update(:revise, %{amount: 6.0}) |> Ash.update!()
    {:ok, changed} = Recompute.recompute(plan.cells["rollups"], ["*"])
    assert changed == ["gf|2025"]
  end

  test ":group claims serialize in PRIMARY-KEY order through the pair mapping" do
    plan = plan()
    {:ok, _} = Recompute.recompute(plan.cells["rollups"], ["*"])

    # the lookup evaluates lines.fund_code/fy and serializes as fund|fy
    assert ReactiveDag.Node.KeyRule.rule(plan.cells["rollups"], "lines", ["l3"]) ==
             {:keys, ["water|2026"]}
  end

  test "an identity field the row can't produce is a compile-time error" do
    alias ReactiveDag.Node.Verifiers.VerifyReactive

    defmodule Uncovered do
      use Ash.Resource,
        domain: ReactiveDag.Fixtures.IdentityKeysGraph.Domain,
        data_layer: Ash.DataLayer.Ets,
        extensions: [ReactiveDag.Node]

      ets do
        private?(true)
      end

      attributes do
        attribute :fund, :string, primary_key?: true, allow_nil?: false, public?: true
        attribute :fy, :string, primary_key?: true, allow_nil?: false, public?: true
        attribute :total, :float, public?: true
      end

      actions do
        defaults [:read]
      end

      reactive do
        id :uncovered
        # groups only by fund — :fy could never be written, so the upsert
        # could not identify its row
        reduce(
          over: :lines,
          group_by: [fund: :fund_code],
          into: [sum: [amount: :total]]
        )
      end
    end

    assert {:error, %Spark.Error.DslError{message: msg}} =
             VerifyReactive.verify(Uncovered.spark_dsl_config())

    assert msg =~ ":fy"
    assert msg =~ "identify its row"
  end

  test "a COMPOSITE unit scopes the read per column (a superset, never the whole table)" do
    plan = plan()
    cell = plan.cells["rollups"]

    # the unit lowered to the pair form, and assembly proved both columns are
    # plain strings — the two facts composite scoping needs
    assert cell.meta.reduce.group_by == [fund: :fund_code, fy: :fy]

    assert cell.meta.over_source.group_key_plan ==
             [{:attr, :fund_code, true}, {:attr, :fy, true}]

    {:ok, _} = Recompute.recompute(cell, ["*"])

    # ONE claim inverts exactly: fund_code in ["gf"] AND fy in ["2025"]
    assert {:all_of, [{:attr, :fund_code, ["gf"]}, {:attr, :fy, ["2025"]}]} =
             scope_for(cell, ["gf|2025"])

    # SEVERAL claims are a cross-product superset — sound (closed over unit
    # boundaries), and still far tighter than reading every line
    assert {:all_of, [{:attr, :fund_code, funds}, {:attr, :fy, fys}]} =
             scope_for(cell, ["gf|2025", "water|2026"])

    assert Enum.sort(funds) == ["gf", "water"]
    assert Enum.sort(fys) == ["2025", "2026"]

    # and the fold still lands on exactly the claimed units
    {:ok, changed} = Recompute.recompute(cell, ["gf|2025"])
    assert changed == []
  end

  # the library's auto-scope for a claim set (what Read filters the query by)
  defp scope_for(cell, keys), do: Recompute.auto_scope(cell, keys)
end

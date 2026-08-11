defmodule ReactiveDag.IdentityKeysTest do
  @moduledoc """
  Ash-identity keys: a payload node declares a COMPOSITE primary key and drops
  the `:key` column entirely — the row upserts by its identity (Ash's own
  machinery) and the cell key is the identity's serialization in primary-key
  order. `group_by` pairs (`parent_column: :child_field`) are the
  relational-join spelling of the DAG edge. Nothing here invents key plumbing:
  the resource's identity IS the key.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.Node.Recompute

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
      # deliberately NOT named like the rollup's column — the pair maps it
      attribute :fund_code, :string, public?: true
      attribute :fy, :string, public?: true
      attribute :amount, :float, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :create do
        accept([:key, :fund_code, :fy, :amount])
      end

      update :revise do
        accept([:amount])
      end
    end

    reactive do
      id(:lines)
      op(:source)
      leaf?(true)
    end
  end

  # IDENTITY-KEYED: composite PK, no :key column, no payload_key — the row IS
  # its identity; cell keys are "fund|fy".
  defmodule Rollups do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
      private?(true)
    end

    attributes do
      attribute :fund, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :fy, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :total, :float, public?: true
      attribute :n, :integer, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        accept([:fund, :fy, :total, :n])
      end
    end

    reactive do
      id(:rollups)
      op(:fold)

      # RELATIONAL-JOIN spelling: rollups.fund = lines.fund_code, rollups.fy =
      # lines.fy — parent column on the left, child field on the right.
      reduce over: :lines,
             group_by: [fund: :fund_code, fy: :fy],
             key_rule: :group,
             into: [sum: [amount: :total], count: :n]
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
    prev = Application.get_env(:reactive_dag, :coordination_writer)
    Application.put_env(:reactive_dag, :coordination_writer, NullWriter)
    on_exit(fn -> Application.put_env(:reactive_dag, :coordination_writer, prev) end)

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
        domain: ReactiveDag.IdentityKeysTest.Domain,
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
        id(:uncovered)
        # groups only by fund — :fy could never be written, so the upsert
        # could not identify its row
        reduce over: :lines,
               group_by: [fund: :fund_code],
               into: [sum: [amount: :total]]
      end
    end

    assert {:error, %Spark.Error.DslError{message: msg}} =
             VerifyReactive.verify(Uncovered.spark_dsl_config())

    assert msg =~ ":fy"
    assert msg =~ "identify its row"
  end
end

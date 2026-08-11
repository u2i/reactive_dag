defmodule ReactiveDag.DeclarativeReduceTest do
  @moduledoc """
  The Ash-first `reduce`: no `read:` (the library reads the over node's
  resource, dirty-key scoped), attribute `group_by:`, declarative `into:`
  folds, derived keys + `key_prefix`. The fn forms remain per-slot escape
  hatches — covered by the existing combinator tests; this file covers the
  declarative ladder and its assembly/verifier errors.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.Node.Recompute

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      allow_unregistered?(true)
    end
  end

  # the input node: a leaf whose resource holds the raw lines.
  defmodule FiscalLines do
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
      attribute :fy, :integer, public?: true
      attribute :amount, :float, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :create do
        accept([:key, :fund, :fy, :amount])
      end

      read :only_gf do
        filter expr(fund == "gf")
      end
    end

    reactive do
      id(:fiscal_lines)
      op(:source)
      leaf?(true)
    end
  end

  # THE ASH-FIRST NODE: no read:, attribute group_by, declarative fold, no key:.
  defmodule BudgetRollups do
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
      attribute :fy, :integer, public?: true
      attribute :total, :float, public?: true
      attribute :n, :integer, public?: true
    end

    identities do
      identity :by_key, [:key], pre_check_with: ReactiveDag.DeclarativeReduceTest.Domain
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        upsert_identity(:by_key)
        accept([:key, :fund, :fy, :total, :n])
      end
    end

    reactive do
      id(:budget_rollups)
      op(:fold)
      key_rule(:all)

      reduce over: :fiscal_lines,
             group_by: [:fund, :fy],
             into: [sum: [amount: :total], count: :n]
    end
  end

  # named read action + key_prefix + single-attr group + fn into over the
  # declarative group term (proves the tuple contract holds across forms).
  defmodule GfTotals do
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
      attribute :total, :float, public?: true
    end

    identities do
      identity :by_key, [:key], pre_check_with: ReactiveDag.DeclarativeReduceTest.Domain
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        upsert_identity(:by_key)
        accept([:key, :fund, :total])
      end
    end

    reactive do
      id(:gf_totals)
      op(:fold)
      key_rule(:all)

      reduce over: :fiscal_lines,
             read: :only_gf,
             group_by: :fund,
             key_prefix: "roll",
             into: [sum: [amount: :total]]
    end
  end

  # same-grain enrichment (:identity): the auto-scoping proof — its dirty keys
  # ARE the over node's keys, so the default read must filter to them.
  defmodule LineMarks do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
      private?(true)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :n, :integer, public?: true
      attribute :amount_seen, :float, public?: true
    end

    identities do
      identity :by_key, [:key], pre_check_with: ReactiveDag.DeclarativeReduceTest.Domain
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        upsert_identity(:by_key)
        accept([:key, :n, :amount_seen])
      end
    end

    reactive do
      id(:line_marks)
      op(:map)

      reduce over: :fiscal_lines,
             group_by: :key,
             into: [count: :n, first: [amount: :amount_seen]]
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
          {"l1", "gf", 2025, 10.0},
          {"l2", "gf", 2025, 5.0},
          {"l3", "gf", 2026, 2.0},
          {"l4", "water", 2025, 7.0},
          # nil amount: excluded from numeric folds (SQL semantics)
          {"l5", "water", 2025, nil}
        ] do
      FiscalLines
      |> Ash.Changeset.for_create(:create, %{key: k, fund: fund, fy: fy, amount: amount})
      |> Ash.create!()
    end

    :ok
  end

  defp plan, do: ReactiveDag.Node.graph([FiscalLines, BudgetRollups, GfTotals, LineMarks])

  test "no read:, attribute group_by, declarative folds — the library does everything" do
    cell = plan().cells["budget_rollups"]
    assert cell.meta.over_source.resource == FiscalLines
    assert cell.meta.over_source.payload_key == :key

    {:ok, changed} = Recompute.recompute(cell, ["*"])
    assert Enum.sort(changed) == ["gf|2025", "gf|2026", "water|2025"]

    rows = BudgetRollups |> Ash.read!() |> Map.new(&{&1.key, &1})
    # group columns present in the row; sums exclude nil sources
    assert %{fund: "gf", fy: 2025, total: 15.0, n: 2} = rows["gf|2025"]
    assert %{fund: "water", fy: 2025, total: 7.0, n: 2} = rows["water|2025"]
    assert rows["gf|2026"].total == 2.0
  end

  test "a second identical recompute reports no changes (payload-loop change detection)" do
    cell = plan().cells["budget_rollups"]
    {:ok, _} = Recompute.recompute(cell, ["*"])
    {:ok, second} = Recompute.recompute(cell, ["*"])
    assert second == []
  end

  test "read: names a :read ACTION on the over resource; key_prefix namespaces the default key" do
    cell = plan().cells["gf_totals"]
    assert cell.meta.over_source.read_action == :only_gf

    {:ok, changed} = Recompute.recompute(cell, ["*"])
    # only gf rows were read (the water fund never appears), keys are prefixed
    assert Enum.sort(changed) == ["roll|gf"]

    [row] = Ash.read!(GfTotals)
    assert row.key == "roll|gf"
    assert row.total == 17.0
  end

  test "the default read AUTO-SCOPES to the claimed dirty keys (:identity same-grain)" do
    cell = plan().cells["line_marks"]

    {:ok, changed} = Recompute.recompute(cell, ["l1"])
    assert changed == ["l1"]

    # ONLY l1 was read and written — the other lines never entered the fold
    keys = LineMarks |> Ash.read!() |> Enum.map(& &1.key)
    assert keys == ["l1"]
    [row] = Ash.read!(LineMarks)
    assert %{n: 1, amount_seen: 10.0} = row

    # whole-cell reads everything
    {:ok, all} = Recompute.recompute(cell, ["*"])
    assert "l5" in all
    assert length(Ash.read!(LineMarks)) == 5
  end

  test "a fn status: over a declarative group_by pattern-matches the group TUPLE" do
    # mixed form: declarative read + group, fn verdict — the tuple contract
    # holds, and the key derives exactly as a payload row's would.
    defmodule MixedRollup do
      use Ash.Resource,
        domain: ReactiveDag.DeclarativeReduceTest.Domain,
        data_layer: Ash.DataLayer.Simple,
        extensions: [ReactiveDag.Node]

      reactive do
        id(:mixed_rollup)
        op(:fold)
        key_rule(:all)
        verdict?(true)

        reduce over: :fiscal_lines,
               group_by: [:fund, :fy],
               status: fn {_fund, _fy}, lines ->
                 if length(lines) > 1, do: "present", else: "thin"
               end
      end
    end

    plan = ReactiveDag.Node.graph([FiscalLines, MixedRollup])
    {:ok, changed} = Recompute.recompute(plan.cells["mixed_rollup"], ["*"])
    assert Enum.sort(changed) == ["gf|2025", "gf|2026", "water|2025"]
  end

  describe "assembly errors (graph/2 is where the over resource is known)" do
    test "a declarative read over a VERDICT node raises with guidance" do
      defmodule SomeVerdict do
        use Ash.Resource,
          domain: ReactiveDag.DeclarativeReduceTest.Domain,
          data_layer: Ash.DataLayer.Simple,
          extensions: [ReactiveDag.Node]

        reactive do
          id(:some_verdict)
          op(:reconcile)
          key_rule(:all)
          verdict?(true)

          reduce over: :fiscal_lines,
                 group_by: :key,
                 status: fn _k, _ -> "present" end
        end
      end

      defmodule OverVerdict do
        use Ash.Resource,
          domain: ReactiveDag.DeclarativeReduceTest.Domain,
          data_layer: Ash.DataLayer.Simple,
          extensions: [ReactiveDag.Node]

        reactive do
          id(:over_verdict)
          op(:fold)
          key_rule(:all)
          verdict?(true)

          reduce over: :some_verdict,
                 group_by: :key,
                 status: fn _k, _ -> "present" end
        end
      end

      assert_raise ArgumentError, ~r/VERDICT node.*no rows to read/s, fn ->
        ReactiveDag.Node.graph([FiscalLines, SomeVerdict, OverVerdict])
      end
    end

    test "a named read action that doesn't exist raises, listing the real ones" do
      defmodule BadAction do
        use Ash.Resource,
          domain: ReactiveDag.DeclarativeReduceTest.Domain,
          data_layer: Ash.DataLayer.Simple,
          extensions: [ReactiveDag.Node]

        reactive do
          id(:bad_action)
          op(:fold)
          key_rule(:all)
          verdict?(true)

          reduce over: :fiscal_lines,
                 read: :no_such_action,
                 group_by: :key,
                 status: fn _k, _ -> "present" end
        end
      end

      assert_raise ArgumentError, ~r/no_such_action.*Available.*only_gf/s, fn ->
        ReactiveDag.Node.graph([FiscalLines, BadAction])
      end
    end

    test "a declarative attribute the over resource lacks raises, listing its attributes" do
      defmodule BadAttr do
        use Ash.Resource,
          domain: ReactiveDag.DeclarativeReduceTest.Domain,
          data_layer: Ash.DataLayer.Simple,
          extensions: [ReactiveDag.Node]

        reactive do
          id(:bad_attr)
          op(:fold)
          key_rule(:all)
          verdict?(true)

          reduce over: :fiscal_lines,
                 group_by: :no_such_attr,
                 status: fn _k, _ -> "present" end
        end
      end

      assert_raise ArgumentError, ~r/no_such_attr.*neither as an attribute nor a calculation.*:fund/s, fn ->
        ReactiveDag.Node.graph([FiscalLines, BadAttr])
      end
    end

    test "to_cell/1 (no assembly) + a declarative read raises the assemble-via-graph/2 error" do
      cell = ReactiveDag.Node.to_cell(BudgetRollups)

      assert_raise ArgumentError, ~r/resolved at graph assembly.*graph\/2/s, fn ->
        Recompute.recompute(cell, ["*"])
      end
    end
  end

  describe "the compile-time verifier" do
    alias ReactiveDag.Node.Verifiers.VerifyReactive

    # Runtime-defined modules report verifier errors as async diagnostics
    # rather than raising (see attestation_record_test for the precedent), so
    # assert the verifier's verdict directly.
    test "declarative into without a declarative group_by is rejected" do
      defmodule IntoNeedsGroup do
        use Ash.Resource,
          domain: ReactiveDag.DeclarativeReduceTest.Domain,
          data_layer: Ash.DataLayer.Simple,
          extensions: [ReactiveDag.Node]

        reactive do
          id(:into_needs_group)
          reduce over: :fiscal_lines,
                 group_by: &Function.identity/1,
                 into: [count: :n],
                 upsert: fn _, _ -> true end
        end
      end

      assert {:error, %Spark.Error.DslError{message: msg}} =
               VerifyReactive.verify(IntoNeedsGroup.spark_dsl_config())

      assert msg =~ "declarative `group_by:`"
    end

    test "a verdict node declares status:, not into: — both directions checked" do
      defmodule VerdictDeclarativeInto do
        use Ash.Resource,
          domain: ReactiveDag.DeclarativeReduceTest.Domain,
          data_layer: Ash.DataLayer.Simple,
          extensions: [ReactiveDag.Node]

        reactive do
          id(:verdict_decl_into)
          verdict?(true)
          reduce over: :fiscal_lines, group_by: :fund, into: [count: :n]
        end
      end

      assert {:error, %Spark.Error.DslError{message: msg}} =
               VerifyReactive.verify(VerdictDeclarativeInto.spark_dsl_config())

      assert msg =~ "status:"

      defmodule StatusOnPayload do
        use Ash.Resource,
          domain: ReactiveDag.DeclarativeReduceTest.Domain,
          data_layer: Ash.DataLayer.Simple,
          extensions: [ReactiveDag.Node]

        reactive do
          id(:status_on_payload)
          reduce over: :fiscal_lines, group_by: :fund, status: fn _, _ -> "present" end
        end
      end

      assert {:error, %Spark.Error.DslError{message: msg}} =
               VerifyReactive.verify(StatusOnPayload.spark_dsl_config())

      assert msg =~ "verdict? true"
    end

    test "an unknown fold kind is rejected, naming the supported set" do
      defmodule BadKind do
        use Ash.Resource,
          domain: ReactiveDag.DeclarativeReduceTest.Domain,
          data_layer: Ash.DataLayer.Simple,
          extensions: [ReactiveDag.Node]

        reactive do
          id(:bad_kind)
          reduce over: :fiscal_lines,
                 group_by: :fund,
                 into: [median: :m],
                 upsert: fn _, _ -> true end
        end
      end

      assert {:error, %Spark.Error.DslError{message: msg}} =
               VerifyReactive.verify(BadKind.spark_dsl_config())

      assert msg =~ "median"
      assert msg =~ "supported"
    end

    test "a fold dest that isn't a payload attribute is rejected (payload-loop nodes)" do
      defmodule BadDest do
        use Ash.Resource,
          domain: ReactiveDag.DeclarativeReduceTest.Domain,
          data_layer: Ash.DataLayer.Ets,
          extensions: [ReactiveDag.Node]

        ets do
          private?(true)
        end

        attributes do
          attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
          attribute :fund, :string, public?: true
        end

        actions do
          defaults [:read]
        end

        reactive do
          id(:bad_dest)
          reduce over: :fiscal_lines, group_by: :fund, into: [sum: [amount: :no_such_column]]
        end
      end

      assert {:error, %Spark.Error.DslError{message: msg}} =
               VerifyReactive.verify(BadDest.spark_dsl_config())

      assert msg =~ "no_such_column"
    end

    test "key_prefix alongside an explicit key fn is rejected" do
      defmodule PrefixAndFn do
        use Ash.Resource,
          domain: ReactiveDag.DeclarativeReduceTest.Domain,
          data_layer: Ash.DataLayer.Simple,
          extensions: [ReactiveDag.Node]

        reactive do
          id(:prefix_and_fn)

          reduce over: :fiscal_lines,
                 group_by: :fund,
                 key: &to_string/1,
                 key_prefix: "x",
                 into: fn f, _ -> %{key: f} end,
                 upsert: fn _, _ -> true end
        end
      end

      assert {:error, %Spark.Error.DslError{message: msg}} =
               VerifyReactive.verify(PrefixAndFn.spark_dsl_config())

      assert msg =~ "key_prefix"
    end
  end
end

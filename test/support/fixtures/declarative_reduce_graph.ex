defmodule ReactiveDag.Fixtures.DeclarativeReduceGraph do
  @moduledoc """
  Declarative `reduce` — the fold stated as data rather than a function.

  `BudgetRollups` folds whole, `GfTotals` folds a filtered slice, and
  `LineMarks` is the leaf both read.
  """

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      resource ReactiveDag.Fixtures.DeclarativeReduceGraph.FiscalLines
      resource ReactiveDag.Fixtures.DeclarativeReduceGraph.BudgetRollups
      resource ReactiveDag.Fixtures.DeclarativeReduceGraph.GfTotals
      resource ReactiveDag.Fixtures.DeclarativeReduceGraph.LineMarks
      resource ReactiveDag.Fixtures.DeclarativeReduceGraph.MixedRollup
      resource ReactiveDag.Fixtures.DeclarativeReduceGraph.OverVerdict
    end
  end

  # the input node: a leaf whose resource holds the raw lines.
  defmodule FiscalLines do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.DeclarativeReduceGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_dr_fiscal_lines"
      repo(ReactiveDag.TestRepo)
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
      domain: ReactiveDag.Fixtures.DeclarativeReduceGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_dr_budget_rollups"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :fund, :string, public?: true
      attribute :fy, :integer, public?: true
      attribute :total, :float, public?: true
      attribute :n, :integer, public?: true
    end

    identities do
      identity :by_key, [:key], pre_check_with: ReactiveDag.Fixtures.DeclarativeReduceGraph.Domain
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

      reduce(
        over: :fiscal_lines,
        group_by: [:fund, :fy],
        into: [sum: [amount: :total], count: :n]
      )
    end
  end

  # named read action + key_prefix + single-attr group + fn into over the
  # declarative group term (proves the tuple contract holds across forms).
  defmodule GfTotals do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.DeclarativeReduceGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_dr_gf_totals"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :fund, :string, public?: true
      attribute :total, :float, public?: true
    end

    identities do
      identity :by_key, [:key], pre_check_with: ReactiveDag.Fixtures.DeclarativeReduceGraph.Domain
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

      reduce(
        over: :fiscal_lines,
        read: :only_gf,
        group_by: :fund,
        key_prefix: "roll",
        into: [sum: [amount: :total]]
      )
    end
  end

  # same-grain enrichment (:identity): the auto-scoping proof — its dirty keys
  # ARE the over node's keys, so the default read must filter to them.
  defmodule LineMarks do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.DeclarativeReduceGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_dr_line_marks"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :n, :integer, public?: true
      attribute :amount_seen, :float, public?: true
    end

    identities do
      identity :by_key, [:key], pre_check_with: ReactiveDag.Fixtures.DeclarativeReduceGraph.Domain
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

      reduce(
        over: :fiscal_lines,
        group_by: :key,
        into: [count: :n, first: [amount: :amount_seen]]
      )
    end
  end

  defmodule MixedRollup do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.DeclarativeReduceGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_dr_mixed_rollup"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :status, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        accept([:key, :status])
      end
    end

    reactive do
      id(:mixed_rollup)
      op(:fold)
      key_rule(:all)

      reduce(
        over: :fiscal_lines,
        group_by: [:fund, :fy],
        into: fn {_fund, _fy}, lines ->
          %{status: if(length(lines) > 1, do: "present", else: "thin")}
        end
      )
    end
  end

  defmodule OverVerdict do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.DeclarativeReduceGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_dr_over_verdict"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :status, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        upsert_identity(:by_key)
        accept([:key, :status])
      end
    end

    identities do
      identity :by_key, [:key]
    end

    reactive do
      id(:over_verdict)
      op(:fold)
      key_rule(:all)

      reduce(
        over: :some_verdict,
        group_by: :key,
        into: fn _k, _ -> %{status: "present"} end
      )
    end
  end

  def tables,
    do: [
      "fx_dr_over_verdict",
      "fx_dr_mixed_rollup",
      "fx_dr_fiscal_lines",
      "fx_dr_budget_rollups",
      "fx_dr_gf_totals",
      "fx_dr_line_marks"
    ]
end

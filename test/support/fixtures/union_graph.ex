defmodule ReactiveDag.Fixtures.UnionGraph do
  @moduledoc """
  A UNION node: several inputs of the same shape, read as one.

  `AllVerdicts` unions its sources; `FundBalance` is one of them.
  """

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      resource ReactiveDag.Fixtures.UnionGraph.CategoryHealth
      resource ReactiveDag.Fixtures.UnionGraph.FundBalance
      resource ReactiveDag.Fixtures.UnionGraph.AllVerdicts
      resource ReactiveDag.Fixtures.UnionGraph.WithHeadroom
      resource ReactiveDag.Fixtures.UnionGraph.Tableless
    end
  end

  # two verdict-shaped inputs — ordinary nodes, each with a :status column
  defmodule CategoryHealth do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.UnionGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_un_category_health"
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
      id :category_health
      leaf? true
    end
  end

  defmodule FundBalance do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.UnionGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_un_fund_balance"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :status, :string, public?: true
      # a column the tuple never had room for — the union can project it
      attribute :headroom, :float, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        accept([:key, :status, :headroom])
      end
    end

    reactive do
      id :fund_balance
      leaf? true
    end
  end

  defmodule AllVerdicts do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.UnionGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_un_all_verdicts"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      # composite PK → identity-keyed, so cell keys are "<input>|<key>"
      attribute :check, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :subject, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :status, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        accept([:check, :subject, :status])
      end
    end

    reactive do
      id :all_verdicts

      union(
        from: [:category_health, :fund_balance],
        into: [check: :cell, subject: :key, status: :status]
      )
    end
  end

  defmodule WithHeadroom do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.UnionGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_un_with_headroom"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :check, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :subject, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :status, :string, public?: true
      attribute :headroom, :float, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        accept([:check, :subject, :status, :headroom])
      end
    end

    reactive do
      id :with_headroom

      union(
        from: [:fund_balance],
        into: [check: :cell, subject: :key, status: :status, headroom: :headroom]
      )
    end
  end

  defmodule Tableless do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.UnionGraph.Domain,
      data_layer: Ash.DataLayer.Simple,
      extensions: [ReactiveDag.Node]

    reactive do
      id :tableless
      leaf? true
    end
  end

  def tables, do: ["fx_un_with_headroom", "fx_un_category_health", "fx_un_fund_balance", "fx_un_all_verdicts"]
end

defmodule ReactiveDag.Fixtures.AggregateGraph do
  @moduledoc """
  An AGGREGATE node, whose `over:` is a RELATIONSHIP on its own resource
  rather than another cell — so it adds no graph edge.

  `PlantMonth` aggregates the `Reading` rows related to it.
  """

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      resource ReactiveDag.Fixtures.AggregateGraph.FundLine
      resource ReactiveDag.Fixtures.AggregateGraph.PlantMonth
      resource ReactiveDag.Fixtures.AggregateGraph.Reading
      resource ReactiveDag.Fixtures.AggregateGraph.FundFy
    end
  end

  # a fine-grained input for the identity-keyed aggregate below
  defmodule FundLine do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.AggregateGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_ag_fund_line"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :fund, :string, public?: true
      attribute :amount, :float, public?: true
    end

    actions do
      defaults [:read]

      create :create do
        accept([:key, :fund, :amount])
      end
    end

    reactive do
      id(:fund_lines)
      op(:source)
      leaf?(true)
    end
  end

  # the GROUP resource: one row per (plant, month). It has_many raw readings.
  defmodule PlantMonth do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.AggregateGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_ag_plant_month"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :avg_flow, :float, public?: true
      attribute :day_count, :integer, public?: true
    end

    relationships do
      has_many :readings, ReactiveDag.Fixtures.AggregateGraph.Reading do
        source_attribute :key
        destination_attribute :pm_key
      end
    end

    identities do
      identity :by_key, [:key], pre_check_with: ReactiveDag.Fixtures.AggregateGraph.Domain
    end

    actions do
      defaults [:read, :destroy]

      create :seed do
        accept([:key])
      end

      create :upsert do
        upsert?(true)
        upsert_identity(:by_key)
        accept([:key, :avg_flow, :day_count])
      end
    end

    # PURE-ASH aggregate: the datastore avgs/counts the :readings relationship.
    reactive do
      op(:fold)
      key_rule(:all)

      aggregate(
        over: :readings,
        avg: [flow: :avg_flow],
        count: :day_count
      )
    end
  end

  defmodule Reading do
    use Ash.Resource, domain: ReactiveDag.Fixtures.AggregateGraph.Domain, data_layer: AshPostgres.DataLayer

    postgres do
      table "fx_ag_reading"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :id, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :pm_key, :string, public?: true
      attribute :flow, :float, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :create do
        accept([:id, :pm_key, :flow])
      end
    end
  end

  defmodule FundFy do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.AggregateGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_ag_fund_fy"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :fund, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :fy, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :total, :float, public?: true
      attribute :n, :integer, public?: true
    end

    relationships do
      has_many :lines, ReactiveDag.Fixtures.AggregateGraph.FundLine do
        source_attribute :fund
        destination_attribute :fund
      end
    end

    actions do
      defaults [:read]

      create :seed do
        accept([:fund, :fy])
      end

      create :upsert do
        upsert?(true)
        accept([:fund, :fy, :total, :n])
      end
    end

    reactive do
      id(:fund_fy)
      op(:fold)
      key_rule(:all)
      aggregate(over: :lines, sum: [amount: :total], count: :n)
    end
  end

  def tables, do: ["fx_ag_fund_fy", "fx_ag_fund_line", "fx_ag_plant_month", "fx_ag_reading"]
end

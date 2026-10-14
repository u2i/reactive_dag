defmodule ReactiveDag.Fixtures.RowsAggregateGraph do
  @moduledoc """
  Reading a cell's rows through an AGGREGATE rather than row by row.
  """

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      resource ReactiveDag.Fixtures.RowsAggregateGraph.Docs
      resource ReactiveDag.Fixtures.RowsAggregateGraph.Totals
      resource ReactiveDag.Fixtures.RowsAggregateGraph.Rollup
      resource ReactiveDag.Fixtures.RowsAggregateGraph.Elsewhere
    end
  end

  defmodule Docs do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.RowsAggregateGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_ra_docs"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :status, :string, public?: true
      attribute :payload, :map, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        accept([:key, :status, :payload])
      end
    end

    reactive do
      id :docs
      leaf? true
    end
  end

  # a rollup: no status column at all
  defmodule Totals do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.RowsAggregateGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_ra_totals"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :total, :float, public?: true
    end

    actions do
      defaults [:read, :destroy]
      create :upsert, upsert?: true, accept: [:key, :total]
    end

    reactive do
      id :totals
      leaf? true
    end
  end

  # composite PK — its cell key is a "|"-join built in the BEAM, which is the
  # one thing that cannot be pushed down
  defmodule Rollup do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.RowsAggregateGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_ra_rollup"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :fund, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :fy, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :status, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]
      create :upsert, upsert?: true, accept: [:fund, :fy, :status]
    end

    reactive do
      id :rollup
      leaf? true
    end
  end

  defmodule Elsewhere do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.RowsAggregateGraph.Domain,
      data_layer: Ash.DataLayer.Simple,
      extensions: [ReactiveDag.Node]

    reactive do
      id :elsewhere
      leaf? true
    end
  end

  def tables, do: ["fx_ra_docs", "fx_ra_totals", "fx_ra_rollup"]
end

defmodule ReactiveDag.Fixtures.RowsPageGraph do
  @moduledoc """
  Paging a cell's rows — a window over the rows a node owns, with a stable
  order so pages do not overlap or skip.
  """

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      resource ReactiveDag.Fixtures.RowsPageGraph.Docs
      resource ReactiveDag.Fixtures.RowsPageGraph.Totals
    end
  end

  defmodule Docs do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.RowsPageGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_rp_docs"
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
      id(:docs)
      leaf?(true)
      row_key([:key])
    end
  end

  defmodule Totals do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.RowsPageGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_rp_totals"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :total, :integer, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        accept([:key, :total])
      end
    end

    reactive do
      id(:totals)
      leaf?(true)
      row_key([:key])
    end
  end

  def tables, do: ["fx_rp_docs", "fx_rp_totals"]
end

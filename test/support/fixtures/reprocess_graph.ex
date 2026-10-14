defmodule ReactiveDag.Fixtures.ReprocessGraph do
  @moduledoc """
  Reprocessing: re-running a node's computation on demand, per key.
  """

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      resource ReactiveDag.Fixtures.ReprocessGraph.Lines
      resource ReactiveDag.Fixtures.ReprocessGraph.Totals
    end
  end

  defmodule Lines do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.ReprocessGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_rw_lines"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute(:key, :string, primary_key?: true, allow_nil?: false, public?: true)
      attribute(:fiscal_year, :string, public?: true)
      attribute(:amount, :float, public?: true)
    end

    actions do
      defaults([:read, :destroy])
      create(:upsert, upsert?: true, accept: [:key, :fiscal_year, :amount])
    end

    reactive do
      id :lines
      leaf? true
      slice :fiscal_year, values: ["FY24", "FY25"]
    end
  end

  defmodule Totals do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.ReprocessGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_rw_totals"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute(:key, :string, primary_key?: true, allow_nil?: false, public?: true)
      attribute(:total, :float, public?: true)
    end

    actions do
      defaults([:read, :destroy])
      create(:upsert, upsert?: true, accept: [:key, :total])
    end

    reactive do
      id :totals
      recompute_by :fiscal_year, to: :lines, from: :fiscal_year

      reduce(
        group_by: :fiscal_year,
        into: fn fy, rows -> %{key: fy, total: rows |> Enum.map(& &1.amount) |> Enum.sum()} end
      )
    end
  end

  def tables, do: ["fx_rw_lines", "fx_rw_totals"]
end

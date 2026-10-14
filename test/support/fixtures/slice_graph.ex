defmodule ReactiveDag.Fixtures.SliceGraph do
  @moduledoc """
  SLICES — a node reading part of another node's rows.
  """

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      resource ReactiveDag.Fixtures.SliceGraph.Lines
      resource ReactiveDag.Fixtures.SliceGraph.TenantedLines
      resource ReactiveDag.Fixtures.SliceGraph.Plain
      resource ReactiveDag.Fixtures.SliceGraph.Docs
      resource ReactiveDag.Fixtures.SliceGraph.Tableless
      resource ReactiveDag.Fixtures.SliceGraph.NotASource
      resource ReactiveDag.Fixtures.SliceGraph.Standing
    end
  end

  defmodule Lines do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.SliceGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_sl_lines"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :fund, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :fiscal_year, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :total, :float, public?: true
      attribute :version, :integer, public?: true
    end

    actions do
      defaults [:read, :destroy]
      create :upsert, upsert?: true, accept: [:fund, :fiscal_year, :total, :version]
    end

    reactive do
      id(:lines)
      leaf?(true)

      # the year is not the recompute unit and not the whole key — it is what a
      # person asks about
      slice(:fiscal_year, values: {ReactiveDag.SliceTest, :available_years, []})
      slice(:version, label: "processed by version")
    end
  end

  # A tenanted node, because that is what a real host has and the untenanted
  # `Lines` above cannot show: `keys_where/3` has to be scopeable, or a menu
  # counting "how many keys would this reprocess claim?" gets a raise.
  defmodule TenantedLines do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.SliceGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_sl_tenanted_lines"
      repo(ReactiveDag.TestRepo)
    end

    multitenancy do
      strategy :attribute
      attribute :org_id
    end

    attributes do
      uuid_primary_key(:id)
      attribute :org_id, :string, public?: true
      attribute :fund, :string, allow_nil?: false, public?: true
      attribute :fiscal_year, :string, allow_nil?: false, public?: true
    end

    actions do
      defaults [:read, :destroy]
      create :upsert, upsert?: true, accept: [:org_id, :fund, :fiscal_year]
    end

    reactive do
      id(:tenanted_lines)
      leaf?(true)
      row_key([:org_id, :fund, :fiscal_year])
      slice(:fiscal_year)
    end
  end

  defmodule Plain do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.SliceGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_sl_plain"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
    end

    actions do
      defaults [:read, :destroy]
      create :upsert, upsert?: true, accept: [:key]
    end

    reactive do
      id(:plain)
      leaf?(true)
    end
  end

  # a SOURCE whose upstream is addressable by the same dimension its rows are
  # sliced by — and which spells it differently, which is the case `poll_as:`
  # exists for. The crawler takes `fiscal:`; the column is `fiscal_year`.
  defmodule Docs do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.SliceGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_sl_docs"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :fiscal_year, :string, public?: true
      attribute :board, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]
      create :upsert, upsert?: true, accept: [:key, :fiscal_year, :board]
    end

    reactive do
      id(:docs)
      poll(ReactiveDag.Fixtures.SliceGraph.Crawler)

      slice(:fiscal_year, values: ["FY24/25", "FY25/26"], poll_as: :fiscal)
      # no `poll_as:` — the scanner spells this one the same as the column
      slice(:board)
    end
  end

  defmodule Tableless do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.SliceGraph.Domain,
      data_layer: Ash.DataLayer.Simple,
      extensions: [ReactiveDag.Node]

    reactive do
      id(:tableless)
      leaf?(true)
      slice(:fiscal_year)
    end
  end

  defmodule NotASource do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.SliceGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_sl_not_a_source"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :fiscal_year, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]
      create :upsert, upsert?: true, accept: [:key, :fiscal_year]
    end

    reactive do
      id(:not_a_source)
      leaf?(true)
      slice(:fiscal_year, poll_as: :fiscal)
    end
  end

  defmodule Standing do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.SliceGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_sl_standing"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :fiscal_year, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]
      create :upsert, upsert?: true, accept: [:key, :fiscal_year]
    end

    reactive do
      id(:standing)
      # a STANDING arg, so the test proves a selection composes with the
      # declaration rather than replacing it
      poll(ReactiveDag.Fixtures.SliceGraph.Recorder, args: [recent: true])
      slice(:fiscal_year, values: ["FY25/26"], poll_as: :fiscal)
    end
  end

  defmodule Recorder do
    @moduledoc """
    The poll `Standing` declares.

    Reports to whoever polls — `send(self(), …)` — so a test still observes
    every call without the fixture knowing about that test.
    """
    @behaviour ReactiveDag.Source

    @impl true
    def poll(opts) do
      send(self(), {:polled_with, opts})
      {:ok, %{changed: []}}
    end
  end

  def tables,
    do: ["fx_sl_standing", "fx_sl_not_a_source", "fx_sl_lines", "fx_sl_tenanted_lines", "fx_sl_plain", "fx_sl_docs"]
end

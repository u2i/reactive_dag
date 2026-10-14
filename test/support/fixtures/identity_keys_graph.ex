defmodule ReactiveDag.Fixtures.IdentityKeysGraph do
  @moduledoc """
  Composite primary keys, where the cell key IS the identity serialisation.
  """

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      resource ReactiveDag.Fixtures.IdentityKeysGraph.Lines
      resource ReactiveDag.Fixtures.IdentityKeysGraph.Rollups
    end
  end

  defmodule Lines do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.IdentityKeysGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_ik_lines"
      repo(ReactiveDag.TestRepo)
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
      id :lines
      op :source
      leaf? true
    end
  end

  # IDENTITY-KEYED: composite PK, no :key column, no payload_key — the row IS
  # its identity; cell keys are "fund|fy".
  defmodule Rollups do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.IdentityKeysGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_ik_rollups"
      repo(ReactiveDag.TestRepo)
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
      id :rollups
      op :fold

      # THE COMPOSITE UNIT, stated once: the grain IS the grouping, so there
      # is no `group_by:` to restate. Reads as
      # "rollups.fund = lines.fund_code AND rollups.fy = lines.fy".
      recompute_by [fund: :fund_code, fy: :fy], to: :lines

      reduce into: [sum: [amount: :total], count: :n]
    end
  end

  def tables, do: ["fx_ik_lines", "fx_ik_rollups"]
end

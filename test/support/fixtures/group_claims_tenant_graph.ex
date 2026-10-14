defmodule ReactiveDag.Fixtures.GroupClaimsTenantGraph do
  @moduledoc """
  A `:group` claim that must carry its tenant.

  Deriving the unit is not enough — the claim names work in a tenant, and a
  tenant dropped on the way makes the drain read where the rows are not.
  """

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      resource ReactiveDag.Fixtures.GroupClaimsTenantGraph.Lines
      resource ReactiveDag.Fixtures.GroupClaimsTenantGraph.Rollup
    end
  end

  defmodule Lines do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.GroupClaimsTenantGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_gct_lines"
      repo(ReactiveDag.TestRepo)
    end

    multitenancy do
      strategy :attribute
      attribute :org_id
    end

    attributes do
      uuid_primary_key(:id)
      attribute :org_id, :string, public?: true
      attribute :line_key, :string, allow_nil?: false, public?: true
      attribute :fund, :string, public?: true
      attribute :amount, :integer, public?: true
    end

    identities do
      identity :by_org_line, [:org_id, :line_key], pre_check_with: Domain
    end

    actions do
      defaults [:read, :destroy]

      create :upsert,
        upsert?: true,
        upsert_identity: :by_org_line,
        accept: [:org_id, :line_key, :fund, :amount]
    end

    reactive do
      id(:lines)
      leaf?(true)
      payload_key(:line_key)
      row_key([:org_id, :line_key])
    end
  end

  defmodule Rollup do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.GroupClaimsTenantGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_gct_rollup"
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
      attribute :total, :integer, public?: true
    end

    identities do
      identity :by_org_fund, [:org_id, :fund], pre_check_with: Domain
    end

    actions do
      defaults [:read, :destroy]

      create :upsert,
        upsert?: true,
        upsert_identity: :by_org_fund,
        accept: [:org_id, :fund, :total]
    end

    reactive do
      id(:rollup)
      op(:fold)
      payload_key(:fund)
      row_key([:org_id, :fund])

      # THE narrowing declaration: a changed line invalidates its FUND's unit,
      # not the whole cell. Lowers to `key_rule :group` plus the `over:` and
      # `group_by:` the reduce needs.
      recompute_by(:fund, to: :lines, from: :fund)
      reduce(into: [sum: [amount: :total]])
    end
  end

  def tables, do: ["fx_gct_lines", "fx_gct_rollup"]
end

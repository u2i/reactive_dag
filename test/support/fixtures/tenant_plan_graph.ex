defmodule ReactiveDag.Fixtures.TenantPlanGraph do
  @moduledoc """
  A plan carrying a TENANT — every read, write and mark scoped to it.

  The failure this guards is silent: a read scoped to the wrong tenant
  returns nothing and reports SUCCESS.
  """

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      resource ReactiveDag.Fixtures.TenantPlanGraph.Lines
      resource ReactiveDag.Fixtures.TenantPlanGraph.Rollup
      resource ReactiveDag.Fixtures.TenantPlanGraph.Total
    end
  end

  defmodule Lines do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.TenantPlanGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_tp_lines"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :fund, :string, public?: true
      attribute :amount, :float, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        accept([:key, :fund, :amount])
      end
    end

    reactive do
      id :lines
      op :source
      leaf? true
    end
  end

  defmodule Rollup do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.TenantPlanGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_tp_rollup"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :fund, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :total, :float, public?: true
      # a constant, so the node above can group every row into one bucket
      attribute :scope, :string, default: "all", public?: true
    end

    identities do
      identity :by_fund, [:fund], pre_check_with: ReactiveDag.Fixtures.TenantPlanGraph.Domain
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        upsert_identity(:by_fund)
        accept([:fund, :total, :scope])
      end
    end

    reactive do
      id :rollup
      op :fold
      payload_key :fund
      reduce over: :lines, group_by: :fund, into: [sum: [amount: :total]]
    end
  end

  # `key_rule :all` makes propagation TO this node take the `:all` branch — a
  # different call site from the keyed one, and it needs the tenant just as much.
  defmodule Total do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.TenantPlanGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_tp_total"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :scope, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :total, :float, public?: true
    end

    identities do
      identity :by_scope, [:scope], pre_check_with: ReactiveDag.Fixtures.TenantPlanGraph.Domain
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        upsert_identity(:by_scope)
        accept([:scope, :total])
      end
    end

    reactive do
      id :total
      op :fold
      key_rule :all
      payload_key :scope
      reduce over: :rollup, group_by: :scope, into: [sum: [total: :total]]
    end
  end

  def tables, do: ["fx_tp_lines", "fx_tp_rollup", "fx_tp_total"]
end

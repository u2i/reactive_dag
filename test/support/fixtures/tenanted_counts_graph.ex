defmodule ReactiveDag.Fixtures.TenantedCountsGraph do
  @moduledoc """
  Counting rows under multitenancy, where an untenanted count silently sums
  across tenants.
  """

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      resource ReactiveDag.Fixtures.TenantedCountsGraph.Scoped
    end
  end

  defmodule Scoped do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.TenantedCountsGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_tc_scoped"
      repo(ReactiveDag.TestRepo)
    end

    multitenancy do
      strategy(:attribute)
      attribute(:org)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :org, :string, allow_nil?: false, public?: true
      attribute :status, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        accept([:key, :org, :status])
      end
    end

    reactive do
      id(:scoped)
      leaf?(true)
    end
  end

  def tables, do: ["fx_tc_scoped"]
end

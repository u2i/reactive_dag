defmodule ReactiveDag.Fixtures.AshTenancyGraph do
  @moduledoc """
  Ash multitenancy through a whole graph: every read, write and mark carries
  the tenant, or finds nothing and reports success.
  """

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      resource ReactiveDag.Fixtures.AshTenancyGraph.Meeting
      resource ReactiveDag.Fixtures.AshTenancyGraph.Shared
      resource ReactiveDag.Fixtures.AshTenancyGraph.Rollup
      resource ReactiveDag.Fixtures.AshTenancyGraph.Summary
      resource ReactiveDag.Fixtures.AshTenancyGraph.Reading
      resource ReactiveDag.Fixtures.AshTenancyGraph.Plant
    end
  end

  # A TENANTED node: `municipality_id` is the tenant attribute, and Ash — not
  # this library — is what puts a value in it.
  defmodule Meeting do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.AshTenancyGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_at_meeting"
      repo(ReactiveDag.TestRepo)
    end

    multitenancy do
      strategy :attribute
      attribute :municipality_id
    end

    # A UUID PRIMARY KEY, and this is forced rather than chosen. The tenant
    # attribute cannot be in the primary key: Spark refuses a nullable PK, and
    # `for_create` validates required attributes BEFORE `Ash.create` applies the
    # tenant — so a non-nullable tenant attribute fails validation while the
    # changeset already carries the tenant. With a UUID PK the tenant is an
    # ordinary column and uniqueness moves to an identity.
    attributes do
      uuid_primary_key(:id)
      attribute :key, :string, allow_nil?: false, public?: true
      attribute :municipality_id, :string, public?: true
      attribute :title, :string, public?: true
    end

    identities do
      identity :by_tenant_key, [:municipality_id, :key], pre_check_with: Domain
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        # by IDENTITY, not the primary key: the PK is a fresh UUID every time, so
        # conflicting on it would insert a duplicate per write.
        upsert_identity(:by_tenant_key)
        accept([:key, :municipality_id, :title])
      end
    end

    reactive do
      id(:meeting)
      op(:source)
      leaf?(true)
      # REQUIRED: `derived_payload_key/1` returns the single-attribute PK, which
      # here is `:id` — without this the library writes cell keys into the UUID
      # column. See ADR-003.
      payload_key(:key)
    end
  end

  # An UNTENANTED node, in the same graph. Declares no multitenancy at all, so
  # every path must behave exactly as it did before this feature existed.
  defmodule Shared do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.AshTenancyGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_at_shared"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :title, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        accept([:key, :title])
      end
    end

    reactive do
      id(:shared)
      op(:source)
      leaf?(true)
    end
  end

  defmodule Rollup do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.AshTenancyGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_at_rollup"
      repo(ReactiveDag.TestRepo)
    end

    multitenancy do
      strategy :attribute
      attribute :municipality_id
    end

    attributes do
      uuid_primary_key(:id)
      attribute :key, :string, allow_nil?: false, public?: true
      attribute :municipality_id, :string, public?: true
      attribute :total, :integer, public?: true
    end

    identities do
      identity :by_tenant_key, [:municipality_id, :key], pre_check_with: Domain
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        upsert_identity(:by_tenant_key)
        accept([:key, :municipality_id, :total])
      end
    end

    reactive do
      id(:rollup)
      op(:fold)
      payload_key(:key)
      # A `reduce` is what RETIRES: `compute Mod` writes its own rows and never
      # reaches `materialize/4`. The `over` leaf holds nothing, so this pass
      # produces nothing and every key the cell holds is retired.
      reduce(over: :meeting, group_by: :key, into: [count: :total])
    end
  end

  defmodule Summary do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.AshTenancyGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_at_summary"
      repo(ReactiveDag.TestRepo)
    end

    multitenancy do
      strategy :attribute
      attribute :municipality_id
    end

    attributes do
      uuid_primary_key(:id)
      attribute :key, :string, allow_nil?: false, public?: true
      attribute :municipality_id, :string, public?: true
      attribute :summary, :string, public?: true
      attribute :fingerprint, :string, public?: true
    end

    identities do
      identity :by_tenant_key, [:municipality_id, :key], pre_check_with: Domain
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        upsert_identity(:by_tenant_key)
        accept([:key, :municipality_id, :summary, :fingerprint])
      end

      action :summarise, :map do
        argument :text, :string
        run(fn input, _ -> {:ok, %{"summary" => "sum:" <> (input.arguments.text || "")}} end)
      end
    end

    reactive do
      id(:summary)
      op(:map)
      payload_key(:key)

      recompute_by(:key, to: :meeting, from: :key)

      per_key(:summarise,
        args: [text: :title],
        fingerprint: [:title],
        into: [summary: :summary]
      )
    end
  end

  defmodule Reading do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.AshTenancyGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_at_reading"
      repo(ReactiveDag.TestRepo)
    end

    multitenancy do
      strategy :attribute
      attribute :municipality_id
    end

    attributes do
      uuid_primary_key(:id)
      attribute :key, :string, allow_nil?: false, public?: true
      attribute :municipality_id, :string, public?: true
      attribute :plant_key, :string, public?: true
      attribute :flow, :integer, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :create do
        accept([:key, :municipality_id, :plant_key, :flow])
      end
    end

    reactive do
      id(:readings)
      op(:source)
      leaf?(true)
    end
  end

  defmodule Plant do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.AshTenancyGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_at_plant"
      repo(ReactiveDag.TestRepo)
    end

    multitenancy do
      strategy :attribute
      attribute :municipality_id
    end

    attributes do
      uuid_primary_key(:id)
      attribute :key, :string, allow_nil?: false, public?: true
      attribute :municipality_id, :string, public?: true
      attribute :total, :integer, public?: true
    end

    identities do
      identity :by_tenant_key, [:municipality_id, :key], pre_check_with: Domain
    end

    relationships do
      has_many :readings, ReactiveDag.Fixtures.AshTenancyGraph.Reading do
        source_attribute :key
        destination_attribute :plant_key
      end
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        upsert_identity(:by_tenant_key)
        accept([:key, :municipality_id, :total])
      end
    end

    reactive do
      id(:plants)
      op(:fold)
      payload_key(:key)
      aggregate(over: :readings, sum: [flow: :total])
    end
  end

  def tables, do: ["fx_at_plant", "fx_at_reading", "fx_at_summary", "fx_at_rollup", "fx_at_meeting", "fx_at_shared"]
end

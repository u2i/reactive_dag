defmodule ReactiveDag.Fixtures.KeylessJoinGraph do
  @moduledoc """
  A join SIDE with no key column — the side is named by the columns the row
  already carries.
  """

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      resource ReactiveDag.Fixtures.KeylessJoinGraph.Machine
      resource ReactiveDag.Fixtures.KeylessJoinGraph.EdrAgent
      resource ReactiveDag.Fixtures.KeylessJoinGraph.Observation
    end
  end

  # Both sides keyed by a natural column, with NO key column — the #223 shape.
  defmodule Machine do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.KeylessJoinGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_kj_machine"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      uuid_primary_key :id
      attribute :serial, :string, allow_nil?: false, public?: true
      attribute :hostname, :string, public?: true
    end

    identities do
      identity :by_serial, [:serial], pre_check_with: ReactiveDag.Fixtures.KeylessJoinGraph.Domain
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        upsert_identity(:by_serial)
        accept([:serial, :hostname])
      end
    end

    reactive do
      id(:machine_register)
      op(:source)
      leaf?(true)
      payload_key(false)
      row_key([:serial])
    end
  end

  defmodule EdrAgent do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.KeylessJoinGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_kj_edr_agent"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      uuid_primary_key :id
      attribute :serial, :string, allow_nil?: false, public?: true
      attribute :version, :string, public?: true
    end

    identities do
      identity :by_serial, [:serial], pre_check_with: ReactiveDag.Fixtures.KeylessJoinGraph.Domain
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        upsert_identity(:by_serial)
        accept([:serial, :version])
      end
    end

    reactive do
      id(:edr_agents)
      op(:source)
      leaf?(true)
      payload_key(false)
      row_key([:serial])
    end
  end

  defmodule Observation do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.KeylessJoinGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_kj_observation"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :hostname, :string, public?: true
      attribute :version, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]
      create :upsert, upsert?: true, accept: [:key, :hostname, :version]
    end

    reactive do
      id(:machine_observations)
      op(:reconcile)

      join(
        left_over: :machine_register,
        right_over: :edr_agents,
        left: :serial,
        right: :serial,
        outer: true,
        into: [left: [hostname: :hostname], right: [version: :version]]
      )
    end
  end

  def tables, do: ["fx_kj_machine", "fx_kj_edr_agent", "fx_kj_observation"]
end

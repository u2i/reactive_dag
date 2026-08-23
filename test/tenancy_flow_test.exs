defmodule ReactiveDag.TenancyFlowTest do
  @moduledoc """
  A tenanted node must not feed an UNTENANTED one, and `graph/2` says so.

  ## Why this needs a check of its own

  Every other tenancy mistake announces itself. An unscoped read of a tenanted
  resource raises; a missing tenant on a write raises. This one does not:

    * Ash IGNORES `tenant:` on a resource declaring no multitenancy — the option
      is dropped, no error, the read succeeds.
    * So each tenant's drain folds its own upstream rows correctly, and then
      writes the result to the SAME downstream row, because that row has no
      tenant to tell them apart.
    * The last drain to run wins. The others' numbers are simply gone.

  Measured before this check existed, on a two-tenant fold with inputs 10 and
  99: one row, total 99. No error, no warning, and a suite with one tenant
  passes forever — the collision needs a second one.

  Nothing else can catch it. A Spark verifier sees one resource at a time and
  cannot know what feeds it, and the datastore is satisfied: one row per key is
  exactly what an untenanted table promises.
  """
  use ExUnit.Case, async: false

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      allow_unregistered?(true)
    end
  end

  defmodule TenantedSource do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
      private?(true)
    end

    multitenancy do
      strategy :attribute
      attribute :org_id
    end

    attributes do
      uuid_primary_key(:id)
      attribute :org_id, :string, public?: true
      attribute :key, :string, allow_nil?: false, public?: true
      attribute :amount, :integer, public?: true
    end

    identities do
      identity :by_org_key, [:org_id, :key], pre_check_with: Domain
    end

    actions do
      defaults [:read, :destroy]

      create :upsert,
        upsert?: true,
        upsert_identity: :by_org_key,
        accept: [:org_id, :key, :amount]
    end

    reactive do
      id(:src)
      leaf?(true)
      row_key([:org_id, :key])
    end
  end

  defmodule UntenantedRollup do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
      private?(true)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :total, :integer, public?: true
    end

    actions do
      defaults [:read, :destroy]
      create :upsert, upsert?: true, accept: [:key, :total]
    end

    reactive do
      id(:rollup)
      op(:fold)
      payload_key(:key)
      depends_on([:src])

      reduce(
        over: :src,
        group_by: [:key],
        into: [sum: [amount: :total]]
      )
    end
  end

  defmodule TenantedRollup do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
      private?(true)
    end

    multitenancy do
      strategy :attribute
      attribute :org_id
    end

    attributes do
      uuid_primary_key(:id)
      attribute :org_id, :string, public?: true
      attribute :key, :string, allow_nil?: false, public?: true
      attribute :total, :integer, public?: true
    end

    identities do
      identity :by_org_key, [:org_id, :key], pre_check_with: Domain
    end

    actions do
      defaults [:read, :destroy]

      create :upsert,
        upsert?: true,
        upsert_identity: :by_org_key,
        accept: [:org_id, :key, :total]
    end

    reactive do
      id(:rollup)
      op(:fold)
      payload_key(:key)
      row_key([:org_id, :key])
      depends_on([:src])

      reduce(
        over: :src,
        group_by: [:key],
        into: [sum: [amount: :total]]
      )
    end
  end

  # An ordinary shared upstream: no tenancy, read by every tenant's graph.
  defmodule SharedRates do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
      private?(true)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :amount, :integer, public?: true
    end

    actions do
      defaults [:read, :destroy]
      create :upsert, upsert?: true, accept: [:key, :amount]
    end

    reactive do
      id(:src)
      leaf?(true)
    end
  end

  describe "a tenanted node feeding an untenanted one" do
    test "is refused at assembly" do
      assert_raise ArgumentError, ~r/is NOT tenanted but reads/, fn ->
        ReactiveDag.Node.graph([TenantedSource, UntenantedRollup])
      end
    end

    test "the message names both cells and says what to do" do
      err =
        assert_raise ArgumentError, fn ->
          ReactiveDag.Node.graph([TenantedSource, UntenantedRollup])
        end

      # WHICH edge, in the direction a reader thinks in.
      assert err.message =~ ~s("rollup")
      assert err.message =~ ~s("src")

      # WHY it is not merely untidy — the reason it cannot be left to a test.
      assert err.message =~ "last drain to run wins"
      assert err.message =~ "Ash ignores"

      # And the fix, both ways round: tenant the consumer, or untenant the input.
      assert err.message =~ "row_key"
      assert err.message =~ "untenanted"
    end

    test "it is refused whether or not the plan names a tenant" do
      # The graph's SHAPE is wrong, not this run of it. A host that has not
      # started passing `tenant:` yet is the likeliest one to have built this by
      # accident, so it must not be the one case that slips through.
      for opts <- [[], [tenant: "org_a"]] do
        assert_raise ArgumentError, ~r/is NOT tenanted but reads/, fn ->
          ReactiveDag.Node.graph([TenantedSource, UntenantedRollup], opts)
        end
      end
    end
  end

  describe "the shapes that are fine" do
    test "tenanted feeding tenanted" do
      plan = ReactiveDag.Node.graph([TenantedSource, TenantedRollup], tenant: "org_a")

      assert plan.tenant == "org_a"
      assert Map.keys(plan.cells) |> Enum.sort() == ["rollup", "src"]
    end

    test "UNTENANTED feeding tenanted — a shared upstream" do
      # The reverse direction, and deliberately allowed: a common corpus (a chart
      # of accounts, an exchange-rate table) serves every tenant's graph. Each
      # tenant's own rows stay separate; only the input is shared.
      plan = ReactiveDag.Node.graph([SharedRates, TenantedRollup], tenant: "org_a")

      assert Map.keys(plan.cells) |> Enum.sort() == ["rollup", "src"]
    end

    test "untenanted feeding untenanted — a host with no tenancy at all" do
      plan = ReactiveDag.Node.graph([SharedRates, UntenantedRollup])

      assert plan.tenant == "*"
      assert Map.keys(plan.cells) |> Enum.sort() == ["rollup", "src"]
    end
  end
end

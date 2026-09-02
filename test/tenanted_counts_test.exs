defmodule ReactiveDag.TenantedCountsTest do
  @moduledoc """
  Counting a TENANTED node's rows.

  Every count in `Rows` went through an unscoped `Ash.count!/1`. Ash refuses
  that on a multitenant resource — `"Queries against … require a tenant to be
  specified"` — so once a host made its resources tenanted, every cell raised,
  `Insights` swallowed it into `rows: :unreadable`, and the dashboard rendered
  `?` in place of the count.

  That is the shape of the bug worth pinning: not a wrong number, but a total
  failure to read that LOOKED like a shrug. Found in production only because
  someone asked why the question marks were there — 33 of 35 cells were
  affected and nothing said so.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.{Insights, Node.Rows, Plan}

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      allow_unregistered?(true)
    end
  end

  defmodule Scoped do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Ets, extensions: [ReactiveDag.Node]

    ets do
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

  defp cell do
    %ReactiveDag.Cell{
      id: "scoped",
      op: :test,
      inputs: [],
      leaf?: true,
      meta: %{resource: Scoped}
    }
  end

  setup do
    for {key, org, status} <- [
          {"a", "acme", "present"},
          {"b", "acme", "failing"},
          {"c", "globex", "present"}
        ] do
      Scoped
      |> Ash.Changeset.for_create(:upsert, %{key: key, org: org, status: status})
      |> Ash.create!(tenant: org)
    end

    on_exit(fn -> Ash.DataLayer.Ets.stop(Scoped) end)
    :ok
  end

  test "status_histogram/2 counts only the named tenant's rows" do
    assert Rows.status_histogram(cell(), tenant: "acme") == %{"present" => 1, "failing" => 1}
    assert Rows.status_histogram(cell(), tenant: "globex") == %{"present" => 1}
  end

  test "without a tenant, a multitenant resource raises rather than counting wrong" do
    # The bug's root. Asserting it RAISES rather than returning a wrong number
    # is the point: a silent miscount would be worse than the `?`.
    assert_raise Ash.Error.Invalid, fn -> Rows.status_histogram(cell()) end
  end

  test "keys_by_status/3 is tenant-scoped too" do
    assert Rows.keys_by_status(cell(), ["failing"], tenant: "acme") == ["b"]
    assert Rows.keys_by_status(cell(), ["failing"], tenant: "globex") == []
  end

  test "Insights.summary/1 takes the tenant from the plan" do
    plan = %Plan{
      cells: %{"scoped" => cell()},
      parents: %{},
      depths: %{"scoped" => 0},
      tenant: "acme"
    }

    assert [status] = Insights.summary(plan)

    # `:stored`, not `:unreadable` — the whole point. Before the fix this was
    # `%{}` with `rows: :unreadable`, which the dashboard drew as `?`.
    assert status.rows == :stored
    assert status.key_count == 2
    assert status.statuses == %{"present" => 1, "failing" => 1}
  end

  test "a plan with no tenant does not send one, so untenanted hosts still work" do
    # `"*"` is the frontier's spelling for "untenanted" and is NOT a wildcard to
    # Ash — sending it to a resource that is not multitenant is an error, so it
    # has to be dropped rather than passed through.
    plan = %Plan{
      cells: %{"scoped" => cell()},
      parents: %{},
      depths: %{"scoped" => 0},
      tenant: "*"
    }

    assert [status] = Insights.summary(plan)

    # This resource IS tenanted, so an unscoped read fails — and that failure
    # must be reported as unreadable rather than crashing the caller.
    assert status.rows == :unreadable
  end
end

defmodule ReactiveDag.KeylessJoinSideTest do
  @moduledoc """
  A join SIDE that declares `payload_key false` + `row_key` (#223).

  Such a node has no key column: it is identified by the columns it names, and its
  `payload_key` is `false` rather than nil. Every path resolved that correctly —
  `Payload.lookup/1` answers `{:identity, [...]}`, `Rows.keyer/1` branches on
  `row_key` first — except the join's side lookup, which guarded on
  `is_nil(payload_key)`. `false` is not nil, so the guard let it through and the read
  built a filter from the literal:

      ** (Ash.Error.Invalid) No such field false for resource …
        at filter

  It only showed on a join, so a keyless node worked everywhere until it became a
  side. In the reporting host it surfaced as six failures reported only as
  `DBConnection.ConnectionError: transaction rolling back` — the Ash error is raised
  inside the drain's transaction and the rollback masks it.

  The fix asks `Payload.lookup/1`, like everywhere else, and reads a keyless side by
  its `row_key` columns rather than degrading the claim to `"*"`.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.{Drain, Frontier}

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false
    resources(do: allow_unregistered?(true))
  end

  # Both sides keyed by a natural column, with NO key column — the #223 shape.
  defmodule Machine do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Ets, extensions: [ReactiveDag.Node]
    ets(do: private?(true))

    attributes do
      uuid_primary_key :id
      attribute :serial, :string, allow_nil?: false, public?: true
      attribute :hostname, :string, public?: true
    end

    identities do
      identity :by_serial, [:serial], pre_check_with: ReactiveDag.KeylessJoinSideTest.Domain
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
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Ets, extensions: [ReactiveDag.Node]
    ets(do: private?(true))

    attributes do
      uuid_primary_key :id
      attribute :serial, :string, allow_nil?: false, public?: true
      attribute :version, :string, public?: true
    end

    identities do
      identity :by_serial, [:serial], pre_check_with: ReactiveDag.KeylessJoinSideTest.Domain
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
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Ets, extensions: [ReactiveDag.Node]
    ets(do: private?(true))

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

  defp plan, do: ReactiveDag.Node.graph([Machine, EdrAgent, Observation])

  defp drain,
    do: Drain.run(plan(), recompute: ReactiveDag.Node.Recompute, key_rule: ReactiveDag.Node.KeyRule)

  setup do
    start_supervised!(ReactiveDag.Test.FakeFrontierRepo)
    ReactiveDag.Test.FakeFrontierRepo.install()

    for m <- [Machine, EdrAgent, Observation], r <- Ash.read!(m), do: Ash.destroy!(r)
    for cell <- ["machine_register", "edr_agents", "machine_observations"], do: Frontier.claim(cell)
    :ok
  end

  defp put_machine(serial, hostname) do
    Ash.create!(Machine, %{serial: serial, hostname: hostname}, action: :upsert)
    Frontier.mark_dirty("machine_register", [serial], "test")
  end

  defp put_agent(serial, version) do
    Ash.create!(EdrAgent, %{serial: serial, version: version}, action: :upsert)
    Frontier.mark_dirty("edr_agents", [serial], "test")
  end

  test "a keyless LEFT side drains — it does not filter on the literal `false`" do
    put_machine("PROBE01", "host-a")
    put_agent("PROBE01", "1.2.3")

    assert {:ok, _} = drain()

    row = Observation |> Ash.read!() |> Enum.find(&(&1.key == "PROBE01"))
    assert row.hostname == "host-a"
    assert row.version == "1.2.3"
  end

  test "a keyless RIGHT side drains too" do
    put_agent("PROBE02", "9.9.9")

    assert {:ok, _} = drain()

    # `outer: true`, so a right-only key still emits.
    row = Observation |> Ash.read!() |> Enum.find(&(&1.key == "PROBE02"))
    assert row.version == "9.9.9"
    assert row.hostname == nil
  end

  test "the claim is NARROWED to the changed key, not widened to the whole cell" do
    put_machine("A", "host-a")
    put_machine("B", "host-b")
    {:ok, _} = drain()

    # One machine changes. Degrading to `"*"` would be sound — and is what #223
    # proposed — but it reprices every observation because one row moved. A side
    # that declares `row_key` has said how to find its rows.
    Ash.create!(Machine, %{serial: "A", hostname: "host-a2"}, action: :upsert)
    Frontier.mark_dirty("machine_register", ["A"], "test")

    {:ok, report} = drain()
    claimed = Map.new(report.steps, &{&1.cell, &1})["machine_observations"].claimed

    refute "*" in claimed, "a keyless join side must not widen the claim"
    assert claimed == ["A"]
  end

  test "`Payload.lookup/1` and the join agree about a keyless side" do
    cell = plan().cells["machine_observations"]
    left = cell.meta[:side_sources][:left]

    assert left[:payload_key] == false, "the shape this is all about"
    assert ReactiveDag.Node.Payload.lookup(Map.to_list(left)) == {:identity, [:serial]}
  end
end

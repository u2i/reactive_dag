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
  use ReactiveDag.PostgresCase

  alias ReactiveDag.Fixtures.KeylessJoinGraph
  alias ReactiveDag.Fixtures.KeylessJoinGraph.{EdrAgent, Machine, Observation}

  alias ReactiveDag.{Cascade}

  defp plan, do: ReactiveDag.Node.graph([Machine, EdrAgent, Observation])

  defp drain,
    do: ReactiveDag.Test.Pending.cascade(plan(), recompute: ReactiveDag.Node.Recompute, key_rule: ReactiveDag.Node.KeyRule)

  setup_all do
    migrate!(KeylessJoinGraph.Domain)
    :ok
  end

  setup do
    pid = Ecto.Adapters.SQL.Sandbox.start_owner!(ReactiveDag.TestRepo, shared: true)
    on_exit(fn -> Ecto.Adapters.SQL.Sandbox.stop_owner(pid) end)

    truncate(KeylessJoinGraph.tables())
    start_supervised!(ReactiveDag.Test.FakeSuspensionRepo)
    ReactiveDag.Test.FakeSuspensionRepo.install()

    for m <- [Machine, EdrAgent, Observation], r <- Ash.read!(m), do: Ash.destroy!(r)
    ReactiveDag.Test.Pending.reset()
    :ok
  end

  defp put_machine(serial, hostname) do
    Ash.create!(Machine, %{serial: serial, hostname: hostname}, action: :upsert)
    ReactiveDag.Test.Pending.add("machine_register", [serial])
  end

  defp put_agent(serial, version) do
    Ash.create!(EdrAgent, %{serial: serial, version: version}, action: :upsert)
    ReactiveDag.Test.Pending.add("edr_agents", [serial])
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
    ReactiveDag.Test.Pending.add("machine_register", ["A"])

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

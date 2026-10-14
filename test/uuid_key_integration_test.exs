defmodule ReactiveDag.UuidKeyIntegrationTest do
  @moduledoc """
  `row_key :uuid` through a whole cascade — not just through `upsert_row/5`.

  `row_key_test.exs` covers the write in isolation: the key becomes the id, the
  same key upserts, an unchanged write reports `:unchanged`. That is three unit
  tests of one function.

  What has never been exercised is a uuid-keyed node inside a RUNNING graph:
  claims arriving from a portal-keyed child, a suspension recording the key and
  replaying it, and retire-by-absence deciding what to remove. No host uses
  `row_key :uuid` today — cascade's 33 nodes are all keyed by upstream strings —
  so this file is written before the first one adopts it rather than after.

  ## The shape under test

      docs (string-keyed leaf)  ──→  entity (row_key :uuid)

  which is the boundary any canonical-identity design has: the leaf is one row
  per OBSERVATION and cannot collapse, so a portal-keyed child always feeds the
  uuid-keyed parent. `key_rule :identity` passes the child's key through, and
  the op translates. These tests pin what the library does around that.
  """
  use ReactiveDag.PostgresCase

  alias ReactiveDag.Fixtures.UuidKeyGraph
  alias ReactiveDag.Fixtures.UuidKeyGraph.{Docs, Entity}

  alias ReactiveDag.Cascade
  alias ReactiveDag.Node.{Payload, Rows}

  # THE TRANSLATION POINT. Every observation whose key starts with the same
  # prefix is one entity; the prefix maps to a fixed uuid so the test can assert
  # on it. A real host resolves this through an identity table.
  @entity_a "0198b6a2-0000-7000-8000-00000000000a"
  @entity_b "0198b6a2-0000-7000-8000-00000000000b"

  defp uuid_for(key), do: UuidKeyGraph.uuid_for(key)

  # The cascade opens a transaction for its suspension bookkeeping, so it needs a
  # repo even when nothing suspends. Same shim as `cascade_test.exs`.

  setup_all do
    migrate!(UuidKeyGraph.Domain)
    :ok
  end

  setup do
    pid = Ecto.Adapters.SQL.Sandbox.start_owner!(ReactiveDag.TestRepo, shared: true)
    on_exit(fn -> Ecto.Adapters.SQL.Sandbox.stop_owner(pid) end)

    migrate_library_tables!()
    truncate(UuidKeyGraph.tables())
    prev = Application.get_env(:reactive_dag, :repo)
    Application.put_env(:reactive_dag, :repo, FakeRepo)

    on_exit(fn ->
      if prev,
        do: Application.put_env(:reactive_dag, :repo, prev),
        else: Application.delete_env(:reactive_dag, :repo)
    end)

    on_exit(fn ->
      Ash.DataLayer.Ets.stop(Docs)
      Ash.DataLayer.Ets.stop(Entity)
    end)

    :ok
  end

  defp plan, do: ReactiveDag.Node.graph([Docs, Entity])

  defp seed(key, body) do
    Docs
    |> Ash.Changeset.for_create(:upsert, %{key: key, body: body})
    |> Ash.create!()
  end

  describe "a uuid-keyed node in a running cascade" do
    test "two observations of one entity collapse to ONE row" do
      # The property the whole canonical-resource design rests on. Two postings
      # of one meeting are two leaf rows and one entity row.
      seed("a-1", "first")
      seed("a-2", "second")

      {:ok, _} = Cascade.run(plan(), [%{cell: "docs", keys: ["a-1", "a-2"]}])

      assert [row] = Ash.read!(Entity)
      assert row.observations == 2
      assert row.bodies == "first,second"
      assert row.id == @entity_a
    end

    test "the cell key IS the row id, and reads back as such" do
      seed("a-1", "x")
      {:ok, _} = Cascade.run(plan(), [%{cell: "docs", keys: ["a-1"]}])

      # `keyer/1` must report the id as the cell key, or a read-back names keys
      # the frontier never saw.
      assert [%{key: @entity_a}] = Rows.all(Entity |> cell_meta())
    end

    test "a claim in the CHILD's vocabulary reaches the right entity" do
      # `key_rule :identity` passes `"a-1"` through, which is not an entity key.
      # The op translates. This pins that the library does not object.
      seed("a-1", "one")
      seed("b-1", "other")
      {:ok, _} = Cascade.run(plan(), [%{cell: "docs", keys: ["a-1", "b-1"]}])

      assert length(Ash.read!(Entity)) == 2

      seed("a-1", "changed")
      {:ok, report} = Cascade.run(plan(), [%{cell: "docs", keys: ["a-1"]}])

      touched = report.steps |> Enum.filter(&(&1.cell == "entity")) |> Enum.flat_map(& &1.changed)

      assert touched == [@entity_a], "only entity A moved"
    end

    test "a second observation updates the entity rather than adding a row" do
      seed("a-1", "first")
      {:ok, _} = Cascade.run(plan(), [%{cell: "docs", keys: ["a-1"]}])

      seed("a-2", "second")
      {:ok, _} = Cascade.run(plan(), [%{cell: "docs", keys: ["a-2"]}])

      assert [row] = Ash.read!(Entity)
      assert row.observations == 2, "the entity absorbed the second observation"
    end
  end

  describe "what a uuid key costs" do
    test "the entity's key survives a whole-cell recompute" do
      # `"*"` takes a different path through `entries_for/6`; a uuid-keyed node
      # must come out with the same id rather than a regenerated one.
      seed("a-1", "x")
      {:ok, _} = Cascade.run(plan(), [%{cell: "docs", keys: ["a-1"]}])
      [before] = Ash.read!(Entity)

      {:ok, _} = Cascade.run(plan(), [%{cell: "docs", keys: ["*"]}])

      assert [after_] = Ash.read!(Entity)
      assert after_.id == before.id, "a whole-cell pass must not re-mint the identity"
    end
  end

  # `Rows.all/2` wants the CELL, not the module.
  defp cell_meta(_mod), do: plan().cells["entity"]
end

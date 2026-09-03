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
  use ExUnit.Case, async: false

  alias ReactiveDag.Cascade
  alias ReactiveDag.Node.{Payload, Rows}

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      allow_unregistered?(true)
    end
  end

  # One row per OBSERVATION, keyed by what the source called it.
  defmodule Docs do
    use Ash.Resource,
      domain: ReactiveDag.UuidKeyIntegrationTest.Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
      private?(true)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :body, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]
      create :upsert, upsert?: true, accept: [:key, :body]
    end

    reactive do
      id :docs
      leaf? true
    end
  end

  # One row per ENTITY, keyed by an identity the graph mints.
  defmodule Entity do
    use Ash.Resource,
      domain: ReactiveDag.UuidKeyIntegrationTest.Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
      private?(true)
    end

    attributes do
      # `writable?: true` is REQUIRED for `row_key :uuid`. Without it Ash refuses
      # `:id` as an action input (`NoSuchInput`) even when the action accepts it,
      # because `uuid_primary_key` is non-writable by default — and the cell key
      # goes in as an ordinary attribute (`payload.ex:168`).
      #
      # A node adopting `row_key :uuid` and forgetting this fails at WRITE time,
      # inside a cascade, not at assembly.
      uuid_primary_key :id, writable?: true
      attribute :bodies, :string, public?: true
      attribute :observations, :integer, public?: true
    end

    actions do
      defaults [:read, :destroy]
      create :upsert, upsert?: true, accept: [:id, :bodies, :observations]
    end

    reactive do
      id :entity
      row_key :uuid
      key_rule :identity
      depends_on [:docs]
      compute ReactiveDag.UuidKeyIntegrationTest.EntityOp
    end
  end

  # THE TRANSLATION POINT. Every observation whose key starts with the same
  # prefix is one entity; the prefix maps to a fixed uuid so the test can assert
  # on it. A real host resolves this through an identity table.
  @entity_a "0198b6a2-0000-7000-8000-00000000000a"
  @entity_b "0198b6a2-0000-7000-8000-00000000000b"

  def uuid_for("a-" <> _), do: @entity_a
  def uuid_for("b-" <> _), do: @entity_b

  defmodule EntityOp do
    @behaviour ReactiveDag.Op

    alias ReactiveDag.UuidKeyIntegrationTest, as: T

    @impl true
    def recompute(cell, keys, opts \\ []) do
      observed = T.Docs |> Ash.read!() |> Enum.group_by(&T.uuid_for(&1.key))

      # Claims arrive in the CHILD's vocabulary; translate, then write per entity.
      wanted =
        case keys do
          ["*"] -> Map.keys(observed)
          ks -> ks |> Enum.map(&T.uuid_for/1) |> Enum.uniq()
        end

      changed =
        for uuid <- wanted, rows = Map.get(observed, uuid, []), rows != [] do
          attrs = %{
            bodies: rows |> Enum.map(& &1.body) |> Enum.sort() |> Enum.join(","),
            observations: length(rows)
          }

          if Payload.upsert_row(T.Entity, cell.meta, uuid, attrs, opts) != :unchanged,
            do: uuid,
            else: nil
        end

      {:ok, Enum.reject(changed, &is_nil/1)}
    end
  end

  # The cascade opens a transaction for its suspension bookkeeping, so it needs a
  # repo even when nothing suspends. Same shim as `cascade_test.exs`.
  defmodule FakeRepo do
    def query!("SELECT" <> _, _), do: %{rows: [], num_rows: 0}
    def query!("INSERT INTO " <> _, _), do: %{rows: [], num_rows: 1}
    def query!("DELETE" <> _, _), do: %{rows: [], num_rows: 0}
    def query!(_, _), do: %{rows: [], num_rows: 0}

    def transaction(fun, _opts \\ []) do
      {:ok, fun.()}
    catch
      :throw, {:rd_rollback, reason} -> {:error, reason}
    end

    def rollback(reason), do: throw({:rd_rollback, reason})
  end

  setup do
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

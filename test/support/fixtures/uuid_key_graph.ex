defmodule ReactiveDag.Fixtures.UuidKeyGraph do
  @moduledoc """
  A node whose rows are UUID-keyed end to end.

  The cell key is the row id, so nothing anywhere splits a string to find a
  row — the failure a prefixed key invites.
  """

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      resource ReactiveDag.Fixtures.UuidKeyGraph.Docs
      resource ReactiveDag.Fixtures.UuidKeyGraph.Entity
    end
  end

  # One row per OBSERVATION, keyed by what the source called it.
  defmodule Docs do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.UuidKeyGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_uk_docs"
      repo(ReactiveDag.TestRepo)
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
      id(:docs)
      leaf?(true)
    end
  end

  # One row per ENTITY, keyed by an identity the graph mints.
  defmodule Entity do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.UuidKeyGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_uk_entity"
      repo(ReactiveDag.TestRepo)
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
      id(:entity)
      row_key(:uuid)
      key_rule(:identity)
      depends_on([:docs])
      compute(ReactiveDag.Fixtures.UuidKeyGraph.EntityOp)
    end
  end

  defmodule EntityOp do
    @moduledoc """
    The compute behind `Entity`.

    In the fixture because the resource NAMES it, so a graph assembled from
    here cannot recompute without it.
    """
    @behaviour ReactiveDag.Op

    alias ReactiveDag.Fixtures.UuidKeyGraph, as: T
    alias ReactiveDag.Node.Payload

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

  # THE TRANSLATION POINT. Every observation whose key starts with the same
  # prefix is one entity; the prefix maps to a fixed uuid so a test can assert
  # on it. A real host resolves this through an identity table.
  @entity_a "0198b6a2-0000-7000-8000-00000000000a"
  @entity_b "0198b6a2-0000-7000-8000-00000000000b"

  @doc "The entity a given observation key belongs to."
  def uuid_for("a-" <> _), do: @entity_a
  def uuid_for("b-" <> _), do: @entity_b

  @doc "The two entity ids, for a test asserting on them."
  def entity_a, do: @entity_a
  def entity_b, do: @entity_b

  def tables, do: ["fx_uk_docs", "fx_uk_entity"]
end

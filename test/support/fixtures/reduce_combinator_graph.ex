defmodule ReactiveDag.Fixtures.ReduceCombinatorGraph do
  @moduledoc """
  The `reduce` combinator's own shapes — what a fold accepts as its
  `into:`, and what it refuses.
  """

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      resource ReactiveDag.Fixtures.ReduceCombinatorGraph.Lines
      resource ReactiveDag.Fixtures.ReduceCombinatorGraph.Summary
    end
  end

  defmodule Lines do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.ReduceCombinatorGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_rc_lines"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :k, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :fp, :string, public?: true
      attribute :flagged, :boolean, public?: true, default: false
    end

    actions do
      defaults [:read, :destroy]

      create :create do
        accept([:k, :fp, :flagged])
      end
    end

    reactive do
      id(:lines)
      op(:source)
      leaf?(true)
    end
  end

  # fn slots over a node that owns its rows.
  defmodule Summary do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.ReduceCombinatorGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_rc_summary"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :fp, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        upsert_identity(:by_key)
        accept([:key, :fp])
      end
    end

    identities do
      identity :by_key, [:key]
    end

    reactive do
      id(:summary)
      op(:map)

      reduce(
        over: :lines,
        query: fn q, dirty ->
          send(ReactiveDag.ReduceCombinatorTest, {:query, dirty})
          q
        end,
        group_by: fn line -> line.k end,
        key: fn k -> k end,
        into: fn _k, [line | _] -> %{fp: line.fp} end
      )
    end
  end

  def tables, do: ["fx_rc_lines", "fx_rc_summary"]
end

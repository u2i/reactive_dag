defmodule ReactiveDag.Fixtures.SourceNodeGraph do
  @moduledoc """
  A SOURCE that is itself a node: one crawl feeding several typed leaves.

  `Documents` is the raw landing table; `AgendaDocs` and `MinutesDocs` are
  the typed views a consumer actually reads.
  """

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      resource ReactiveDag.Fixtures.SourceNodeGraph.Documents
      resource ReactiveDag.Fixtures.SourceNodeGraph.AgendaDocs
      resource ReactiveDag.Fixtures.SourceNodeGraph.MinutesDocs
    end
  end

  defmodule Documents do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.SourceNodeGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_san_documents"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :kind, :string, public?: true
      attribute :filed_as, :string, public?: true
      attribute :body, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]
      create :upsert, upsert?: true, accept: [:key, :kind, :filed_as, :body]
    end

    reactive do
      id(:documents)
      leaf?(true)
    end
  end

  defmodule AgendaDocs do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.SourceNodeGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_san_agenda_docs"
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
      id(:agenda_docs)

      # a projection of one kind out of the crawl. `expand` because a group
      # yields zero rows or one — a document that is not this kind simply is
      # not here.
      reduce(
        over: :documents,
        group_by: :key,
        expand: fn key, rows ->
          case Enum.filter(rows, &(&1.kind == "agenda")) do
            # not this leaf's document. DECLINE it — writing nothing would read
            # as a retirement and churn on every poll.
            [] -> [{:skip, key}]
            [row | _] -> [%{key: key, body: row.body}]
          end
        end
      )
    end
  end

  defmodule MinutesDocs do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.SourceNodeGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_san_minutes_docs"
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
      id(:minutes_docs)

      # a projection of one kind out of the crawl. `expand` because a group
      # yields zero rows or one — a document that is not this kind simply is
      # not here.
      reduce(
        over: :documents,
        group_by: :key,
        expand: fn key, rows ->
          case Enum.filter(rows, &(&1.kind == "minutes")) do
            # not this leaf's document. DECLINE it — writing nothing would read
            # as a retirement and churn on every poll.
            [] -> [{:skip, key}]
            [row | _] -> [%{key: key, body: row.body}]
          end
        end
      )
    end
  end

  def tables, do: ["fx_san_documents", "fx_san_agenda_docs", "fx_san_minutes_docs"]
end

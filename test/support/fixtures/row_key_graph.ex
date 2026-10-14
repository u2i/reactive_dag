defmodule ReactiveDag.Fixtures.RowKeyGraph do
  @moduledoc """
  `row_key` — how a cell key maps to a row, DECLARED rather than inferred.

  The three rungs, one resource each: the key IS the row id, the columns that
  identify the row, and a resolver where sameness is a judgement.
  """

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      resource ReactiveDag.Fixtures.RowKeyGraph.Doc
      resource ReactiveDag.Fixtures.RowKeyGraph.Line
      resource ReactiveDag.Fixtures.RowKeyGraph.Meeting
      resource ReactiveDag.Fixtures.RowKeyGraph.Filing
    end
  end

  # ── rung 1: the key IS the id ──────────────────────────────────────────────
  defmodule Doc do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.RowKeyGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_rk_doc"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      uuid_primary_key(:id, writable?: true)
      attribute :title, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        accept([:id, :title])
      end
    end

    reactive do
      id :docs
      op :source
      leaf? true
      row_key :uuid
    end
  end

  # ── rung 2: the columns that identify the row ──────────────────────────────
  defmodule Line do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.RowKeyGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_rk_line"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      uuid_primary_key(:id)
      attribute :fund, :string, public?: true
      attribute :fiscal_year, :string, public?: true
      attribute :total, :integer, public?: true
    end

    identities do
      identity :by_unit, [:fund, :fiscal_year], pre_check_with: Domain
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        upsert_identity(:by_unit)
        accept([:fund, :fiscal_year, :total])
      end
    end

    reactive do
      id :lines
      op :source
      leaf? true
      row_key [:fund, :fiscal_year]
    end
  end

  defmodule Meeting do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.RowKeyGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_rk_meeting"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      uuid_primary_key(:id)
      attribute :board, :string, public?: true
      attribute :date, :date, public?: true
      attribute :starts_at, :utc_datetime, public?: true
      attribute :title, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        accept([:board, :date, :starts_at, :title])
      end

      update :revise do
        accept([:board, :date, :starts_at, :title])
      end
    end

    reactive do
      id :meetings
      op :source
      leaf? true
      row_key &ReactiveDag.Fixtures.RowKeyGraph.Meetings.resolve/3
      payload_update :revise
    end
  end

  # A `row_key` that includes the TENANT attribute. The tenant is a COLUMN,
  # twice — on the row and on the frontier — and never part of a cell key: the
  # plan already carries it. Two tenants must therefore report the SAME cell
  # key, because it is the same unit.
  defmodule Filing do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.RowKeyGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_rk_filing"
      repo(ReactiveDag.TestRepo)
    end

    multitenancy do
      strategy :attribute
      attribute :org_id
    end

    attributes do
      uuid_primary_key(:id)
      attribute :doc_key, :string, allow_nil?: false, public?: true
      attribute :org_id, :string, public?: true
      attribute :total, :integer, public?: true
    end

    identities do
      identity :by_org_doc, [:org_id, :doc_key], pre_check_with: ReactiveDag.Fixtures.RowKeyGraph.Domain
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        upsert_identity(:by_org_doc)
        accept([:doc_key, :org_id, :total])
      end
    end

    reactive do
      id :filings
      op :source
      leaf? true
      payload_key :doc_key
      row_key [:org_id, :doc_key]
    end
  end

  defmodule Meetings do
    @moduledoc "The resolver: a meeting within the same session, not merely the same key."
    def resolve(_cell_key, attrs, opts) do
      # The judgement: the SAME meeting if the board matches and the times are
      # within an hour. Two rows already share (date, board) in a real host, so
      # a tuple cannot answer this — which is why the rung exists.
      ReactiveDag.Fixtures.RowKeyGraph.Meeting
      |> Ash.Query.do_filter(board: attrs.board, date: attrs.date)
      |> then(fn q ->
        case Keyword.get(opts, :tenant) do
          nil -> q
          t -> Ash.Query.set_tenant(q, t)
        end
      end)
      |> Ash.read!()
      |> Enum.find(fn row ->
        abs(DateTime.diff(row.starts_at, attrs.starts_at, :minute)) <= 60
      end)
    end
  end

  def tables, do: ["fx_rk_filing", "fx_rk_doc", "fx_rk_line", "fx_rk_meeting"]
end

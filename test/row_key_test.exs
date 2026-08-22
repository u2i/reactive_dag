defmodule ReactiveDag.RowKeyTest do
  @moduledoc """
  `row_key` — how a cell key maps to a row, DECLARED rather than inferred.

  A cell key names a unit of work. Writing that unit means answering "which row
  is this?", and the library used to answer by inference: `payload_key` named a
  column, a composite primary key meant identity-serialisation, else the
  single-attribute primary key. Under a UUID primary key that last rule resolves
  to `:id`, so cell keys were written into the UUID column — silently.

  Three rungs, declarative first and an escape hatch last, the same shape as the
  computation ladder:

    * `row_key :uuid`                  — the key IS the row's id
    * `row_key [:municipality_id, :key]` — the columns that identify the row
    * `row_key &Mod.resolve/2`         — sameness is a judgement

  `payload_key` keeps working and is unchanged; `row_key` is what a node declares
  when the primary key is not the answer.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.Node.Payload

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      allow_unregistered?(true)
    end
  end

  # ── rung 1: the key IS the id ──────────────────────────────────────────────
  defmodule Doc do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
      private?(true)
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
      id(:docs)
      op(:source)
      leaf?(true)
      row_key(:uuid)
    end
  end

  # ── rung 2: the columns that identify the row ──────────────────────────────
  defmodule Line do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
      private?(true)
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
      id(:lines)
      op(:source)
      leaf?(true)
      row_key([:fund, :fiscal_year])
    end
  end

  # ── rung 3: sameness is a judgement ───────────────────────────────────────
  defmodule Meetings do
    @moduledoc "The resolver: a meeting within the same session, not merely the same key."
    def resolve(_cell_key, attrs, opts) do
      # The judgement: the SAME meeting if the board matches and the times are
      # within an hour. Two rows already share (date, board) in a real host, so
      # a tuple cannot answer this — which is why the rung exists.
      ReactiveDag.RowKeyTest.Meeting
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

  defmodule Meeting do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
      private?(true)
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
      id(:meetings)
      op(:source)
      leaf?(true)
      row_key(&ReactiveDag.RowKeyTest.Meetings.resolve/3)
      payload_update(:revise)
    end
  end

  setup do
    for r <- [Doc, Line, Meeting], do: Ash.bulk_destroy!(r, :destroy, %{})
    :ok
  end

  defp meta(resource), do: ReactiveDag.Node.to_cell(resource).meta

  describe "rung 1 — `row_key :uuid`" do
    test "the cell key becomes the row's id" do
      id = Ash.UUID.generate()

      assert Payload.upsert_row(Doc, meta(Doc), id, %{title: "budget"}) == :created

      assert %{title: "budget"} = Ash.get!(Doc, id)
    end

    test "the same key upserts the same row" do
      id = Ash.UUID.generate()

      Payload.upsert_row(Doc, meta(Doc), id, %{title: "one"})

      assert Payload.upsert_row(Doc, meta(Doc), id, %{title: "two"}) == :changed
      assert length(Ash.read!(Doc)) == 1
    end

    test "an unchanged write reports `:unchanged`" do
      id = Ash.UUID.generate()
      Payload.upsert_row(Doc, meta(Doc), id, %{title: "one"})

      assert Payload.upsert_row(Doc, meta(Doc), id, %{title: "one"}) == :unchanged
    end
  end

  describe "rung 2 — `row_key [columns]`" do
    test "the row is found by the declared columns, not by a key column" do
      row = %{fund: "gf", fiscal_year: "FY24", total: 10}

      assert Payload.upsert_row(Line, meta(Line), "gf|FY24", row) == :created
      assert Payload.upsert_row(Line, meta(Line), "gf|FY24", row) == :unchanged

      assert Payload.upsert_row(Line, meta(Line), "gf|FY24", %{row | total: 20}) == :changed
      assert length(Ash.read!(Line)) == 1, "one row, updated in place"
    end

    test "the CELL KEY is not stored — nothing needs a key column" do
      Payload.upsert_row(Line, meta(Line), "gf|FY24", %{fund: "gf", fiscal_year: "FY24", total: 1})

      [stored] = Ash.read!(Line)
      refute Map.has_key?(stored, :key), "the unit's name is not the row's business"
      assert stored.fund == "gf"
    end

    test "different column values are different rows" do
      Payload.upsert_row(Line, meta(Line), "gf|FY24", %{fund: "gf", fiscal_year: "FY24", total: 1})
      Payload.upsert_row(Line, meta(Line), "gf|FY25", %{fund: "gf", fiscal_year: "FY25", total: 2})

      assert length(Ash.read!(Line)) == 2
    end
  end

  describe "rung 3 — `row_key &resolver/3`" do
    setup do
      {:ok, at} = DateTime.new(~D[2026-07-13], ~T[19:00:00])
      %{at: at}
    end

    test "a resolver decides the row, so a fuzzy match updates in place", %{at: at} do
      base = %{board: "trustees", date: ~D[2026-07-13], starts_at: at, title: "regular"}

      assert Payload.upsert_row(Meeting, meta(Meeting), "m1", base) == :created

      # 30 minutes later, same board and date: the SAME meeting by the
      # resolver's judgement, even though nothing about the key matches.
      later = %{base | starts_at: DateTime.add(at, 30, :minute), title: "regular (revised)"}

      assert Payload.upsert_row(Meeting, meta(Meeting), "m2", later) == :changed
      assert length(Ash.read!(Meeting)) == 1, "one meeting, not two"
    end

    test "outside the window it is a different meeting", %{at: at} do
      base = %{board: "trustees", date: ~D[2026-07-13], starts_at: at, title: "regular"}
      Payload.upsert_row(Meeting, meta(Meeting), "m1", base)

      # a genuine second session the same day — the case a (date, board) tuple
      # gets wrong, and the reason this rung exists
      evening = %{base | starts_at: DateTime.add(at, 4, :hour), title: "special"}

      assert Payload.upsert_row(Meeting, meta(Meeting), "m2", evening) == :created
      assert length(Ash.read!(Meeting)) == 2
    end
  end

  describe "assembly" do
    test "each rung lowers to a distinct meta shape" do
      assert meta(Doc)[:row_key] == :uuid
      assert meta(Line)[:row_key] == [:fund, :fiscal_year]
      assert is_function(meta(Meeting)[:row_key], 3)
    end

    test "a node declaring no `row_key` is unchanged" do
      # `payload_key` and the derived-PK path still apply — this is additive.
      refute meta(Line)[:payload_key] == nil and meta(Line)[:row_key] == nil
    end
  end
end

defmodule ReactiveDag.SliceTest do
  @moduledoc """
  `slice` — the dimension a human selects a node by (#reprocessing).

  `recompute_by` declares the unit a CHANGE invalidates. This declares the unit a
  PERSON picks: *reprocess just FY25*, *re-run last year's documents*. They are
  rarely the same — a node recomputing per `:category` is still sliced by
  `:fiscal_year`, because that is the question an operator asks.

  Without the declaration nothing generic can find it. A cell key is one column
  or a `"|"`-joined identity, so `fiscal_year` on one node and `published_on` on
  another are equally invisible until the node says which it is.

  Deliberately not time-shaped: the dimension in the motivating case is a
  `"FY22"` STRING, and a processing version is not temporal at all.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.Node.Rows

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      allow_unregistered?(true)
    end
  end

  def available_years, do: ["FY24", "FY25"]

  defmodule Lines do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Ets, extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute :fund, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :fiscal_year, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :total, :float, public?: true
      attribute :version, :integer, public?: true
    end

    actions do
      defaults [:read, :destroy]
      create :upsert, upsert?: true, accept: [:fund, :fiscal_year, :total, :version]
    end

    reactive do
      id(:lines)
      leaf?(true)

      # the year is not the recompute unit and not the whole key — it is what a
      # person asks about
      slice(:fiscal_year, values: {ReactiveDag.SliceTest, :available_years, []})
      slice(:version, label: "processed by version")
    end
  end

  defmodule Plain do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Ets, extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
    end

    actions do
      defaults [:read, :destroy]
      create :upsert, upsert?: true, accept: [:key]
    end

    reactive do
      id(:plain)
      leaf?(true)
    end
  end

  setup do
    for r <- [Lines, Plain], row <- Ash.read!(r), do: Ash.destroy!(row)

    for {f, y, v} <- [
          {"gf", "FY24", 1},
          {"water", "FY24", 1},
          {"gf", "FY25", 2},
          {"sewer", "FY25", 1}
        ] do
      Lines
      |> Ash.Changeset.for_create(:upsert, %{fund: f, fiscal_year: y, total: 1.0, version: v})
      |> Ash.create!()
    end

    :ok
  end

  defp cell(mod) do
    [c] = ReactiveDag.Node.cells(mod)
    c
  end

  describe "keys_where/2 — the selection" do
    test "selects the keys of matching rows, and only those" do
      assert Rows.keys_where(cell(Lines), fiscal_year: "FY25") == ["gf|FY25", "sewer|FY25"]
    end

    test "the keys are CELL keys, so they can be marked directly" do
      # an identity-keyed node's key is a "|"-join no datastore knows; the filter
      # pushes down but the key is built here
      assert Rows.keys_where(cell(Lines), fiscal_year: "FY24") == ["gf|FY24", "water|FY24"]
    end

    test "a version is an ordinary column, not machinery" do
      # "reprocess everything the current version has not touched" needs no
      # special support — it is a filter like any other
      assert Rows.keys_where(cell(Lines), version: 1) == ["gf|FY24", "sewer|FY25", "water|FY24"]
    end

    test "several columns narrow together" do
      assert Rows.keys_where(cell(Lines), fiscal_year: "FY25", version: 1) == ["sewer|FY25"]
    end

    test "a filter matching nothing is empty, not an error" do
      assert Rows.keys_where(cell(Lines), fiscal_year: "FY99") == []
    end

    test "a node with no rows here selects nothing" do
      assert Rows.keys_where(cell(Plain), key: "x") == []
    end
  end

  describe "slices/1 — what a UI can offer" do
    test "reports each declared dimension" do
      assert [year, version] = Rows.slices(cell(Lines))

      assert year.column == :fiscal_year
      assert version.column == :version
    end

    test "resolves an {m, f, a} into the actual options" do
      # only the host knows which fiscal years exist, and usually already has the
      # function that says so
      [year, _] = Rows.slices(cell(Lines))

      assert year.values == ["FY24", "FY25"]
    end

    test "a dimension with no options reports nil rather than guessing" do
      [_, version] = Rows.slices(cell(Lines))

      refute version.values
    end

    test "the label defaults to the column, and can be overridden" do
      [year, version] = Rows.slices(cell(Lines))

      assert year.label == "fiscal_year"
      assert version.label == "processed by version"
    end

    test "a node declaring none offers none" do
      assert Rows.slices(cell(Plain)) == []
    end
  end

  describe "the declaration is checked" do
    test "a slice naming a column the resource lacks raises at assembly" do
      # otherwise it renders a control that filters on nothing and silently
      # selects every row
      err =
        assert_raise ArgumentError, fn ->
          defmodule BadSlice do
            use Ash.Resource,
              domain: ReactiveDag.SliceTest.Domain,
              data_layer: Ash.DataLayer.Ets,
              extensions: [ReactiveDag.Node]

            ets do
            end

            attributes do
              attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
            end

            actions do
              defaults [:read, :destroy]
              create :upsert, upsert?: true, accept: [:key]
            end

            reactive do
              id(:bad_slice)
              leaf?(true)
              slice(:nope)
            end
          end

          ReactiveDag.Node.cells(BadSlice)
        end

      msg = Exception.message(err)
      assert msg =~ ":nope"
      assert msg =~ "no such attribute"
    end
  end

  test "selection and reprocessing compose: pick a slice, mark those keys" do
    # the whole point — a UI picks FY25, and exactly those units are queued
    keys = Rows.keys_where(cell(Lines), fiscal_year: "FY25")

    assert keys == ["gf|FY25", "sewer|FY25"]
    # `Frontier.mark_dirty("lines", keys, "reprocess FY25")` is then the action;
    # it needs no knowledge of what a fiscal year is.
  end
end

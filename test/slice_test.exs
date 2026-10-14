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
  use ReactiveDag.PostgresCase

  alias ReactiveDag.Fixtures.SliceGraph
  alias ReactiveDag.Fixtures.SliceGraph.{Lines, NotASource, Plain, Tableless, TenantedLines}

  # DELIBERATELY INCOMPLETE, and therefore inline rather than in the fixture.
  # These declare no computation, which is the thing under test: `Rows.slices/1`
  # and `Rows.poll_opts/2` read their META and never their rows. Assembly warns
  # at the `defmodule`, which is correct and expected here — in a fixture it
  # would warn on every compile of every unrelated test.

  # a SOURCE whose upstream is addressable by the same dimension its rows are
  # sliced by — and which spells it differently, which is the case `poll_as:`
  # exists for. The crawler takes `fiscal:`; the column is `fiscal_year`.
  defmodule Docs do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.SliceGraph.Domain,
      data_layer: Ash.DataLayer.Simple,
      extensions: [ReactiveDag.Node]

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :fiscal_year, :string, public?: true
      attribute :board, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]
      create :upsert, upsert?: true, accept: [:key, :fiscal_year, :board]
    end

    reactive do
      id(:docs)
      poll(ReactiveDag.Fixtures.SliceGraph.Crawler)

      slice(:fiscal_year, values: ["FY24/25", "FY25/26"], poll_as: :fiscal)
      # no `poll_as:` — the scanner spells this one the same as the column
      slice(:board)
    end
  end

  defmodule Standing do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.SliceGraph.Domain,
      data_layer: Ash.DataLayer.Simple,
      extensions: [ReactiveDag.Node]

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :fiscal_year, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]
      create :upsert, upsert?: true, accept: [:key, :fiscal_year]
    end

    reactive do
      id(:standing)
      # a STANDING arg, so the test proves a selection composes with the
      # declaration rather than replacing it
      poll(ReactiveDag.Fixtures.SliceGraph.Recorder, args: [recent: true])
      slice(:fiscal_year, values: ["FY25/26"], poll_as: :fiscal)
    end
  end

  alias ReactiveDag.Node.Rows

  def available_years, do: ["FY24", "FY25"]

  defmodule Crawler do
    @behaviour ReactiveDag.Source

    @impl true
    def poll(opts), do: %{changed: [], polled_with: opts}
  end

  setup_all do
    migrate!(SliceGraph.Domain)
    :ok
  end

  setup do
    pid = Ecto.Adapters.SQL.Sandbox.start_owner!(ReactiveDag.TestRepo, shared: true)
    on_exit(fn -> Ecto.Adapters.SQL.Sandbox.stop_owner(pid) end)

    migrate_library_tables!()
    truncate(SliceGraph.tables())
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

    test "the selection is TENANT-scoped" do
      for {org, fund} <- [{"org_a", "gf"}, {"org_a", "water"}, {"org_b", "gf"}] do
        Ash.create!(TenantedLines, %{fund: fund, fiscal_year: "FY25"},
          action: :upsert,
          tenant: org
        )
      end

      # Each tenant sees its own rows, and the key carries no tenant — the count
      # behind a "reprocess FY25" button is that municipality's, not everyone's.
      assert Rows.keys_where(cell(TenantedLines), [fiscal_year: "FY25"], tenant: "org_a") ==
               ["gf|FY25", "water|FY25"]

      assert Rows.keys_where(cell(TenantedLines), [fiscal_year: "FY25"], tenant: "org_b") ==
               ["gf|FY25"]
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
              domain: ReactiveDag.Fixtures.SliceGraph.Domain,
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

  test "a slice on a node with NO rows of its own raises, naming that" do
    # `keys_where/2` queries the node's own resource, so on a write-elsewhere
    # or escape-hatch node every button would select nothing.
    #
    # NOTE this pins the MESSAGE, not new behaviour: the column check already
    # raised here, just saying "no such attribute", which is true and
    # misleading when the answer is "no attributes at all". Disabling the
    # tableless branch still raises, so this test cannot fail on the
    # distinction — it guards the wording only.
    err =
      assert_raise ArgumentError, fn ->
        ReactiveDag.Node.cells(Tableless)
      end

    msg = Exception.message(err)
    assert msg =~ "keeps no rows of its own"
    assert msg =~ "Declare it on the node that holds the rows"
  end

  describe "poll_as — asking the SOURCE for one slice" do
    test "a slice reports what a poll should call it" do
      [year, board] = Rows.slices(cell(Docs))

      assert year.poll_as == :fiscal, "the scanner's own vocabulary"
      assert board.poll_as == :board, "defaults to the column when they agree"
    end

    test "a selection becomes the options a poll is asked with" do
      # a UI has the COLUMN — it rendered a button per value under the slice's
      # own name — and the scanner wants its own spelling. Translating here is
      # the point: neither side learns the other's.
      assert Rows.poll_opts(cell(Docs), %{"fiscal_year" => "FY25/26"}) == [fiscal: "FY25/26"]
    end

    test "atom keys work too, since a selection may not come from a form" do
      assert Rows.poll_opts(cell(Docs), fiscal_year: "FY24/25") == [fiscal: "FY24/25"]
    end

    test "a slice with no poll_as passes through under its column" do
      assert Rows.poll_opts(cell(Docs), %{"board" => "zoning"}) == [board: "zoning"]
    end

    test "several slices narrow together" do
      opts = Rows.poll_opts(cell(Docs), %{"fiscal_year" => "FY25/26", "board" => "zoning"})

      assert Enum.sort(opts) == [board: "zoning", fiscal: "FY25/26"]
    end

    test "a column this node never declared is IGNORED, not passed through" do
      # an unrecognised option would otherwise reach `poll/1` as if the node had
      # offered it, and a scanner that pattern matches its arguments would crash
      # on a typo the DSL could not vouch for
      assert Rows.poll_opts(cell(Docs), %{"fscal_year" => "FY25/26"}) == []
      assert Rows.poll_opts(cell(Docs), %{"version" => 1}) == []
    end

    test "an empty selection asks for nothing in particular" do
      assert Rows.poll_opts(cell(Docs), %{}) == []
    end

    test "the same slice still filters stored rows by its COLUMN" do
      # the two halves are independent: `poll_as` narrows the FETCH, `column`
      # narrows what is already held, and declaring one must not break the other
      [year, _] = Rows.slices(cell(Docs))

      assert year.column == :fiscal_year
      assert year.values == ["FY24/25", "FY25/26"]
    end
  end

  describe "end to end — a selection reaches the scanner" do
    test "the slice arrives under poll_as, and the declared args survive" do
      # the whole point, through the real path: `poll_opts/2` translates, and
      # `poll_cell/3` merges caller opts OVER the declared ones — so asking for
      # one year does not silently drop `recent: true`
      [cell] = ReactiveDag.Node.cells(Standing)
      plan = ReactiveDag.Graph.build([cell])

      opts = Rows.poll_opts(cell, %{"fiscal_year" => "FY25/26"})
      assert {:ok, _} = ReactiveDag.Source.refresh(plan, "standing", opts)

      assert_received {:polled_with, got}
      assert got[:fiscal] == "FY25/26"
      assert got[:recent] == true
    end
  end

  describe "poll_as is checked" do
    test "declaring it on a node with no scanner raises at assembly" do
      # it names the option a POLL is asked with, and a node nothing polls will
      # never be asked — most often the slice landed on the derived node
      # instead of the source feeding it
      err =
        assert_raise ArgumentError, fn ->
          ReactiveDag.Node.cells(NotASource)
        end

      msg = Exception.message(err)
      assert msg =~ "declares no `poll`"
      assert msg =~ "Declare it on the polling node"
    end

    test "a slice WITHOUT poll_as on a non-source is still fine" do
      # the common case: a derived node sliced for reprocess only. The check
      # must not make every existing slice declaration illegal.
      assert [%{column: :fiscal_year}, %{column: :version}] = Rows.slices(cell(Lines))
    end
  end

  test "selection and reprocessing compose: pick a slice, mark those keys" do
    # the whole point — a UI picks FY25, and exactly those units are queued
    keys = Rows.keys_where(cell(Lines), fiscal_year: "FY25")

    assert keys == ["gf|FY25", "sewer|FY25"]
    # `CascadeWorker.enqueue("lines", keys)` is then the action;
    # it needs no knowledge of what a fiscal year is.
  end
end

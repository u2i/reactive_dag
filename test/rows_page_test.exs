defmodule ReactiveDag.RowsPageTest do
  @moduledoc """
  `page_by_status/3` — reading what a cell actually HOLDS.

  `all/2` reads the whole table and `keys_by_status/3` returns keys without
  records, so neither answers "show me the 727 rows behind that count". A cell
  can hold 10,000 rows, which is why the limit goes to the datastore and the
  total comes from a COUNT rather than `length/1` on the page.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.Node.Rows

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      allow_unregistered?(true)
    end
  end

  defmodule Docs do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Ets, extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :status, :string, public?: true
      attribute :payload, :map, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        accept([:key, :status, :payload])
      end
    end

    reactive do
      id(:docs)
      leaf?(true)
      row_key([:key])
    end
  end

  defp cell do
    [c] = ReactiveDag.Node.cells(Docs)
    c
  end

  setup do
    # Keys inserted in a SHUFFLED order, so neither insertion order nor its
    # reverse happens to be sorted order. My first version inserted them
    # descending, which ETS returns in insertion order — so removing the
    # datastore sort still produced sorted pages by luck and the mutation
    # survived. A fixed seed keeps the shuffle reproducible.
    :rand.seed(:exsss, {101, 202, 303})

    1..120
    |> Enum.shuffle()
    |> Enum.each(fn i ->
      Docs
      |> Ash.Changeset.for_create(:upsert, %{
        key: "k#{String.pad_leading(to_string(i), 3, "0")}",
        status: "present",
        payload: %{"n" => i}
      })
      |> Ash.create!()
    end)

    for i <- 1..3 do
      Docs
      |> Ash.Changeset.for_create(:upsert, %{key: "f#{i}", status: "failing", payload: %{}})
      |> Ash.create!()
    end

    on_exit(fn -> Ash.DataLayer.Ets.stop(Docs) end)
    :ok
  end

  test "returns a bounded page and the FULL total" do
    # "showing 50 of 123" is the sentence this makes possible. `length/1` on the
    # page could only ever say "50 of 50".
    page = Rows.page_by_status(cell(), ["present"], limit: 50)

    assert length(page.rows) == 50
    assert page.total == 120
  end

  test "a row carries its key, status and record" do
    %{rows: [row | _]} = Rows.page_by_status(cell(), ["present"], limit: 1)

    assert row.key
    assert row.status == "present"
    assert row.record.payload
  end

  test "paging does not lose or repeat a row" do
    # PINS THE CONTRACT; cannot catch the bug. Ash's Ets data layer returns rows
    # in primary-key order on its own, so removing the datastore sort still
    # passes here — verified by mutation, and shuffling the insertion order
    # did not change it.
    #
    # The failure is a Postgres one: `LIMIT`/`OFFSET` with no `ORDER BY` has no
    # guaranteed order, so page 2 may repeat a row from page 1 and another row
    # is never seen. `real_postgres_rows_test.exs` asserts it where it can fail.
    #
    # Same gap `rows_aggregate_test.exs` records for the aggregate pushdown:
    # the tests pin the contract, the behaviour lives where the rows do.
    p1 = Rows.page_by_status(cell(), ["present"], limit: 50, offset: 0)
    p2 = Rows.page_by_status(cell(), ["present"], limit: 50, offset: 50)
    p3 = Rows.page_by_status(cell(), ["present"], limit: 50, offset: 100)

    keys = Enum.map(p1.rows ++ p2.rows ++ p3.rows, & &1.key)

    assert length(keys) == 120
    assert keys == Enum.uniq(keys), "these rows appeared on two pages: #{inspect(keys -- Enum.uniq(keys))}"
    assert keys == Enum.sort(keys), "pages are not in key order, so the boundary is arbitrary"
  end

  test "filters by status" do
    assert Rows.page_by_status(cell(), ["failing"]).total == 3
    assert Rows.page_by_status(cell(), ["present"]).total == 120
  end

  test "a node with no rows of its own returns an empty page, not a crash" do
    # A `compose` leg has no resource; that is a shape, not a fault.
    assert Rows.page_by_status(%{}, ["present"]) == %{rows: [], total: 0}
  end
end

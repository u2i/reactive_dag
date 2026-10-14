defmodule ReactiveDag.StaleGrainTest do
  @moduledoc """
  THE MOVE CASE — the whole reason `:group` is not `:identity`.

  A fold's child row is a UNIT (a fund-year total), not a copy of a parent row.
  When a parent row changes which unit it belongs to, TWO units go stale: the
  one it joined, and the one it left. Only the first is findable by looking at
  where the row is now. The second is findable only by looking at where it WAS,
  which is what the second pinned read is for.

  Getting this wrong is the quiet failure: the unit the row left keeps a total
  that still counts it, and nothing reports a problem.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.Stale

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false
    resources(do: allow_unregistered?(true))
  end

  defmodule Lines do
    use Ash.Resource,
      domain: ReactiveDag.StaleGrainTest.Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets(do: private?(true))

    temporal do
      attribute :valid_at
      recorded_at :recorded_at
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :fy, :string, public?: true
      attribute :amount, :float, public?: true

      attribute :recorded_at, :utc_datetime_usec,
        public?: true,
        default: &DateTime.utc_now/0,
        update_default: &DateTime.utc_now/0
    end

    actions do
      defaults [:read, :destroy]

      create :upsert,
        upsert?: true,
        accept: [:key, :fy, :amount],
        upsert_fields: [:fy, :amount, :recorded_at]
    end

    reactive do
      id(:lines)
      leaf?(true)
    end
  end

  defmodule Rollups do
    use Ash.Resource,
      domain: ReactiveDag.StaleGrainTest.Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets(do: private?(true))

    temporal do
      attribute :valid_at
      recorded_at :recorded_at
    end

    attributes do
      attribute :fy, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :total, :float, public?: true

      attribute :recorded_at, :utc_datetime_usec,
        public?: true,
        default: &DateTime.utc_now/0,
        update_default: &DateTime.utc_now/0
    end

    actions do
      defaults [:read, :destroy]

      create :upsert,
        upsert?: true,
        accept: [:fy, :total],
        upsert_fields: [:total, :recorded_at]
    end

    reactive do
      id(:rollups)
      op(:fold)
      recompute_by([fy: :fy], to: :lines)
      reduce(into: [sum: [amount: :total]])
    end
  end

  defp plan, do: ReactiveDag.Node.graph([Lines, Rollups])

  defp write(resource, attrs) do
    resource |> Ash.Changeset.for_create(:upsert, attrs) |> Ash.create!()
  end

  setup do
    for r <- [Lines, Rollups], row <- Ash.read!(r), do: Ash.destroy!(row)
    :ok
  end

  test "the rule lowers to :group, so the grain join is what gets asked" do
    assert [{"lines", "rollups", :group}] = Stale.edges(plan())
  end

  test "nothing derived yet is not stale — it is unstarted" do
    write(Lines, %{key: "l1", fy: "FY24", amount: 10.0})

    assert {:ok, []} = Stale.on_edge(plan(), "lines", "rollups")
  end

  test "a parent that moved after its unit was derived marks that unit" do
    write(Lines, %{key: "l1", fy: "FY24", amount: 10.0})
    Process.sleep(5)
    write(Rollups, %{fy: "FY24", total: 10.0})

    assert {:ok, []} = Stale.on_edge(plan(), "lines", "rollups"),
           "derived after its input — nothing is behind"

    Process.sleep(5)
    write(Lines, %{key: "l1", fy: "FY24", amount: 99.0})

    assert {:ok, ["FY24"]} = Stale.on_edge(plan(), "lines", "rollups")
  end

  test "A ROW THAT CHANGED UNIT MARKS BOTH — the one it joined and the one it left" do
    write(Lines, %{key: "l1", fy: "FY24", amount: 10.0})
    Process.sleep(5)
    write(Rollups, %{fy: "FY24", total: 10.0})
    write(Rollups, %{fy: "FY25", total: 0.0})

    assert {:ok, []} = Stale.on_edge(plan(), "lines", "rollups")

    Process.sleep(5)
    # the move: same row, different unit. Nothing propagates.
    write(Lines, %{key: "l1", fy: "FY25", amount: 10.0})

    assert {:ok, units} = Stale.on_edge(plan(), "lines", "rollups")

    assert Enum.sort(units) == ["FY24", "FY25"],
           """
           Expected BOTH units. Got #{inspect(Enum.sort(units))}.

           FY25 gained the row. FY24 still holds a total that counts a row no
           longer in it — and claiming only FY25 leaves that total wrong with
           nothing reporting it.
           """
  end
end

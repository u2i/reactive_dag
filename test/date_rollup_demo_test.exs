defmodule ReactiveDag.DateRollupDemoTest do
  @moduledoc """
  THE CLASSIC, end to end: date-marked records → a time-bucketed aggregate,
  driven through the real drain so the Report shows the incremental story —
  touch ONE reading, and exactly one month's rollup change propagates.

  The Ash-first shape: the calendar bucket is an ASH CALCULATION on the data's
  own resource (`ReactiveDag.Calendar`), and the rollup just groups by it —
  the bucket label is the group column AND the derived cell key.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.{Drain, Frontier}

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      allow_unregistered?(true)
    end
  end

  defmodule Readings do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
      private?(true)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :date, :date, public?: true
      attribute :value, :float, public?: true
    end

    calculations do
      # the bucket lives with the data — any Ash consumer can use it, and a
      # Postgres host could swap in an expr/fragment version for pushdown.
      calculate :month, :string, {ReactiveDag.Calendar, bucket: :month, of: :date}
    end

    actions do
      defaults [:read, :destroy]

      create :create do
        accept([:key, :date, :value])
      end

      update :revise do
        accept([:value])
      end
    end

    reactive do
      id(:readings)
      op(:source)
      leaf?(true)
    end
  end

  defmodule MonthlyReadings do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
      private?(true)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :month, :string, public?: true
      attribute :total, :float, public?: true
      attribute :n, :integer, public?: true
    end

    identities do
      identity :by_key, [:key], pre_check_with: ReactiveDag.DateRollupDemoTest.Domain
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        upsert_identity(:by_key)
        accept([:key, :month, :total, :n])
      end
    end

    reactive do
      id(:monthly_readings)
      op(:fold)
      # THE UNIT: recompute by month, resolved PURELY from the changed key's
      # leading segments (`from_key: true`) — the key's date prefix relabels
      # through the SAME Calendar calculation `group_by` names. No data lookup,
      # deletion-safe, and the library auto-scopes the read to the claimed
      # months' date range through the same plan.
      recompute_by :month, to: :readings, from_key: true

      reduce group_by: [:month],
             into: [sum: [value: :total], count: :n]
    end
  end

  # a same-grain consumer (:identity): proves only the touched month propagates.
  defmodule MonthHealth do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :status, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        accept([:key, :status])
      end
    end

    reactive do
      id(:month_health)
      op(:check)

      reduce over: :monthly_readings,
             group_by: :key,
             into: fn _month, [row | _] -> %{status: if(row.n > 0, do: "present", else: "failing")} end
    end
  end

  # in-memory dirty table (the drain_test pattern — no Postgres in this suite)
  defmodule FakeRepo do
    def start_link, do: Agent.start_link(fn -> MapSet.new() end, name: __MODULE__)

    def query!("INSERT INTO " <> _, params) do
      params
      |> Enum.chunk_every(6)
      |> Enum.each(fn [cell, _tenant, key, _r, _t, _prior] -> Agent.update(__MODULE__, &MapSet.put(&1, {cell, key})) end)

      %{rows: []}
    end

    def query!("SELECT DISTINCT cell_id" <> _, _params) do
      ids = Agent.get(__MODULE__, & &1) |> Enum.map(&elem(&1, 0)) |> Enum.uniq()
      %{rows: Enum.map(ids, &[&1])}
    end

    def query!("DELETE FROM " <> _, [cell | _tenant]) do
      keys =
        Agent.get_and_update(__MODULE__, fn set ->
          {mine, rest} = Enum.split_with(set, fn {c, _} -> c == cell end)
          {Enum.map(mine, &elem(&1, 1)), MapSet.new(rest)}
        end)

      %{rows: Enum.map(keys, &[&1, nil])}
    end

    def query!("SELECT COUNT" <> _, _params), do: %{rows: [[Agent.get(__MODULE__, &MapSet.size/1)]]}
  end


  setup do
    start_supervised!(%{id: FakeRepo, start: {FakeRepo, :start_link, []}})
    prev_repo = Application.get_env(:reactive_dag, :repo)
    Application.put_env(:reactive_dag, :repo, FakeRepo)

    on_exit(fn ->
      Application.put_env(:reactive_dag, :repo, prev_repo)
    end)

    # date-PREFIXED keys — the key carries its coordinates, so the bucket rule
    # is pure string work and a deleted reading still names the month it left.
    for {k, date, v} <- [
          {"2026-07-03|r1", ~D[2026-07-03], 1.0},
          {"2026-07-19|r2", ~D[2026-07-19], 2.0},
          {"2026-08-05|r3", ~D[2026-08-05], 4.0},
          {"2026-08-11|r4", ~D[2026-08-11], 8.0}
        ] do
      Readings |> Ash.Changeset.for_create(:create, %{key: k, date: date, value: v}) |> Ash.create!()
    end

    :ok
  end

  defp plan, do: ReactiveDag.Node.graph([Readings, MonthlyReadings, MonthHealth])
  defp drain(plan),
    do: Drain.run(plan, recompute: ReactiveDag.Node.Recompute, key_rule: ReactiveDag.Node.KeyRule)

  test "the classic: readings roll up per month; touching one reading moves ONE month" do
    plan = plan()

    # ── first drain: everything computes ─────────────────────────────────────
    Frontier.mark_dirty("readings", ["*"], "seed")
    {:ok, _report} = drain(plan)

    rows = MonthlyReadings |> Ash.read!() |> Map.new(&{&1.key, &1})
    assert %{month: "2026-07", total: 3.0, n: 2} = rows["2026-07"]
    assert %{month: "2026-08", total: 12.0, n: 2} = rows["2026-08"]

    # ── revise ONE August reading, mark it dirty, drain again ────────────────
    Readings
    |> Ash.get!("2026-08-11|r4")
    |> Ash.Changeset.for_update(:revise, %{value: 10.0})
    |> Ash.update!()

    Frontier.mark_dirty("readings", ["2026-08-11|r4"], "revised")
    {:ok, report} = drain(plan)

    steps = Map.new(report.steps, &{&1.cell, &1})

    # `recompute_by :month, from_key: true` mapped the changed reading key to
    # ITS month by pure string work — the rollup was claimed per-unit, not
    # whole-cell, and the library scoped the read to that month's date range.
    assert steps["monthly_readings"].claimed == ["2026-08"]
    assert steps["monthly_readings"].changed == ["2026-08"]

    # and the same-grain consumer was dirtied for EXACTLY that month
    assert steps["month_health"].claimed == ["2026-08"]
    assert steps["month_health"].triggered_by == "monthly_readings"

    # July's row is untouched; August reflects the revision
    rows = MonthlyReadings |> Ash.read!() |> Map.new(&{&1.key, &1})
    assert rows["2026-07"].total == 3.0
    assert rows["2026-08"].total == 14.0
  end

  test "the granularity ladder is PURE relabeling — day keys claim their month" do
    alias ReactiveDag.{Calendar, Cell}

    # a daily cell's keys ARE day labels; its monthly parent relabels purely
    # through the group plan assembly would stamp (a :month Calendar calc).
    # NB this builds the CELL directly, so it carries the internal rule
    # `{:group, from: :key}` that `recompute_by ... from_key: true` lowers to.
    monthly = %Cell{
      id: "monthly",
      meta: %{
        key_rule: {:group, from: :key},
        reduce: %ReactiveDag.Node.Reduce{group_by: [:month]},
        over_source: %{group_key_plan: [{:calendar, :month, :day_date}]}
      }
    }

    assert ReactiveDag.Node.KeyRule.rule(monthly, "daily", ["2026-08-11", "2026-08-30"]) ==
             {:keys, ["2026-08"]}

    # a key whose leading segment isn't date-shaped degrades the whole
    # propagation to :all — correctness over precision (e.g. a deleted entry
    # whose key carries no coordinates)
    assert ReactiveDag.Node.KeyRule.rule(monthly, "daily", ["2026-08-11", "oops"]) == :all

    # the pure inverse trio behind it
    assert Calendar.parse("2026-08") == {:month, ~D[2026-08-01]}
    assert Calendar.parse("2026-Q3") == {:quarter, ~D[2026-07-01]}
    assert Calendar.parse("not-a-label") == :error
    assert Calendar.range(:month, "2026-08") == {~D[2026-08-01], ~D[2026-09-01]}
    assert Calendar.range(:year, "2026") == {~D[2026-01-01], ~D[2027-01-01]}
    assert Calendar.bucket_of_key(:month, "2026-08-11|r4") == "2026-08"
    assert Calendar.bucket_of_key(:quarter, "2026-08") == "2026-Q3"
    assert Calendar.bucket_of_key(:month, "r4") == :error
  end

  test "bucket labels: the calculation is ordinary Ash, loaded by the library" do
    # read the calculation directly — nothing reactive_dag-specific about it
    reading =
      Readings |> Ash.Query.load(:month) |> Ash.read!() |> Enum.find(&(&1.key =~ "r1"))

    assert reading.month == "2026-07"

    # and the other bucket kinds
    assert ReactiveDag.Calendar.label(:day, ~D[2026-08-11]) == "2026-08-11"
    assert ReactiveDag.Calendar.label(:week, ~D[2026-08-11]) == "2026-W33"
    assert ReactiveDag.Calendar.label(:quarter, ~D[2026-08-11]) == "2026-Q3"
    assert ReactiveDag.Calendar.label(:year, ~D[2026-08-11]) == "2026"
    assert ReactiveDag.Calendar.label(:month, nil) == nil
  end
end

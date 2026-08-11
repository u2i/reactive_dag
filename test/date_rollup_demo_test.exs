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
      # a month's grain differs from a reading's → whole-cell recompute; the
      # payload loop's change detection keeps PROPAGATION per-month.
      key_rule(:all)

      reduce over: :readings,
             group_by: [:month],
             into: [sum: [value: :total], count: :n]
    end
  end

  # a same-grain consumer (:identity): proves only the touched month propagates.
  defmodule MonthHealth do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Simple,
      extensions: [ReactiveDag.Node]

    reactive do
      id(:month_health)
      op(:check)
      verdict?(true)

      reduce over: :monthly_readings,
             group_by: :key,
             status: fn _month, [row | _] -> if row.n > 0, do: "present", else: "failing" end
    end
  end

  # in-memory dirty table (the drain_test pattern — no Postgres in this suite)
  defmodule FakeRepo do
    def start_link, do: Agent.start_link(fn -> MapSet.new() end, name: __MODULE__)

    def query!("INSERT INTO " <> _, params) do
      params
      |> Enum.chunk_every(4)
      |> Enum.each(fn [cell, key, _r, _t] -> Agent.update(__MODULE__, &MapSet.put(&1, {cell, key})) end)

      %{rows: []}
    end

    def query!("SELECT DISTINCT cell_id" <> _, _params) do
      ids = Agent.get(__MODULE__, & &1) |> Enum.map(&elem(&1, 0)) |> Enum.uniq()
      %{rows: Enum.map(ids, &[&1])}
    end

    def query!("DELETE FROM " <> _, [cell]) do
      keys =
        Agent.get_and_update(__MODULE__, fn set ->
          {mine, rest} = Enum.split_with(set, fn {c, _} -> c == cell end)
          {Enum.map(mine, &elem(&1, 1)), MapSet.new(rest)}
        end)

      %{rows: Enum.map(keys, &[&1])}
    end

    def query!("SELECT COUNT" <> _, _params), do: %{rows: [[Agent.get(__MODULE__, &MapSet.size/1)]]}
  end

  defmodule NullWriter do
    @behaviour ReactiveDag.CoordinationWriter
    @impl true
    def put(_cell_id, _key, _opts), do: :ok
    @impl true
    def delete(_cell_id, _keys), do: :ok
  end

  setup do
    {:ok, _} = FakeRepo.start_link()
    prev_repo = Application.get_env(:reactive_dag, :repo)
    prev_writer = Application.get_env(:reactive_dag, :coordination_writer)
    Application.put_env(:reactive_dag, :repo, FakeRepo)
    Application.put_env(:reactive_dag, :coordination_writer, NullWriter)

    on_exit(fn ->
      Application.put_env(:reactive_dag, :repo, prev_repo)
      Application.put_env(:reactive_dag, :coordination_writer, prev_writer)
    end)

    for {k, date, v} <- [
          {"r1", ~D[2026-07-03], 1.0},
          {"r2", ~D[2026-07-19], 2.0},
          {"r3", ~D[2026-08-05], 4.0},
          {"r4", ~D[2026-08-11], 8.0}
        ] do
      Readings |> Ash.Changeset.for_create(:create, %{key: k, date: date, value: v}) |> Ash.create!()
    end

    :ok
  end

  # the issue's item 3: a HOST KeyRule mapping changed reading keys → their
  # month keys keeps the rollup's claim per-bucket instead of whole-cell. The
  # mapping consults the data (the same :month calculation); a changed key it
  # can't resolve (a deleted reading) escalates to :all — vanish must reprice
  # everything it might have left.
  defmodule MonthRule do
    @behaviour ReactiveDag.KeyRule

    @impl true
    def rule(%{id: "monthly_readings"}, _child, changed) do
      found =
        Readings
        |> Ash.Query.do_filter([{:key, [in: changed]}])
        |> Ash.Query.load(:month)
        |> Ash.read!()

      if length(found) == length(changed),
        do: {:keys, found |> Enum.map(& &1.month) |> Enum.uniq()},
        else: :all
    end

    def rule(parent, child, changed), do: ReactiveDag.Node.KeyRule.rule(parent, child, changed)
  end

  defp plan, do: ReactiveDag.Node.graph([Readings, MonthlyReadings, MonthHealth])
  defp drain(plan),
    do: Drain.run(plan, recompute: ReactiveDag.Node.Recompute, key_rule: MonthRule)

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
    |> Ash.get!("r4")
    |> Ash.Changeset.for_update(:revise, %{value: 10.0})
    |> Ash.update!()

    Frontier.mark_dirty("readings", ["r4"], "revised")
    {:ok, report} = drain(plan)

    steps = Map.new(report.steps, &{&1.cell, &1})

    # the host MonthRule mapped the changed reading to ITS month — the rollup
    # was claimed per-bucket, not whole-cell. (The library's payload-key
    # auto-scope correctly stands down here: the claim is month-grain, the
    # read is reading-grain — key_rule :all disables the identity filter.)
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

  test "bucket labels: the calculation is ordinary Ash, loaded by the library" do
    # read the calculation directly — nothing reactive_dag-specific about it
    reading = Readings |> Ash.Query.load(:month) |> Ash.read!() |> Enum.find(&(&1.key == "r1"))
    assert reading.month == "2026-07"

    # and the other bucket kinds
    assert ReactiveDag.Calendar.label(:day, ~D[2026-08-11]) == "2026-08-11"
    assert ReactiveDag.Calendar.label(:week, ~D[2026-08-11]) == "2026-W33"
    assert ReactiveDag.Calendar.label(:quarter, ~D[2026-08-11]) == "2026-Q3"
    assert ReactiveDag.Calendar.label(:year, ~D[2026-08-11]) == "2026"
    assert ReactiveDag.Calendar.label(:month, nil) == nil
  end
end

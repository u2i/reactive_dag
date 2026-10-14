defmodule ReactiveDag.StaleGrainPostgresTest do
  @moduledoc """
  The grain join, against a REAL temporal table.

  `stale_grain_test.exs` proves the logic on ETS. This proves it survives the
  things ETS does not have:

    * a `WITHOUT OVERLAPS` primary key, which actually ENFORCES non-overlap —
      ETS stores `valid_at` as a plain field and would accept two periods
      covering the same instant for the same key.

    * AshPostgres's temporal upsert, which cannot use `ON CONFLICT` against an
      exclusion PK and instead emits data-modifying CTEs that split the period
      by hand. That is a completely different code path from the ETS upsert,
      and it is the one that has to get `recorded_at` right.

    * `Ash.Query.as_of` lowering to SQL. The historical read is the whole
      mechanism of the move case — if the period predicate is wrong in SQL,
      the "before" read returns the row AFTER the change and the join silently
      claims one unit instead of two.
  """
  use ReactiveDag.PostgresCase

  alias ReactiveDag.Stale

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false
    resources(do: allow_unregistered?(true))
  end

  defmodule Lines do
    use Ash.Resource,
      domain: ReactiveDag.StaleGrainPostgresTest.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "rd_grain_lines"
      repo(ReactiveDag.TestRepo)
    end

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
      domain: ReactiveDag.StaleGrainPostgresTest.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "rd_grain_rollups"
      repo(ReactiveDag.TestRepo)
    end

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

  setup_all do
    temporal_table(
      "rd_grain_lines",
      [
        {"key", "text NOT NULL"},
        {"fy", "text"},
        {"amount", "double precision"},
        {"recorded_at", "timestamptz NOT NULL"},
        {"valid_at", "tstzrange NOT NULL"}
      ],
      keys: ["key"],
      period: "valid_at"
    )

    temporal_table(
      "rd_grain_rollups",
      [
        {"fy", "text NOT NULL"},
        {"total", "double precision"},
        {"recorded_at", "timestamptz NOT NULL"},
        {"valid_at", "tstzrange NOT NULL"}
      ],
      keys: ["fy"],
      period: "valid_at"
    )

    on_exit(fn ->
      exec("DROP TABLE IF EXISTS rd_grain_lines")
      exec("DROP TABLE IF EXISTS rd_grain_rollups")
    end)

    :ok
  end

  setup do
    # SHARED, not per-test-process: the temporal upsert runs its CTEs through
    # the repo directly, and an Ash read may resolve on another process.
    pid = Ecto.Adapters.SQL.Sandbox.start_owner!(ReactiveDag.TestRepo, shared: true)
    on_exit(fn -> Ecto.Adapters.SQL.Sandbox.stop_owner(pid) end)

    truncate(["rd_grain_lines", "rd_grain_rollups"])
    :ok
  end

  defp plan, do: ReactiveDag.Node.graph([Lines, Rollups])

  defp write(resource, attrs) do
    resource |> Ash.Changeset.for_create(:upsert, attrs) |> Ash.create!()
  end

  test "the table really is WITHOUT OVERLAPS" do
    # Guards the fixture itself. On a plain primary key every other test in
    # this file would still pass while testing nothing about history — the
    # table would simply hold one version per key, exactly as ETS does.
    %{rows: [[definition]]} =
      exec_sandboxed("""
      SELECT pg_get_constraintdef(oid)
      FROM pg_constraint
      WHERE conrelid = 'rd_grain_lines'::regclass AND contype = 'p'
      """)

    assert definition =~ "WITHOUT OVERLAPS",
           "expected a period primary key, got: #{definition}"
  end

  test "an upsert SPLITS the period rather than overwriting it" do
    write(Lines, %{key: "l1", fy: "FY24", amount: 10.0})
    Process.sleep(5)
    write(Lines, %{key: "l1", fy: "FY25", amount: 10.0})

    %{rows: [[versions]]} =
      exec_sandboxed("SELECT count(*) FROM rd_grain_lines WHERE key = 'l1'")

    assert versions == 2,
           """
           Expected two periods — the history the move case reads.
           Got #{versions}. An upsert that overwrote in place would leave one,
           and the "before" read would have nothing older to find.
           """
  end

  test "recorded_at MOVES on upsert through the CTE path" do
    # The ETS answer to this is in `spike/temporal_upsert_recorded_at.exs`.
    # The CTE path is different code and gets its own answer: a stamp that
    # stuck at its create value would make staleness report that nothing moved.
    first = write(Lines, %{key: "l1", fy: "FY24", amount: 10.0})
    Process.sleep(5)
    second = write(Lines, %{key: "l1", fy: "FY24", amount: 99.0})

    assert DateTime.compare(second.recorded_at, first.recorded_at) == :gt,
           "recorded_at did not advance: #{inspect(first.recorded_at)} -> #{inspect(second.recorded_at)}"
  end

  test "a parent that moved after its unit was derived marks that unit" do
    write(Lines, %{key: "l1", fy: "FY24", amount: 10.0})
    Process.sleep(5)
    write(Rollups, %{fy: "FY24", total: 10.0})

    assert {:ok, []} = Stale.on_edge(plan(), "lines", "rollups")

    Process.sleep(5)
    write(Lines, %{key: "l1", fy: "FY24", amount: 99.0})

    assert {:ok, ["FY24"]} = Stale.on_edge(plan(), "lines", "rollups")
  end

  test "A ROW THAT CHANGED UNIT MARKS BOTH — through real SQL history" do
    write(Lines, %{key: "l1", fy: "FY24", amount: 10.0})
    Process.sleep(5)
    write(Rollups, %{fy: "FY24", total: 10.0})
    write(Rollups, %{fy: "FY25", total: 0.0})

    assert {:ok, []} = Stale.on_edge(plan(), "lines", "rollups")

    Process.sleep(5)
    write(Lines, %{key: "l1", fy: "FY25", amount: 10.0})

    assert {:ok, units} = Stale.on_edge(plan(), "lines", "rollups")

    assert Enum.sort(units) == ["FY24", "FY25"],
           """
           Expected BOTH units. Got #{inspect(Enum.sort(units))}.

           This is the assertion that needs a real database: the "before" read
           is `Ash.Query.as_of` lowered to a period predicate in SQL. If that
           predicate is wrong, the read returns the row AFTER the move and
           FY24 — still holding a total that counts a row no longer in it —
           goes unclaimed.
           """
  end
end

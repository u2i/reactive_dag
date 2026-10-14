defmodule ReactiveDag.MigrationUpgradeTest do
  @moduledoc """
  The upgrade path a host on the OLD schema has to take.

  `up/1` now creates `recorded_at`, but a host that already ran it has
  `version_id NOT NULL` and no `recorded_at` — so without
  `suspensions_to_recorded_at/1` the new code would write to a column that
  does not exist. This proves the swap works against the real old shape rather
  than against a description of it.
  """
  use ReactiveDag.PostgresCase

  @table "rd_upgrade_susp"

  setup do
    # No sandbox checkout: everything here is DDL on its own connection, and a
    # sandboxed transaction would hide the migrations' committed work.
    exec("DROP TABLE IF EXISTS #{@table}")
    on_exit(fn -> exec("DROP TABLE IF EXISTS #{@table}") end)
    :ok
  end

  defmodule OldSchema do
    @moduledoc false
    use Ecto.Migration

    def up do
      # The pre-temporal shape, as `up/1` used to emit it.
      create table("rd_upgrade_susp", primary_key: false) do
        add(:id, :text, primary_key: true)
        add(:tenant, :text, null: false, default: "*")
        add(:row_uuid, :text, null: false, default: "*")
        add(:version_id, :text, null: false, default: "*")
      end
    end
  end

  defmodule Upgrade do
    @moduledoc false
    use Ecto.Migration

    def up, do: ReactiveDag.Migration.suspensions_to_recorded_at(suspension_table: "rd_upgrade_susp")
  end

  # Read OUTSIDE the sandbox: the migrations commit on their own connection,
  # and a sandboxed read sits in a transaction that began before them.
  defp columns do
    %{rows: rows} =
      exec("SELECT column_name, is_nullable FROM information_schema.columns WHERE table_name = $1", [
        @table
      ])

    Map.new(rows, fn [c, n] -> {c, n} end)
  end

  # A version unique ACROSS RUNS. `schema_migrations` persists in a long-lived
  # container, and `System.unique_integer/1` restarts at 1 in each VM — so a
  # counter repeats, the migrator silently runs NOTHING (it returns `[]`), the
  # table is absent and the assertion fails on a fixture that was never built.
  # Measured; that is exactly what happened.
  defp run(mod) do
    version = System.system_time(:microsecond)
    {:ok, repo} = ReactiveDag.MigrationRepo.start_link()

    try do
      [_ | _] =
        Ecto.Migrator.run(ReactiveDag.MigrationRepo, [{version, mod}], :up, all: true, log: false)
    after
      Supervisor.stop(repo)
    end
  end

  test "the swap removes version_id and adds a NULLABLE recorded_at" do
    exec("DROP TABLE IF EXISTS #{@table}")
    run(OldSchema)

    before = columns()
    assert Map.has_key?(before, "version_id"), "fixture is not the old shape"
    refute Map.has_key?(before, "recorded_at")

    run(Upgrade)

    after_ = columns()

    refute Map.has_key?(after_, "version_id"),
           "the old column survived, so a host would carry a NOT NULL column nothing writes"

    assert after_["recorded_at"] == "YES",
           "recorded_at must be nullable — NULL is how an unattributable change is recorded"
  end
end

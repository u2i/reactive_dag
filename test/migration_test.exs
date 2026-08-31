defmodule ReactiveDag.MigrationTest do
  @moduledoc """
  Table-name resolution for the library-owned DDL. Regression: `up/1` used to
  read only its table-name OPTION, so a host that set the config (as the guide
  instructs) migrated the default name while the runtime queried the configured
  one — every read empty, and the engine a silent no-op.

  The bug is table-agnostic, so the test moved with the table: what matters is
  that `Migration` and `Suspension` resolve the name the same way, with no
  second place to keep in sync.
  """
  use ExUnit.Case, async: false

  setup do
    prev = Application.get_env(:reactive_dag, :suspension_table)

    on_exit(fn ->
      if prev,
        do: Application.put_env(:reactive_dag, :suspension_table, prev),
        else: Application.delete_env(:reactive_dag, :suspension_table)
    end)

    :ok
  end

  test "resolves option > config > default — matching Suspension's reads" do
    Application.delete_env(:reactive_dag, :suspension_table)
    assert ReactiveDag.Migration.table_name() == "reactive_dag_suspension"
    assert ReactiveDag.Suspension.table() == "reactive_dag_suspension"

    Application.put_env(:reactive_dag, :suspension_table, "my_suspensions")
    assert ReactiveDag.Migration.table_name() == "my_suspensions"

    assert ReactiveDag.Suspension.table() == "my_suspensions",
           "the migrated table and the queried table must not diverge"

    assert ReactiveDag.Migration.table_name(suspension_table: "override") == "override"
  end

  test "an invalid table name fails loudly, at read time" do
    Application.put_env(:reactive_dag, :suspension_table, "my suspensions")

    assert_raise ArgumentError, ~r/not a valid table identifier/, fn ->
      ReactiveDag.Suspension.table()
    end
  end

  test "drop_dirty resolves the OLD table, separately" do
    assert ReactiveDag.Migration.dirty_table_name() == "reactive_dag_dirty"
    assert ReactiveDag.Migration.dirty_table_name(dirty_table: "legacy") == "legacy"
  end
end

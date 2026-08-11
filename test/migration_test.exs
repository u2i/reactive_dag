defmodule ReactiveDag.MigrationTest do
  @moduledoc """
  Table-name resolution for the library-owned frontier DDL. Regression: `up/1`
  used to read only its `:dirty_table` OPTION, so a host that set
  `config :reactive_dag, dirty_table:` (as the guide instructs) migrated
  `reactive_dag_dirty` while the runtime queried the configured name — every
  claim empty, the drain a silent no-op.
  """
  use ExUnit.Case, async: false

  setup do
    prev = Application.get_env(:reactive_dag, :dirty_table)
    on_exit(fn ->
      if prev,
        do: Application.put_env(:reactive_dag, :dirty_table, prev),
        else: Application.delete_env(:reactive_dag, :dirty_table)
    end)

    :ok
  end

  test "resolves option > config > default — matching Frontier's reads" do
    Application.delete_env(:reactive_dag, :dirty_table)
    assert ReactiveDag.Migration.table_name() == "reactive_dag_dirty"

    Application.put_env(:reactive_dag, :dirty_table, "my_dirty")
    assert ReactiveDag.Migration.table_name() == "my_dirty"
    assert ReactiveDag.Migration.table_name(dirty_table: "override") == "override"
  end
end

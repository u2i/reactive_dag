defmodule ReactiveDag.Migration do
  @moduledoc """
  The library-owned DDL, callable from a host migration — the dirty-frontier
  table `ReactiveDag.Frontier` reads and writes (coalesced by `(cell_id, key)`;
  claim is a `DELETE … RETURNING`):

      defmodule MyApp.Repo.Migrations.AddReactiveDag do
        use Ecto.Migration

        def up, do: ReactiveDag.Migration.up()
        def down, do: ReactiveDag.Migration.down()
      end

  The table name resolves exactly as `Frontier`'s reads do — an explicit
  `:dirty_table` option, else `config :reactive_dag, dirty_table:`, else
  `"reactive_dag_dirty"` — so a host that sets the config gets a migration
  matching the table the runtime queries, with no second place to keep in sync.

  Options (both directions):

    * `:dirty_table` — override the resolved table name for this migration
      only (rare; the config is the normal home).

  The coordination TUPLE table is deliberately not created here: its schema is
  host-extended (extension columns like `strength` ride the host's
  `CoordinationWriter`), so its migration belongs to the host.
  """
  use Ecto.Migration

  @default_dirty "reactive_dag_dirty"

  @doc false
  # option > config > default — the same resolution Frontier's reads use, so
  # the migrated table and the queried table cannot silently diverge.
  def table_name(opts \\ []) do
    Keyword.get_lazy(opts, :dirty_table, fn ->
      Application.get_env(:reactive_dag, :dirty_table, @default_dirty)
    end)
  end

  @doc "Create the dirty-frontier table + its coalescing unique index."
  def up(opts \\ []) do
    name = table_name(opts)

    create_if_not_exists table(name, primary_key: false) do
      add(:cell_id, :text, null: false)
      add(:key, :text, null: false)
      add(:reason, :text)
      add(:enqueued_at, :utc_datetime_usec)
      # the row as it was when marked — see ReactiveDag.Frontier. Nullable: a
      # source-fed key has no Ash record behind it. Cheap because this table is
      # a QUEUE: rows live from mark to claim, then DELETE … RETURNING removes
      # them.
      add(:prior, :map)
    end

    # one index serves everything: uniqueness backs mark_dirty's ON CONFLICT,
    # and its leading column (cell_id) covers claim's WHERE and next_cell's
    # SELECT DISTINCT cell_id as an index-only scan.
    create_if_not_exists(unique_index(name, [:cell_id, :key]))
  end

  @doc "Drop the dirty-frontier table."
  def down(opts \\ []) do
    name = table_name(opts)
    drop_if_exists(unique_index(name, [:cell_id, :key]))
    drop_if_exists(table(name))
  end
end

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

  This is the ONLY table the library owns. Every node's results live in that
  node's own resource, with its own migration — there is no second table
  shadowing them.
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
      # WHICH GRAPH this work belongs to. One frontier serves every plan in the
      # application, so without it "a cell this plan does not know" and "a cell
      # nobody owns" are the same observation — and they need opposite handling:
      # another tenant's row must be left alone, an orphaned row must be cleared.
      #
      # NOT NULL with a sentinel rather than nullable, because the unique index
      # below backs `mark_dirty`'s ON CONFLICT and Postgres treats NULLs as
      # DISTINCT in a unique index: a nullable column would silently stop
      # untenanted marks coalescing, growing a queue row per mark.
      add(:tenant, :text, null: false, default: "*")
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
    # and its leading columns cover claim's WHERE and next_cell's
    # SELECT DISTINCT as an index-only scan.
    #
    # `tenant` leads `cell_id` because every read is tenant-first: a drain asks
    # "what is dirty for me", never "what is dirty for this cell across
    # tenants".
    create_if_not_exists(unique_index(name, [:tenant, :cell_id, :key]))
  end

  @doc """
  Add the `tenant` column to a frontier table created before it existed.

  For hosts already running the library: `up/1` is `create_if_not_exists`, so
  re-running it will NOT add a column to an existing table.

      def up, do: ReactiveDag.Migration.add_tenant()
      def down, do: ReactiveDag.Migration.remove_tenant()

  Existing rows become `"*"` — untenanted, which is what they were. The old
  `(cell_id, key)` index is dropped only after the new one exists, so
  `mark_dirty`'s ON CONFLICT is backed by an index at every point.
  """
  def add_tenant(opts \\ []) do
    name = table_name(opts)

    alter table(name) do
      add_if_not_exists(:tenant, :text, null: false, default: "*")
    end

    create_if_not_exists(unique_index(name, [:tenant, :cell_id, :key]))
    drop_if_exists(unique_index(name, [:cell_id, :key]))
  end

  @doc "Reverse `add_tenant/1`."
  def remove_tenant(opts \\ []) do
    name = table_name(opts)

    create_if_not_exists(unique_index(name, [:cell_id, :key]))
    drop_if_exists(unique_index(name, [:tenant, :cell_id, :key]))

    alter table(name) do
      remove_if_exists(:tenant, :text)
    end
  end

  @doc "Drop the dirty-frontier table."
  def down(opts \\ []) do
    name = table_name(opts)
    drop_if_exists(unique_index(name, [:tenant, :cell_id, :key]))
    drop_if_exists(unique_index(name, [:cell_id, :key]))
    drop_if_exists(table(name))
  end
end

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

  Options (both directions):

    * `:dirty_table` — the table name, when the host overrides
      `config :reactive_dag, dirty_table:` (default `"reactive_dag_dirty"`,
      matching `Frontier`). A host adopting the library onto an existing
      hand-rolled table keeps its name in BOTH places.

  The coordination TUPLE table is deliberately not created here: its schema is
  host-extended (extension columns like `strength` ride the host's
  `CoordinationWriter`), so its migration belongs to the host.
  """
  use Ecto.Migration

  @default_dirty "reactive_dag_dirty"

  @doc "Create the dirty-frontier table + its coalescing unique index."
  def up(opts \\ []) do
    name = Keyword.get(opts, :dirty_table, @default_dirty)

    create_if_not_exists table(name, primary_key: false) do
      add(:cell_id, :text, null: false)
      add(:key, :text, null: false)
      add(:reason, :text)
      add(:enqueued_at, :utc_datetime_usec)
    end

    create_if_not_exists(unique_index(name, [:cell_id, :key]))
  end

  @doc "Drop the dirty-frontier table."
  def down(opts \\ []) do
    name = Keyword.get(opts, :dirty_table, @default_dirty)
    drop_if_exists(unique_index(name, [:cell_id, :key]))
    drop_if_exists(table(name))
  end
end

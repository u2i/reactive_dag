defmodule ReactiveDag.Migration do
  @moduledoc """
  The library-owned DDL, callable from a host migration — the suspension table
  `ReactiveDag.Suspension` reads and writes:

      defmodule MyApp.Repo.Migrations.AddReactiveDag do
        use Ecto.Migration

        def up, do: ReactiveDag.Migration.up()
        def down, do: ReactiveDag.Migration.down()
      end

  The table name resolves exactly as `Suspension`'s reads do — an explicit
  `:suspension_table` option, else `config :reactive_dag, suspension_table:`,
  else `"reactive_dag_suspension"` — so a host that sets the config gets a
  migration matching the table the runtime queries, with no second place to
  keep in sync.

  Options (both directions):

    * `:suspension_table` — override the resolved table name for this migration
      only (rare; the config is the normal home).

  This is the ONLY table the library owns. Every node's results live in that
  node's own resource, with its own migration — there is no second table
  shadowing them.

  ## Migrating from the dirty queue

  There is no data migration from the old `reactive_dag_dirty` table, and this
  is deliberate rather than an omission. A queue row recorded a CONCLUSION —
  "this cell needs recomputing" — while a suspension records a CAUSE — "this
  change stopped here". Translating one into the other would mean re-deriving,
  at migration time, the graph walk that produced the conclusion, against a
  graph that has since changed. The translation is not sound.

  The correct sequence is: drain the old queue to empty on the old code, deploy
  the new code, then `drop_dirty/1`. It is kept out of `down/1` so a host
  sequences it deliberately.
  """
  use Ecto.Migration

  @default_table "reactive_dag_suspension"
  @default_dirty "reactive_dag_dirty"

  @doc false
  # option > config > default — the same resolution Suspension's reads use, so
  # the migrated table and the queried table cannot silently diverge.
  def table_name(opts \\ []) do
    Keyword.get_lazy(opts, :suspension_table, fn ->
      Application.get_env(:reactive_dag, :suspension_table, @default_table)
    end)
  end

  @doc "Create the suspension table and its point index."
  def up(opts \\ []) do
    name = table_name(opts)

    create_if_not_exists table(name, primary_key: false) do
      # UUIDv7, generated in Elixir: it sorts by creation, so `ORDER BY id` is
      # `ORDER BY when this change happened`. A resumption merging several
      # versions into one recompute must apply them in that order, and this is
      # what lets it, with no companion timestamp to keep consistent.
      #
      # `text`, not `uuid`, matching `row_uuid` below. The library speaks to the
      # host's repo through raw SQL with plain parameters, and a `uuid` column
      # wants Postgrex's 16-byte binary rather than the textual form — so every
      # id would need encoding on the way in and decoding on the way out, in a
      # module whose whole job is to stay small. Storing text costs a few bytes
      # a row and keeps `discharge/1`'s `id = ANY($1)` a list of plain strings.
      # Sortability, which is the only property this column is chosen for, is
      # unaffected: UUIDv7's hex form sorts identically to its bytes.
      add(:id, :text, primary_key: true)

      # WHOSE GRAPH. Every tenant's graph has the same topology; what differs
      # is the rows. So the tenant is a column, never part of a key and never
      # part of a resource name.
      #
      # `"*"` rather than NULL for an untenanted suspension, because a null
      # never equals itself: every read here matches on the tenant, and
      # `tenant = NULL` matches nothing. An untenanted suspension would be
      # written and then never found — and a resumption that finds no work
      # reports SUCCESS, so the failure would be silent.
      add(:tenant, :text, null: false, default: "*")

      # WHAT STOPPED, and WHAT CHANGED. Both are resource names, and neither
      # implies the other: one changed row may stop several resources, and they
      # complete independently. There is no `cell_id` here — a host writing a
      # row names a stopping point without knowing the graph's internal cell
      # names.
      add(:waiting, :text, null: false)
      add(:resource, :text, null: false)

      # WHICH ROW MOVED, and WHAT THE MOVE WAS.
      #
      # `text`, not `uuid`, because `"*"` is a declared sentinel for a change
      # that could not be attributed — a source that cannot say which of its
      # items moved. Resumption then recomputes the whole cell: expensive,
      # correct, and honest. `"*"` is not a valid uuid, and making the column
      # nullable instead would reintroduce the null-never-equals-itself problem
      # the tenant column documents above.
      add(:row_uuid, :text, null: false, default: "*")
      add(:version_id, :text, null: false, default: "*")

      # WHY IT STOPPED: 'expensive' or 'approval'. One table serves both,
      # because the situation is the same — a cascade reached here and could
      # not continue. Only the job that clears it differs: one computes before
      # it propagates, the other only propagates.
      add(:reason, :text, null: false)

      add(:inserted_at, :utc_datetime_usec, null: false, default: fragment("now()"))
    end

    # WHICH LAP of a declared feedback loop this suspension is on — 0 for every
    # suspension outside a loop, and for loop work triggered by a fresh
    # external change.
    #
    # A loop through a suspending cell ends each cascade CLEANLY: suspend,
    # commit, resume, repeat — each pass an individually-successful job, so no
    # per-cascade counter can see the chain. The suspension row is the only
    # thing that survives from one lap to the next, so the count lives here;
    # `ReactiveDag.Cascade` raises rather than record a lap past
    # `max_feedback_laps`, which is what bounds an oscillating loop.
    #
    # In an ALTER rather than inside the create above, and only here, so that
    # BOTH kinds of install get the column from one declaration: a fresh
    # install creates the table and then adds it, and an install migrated
    # before this column existed picks it up by re-running `up/1` in a new
    # host migration — where `create_if_not_exists` skips the whole table and
    # would silently leave the column missing, failing every
    # `Suspension.record/4` after that on an unknown column.
    alter table(name) do
      add_if_not_exists(:lap, :integer, null: false, default: 0)
    end

    # NOT UNIQUE, and that is the design.
    #
    # Suspensions are append-only: a second change to the same stopping point
    # writes a SECOND row rather than merging into the first. What coalesces is
    # the WORK — a job reads every suspension at its point and does the
    # expensive thing once — not the rows. A unique index would make the table
    # mutable, and a mutable row must be deleted conditionally (or it discards
    # changes that arrived while the job ran), which is an invariant every
    # future write path has to remember. Duplicate rows are cheap; a lost
    # cascade is not.
    #
    # So this index exists solely to serve the point lookup in
    # `Suspension.at/1` and the grouping in `points/1`. Tenant leads because
    # every read is tenant-first.
    create_if_not_exists(index(name, [:tenant, :waiting, :resource, :row_uuid]))
  end

  @doc "Drop the suspension table and its index."
  def down(opts \\ []) do
    name = table_name(opts)

    drop_if_exists(index(name, [:tenant, :waiting, :resource, :row_uuid]))
    drop_if_exists(table(name))
  end

  @doc false
  def dirty_table_name(opts \\ []) do
    Keyword.get_lazy(opts, :dirty_table, fn ->
      Application.get_env(:reactive_dag, :dirty_table, @default_dirty)
    end)
  end

  @doc """
  Drop the old dirty-frontier table.

  Run this ONLY after the old queue has drained to empty on the old code and
  the new code is deployed. An undrained mark is outstanding WORK, and this
  destroys it with no way to recover what it named — the queue was the only
  record that the work was pending.

  Kept out of `down/1` on purpose, so a host sequences it as its own migration
  rather than having it ride along with a rollback.
  """
  def drop_dirty(opts \\ []) do
    drop_if_exists(table(dirty_table_name(opts)))
  end
end

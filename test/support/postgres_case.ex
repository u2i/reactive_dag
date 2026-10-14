defmodule ReactiveDag.PostgresCase do
  @moduledoc """
  A case template for tests whose subject is real Postgres behaviour.

  Skips the whole module unless `REACTIVE_DAG_TEST_DATABASE_URL` is set, so a
  normal `mix test` on a machine with no container is unaffected.

  ## The DDL comes from the DSL

  `migrate!/1` hands the test's own resources to
  `AshPostgres.MigrationGenerator`, runs what it generates, and drops the
  tables afterwards. Nothing about the schema is written twice.

  That matters more here than it would elsewhere. These tests exist to catch
  the places where ETS cannot answer — `WITHOUT OVERLAPS` primary keys, the
  CTE upsert path, `as_of` lowering to SQL — and hand-written DDL that drifts
  from what the generator emits would hide exactly that class of bug behind a
  table shaped differently from the real one. A test running against a PLAIN
  primary key, for instance, would pass every assertion while testing nothing
  about history: the table would hold one version per key, exactly as ETS does.

  The generator works on resources declared inline in a test body. The one
  requirement is a domain that LISTS them — `allow_unregistered?` returns an
  empty resource list, and the generator would find nothing to generate.
  """
  use ExUnit.CaseTemplate

  using do
    quote do
      import ReactiveDag.PostgresCase
      alias ReactiveDag.TestRepo

      @moduletag :postgres

      # Skip the module outright when no database is configured. A
      # `@moduletag` is evaluated at COMPILE time, which is the only place a
      # skip can be declared: ExUnit reads `:skip` as a TAG, and a value
      # returned from `setup_all` lands in the context where nothing acts on
      # it — every test body then runs on into a missing repo and reports as
      # FAILING, which reads as broken code rather than an absent database.
      if not ReactiveDag.TestRepo.available?() do
        @moduletag skip: "REACTIVE_DAG_TEST_DATABASE_URL is not set"
      end
    end
  end

  @doc """
  Generate migrations for `domain`'s resources and run them.

  Call from `setup_all`. Returns the tables it created.

  ## Why this does NOT drop on exit

  It used to, and that is wrong for a SHARED fixture. Two modules using the
  same fixture domain each run `setup_all`; whichever finishes first dropped
  the tables out from under the other, and the second module failed with
  `undefined_table` on a table it had successfully created. Measured — it is
  how `group_rule_test` and `fixture_smoke_test` first interacted.

  So tables persist for the run. They are created `IF NOT EXISTS`-style (the
  generated migration is re-run only when absent), truncated per test by
  `truncate/1`, and the database is a disposable container. A table left
  behind costs nothing; one dropped mid-run costs a false failure.
  """
  def migrate!(domain) do
    dir = Path.join(System.tmp_dir!(), "rd_mig_#{:erlang.phash2(domain)}")
    File.rm_rf!(dir)

    AshPostgres.MigrationGenerator.generate([domain],
      migration_path: dir,
      snapshot_path: Path.join(dir, "snapshots"),
      quiet: true,
      no_shell: true,
      auto_name: true,
      format: false
    )

    # Only the resources on AshPostgres. A fixture may deliberately include a
    # resource on `Ash.DataLayer.Simple` — one that owns no table is a shape
    # worth testing — and asking it for a table name yields nil, which reaches
    # the database as `DROP TABLE ""`.
    tables =
      domain
      |> Ash.Domain.Info.resources()
      |> Enum.filter(&(Ash.DataLayer.data_layer(&1) == AshPostgres.DataLayer))
      |> Enum.map(&AshPostgres.DataLayer.Info.table/1)
      |> Enum.reject(&is_nil/1)

    # Already migrated by another module in this run? Then the tables are
    # there and shared — nothing to do. Checked rather than assumed, because
    # the generated migration is a plain `create table` that would fail.
    if Enum.all?(tables, &table_exists?/1) do
      tables
    else
      do_migrate(dir, tables)
    end
  end

  defp do_migrate(dir, tables) do
    # A run that died mid-migration can leave SOME of the tables. Drop the
    # partial set so `create table` starts from a known state.
    drop(tables)

    # ONLY the resource migrations. The extensions migration installs Ash's SQL
    # functions, which are repo-wide, already present, and slow to reinstall per
    # module.
    # Run the migration on a SEPARATE repo, not the sandboxed one. The migrator
    # has no way to take a connection of its own, and the test repo's pool is
    # `Ecto.Adapters.SQL.Sandbox` — a checkout there either finds no owner (in
    # `setup_all` nothing holds one) or wraps the DDL in a transaction that the
    # first test rolls back. `MigrationRepo` is the same database with an
    # ordinary pool, started on demand and stopped straight after.
    {:ok, mig_repo} = ReactiveDag.MigrationRepo.start_link()

    try do
      dir
      |> Path.join("*migrate_resources[0-9]*.exs")
      |> Path.wildcard()
      |> Enum.sort()
      |> Enum.each(fn path ->
        # The file's own timestamp as the version, NOT a counter. A counter
        # collides with versions `schema_migrations` already holds from an
        # earlier module or run, and the migrator then silently SKIPS the
        # migration — leaving the test to fail on a table that was never
        # created, which reads like a bug in the code under test.
        version = path |> Path.basename() |> String.split("_") |> hd() |> String.to_integer()

        Ecto.Migrator.run(ReactiveDag.MigrationRepo, [{version, path}], :up,
          all: true,
          log: false
        )
      end)
    after
      Supervisor.stop(mig_repo)
    end

    File.rm_rf!(dir)

    tables
  end

  defp table_exists?(table) do
    %{rows: [[exists]]} =
      exec("SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = $1)", [table])

    exists
  end

  defp drop(tables) do
    for t <- tables, do: exec(~s|DROP TABLE IF EXISTS "#{t}" CASCADE|)
    :ok
  end

  @doc """
  Create the LIBRARY's own tables — suspensions, dirty marks, runs.

  These are raw SQL inside `ReactiveDag.Migration`, not Ash resources, so the
  resource generator does not produce them. Running the library's real
  migration is the point: the alternative that several tests used was a
  hand-written `FakeRepo` matching on `"INSERT INTO " <> _`, which covers the
  logic and cannot see inside the SQL — which is where
  `real_postgres_suspension_test` found three real bugs.

  Idempotent, and sets `:repo` for the duration so `Suspension`, `Lock` and
  `Run` reach this database.
  """
  def migrate_library_tables! do
    {:ok, mig_repo} = ReactiveDag.MigrationRepo.start_link()

    try do
      Ecto.Migrator.run(ReactiveDag.MigrationRepo, [{20_260_101_000_001, __MODULE__.LibraryTablesMigration}], :up,
        all: true,
        log: false
      )
    after
      Supervisor.stop(mig_repo)
    end

    prev = Application.get_env(:reactive_dag, :repo)
    Application.put_env(:reactive_dag, :repo, ReactiveDag.TestRepo)
    ExUnit.Callbacks.on_exit(fn -> Application.put_env(:reactive_dag, :repo, prev) end)

    :ok
  end

  defmodule LibraryTablesMigration do
    @moduledoc false
    use Ecto.Migration

    def up do
      ReactiveDag.Migration.up()
      ReactiveDag.Migration.runs_up()
    end

    def down do
      ReactiveDag.Migration.runs_down()
      ReactiveDag.Migration.down()
    end
  end

  @doc """
  Run a statement on the test repo, OUTSIDE the sandbox.

  DDL has to outlive the per-test transaction that the sandbox rolls back, so
  these run on their own short-lived connection. Using the pooled repo here
  would either fail for want of a checkout (in `setup_all`, before any test
  owns one) or create a table that vanishes at the first rollback.
  """
  def exec(sql, params \\ []) do
    {:ok, conn} = Postgrex.start_link(url_opts())

    try do
      Postgrex.query!(conn, sql, params)
    after
      GenServer.stop(conn)
    end
  end

  @doc "Run a statement on the sandboxed connection the current test owns."
  def exec_sandboxed(sql, params \\ []) do
    Ecto.Adapters.SQL.query!(ReactiveDag.TestRepo, sql, params)
  end

  @doc """
  Truncate the given tables between tests.

  Sandboxed deliberately: this runs inside the test's own transaction so it
  sees — and clears — rows the test itself wrote.
  """
  def truncate(tables) do
    for t <- tables, do: exec_sandboxed(~s|TRUNCATE "#{t}"|)
    :ok
  end

  defp url_opts do
    ReactiveDag.TestRepo.url()
    |> URI.parse()
    |> then(fn uri ->
      [user, pass] = String.split(uri.userinfo || "postgres:postgres", ":", parts: 2)

      [
        hostname: uri.host,
        port: uri.port,
        username: user,
        password: pass,
        database: String.trim_leading(uri.path || "", "/")
      ]
    end)
  end
end

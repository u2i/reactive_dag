defmodule ReactiveDag.PostgresCase do
  @moduledoc """
  A case template for tests whose subject is real Postgres behaviour.

  Skips the whole module unless `REACTIVE_DAG_TEST_DATABASE_URL` is set, so a
  normal `mix test` on a machine with no container is unaffected.

  ## Why the DDL is written by hand

  These resources are defined INLINE in the test body — the fixture is the
  resource, which is what a DSL test wants. The migration generator works off
  a compiled application's resource list, so it has nothing to generate from
  here.

  Writing the `CREATE TABLE` by hand is the same bargain `real_postgres_run_test`
  already takes, and it carries the same hazard: DDL that drifts from what the
  generator would emit hides the very class of bug the file exists to catch.
  `temporal_table/3` below is therefore built to mirror
  `AshPostgres.MigrationGenerator.Operation`'s temporal branch exactly —
  `PRIMARY KEY (<keys>, <period> WITHOUT OVERLAPS)`.
  """
  use ExUnit.CaseTemplate

  using do
    quote do
      import ReactiveDag.PostgresCase
      alias ReactiveDag.TestRepo

      @moduletag :postgres

      # Skip the module outright when no database is configured. A
      # `@moduletag` is evaluated at COMPILE time, which is the only place a
      # skip can be declared — `setup_all` runs too late to turn a test into a
      # skipped one, so the alternative is every test failing on a missing repo.
      if not ReactiveDag.TestRepo.available?() do
        @moduletag skip: "REACTIVE_DAG_TEST_DATABASE_URL is not set"
      end
    end
  end

  @doc """
  Create a temporal table with a `WITHOUT OVERLAPS` primary key.

  `keys` are the plain primary-key columns; `period` is the temporal
  attribute. Mirrors the migration generator's temporal branch.
  """
  def temporal_table(table, columns, keys: keys, period: period) do
    cols = Enum.map_join(columns, ",\n  ", fn {name, type} -> "#{name} #{type}" end)
    key_list = Enum.join(keys, ", ")

    exec("DROP TABLE IF EXISTS #{table}")

    exec("""
    CREATE TABLE #{table} (
      #{cols}
    )
    """)

    # The generator's exact shape. A plain `PRIMARY KEY (#{key_list})` would
    # let the table hold one version per key — which is precisely the
    # `PlainPrimaryKey` error AshPostgres raises against, and a test running on
    # such a table would silently stop exercising history.
    exec("ALTER TABLE #{table} ADD PRIMARY KEY (#{key_list}, #{period} WITHOUT OVERLAPS)")
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

  @doc """
  Truncate the given tables between tests.

  Sandboxed deliberately: this runs inside the test's own transaction so it
  sees — and clears — rows the test itself wrote.
  """
  def truncate(tables) do
    for t <- tables, do: exec_sandboxed("TRUNCATE #{t}")
    :ok
  end
end

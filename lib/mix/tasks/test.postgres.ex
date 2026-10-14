defmodule Mix.Tasks.Test.Postgres do
  @moduledoc """
  Run the tests whose subject is real Postgres behaviour.

      mix test.postgres
      mix test.postgres test/stale_grain_postgres_test.exs

  Starts a PostgreSQL 18 container if one is not already listening, creates the
  test database and the `btree_gist` extension, then runs `mix test` with the
  `:postgres` tag included.

  ## Why a container

  Temporal resources need PG18+ for `WITHOUT OVERLAPS` primary keys — a feature
  most installed Postgres versions do not have. Pinning the version here means
  a failure is a real finding rather than an artefact of whatever happens to be
  on the machine.
  """
  @shortdoc "Run the Postgres-backed tests (starts a PG18 container)"

  use Mix.Task

  @container "rdag-pg18"
  @port 5545
  @db "reactive_dag_test"
  @url "postgres://cascade:cascade@localhost:#{@port}/#{@db}"

  @impl Mix.Task
  def run(args) do
    ensure_container!()
    ensure_database!()

    Mix.shell().info("==> running Postgres tests against #{@url}")

    System.put_env("REACTIVE_DAG_TEST_DATABASE_URL", @url)
    Mix.Task.run("test", ["--include", "postgres" | args])
  end

  defp ensure_container! do
    if running?() do
      Mix.shell().info("==> #{@container} already running on :#{@port}")
    else
      Mix.shell().info("==> starting #{@container} (postgres:18-alpine) on :#{@port}")

      {_, 0} =
        System.cmd("docker", [
          "run", "--rm", "-d",
          "--name", @container,
          "-e", "POSTGRES_USER=cascade",
          "-e", "POSTGRES_PASSWORD=cascade",
          "-e", "POSTGRES_DB=reactive_dag_temporal",
          "-p", "#{@port}:5432",
          "postgres:18-alpine"
        ])

      wait_for_ready!()
    end
  end

  defp running? do
    case System.cmd("docker", ["ps", "--filter", "name=#{@container}", "--format", "{{.Names}}"]) do
      {out, 0} -> String.trim(out) == @container
      _ -> false
    end
  end

  defp wait_for_ready!(attempts \\ 30)

  defp wait_for_ready!(0) do
    Mix.raise("#{@container} did not become ready")
  end

  defp wait_for_ready!(attempts) do
    case System.cmd("docker", ["exec", @container, "pg_isready", "-U", "cascade"],
           stderr_to_stdout: true
         ) do
      {_, 0} ->
        :ok

      _ ->
        Process.sleep(500)
        wait_for_ready!(attempts - 1)
    end
  end

  defp ensure_database! do
    # `CREATE DATABASE` fails harmlessly when it already exists; the extension
    # is `IF NOT EXISTS`. Both are idempotent, so this runs every time rather
    # than guessing at state.
    psql("postgres", "CREATE DATABASE #{@db}")
    psql(@db, "CREATE EXTENSION IF NOT EXISTS btree_gist")
  end

  defp psql(db, sql) do
    System.cmd("docker", ["exec", @container, "psql", "-U", "cascade", "-d", db, "-c", sql],
      stderr_to_stdout: true
    )
  end
end

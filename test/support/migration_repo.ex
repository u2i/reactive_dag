defmodule ReactiveDag.MigrationRepo do
  @moduledoc """
  The test database again, with an ORDINARY pool.

  Exists only so `ReactiveDag.PostgresCase.migrate!/1` has somewhere to run
  DDL. The test repo's pool is `Ecto.Adapters.SQL.Sandbox`, and a migration
  cannot run there: in `setup_all` no process owns a checkout, and a sandboxed
  connection would wrap the `CREATE TABLE` in a transaction that the first
  test's rollback discards.

  Same database, same credentials — only the pool differs. Started on demand
  and stopped immediately, so it holds no connections during the tests
  themselves.
  """
  use AshPostgres.Repo, otp_app: :reactive_dag

  @impl true
  def installed_extensions, do: ReactiveDag.TestRepo.installed_extensions()

  @impl true
  def min_pg_version, do: ReactiveDag.TestRepo.min_pg_version()
end

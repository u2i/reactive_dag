defmodule ReactiveDag.TestRepo do
  @moduledoc """
  The test database — a REAL Postgres, opt-in.

  The suite's 64 resource files run on ETS, and that is the right default: a
  DSL test's fixture IS the resource, defined inline, and ETS needs no
  migration to hold one. But ETS and Postgres do not agree about everything,
  and where they disagree the ETS answer is the one that cannot be trusted:

    * `WITHOUT OVERLAPS` primary keys are a PG18 feature with no ETS analogue.
      ETS stores periods as plain fields and never enforces non-overlap, so a
      write that Postgres would REJECT succeeds there.

    * Upserts go down a completely different path. Postgres cannot
      `INSERT ... ON CONFLICT` against an exclusion PK at all, so AshPostgres
      emits data-modifying CTEs that split the period by hand. That machinery
      is simply absent on ETS.

    * `recorded_at` in `upsert_fields` is the bug that started this: an upsert
      leaving the stamp at its create value makes staleness report that
      nothing moved. Measured once on ETS; the CTE path deserves its own
      answer rather than an inherited one.

  So this exists for the tests whose SUBJECT is one of those differences, not
  as a replacement substrate for the suite.

  ## Opt-in

  Skipped unless `REACTIVE_DAG_TEST_DATABASE_URL` is set:

      REACTIVE_DAG_TEST_DATABASE_URL=postgres://cascade:cascade@localhost:5545/reactive_dag_test \\
        mix test --include postgres

  `mix test.postgres` starts the container, creates the database and runs
  exactly those tests.
  """
  use AshPostgres.Repo, otp_app: :reactive_dag

  @doc "The configured URL, or nil when the suite should skip Postgres tests."
  def url, do: System.get_env("REACTIVE_DAG_TEST_DATABASE_URL")

  @doc "Whether a real database is configured for this run."
  def available?, do: not is_nil(url())

  @impl true
  def installed_extensions do
    # `btree_gist` is REQUIRED, not optional: a `WITHOUT OVERLAPS` primary key
    # is backed by a GiST exclusion constraint, and a text or uuid key column
    # has no GiST opclass without it. Without the extension the migration
    # fails at CREATE TABLE rather than at some later write.
    ["ash-functions", "uuid-ossp", "citext", "btree_gist"]
  end

  @impl true
  def min_pg_version do
    # Temporal resources need SQL:2011 period primary keys — PG18+. Declaring
    # it here makes a too-old database fail with that sentence rather than a
    # syntax error pointing at WITHOUT OVERLAPS.
    %Version{major: 18, minor: 0, patch: 0}
  end
end

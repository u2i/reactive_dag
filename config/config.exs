import Config

# Ash 3.33+ refuses to compile a resource without an explicit choice, and this
# library defines Ash resources in `test/support` for its fixture graphs —
# compiled at BUILD time, before any test helper runs, so the setting cannot
# live there.
#
# `:codepoints` is Ash's own recommendation: it counts the way SQL data layers
# count, so a validation in Elixir agrees with the database and `max_length`
# genuinely bounds a stored value. `:mixed` keeps the pre-3.33 behaviour, under
# which a grapheme can carry unboundedly many combining characters and
# `max_length` bounds nothing.
#
# A HOST's own config wins for a host's own resources; this file is not part of
# the published package's runtime configuration.
config :ash, default_string_length_count: :codepoints

# The opt-in Postgres test database. Only `:test` configures it, and only a run
# with `REACTIVE_DAG_TEST_DATABASE_URL` set actually starts it — see
# `ReactiveDag.TestRepo`. Everything else in the suite runs on ETS.
if config_env() == :test do
  config :reactive_dag, ecto_repos: [ReactiveDag.TestRepo]

  # Same database, ordinary pool — see `ReactiveDag.MigrationRepo`.
  config :reactive_dag, ReactiveDag.MigrationRepo,
    url:
      System.get_env("REACTIVE_DAG_TEST_DATABASE_URL") ||
        "postgres://cascade:cascade@localhost:5545/reactive_dag_test",
    pool_size: 2

  config :reactive_dag, ReactiveDag.TestRepo,
    url:
      System.get_env("REACTIVE_DAG_TEST_DATABASE_URL") ||
        "postgres://cascade:cascade@localhost:5545/reactive_dag_test",
    pool: Ecto.Adapters.SQL.Sandbox,
    pool_size: 10,
    # A temporal upsert runs as several data-modifying CTEs and retries on a
    # lost race, so the default 15s is not generous for a slow container.
    ownership_timeout: 60_000
end

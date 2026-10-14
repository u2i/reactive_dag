# `--repeat-until-failure` DOES NOT WORK with this suite, and the failures it
# reports are artefacts rather than findings.
#
# Many tests define their resources with `defmodule` inside the test body — the
# shape a DSL test wants, since the resource IS the fixture. On a second run in the
# same VM, Elixir redefines those modules (138 of them, observably), and an Ash
# resource does not survive that: its ETS table and its registration belong to the
# first definition, so reads come back empty and the recompute reports no changes.
#
# Measured: repeat 1 passes clean, repeat 2 fails 4 tests, every time. Those four are
# not flaky — they are the first tests to read a resource whose table went with the
# module it was defined in.
#
# To hunt a real flake, run separate VMs instead:
#
#     for i in $(seq 1 30); do mix test --max-cases 128 --seed $RANDOM; done
#
# The Postgres tests are OPT-IN and excluded by default: the suite must stay
# runnable on a machine with no container. `mix test.postgres` (or
# `--include postgres` with the URL set) turns them on.
#
# Started here rather than in the case template because a repo is an
# application-level process — starting it per module would fight the sandbox.
if url = System.get_env("REACTIVE_DAG_TEST_DATABASE_URL") do
  Application.put_env(
    :reactive_dag,
    ReactiveDag.TestRepo,
    Keyword.put(Application.get_env(:reactive_dag, ReactiveDag.TestRepo, []), :url, url)
  )

  {:ok, _} = ReactiveDag.TestRepo.start_link()
  Ecto.Adapters.SQL.Sandbox.mode(ReactiveDag.TestRepo, :manual)

  ExUnit.start()
else
  ExUnit.start(exclude: [:postgres])
end

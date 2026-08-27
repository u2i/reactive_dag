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
ExUnit.start()

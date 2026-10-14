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

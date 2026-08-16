# Ash and Spark export the `locals_without_parens` for their DSLs. WITHOUT this,
# `mix format` parenthesises every DSL call — `attribute(:key, :string, ...)`,
# `defaults([:read])` — which is not how any resource in this repo is written,
# and which turns a small change into a forty-file diff.
#
# Deliberately no `inputs:`. The repo has never been formatted as a whole (39
# files differ from the formatter's opinion), so a blanket `mix format` would
# rewrite files nobody is touching. Formatting the repo is a decision worth
# making on its own; this only stops the DSL from being mangled when someone
# formats a file by name.
[
  import_deps: [:ash, :spark],
  line_length: 124
]

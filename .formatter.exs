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
# THIS LIBRARY'S OWN DSL. `import_deps` above covers Ash and Spark's calls, but
# not `reactive_dag`'s — so `id :docs`, `leaf? true` and `recompute_by :category`
# were being parenthesised into `id(:docs)` every time a file was formatted.
#
# Generated, not hand-written: `mix spark.formatter --extensions ReactiveDag.Node`
# rewrites the list below from the DSL's own entity definitions. Run it after
# adding or renaming an entity, or the formatter silently goes back to
# parenthesising the new one.
spark_locals_without_parens = [
  aggregate: 0,
  aggregate: 1,
  approved_by: 1,
  args: 1,
  as: 1,
  augmented_by: 1,
  avg: 1,
  companion: 1,
  compare: 1,
  compose: 1,
  compose: 2,
  compute: 1,
  compute: 2,
  context: 1,
  context: 2,
  count: 1,
  depends_on: 1,
  dirties_on: 1,
  every: 1,
  expand: 1,
  feedback: 1,
  feedback: 2,
  fingerprint: 1,
  fingerprint_attribute: 1,
  first: 1,
  for_each: 1,
  from: 1,
  from_key: 1,
  gated: 1,
  group_by: 1,
  id: 1,
  into: 1,
  join: 0,
  join: 1,
  key: 1,
  key_prefix: 1,
  key_rule: 1,
  label: 1,
  lapse: 1,
  lapse: 2,
  lapse_action: 1,
  leaf?: 1,
  left: 1,
  left_over: 1,
  max: 1,
  max_concurrency: 1,
  meta: 1,
  min: 1,
  op: 1,
  outer: 1,
  over: 1,
  payload_action: 1,
  payload_key: 1,
  payload_update: 1,
  per_key: 1,
  per_key: 2,
  poll: 1,
  poll: 2,
  poll_as: 1,
  query: 1,
  read: 1,
  recompute_by: 1,
  recompute_by: 2,
  reduce: 0,
  reduce: 1,
  ref: 1,
  ref: 2,
  retain_if_vanished: 1,
  right: 1,
  right_over: 1,
  row_key: 1,
  run: 1,
  run: 2,
  schedule_drain: 1,
  slice: 1,
  slice: 2,
  source: 1,
  sum: 1,
  suspends: 1,
  timeout: 1,
  to: 1,
  union: 0,
  union: 1,
  values: 1,
  when_changed: 1
]

[
  import_deps: [:ash, :spark],
  locals_without_parens: spark_locals_without_parens,
  export: [locals_without_parens: spark_locals_without_parens],
  line_length: 124
]

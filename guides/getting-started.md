# Getting started

`reactive_dag` turns a set of Ash resources into an incrementally-recomputed
dependency graph: leaves are fed by scanners or humans, derived cells recompute
when — and only when — something beneath them changed, and every cell's result
is readable through one shared coordination table.

The library owns the *schedule* (what is dirty, what recomputes, in what
order); your app owns the *meaning* (what an op computes, what a status string
says, what the keys are). This guide takes you from an empty host app to a
running two-cell graph.

## Installation

```elixir
# mix.exs
defp deps do
  [
    # pin the latest tag — https://github.com/u2i/reactive_dag/tags
    {:reactive_dag, git: "git@github.com:u2i/reactive_dag.git", tag: "v0.16.0"}
  ]
end
```

The library assumes an Ash 3.x / AshPostgres host — it is an Ash extension, not
a standalone framework.

## Configuration

```elixir
config :reactive_dag,
  repo: MyApp.Repo,                    # REQUIRED — raises if unset
  tuple_table: "my_tuple",             # the coordination spine
  dirty_table: "my_dirty",             # the frontier
  coordination_writer: MyApp.Writer    # OPTIONAL — a spine-only default ships
```

`tuple_table` / `dirty_table` **default silently** (`reactive_dag_tuple` /
`reactive_dag_dirty`); a name that doesn't match your migration yields empty
results with no error, so set them explicitly.

## Migrations (the host owns the tuple; the lib offers the frontier)

The library defines the columns it needs and is their only reader/writer, but
the physical tables live in *your* migrations — that's what lets a host add its
own extension columns beside the spine.

The dirty frontier has no extension columns, so the library can own its DDL —
`ReactiveDag.Migration.up/1` / `down/1` create it for you, resolving the table
name exactly as the runtime does (`config :reactive_dag, dirty_table:`, with an
explicit `dirty_table:` option overriding), so the migrated table and the
queried table can't silently diverge:

```elixir
defmodule MyApp.Repo.Migrations.AddReactiveDag do
  use Ecto.Migration

  def up, do: ReactiveDag.Migration.up()
  def down, do: ReactiveDag.Migration.down()
end
```

Hand-write it only if you want to co-locate it with the tuple migration, as
below. The TUPLE table stays yours either way (extension columns).

```elixir
def change do
  # the coordination tuple: one row per (cell, key), carrying the verdict +
  # freshness. A cell IS its set of these rows.
  create table(:my_tuple, primary_key: false) do
    add :cell_id, :string, null: false
    add :key, :string, null: false
    add :status, :string, null: false, default: "present"
    add :observed_at, :utc_datetime_usec
    add :stale_after, :utc_datetime_usec
    add :updated_at, :utc_datetime_usec
    # ... your extension columns here (strength, source_ref, …) — the library
    # neither reads nor writes them; see the "Seams" guide.
  end

  create unique_index(:my_tuple, [:cell_id, :key])
  create index(:my_tuple, [:cell_id, :status])

  # the dirty frontier: pending recompute work, claimed-as-deleted by the drain.
  create table(:my_dirty, primary_key: false) do
    add :cell_id, :string, null: false
    add :key, :string, null: false
    add :reason, :string
    add :enqueued_at, :utc_datetime_usec
  end

  # UNIQUE is load-bearing: mark_dirty coalesces via ON CONFLICT (cell_id, key).
  create unique_index(:my_dirty, [:cell_id, :key])
end
```

(If you use attestations, the record store is different: it is an **Ash
resource you define**, with generated migrations — see the
[Attestations](attestations.md) guide.)

## A first graph

Two nodes: a leaf fed from outside, and a derived rollup over it.

```elixir
defmodule MyApp.FiscalLines do
  use Ash.Resource, data_layer: Ash.DataLayer.Simple, extensions: [ReactiveDag.Node]

  reactive do
    op :source
    leaf? true            # fed by a source, never recomputed by the drain
  end
end

defmodule MyApp.BudgetRollups do
  use Ash.Resource,
    data_layer: AshPostgres.DataLayer,     # this node's OWN payload table
    extensions: [ReactiveDag.Node]

  attributes do
    attribute :key, :string, primary_key?: true
    attribute :fund, :string
    attribute :total, :float
  end

  actions do
    create :upsert do
      upsert? true
      upsert_identity :key
      accept [:key, :fund, :total]
    end
  end

  reactive do
    op :fold
    key_rule :all
    # Ash-first: the library reads :fiscal_lines itself (dirty-key scoped),
    # groups by the attribute, folds each group, derives the key ("gf"), and
    # writes the row into THIS resource with change detection. No fns.
    reduce over: :fiscal_lines,
           group_by: :fund,
           into: [sum: [amount: :total]]
  end
end
```

The resource **is** the node *and* its payload table — and the computation is
declared, not coded: no read plumbing, no write plumbing, no key derivation to
author. When a shape outgrows attributes, each slot has an escape hatch. See [Authoring nodes](authoring-nodes.md) for every
node shape.

## Assemble and run

```elixir
plan = ReactiveDag.Node.graph([MyApp.FiscalLines, MyApp.BudgetRollups])

{:ok, report} =
  ReactiveDag.Drain.run(plan,
    recompute: ReactiveDag.Node.Recompute,   # dispatches reduce/join/aggregate/compute
    key_rule: ReactiveDag.Node.KeyRule       # reads :identity | :all off the block
  )

report.steps
# one entry per cell recompute, in execution order:
# %{cell: "budget_rollups", triggered_by: "fiscal_lines",
#   claimed: ["fy24"], changed: ["fy24"], pass: 1, duration_us: 812}
```

The `%ReactiveDag.Drain.Report{}` is the drain's processing trace — what ran,
why (`triggered_by` reconstructs the causal tree), what actually changed, and
how long each step took. Persist it wherever your runs live (a job's meta, a
run table): the library reports, the host records. For progress *during* a
long drain, pass `on_step: fn cell, step -> ... end`.

`graph/2` validates the whole thing at assembly: every edge resolves, ids are
unique, the graph is acyclic — an authoring mistake fails loudly here, not
silently at runtime.

The drain is **incremental**: it processes only cells with dirty keys, in
dependency (depth) order, and propagates only the keys each recompute reports
as actually changed. An empty frontier is a no-op.

## Feeding the leaf

Data enters through a **source** — anything that writes a leaf's tuples and
marks its parents dirty. The typical shape:

```elixir
# 1. write the leaf's tuples (reconcile computes what changed/vanished)
{:ok, changed} =
  ReactiveDag.Tuple.reconcile("fiscal_lines", keys,
    upsert: fn key -> ReactiveDag.Tuple.put("fiscal_lines", key) == :ok end
  )

# 2. mark the changed keys dirty upward
ReactiveDag.Graph.dirty_parents(plan, "fiscal_lines", changed, ReactiveDag.Node.KeyRule)

# 3. drain
ReactiveDag.Drain.run(plan, recompute: ..., key_rule: ...)
```

For a scanner with a real contract (id, leaf binding, failure containment),
implement `ReactiveDag.Source` — see [Sources and scanning](sources.md), which
also covers the one discipline that matters most: *an unreachable upstream
writes nothing*, because an estate you could not survey must not render as an
empty estate.

## Reading results

```elixir
ReactiveDag.Tuple.status_histogram("budget_rollups")  # %{"present" => 12}
ReactiveDag.Tuple.rows("budget_rollups")              # [%{key:, status:, observed_at:}]
ReactiveDag.Verdict.for_cell("budget_rollups")        # a rolled verdict + failing sample
```

Payload (the typed values) stays in each node's own resource; the tuple carries
only the verdict and freshness, joined back by `key`.

## Where next

- [Authoring nodes](authoring-nodes.md) — every node shape and combinator.
- [Sources and scanning](sources.md) — the poll/drain split and the
  honest-gap discipline.
- [Attestations](attestations.md) — human sign-off as a first-class input.
- [The seams](seams.md) — custom recompute strategies, key rules, extension
  columns, and hand-assembled graphs.

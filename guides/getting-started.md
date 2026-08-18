# Getting started

`reactive_dag` turns a set of Ash resources into an incrementally-recomputed
dependency graph: leaves are fed by scanners or humans, derived cells recompute
when — and only when — something beneath them changed, and every cell's result
is rows in that cell's own resource — queryable with ordinary Ash reads.

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
  repo: MyApp.Repo,        # REQUIRED — raises if unset
  dirty_table: "my_dirty"  # the frontier (optional; defaults silently)
```

Only `:repo` is required. Every key the library reads — with its default, what
reads it, and when you would change it — is in
[Configuration](configuration.html).

`dirty_table` **defaults silently** to `reactive_dag_dirty`; a name that doesn't
match your migration yields empty results with no error, so set it explicitly if
you are adopting an existing table.

## Migrations

The library owns exactly one table — the dirty frontier — and offers its DDL:

```elixir
defmodule MyApp.Repo.Migrations.AddReactiveDag do
  use Ecto.Migration

  def up, do: ReactiveDag.Migration.up()
  def down, do: ReactiveDag.Migration.down()
end
```

It resolves the table name exactly as the runtime does (`config :reactive_dag,
dirty_table:`, with an explicit `dirty_table:` option overriding), so the
migrated table and the queried table cannot silently diverge.

Hand-write it only if you want it beside your own migrations:

```elixir
def change do
  # pending recompute work, claimed-as-deleted by the drain
  create table(:my_dirty, primary_key: false) do
    add :cell_id, :string, null: false
    add :key, :string, null: false
    add :reason, :string
    add :enqueued_at, :utc_datetime_usec
    add :prior, :map
  end

  # UNIQUE is load-bearing: mark_dirty coalesces via ON CONFLICT (cell_id, key).
  create unique_index(:my_dirty, [:cell_id, :key])
end
```

**Everything else is your own resources.** A node's results live in the node's
resource, with that resource's migration — there is no second table shadowing
them, and nothing to keep in sync.

## A first graph

Two nodes: a leaf fed from outside, and a derived rollup over it.

```elixir
defmodule MyApp.FiscalLines do
  use Ash.Resource, data_layer: AshPostgres.DataLayer, extensions: [ReactiveDag.Node]

  # A leaf is an ordinary resource holding ordinary rows — the rollup below
  # READS them, so it needs real attributes and a read action.
  attributes do
    attribute :id, :string, primary_key?: true
    attribute :fund, :string
    attribute :amount, :float
  end

  actions do
    defaults [:read, :destroy]
    create :upsert do upsert?(true); accept([:id, :fund, :amount]) end
  end

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
    defaults [:read, :destroy]

    create :upsert do
      upsert? true
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

Draining this writes one row per fund — `{"gf", 150.0}`, `{"water", 20.0}` — and
reports `["gf", "water"]` as changed.

The resource **is** the node *and* its payload table — and the computation is
declared, not coded: no read plumbing, no write plumbing, no key derivation to
author. Note the `:read` action on both: the library reads the input's resource
itself, so a node with nothing to read fails at `graph/2` with a message saying
so. When a shape outgrows attributes, each slot has an escape hatch. See [Authoring nodes](authoring-nodes.md) for every
node shape.

## Assemble and run

```elixir
plan = ReactiveDag.Node.graph([MyApp.FiscalLines, MyApp.BudgetRollups])

{:ok, report} = ReactiveDag.Drain.run(plan)

report.steps
# one entry per cell recompute, in execution order:
# %{cell: "budget_rollups", triggered_by: "fiscal_lines",
#   claimed: ["fy24"], changed: ["fy24"], pass: 1, duration_us: 812}
```

The `%ReactiveDag.Drain.Report{}` is the drain's processing trace — what ran,
why (`triggered_by` reconstructs the causal tree), what actually changed, and
how long each step took. Persist it wherever your runs live (a job's meta, a
run table): the library reports, the host records. For progress *during* a long
drain, attach to the `[:reactive_dag, :drain, :step]` telemetry event — see
`ReactiveDag.Drain` for the full event list.

`graph/2` validates the whole thing at assembly: every edge resolves, ids are
unique, the graph is acyclic — an authoring mistake fails loudly here, not
silently at runtime.

**Nothing is passed to the drain but the plan.** How a node recomputes and how
its changes propagate are declared in its `reactive` block, and the drain reads
them from there — `run/2`'s only option is `:max_passes`, a runaway guard.

The drain is **incremental**: it processes only cells with dirty keys, in
dependency (depth) order, and propagates only the keys each recompute reports
as actually changed. An empty frontier is a no-op.

## Feeding the leaf

A leaf is an ordinary resource. Data enters by writing rows to it, and the
cascade starts when those writes are marked dirty.

**The simple case — `dirties_on`.** Declare it on the leaf and ordinary Ash
writes trigger the cascade themselves:

```elixir
reactive do
  id :fiscal_lines
  leaf? true
  dirties_on [:create, :update, :destroy]
end
```

Now `Ash.create!/1` on that resource marks its key dirty *inside the write's
transaction* — a rolled-back write leaves no dirty key, a committed one always
leaves one.

**But marking is not draining.** The mark sits in the frontier until something
drains, and `dirties_on` alone schedules nothing:

```elixir
MyApp.FiscalLines |> Ash.Changeset.for_create(:create, attrs) |> Ash.create!()
ReactiveDag.Drain.run(plan)                                  # ← still yours
```

For a graph with a polling source that is fine — the next sweep picks it up. For
a write-fed leaf it means the result of a user's action appears whenever
something else happens to drain, which may be never. Add `schedule_drain:`:

```elixir
reactive do
  id :fiscal_lines
  leaf? true
  dirties_on [:create, :update, :destroy]
  schedule_drain true
end
```

That enqueues a `ReactiveDag.DrainWorker` job in the same transaction as the
mark, so both commit or neither does, and the drain runs after the request rather
than inside it. A burst of writes coalesces to one pending job. Needs Oban and a
`drain` queue:

```elixir
config :my_app, Oban, queues: [drain: 1]
```

**The scanner case.** When the data comes from outside — a fleet API, a repo, an
LLM — the fetch is effectful and fallible, so it stays outside the drain. Write
the leaf's rows, then mark what changed:

```elixir
# 1. write the leaf's rows (your resource, your upsert)
# 2. mark the changed keys dirty upward
ReactiveDag.Graph.dirty_parents(plan, "fiscal_lines", changed)
# 3. drain
ReactiveDag.Drain.run(plan)
```

For a scanner with a real contract (id, leaf binding, failure containment),
implement `ReactiveDag.Source` — see [Sources and scanning](sources.md), which
also covers the one discipline that matters most: *an unreachable upstream
writes nothing*, because an estate you could not survey must not render as an
empty estate.

## Reading results

A node's results are **its own rows**, so the first answer is an ordinary Ash
read — with policies, filters, loads and joins:

```elixir
MyApp.BudgetRollups |> Ash.Query.filter(status == "failing") |> Ash.read!()
```

When you want the same rows addressed by *cell key* rather than by the
resource's primary key — which is how the DAG talks about them — go through the
cell:

```elixir
cell = plan.cells["budget_rollups"]

ReactiveDag.Node.Rows.all(cell)              # [%{key:, status:, record:}]
ReactiveDag.Node.Rows.status_histogram(cell) # %{"present" => 12}
ReactiveDag.Verdict.for_cell(cell)           # a rolled verdict + failing sample
ReactiveDag.Insights.cell_status(plan, "budget_rollups")
```

There is no second store to consult. A node's results are its own rows; the one
table the library owns is the dirty frontier, and that holds pending work rather
than answers.

## Where next

- [Authoring nodes](authoring-nodes.md) — every node shape and combinator.
- [Sources and scanning](sources.md) — the poll/drain split and the
  honest-gap discipline.
- [One engine, and where the domain enters](seams.md) — what the drain reads off
  the plan, the two seams that remain (`Source`, `compute`), and hand-assembled
  graphs.

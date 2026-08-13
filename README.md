# reactive_dag

Build **incremental data pipelines** out of Ash resources.

You declare what each table is derived from; the engine works out what to
recompute when something upstream changes, and recomputes only that. A resource
is a node, its `reactive do … end` block is the computation, and its rows are the
result — there is no separate store, no shadow table, and nothing to keep in
sync.

```elixir
reactive do
  recompute_by :category, to: :expenses, from: :category
  reduce group_by: :category, into: [sum: [amount: :total], count: :n]
end
```

Edit one expense and one category's total is recomputed. Not the table, not the
graph — the row that moved.

Extracted from two apps that independently grew the same engine (the Red Hook
`cascade` pipeline and the u2i compliance portal's `model_eval`), and now shared
by both.

**Guides:** [Getting started](https://hexdocs.pm/reactive_dag/getting-started.html)
· [Authoring nodes](https://hexdocs.pm/reactive_dag/authoring-nodes.html)
· [LLM nodes](https://hexdocs.pm/reactive_dag/llm-nodes.html)
· [Sources and scanning](https://hexdocs.pm/reactive_dag/sources.html)
· [Configuration](https://hexdocs.pm/reactive_dag/configuration.html)
· [The seams](https://hexdocs.pm/reactive_dag/seams.html)

---

## A pipeline, end to end

Three resources: raw expenses, a rollup, and a verdict over the rollup. This is
the whole thing — there is no wiring file, no registry, and no dispatch to write.

### 1. The leaf — where data enters

```elixir
defmodule MyApp.Expenses do
  use Ash.Resource, data_layer: AshPostgres.DataLayer, extensions: [ReactiveDag.Node]

  attributes do
    attribute :id, :string, primary_key?: true
    attribute :category, :string
    attribute :amount, :float
  end

  actions do
    defaults [:read, :destroy]
    create :upsert do upsert?(true); accept([:id, :category, :amount]) end
  end

  reactive do
    leaf? true
    dirties_on [:create, :update, :destroy]   # ordinary writes start the cascade
  end
end
```

`dirties_on` is the important line. With it, `Ash.create!/1` on this resource
marks its key dirty **inside the write's own transaction** — so a rolled-back
write leaves nothing to recompute, and a committed one always leaves a mark. No
call site has to remember.

### 2. The rollup — derived from the leaf

```elixir
defmodule MyApp.CategoryTotals do
  use Ash.Resource, data_layer: AshPostgres.DataLayer, extensions: [ReactiveDag.Node]

  attributes do
    attribute :category, :string, primary_key?: true
    attribute :total, :float
    attribute :n, :integer
  end

  actions do
    defaults [:read, :destroy]
    create :upsert do upsert?(true); accept([:category, :total, :n]) end
  end

  reactive do
    # "a change to one expense invalidates one category" — this single line
    # supplies the input edge, the grouping, and the read scope.
    recompute_by :category, to: :expenses, from: :category

    reduce group_by: :category, into: [sum: [amount: :total], count: :n]
  end
end
```

No `read:`, no `key:`, no `upsert:`. The library reads `expenses` (scoped to the
dirty categories), folds each group, and upserts the row into *this* resource by
its own identity.

### 3. The verdict — derived from the rollup

```elixir
defmodule MyApp.BudgetHealth do
  use Ash.Resource, data_layer: AshPostgres.DataLayer, extensions: [ReactiveDag.Node]

  attributes do
    attribute :key, :string, primary_key?: true
    attribute :status, :string
    attribute :headroom, :float      # why a table is worth having
  end

  actions do
    defaults [:read, :destroy]
    create :upsert do upsert?(true); accept([:key, :status, :headroom]) end
  end

  reactive do
    reduce over: :category_totals,
           group_by: :category,
           into: fn _cat, [row | _] ->
             %{status: if(row.total < 1000.0, do: "present", else: "failing"),
               headroom: 1000.0 - row.total}
           end
  end
end
```

A verdict is an ordinary row with a `:status` column. "What is failing?" is
`filter(status == "failing")` — a plain Ash read, with policies, loads and joins.

### 4. Run it

```elixir
plan = ReactiveDag.Node.graph([MyApp.Expenses, MyApp.CategoryTotals, MyApp.BudgetHealth])

{:ok, report} =
  ReactiveDag.Drain.run(plan,
    recompute: ReactiveDag.Node.Recompute,
    key_rule:  ReactiveDag.Node.KeyRule)
```

`graph/2` assembles and validates at that point: every edge resolves, ids are
unique, the graph is acyclic, declared attributes exist. An authoring mistake
fails here rather than at 3am mid-drain.

Writing two expenses and draining once gives this — depth order, and only the
keys that actually moved:

```
expenses         changed: ["e1", "e2"]           triggered_by: nil
category_totals  changed: ["meals", "travel"]    triggered_by: "expenses"
budget_health    changed: ["meals", "travel"]    triggered_by: "category_totals"
```

Now change one expense in `travel` and drain again: `category_totals` claims
`["travel"]` only, `meals` is never read, and `budget_health` recomputes one row.
That proportionality is the whole point of the library.

The `%Drain.Report{}` is the processing trace — one step per recompute with
`cell`, `claimed`, `changed`, `triggered_by`, `duration_us`, plus run totals.
`triggered_by` reconstructs the causal chain above.

### Configuration

```elixir
config :reactive_dag,
  repo: MyApp.Repo,          # REQUIRED
  dirty_table: "my_dirty"    # optional; defaults to reactive_dag_dirty
```

One table, the dirty frontier, and `ReactiveDag.Migration.up/1` creates it.
Everything else is your own resources with their own migrations. Call
`ReactiveDag.Config.validate!/0` at boot to catch a misconfiguration there rather
than at the first query.

---

## Declaring the computation

Authoring is **Ash-first**: start with what Ash expresses declaratively and step
outward only as far as the shape demands. Every rung writes its rows and reports
only the changed keys.

| rung | when |
|---|---|
| `aggregate` | the datastore can do it — one GROUP BY, no rows in the BEAM |
| `reduce` | a fold over a group |
| `join` | correlate two sides of one input |
| `union` | roll many nodes' rows into one queryable table |
| `per_key` | one call per row — an LLM, an embedding, a fetch |
| `run :action` | a generic Ash action on this resource |
| `compute Mod` | arbitrary Elixir |

**`aggregate`** — the datastore does it: group and aggregate a relationship in
ONE query, nothing crossing into the BEAM. Only for relationship aggregates,
since Ash has no arbitrary `GROUP BY … → rows`.

```elixir
aggregate over: :readings, avg: [flow: :avg_flow], count: :day_count
```

**`reduce`** — an in-BEAM fold. The library reads the input's resource
auto-scoped to the dirty keys; `group_by:` names attributes, `into:` declares the
fold. Keys derive as `"gf|2025"` from a composite group.

```elixir
reduce group_by: [:fund, :fy], into: [sum: [amount: :total], count: :n]
```

Escapes, each independent: `query:` shapes the read without leaving Ash
(`fn q, dirty -> … end`); a fn `group_by`/`key`/`into` for computed shapes;
`expand:` when one group produces many rows.

**`join`** — a left join over ONE input. Sides are attributes (`left: :acct`) or
`[key: :acct, where: [kind: "budget"]]` discriminator splits. An absent side
yields nils, because the declared-vs-observed gap is usually information.

```elixir
join over: :entries,
     left:  [key: :acct, where: [kind: "budget"]],
     right: [key: :acct, where: [kind: "actual"]],
     outer: true,
     into:  [left: [amount: :budget], right: [amount: :actual]]
```

**`union`** — one row per `(input, key)` across several inputs, which is how
"what is failing *anywhere*?" becomes one indexed table instead of a scan per
cell. Maintained incrementally: a verdict flips, one row updates.

```elixir
union from: [:category_health, :fund_balance],
      into: [check: :cell, subject: :key, status: :status]
```

**`per_key`** — one action call per input row, with `fingerprint:` to skip the
call when nothing it depends on moved. That skip is the point when the call is an
LLM.

```elixir
per_key :summarise,
  args: [text: :body],
  fingerprint: [:body],
  into: [summary: :summary]
```

**`run :action`** — the Ash-native escape hatch: a generic action on this
resource takes `(keys, cell_id)`, does its own writes, and returns the changed
keys. Arguments, policies and `Ash.run_action` testability all still apply.

**`compute Mod`** — arbitrary Elixir implementing `ReactiveDag.Op`. Mirrors Ash's
`calculate :x, :type, MyModule`: the arbitrary case is an entity too, not a
schema key beside the declarative ones.

### `recompute_by` — the declaration the engine cares about

*What unit does a change invalidate?* It supplies the input edge, the grouping,
the claim resolution and the read scope in one line, which is why it replaces
`key_rule` on combinator nodes.

| form | meaning |
|---|---|
| omitted | key-for-key |
| `recompute_by :category, to: :expenses, from: :category` | per unit, resolved by lookup |
| `recompute_by :month, from_key: true` | per unit, parsed from the key's segments |
| `recompute_by :cell` | redo everything |

It is the *recompute* unit, not the output's grain — percentiles
`recompute_by :day` while their rows are keyed day+percentile.

### Input edges: `ref` vs `context`

- **`ref :x`** (also `depends_on [:x]`, or a combinator's `over:`) — a
  **recompute edge**: when `x` changes, this node is dirtied.
- **`context :x`** — a **context edge**: the node reads `x` as settled context
  but is *not* recomputed when `x` changes. Still a real input — validated, and
  ordered by depth so `x` settles first — it just doesn't propagate.

`context` earns its keep when recompute is expensive or non-deterministic and
consults mutable context it shouldn't be re-triggered by:

```elixir
reactive do
  compute MyApp.EnhanceMinutes   # an LLM pass
  ref :transcripts               # a transcript change RE-RUNS the LLM
  context :people                # a people edit does NOT — the LLM reads
                                 # current people next time it runs anyway
end
```

---

## Getting data in

**Ordinary writes** — `dirties_on [:create, :update, :destroy]`, as above. The
mark happens inside the write's transaction.

**Scanners** — when the data comes from outside (a fleet API, a crawler, an LLM),
the fetch is effectful and fallible, so it stays outside the drain. A leaf names
its source and `ReactiveDag.Node.Rows.reconcile/3` does the set math:

```elixir
reactive do
  leaf? true
  scan MyApp.Sources.FleetScan
  fingerprint [:content_md5]     # what counts as a changed observation
end
```

`fingerprint` matters for a scanned leaf: its row carries fields that move on
*every* observation without the observation having changed — a `last_seen_at` by
definition, an `etag` a server may re-issue for identical bytes. Comparing every
attribute would fire the cascade on each poll. Name the one value that decides
instead.

The discipline that matters most: **an upstream you could not reach writes
nothing.** A scan that failed must not hand `reconcile/3` an empty set, or every
key reads as vanished and a downstream rollup goes vacuously green. See
[Sources and scanning](https://hexdocs.pm/reactive_dag/sources.html).

**Human edits** — a managed list or an approval writes a leaf like anything else:
the host's normal write, then a dirty mark.

## Reading results

A node's results are its own rows, so the first answer is an ordinary Ash read:

```elixir
MyApp.BudgetHealth |> Ash.Query.filter(status == "failing") |> Ash.read!()
```

For the same rows addressed by **cell key** — how the DAG talks about them — go
through the cell:

```elixir
cell = plan.cells["budget_health"]

ReactiveDag.Node.Rows.all(cell)               # [%{key:, status:, record:}]
ReactiveDag.Node.Rows.status_histogram(cell)  # %{"failing" => 1, "present" => 4}
ReactiveDag.Verdict.for_cell(cell)            # a rolled verdict + failing sample
```

`ReactiveDag.Insights` is the graph viewed from outside — `levels/1` and
`edges/1` for structure, `cell_status/2` and `summary/1` for per-cell state,
`pending/1` for what the next drain would do. No UI dependency;
[reactive_dag_dashboard](https://github.com/u2i/reactive_dag_dashboard) renders
it.

## Watching a drain

The drain emits `:telemetry`, so a dashboard, a metrics backend and a log can
each observe it without any of them changing how it is called:

```elixir
:telemetry.attach("drain-log", [:reactive_dag, :drain, :stop], fn _e, m, meta, _ ->
  Logger.info("drained #{length(meta.cells_touched)} cells in #{m.duration_us}us")
end, nil)
```

`start` / `step` / `stop` / `exception`. The `step` event carries the changed
**keys**, not just a count — which is what lets a consumer refresh only what
moved rather than re-reading the graph.

---

## The seams

The engine decides *when* and *in what order* cells recompute. It never decides
*how*, or *what a value means*. Three named seams take the domain:

- **`ReactiveDag.RecomputeStrategy`** — how a cell recomputes. `Node.Recompute`
  handles everything above; a host with a different execution model (the
  compliance portal runs set-based SQL keyed on `op`) brings its own.
- **`ReactiveDag.KeyRule`** — how a change propagates to a parent. `Node.KeyRule`
  reads it off the block; `recompute_by` usually means you never touch this.
- **`ReactiveDag.Source`** — how outside state gets into a leaf, in a poll phase
  outside the drain so one unreachable vendor cannot wedge the rest.

A host can also skip the DSL entirely: build `ReactiveDag.Cell` structs, call
`ReactiveDag.Graph.build/1`, and bring its own strategy. That is how both apps
ran before adopting the `Node` surface, and the substrate still supports it —
`Cell`'s `meta` is an open map with an `Access` impl, so host fields ride along
and read as `cell[:field]`.

The library owns the schedule and one table. The host owns its resources, its op
algebra, and its executor.

## Design notes

A few decisions worth knowing, because each replaced something that seemed
reasonable first.

**Verdicts are rows.** There used to be a tableless node shape (`verdict? true`)
that wrote a status straight into a coordination table. It saved a migration when
the answer was one word, and cost a ceiling: that table's schema was fixed, so
the moment a verdict wanted a `headroom` the shape had nothing to offer and you
wrote the table anyway. A row costs a migration and answers every later question.

**There is no shadow table.** The library used to write a coordination row per
`(cell, key)`, carrying a status and freshness. Once every node had a resource,
that table recorded nothing the resource didn't already say — so it went, along
with the writer seam that existed to extend it. A host that wants a `source_ref`
or a `last_seen_at` puts it on the node's resource, where the rest of the row is.

**Retirement destroys the row.** A unit whose inputs have all gone produces
nothing, and a derived row you cannot distinguish from a live one defeats the
point of materializing it. So a vanished unit's row is destroyed and its key
reported as changed, which is why a node that can retire needs a destroy action.

**No command frontier.** There was a second `seq`-ordered frontier for intents,
with per-scope serialization and a human-in-the-loop blocked state. In both hosts
the commands turned out to be straight CRUD drained inline, so nothing was ever
queued; the database already provided the serialization, and its scope-freeze
turned a failed edit into a wedged queue. A genuinely deferred, approval-gated
write is the case that would justify bringing it back.

## Status

Both hosts run on this substrate — a per-key Elixir recompute that calls LLMs and
parses PDFs (cascade) and a set-based SQL recompute (the compliance portal) —
which is the evidence that the seams are in the right places. See
[ADR-001](https://hexdocs.pm/reactive_dag/adr-001-reactive-dag-library.html) for
the boundary and the design law behind it.

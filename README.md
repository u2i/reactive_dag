# reactive_dag

A domain-agnostic **reactive DAG engine** for Elixir/Ash apps: a dirty frontier
+ depth-ordered incremental drain + change propagation, plus the coordination
tuple, leaf-reconcile, and nested-expression lowering that go with it. Extracted
from two apps that independently grew the same engine (the Red Hook `cascade`
pipeline and the u2i compliance portal's `model_eval`), and now shared by both.

**Documentation:** the guides are the front door — [Getting
started](https://hexdocs.pm/reactive_dag/getting-started.html), [Authoring
nodes](https://hexdocs.pm/reactive_dag/authoring-nodes.html), [Sources and
scanning](https://hexdocs.pm/reactive_dag/sources.html),
[Attestations](https://hexdocs.pm/reactive_dag/attestations.html) (human
sign-off as a first-class input), and [The
seams](https://hexdocs.pm/reactive_dag/seams.html). This
README is the reference-style overview.

The substrate decides *when* and *in what order* cells recompute; it never
decides *how* or *what a value means*. Each host brings its domain at the seams:

- **`ReactiveDag.RecomputeStrategy`** — how a cell recomputes (cascade: per-key
  Elixir that may call an LLM / parse a PDF; the portal: one set-based SQL join).
  Returns the keys that actually changed.
- **`ReactiveDag.KeyRule`** — how a change propagates to a parent (identity, a
  remap, or `:all` for a whole-cell recompute).
- **`ReactiveDag.CoordinationWriter`** — how a cell's coordination tuples are
  written (the host writes its spine + extension columns in one atomic upsert).
  A default spine-only writer ships; hosts with extension columns supply their own.

## What the library owns

| Layer | Module | What it provides |
|---|---|---|
| Node IR | `ReactiveDag.Cell` | domain-neutral node; `op` is an **optional free-atom label** (load-bearing only for an op-dispatching `RecomputeStrategy` like `SetOp`); app fields ride in `meta` (with an `Access` impl so `cell[:field]` reads meta transparently). |
| Compiled plan | `ReactiveDag.Plan` | pure data: `cells / parents / depths`. |
| Graph math | `ReactiveDag.Graph` | `build/1` (validate + parent edges + longest-path depths + cycle check); `dirty_parents/4` (propagation via the host `KeyRule`). |
| Dirty frontier | `ReactiveDag.Frontier` | claim-as-delete over the host's dirty table; `mark_dirty / next_cell / claim / empty?`. |
| Drain loop | `ReactiveDag.Drain` | depth-ordered incremental propagation; `run/2` parameterized by the two seams, returning `{:ok, %Drain.Report{}}` — the processing trace (per-step cell/claimed/changed/`triggered_by`/`duration_us` + totals). An optional `:on_step` hook streams the same fields live. |
| Coordination tuple | `ReactiveDag.Tuple` | the shared `(cell_id, key, status, freshness)` spine over the host's tuple table: `put / put_changed / rows / present_keys / all_keys / keys_by_status / status_histogram / max_observed_at / reconcile / reconcile_set` + a `:key_scope` selector. Payload stays in the host's typed resources, joined by `key`. |
| Nested-expr lowering | `ReactiveDag.Lowering` | `walk/3` — the nested op-expression → flat-cell recursion both DSLs grew, parameterized by host callbacks (id grammar, ref resolution, cell construction). |
| Compile pipeline | `ReactiveDag.Dsl` | `compile / validate_cells` — resolve → structural-validate, with a domain-validation hook. |
| Op contract | `ReactiveDag.Op` | the behaviour a cell's compute module implements (`recompute(cell, keys) -> {:ok, changed}`) + the write API ops call (`put / tombstone / delete`, routed to the `CoordinationWriter`). |
| **Node authoring** | `ReactiveDag.Node` | the authoring surface — an **Ash resource extension**: a resource declares its op + dependencies + computation in a `reactive do … end` block. The resource *is* the node **and** its own payload table. `ReactiveDag.Node.graph/2` assembles the `Plan` from the node resources. |
| **Payload loop** | `ReactiveDag.Node.Payload` | writes a combinator's row into the node's own resource (the default; omit `upsert:`). A `verdict? true` node stores nothing of its own — its result is the coordination tuple. |
| **Scanner seam** | `ReactiveDag.Source` | the behaviour a scanner implements (`id / leaf_cells / poll`) — reads external state into a leaf in a *poll* phase outside the drain; `verify!/2` checks every declared leaf resolves to a real cell. |

The host owns its **physical tables** (dirty + tuple, named via config), its
**op algebra**, its **recompute executor**, and any **extension columns** on the
tuple (the portal's `strength` modality, cascade's tombstone/fingerprint
policy). The library owns the spine and the schedule; the domain differences sit
on named seams, not forks.

## Authoring a node

A node is an Ash resource with the `ReactiveDag.Node` extension. **The resource IS
the node and its own payload table** — its `reactive` block is the computation, its
`attributes` are the rows it materializes. The library **closes the payload loop**:
`into` returns a row and the lib writes it into *this* resource — no `upsert:`
needed for the common case.

```elixir
defmodule MyApp.BudgetRollups do
  use Ash.Resource, data_layer: AshPostgres.DataLayer,   # its OWN payload table
    extensions: [ReactiveDag.Node]

  attributes do
    attribute :fund, :string, primary_key?: true         # the row IS its identity —
    attribute :fy, :integer, primary_key?: true          # no :key column; the cell
    attribute :total, :float                             # key is "gf|2025", derived
  end
  actions do
    create :upsert do upsert?(true); accept([:fund, :fy, :total]) end
  end

  reactive do
    op :fold
    # ASH-FIRST: the library reads :fiscal_lines, groups by the attributes,
    # folds each group, upserts the row by its Ash IDENTITY, and Op.puts only
    # the changed keys. `key_rule: :group` claims per changed group. Every
    # slot has an escape hatch when the shape outgrows attributes.
    reduce over: :fiscal_lines,
           group_by: [:fund, :fy],
           key_rule: :group,
           into: [sum: [amount: :total]]
  end
end
```

`upsert:` is an **optional override** — supply it only to write somewhere *other*
than the node's own resource (e.g. an existing shadow table). A tableless node
(`data_layer: Ash.DataLayer.Simple`, no attributes) either supplies `upsert:` or
uses the `compute Module` escape hatch.

Authoring is **Ash-first** — start from what Ash expresses declaratively and
step outward only as far as the shape demands. Each form writes the result set
(into the node's resource, or a custom `upsert:`) and `Op.put`s only the
changed keys:

- **`aggregate`** — the datastore does it: group + aggregate a relationship
  (`avg`/`sum`/`count`/…) in ONE query — no rows cross into the BEAM. The
  node's resource is the group's resource; `over` is its `has_many`. Only for
  relationship aggregates (Ash has no arbitrary `GROUP BY … → rows`).
  Example: `aggregate over: :readings, avg: [flow: :avg_flow], count: :day_count`.
- **`reduce over_rel:`** — the edge as an **Ash relationship**: name a
  `has_many` on this resource and it supplies the input node (its destination),
  the grouping (its `source_attribute`/`destination_attribute` pair) and the
  `:group` claim traversal — the correspondence declared once and referenced by
  name, as Ash does everywhere else. Sugar for `over:` + `group_by:` pairs.
  Example: `reduce over_rel: :expenses, into: [sum: [amount: :total]]`.
- **`reduce`** — an in-BEAM fold, declared: the library reads the over node's
  resource (primary or a named `:read` action), auto-scoped to the dirty keys;
  `group_by:` names attributes, `into:` declares the fold
  (`[sum: [amount: :total], count: :n]`), keys derive as `"gf|2025"`
  (`key_prefix:` namespaces). Escapes: `query:` shapes the read WITHOUT leaving
  Ash (`fn q, dirty -> … end`); fn `group_by`/`key`/`into` for computed shapes;
  `expand:` for the group → many-rows shape (self-`:key`ed rows); a verdict
  node declares `status:` instead of `into:`.
- **`join`** — a left join over ONE input, declared: sides are attributes
  (`left: :declared_id`) or `[key: :acct, where: [kind: "budget"]]`
  discriminator splits; `into:` picks columns per side, absent sides yielding
  nils (the gap is information). fn escapes for computed side keys/columns;
  verdicts declare `status:`.
- **`run :action`** — the Ash-native escape hatch: the recompute is a GENERIC
  action on the node's own resource (`(keys, cell_id) -> changed keys`; the
  action writes its domain, the library `Op.put`s). Arguments, policies,
  `Ash.run_action` testability — the computation stays a first-class action.

Beyond Ash entirely — an LLM call, a PDF/Tigris fetch, a bespoke multi-input
recompute — the outermost escape hatch is a module: `compute MyOp` where `MyOp`
implements `ReactiveDag.Op`. (Mirrors Ash's `calculate :x, :type, MyModule` —
the arbitrary case is an entity too, not a schema key beside the declarative
ones.)

### Input edges: `ref` (recompute) vs `context` (read-as-context)

An input is one of two kinds:

- **`ref :x`** (also `depends_on [:x]`, or a combinator's `over:`) — a **recompute
  edge**: when `x` changes, this node is dirtied and recomputes. The normal edge.
- **`context :x`** — a **context edge**: the node READS `x` as settled context but
  is **not** recomputed when `x` changes. It's still a real input (validated,
  ordered by depth so `x` settles first, read at recompute) — it just doesn't
  propagate.

Use `context` when recompute is expensive/non-deterministic and consults mutable
context it shouldn't be re-triggered by — e.g. an LLM step that looks up a
human-curated table:

```elixir
reactive do
  op :map
  compute MyApp.EnhanceMinutes   # an LLM pass
  ref :transcripts               # a transcript change RE-RUNS the LLM
  context :people                # a people edit does NOT — the LLM just reads
                                 # current people the next time it runs
end
```

So an edit to a `context` input updates it, but drives no regeneration; the
consuming node picks up the current value whenever it next recomputes for its
own (recompute-edge) reasons.

```elixir
reactive do
  op :map
  compute MyApp.Ops.EventsExtract   # arbitrary recompute (LLM, fetch, …)
end
```

```elixir
# assemble + run a Node-authored graph (no host-written dispatch):
plan = ReactiveDag.Node.graph([BudgetRollups, FiscalLines, …], for_each: &fetch/1)
{:ok, report} =
  ReactiveDag.Drain.run(plan,
    recompute: ReactiveDag.Node.Recompute,   # runs reduce/join/aggregate or compute:
    key_rule:  ReactiveDag.Node.KeyRule)       # reads :identity | :all from the block
# report is a ReactiveDag.Drain.Report — the processing trace: one step per
# recompute (cell, claimed, changed, triggered_by, duration_us) + run totals.

# config
config :reactive_dag,
  repo: MyApp.Repo,
  dirty_table: "my_dirty",
  tuple_table: "my_tuple",
  coordination_writer: MyApp.Writer   # optional; a spine-only default ships
```

A host can also assemble cells by hand and bring its own strategy/key_rule —
`ReactiveDag.Graph.build(cells)` + `ReactiveDag.Drain.run(plan, recompute:,
key_rule:)` — which is how both apps ran before adopting the `Node` surface.

## Verdict nodes (no payload of their own)

A node whose computed result fits the coordination tuple — a status (and, if the
host extends the tuple, a strength) — needs **no payload table**. Mark it
`verdict? true` and declare `status:` — the verdict IS the result, so there is
no `into:` row to build. The key derives exactly as a payload row's would. No
`data_layer` table, no attributes, no `upsert:`.

```elixir
defmodule MyApp.StoreEncrypted do
  use Ash.Resource, data_layer: Ash.DataLayer.Simple, extensions: [ReactiveDag.Node]

  reactive do
    op :reconcile
    key_rule :all
    verdict? true                       # result lives in the tuple, not a table
    reduce over: :stores,
           group_by: :store,
           status: fn _store, [r | _] -> if r.enc, do: "present", else: "failing" end
  end
end
```

`status:` may return `{status, strength}` for a host whose tuple carries the
strength modality. This is the "purely calculated" node: it computes a verdict
per key and persists nothing beyond the coordination row. A **payload-bearing**
node (above) computes a typed value that doesn't fit the tuple, so it
materializes rows into its own resource. The line between them is exactly
whether the result fits the tuple's fixed schema.

## Human input

Scanners feed leaves out-of-band; a **human** edit (a managed list, an approval)
writes a leaf too — via whatever the host uses for writes (an Ash action, a plain
upsert), then marks the affected cells dirty so the drain propagates the
consequences.

The library previously shipped a *command frontier* — a second, `seq`-ordered
frontier for INTENTS, with per-scope serialization, a blocked/answer
human-in-the-loop state, and an audit table. It was **removed**: in both hosts the
commands turned out to be straight CRUD drained inline (enqueue immediately
followed by run), so nothing was ever actually queued. The serialization it offered
was already provided by the database, the audit trail is better served by a
change-log on the resource, and its scope-freeze turned a failed edit into a wedged
queue. A deferred/approval-gated write — where a change genuinely waits, unapplied,
for a human — is the case that would justify bringing it back.

Status: **both hosts run on the substrate** — the shared engine spans a per-key
Elixir recompute (cascade) and a set-based SQL recompute (the portal), all
coordination writes routed through the seam, proven by both suites green. Cascade
authors several ops via the `Node` `reduce`/`join` combinators; the standalone
compliance app consumes tagged releases. See
[ADR-001](https://hexdocs.pm/reactive_dag/adr-001-reactive-dag-library.html)
for the boundary, the seams, and the design law behind them.

# ADR-002 — A graph per tenant

**Status:** proposed — no code written. This document exists to be argued with
before the library is touched.
**Date:** 2026-08-21

## Context

A host runs one graph. It needs to run the same graph for several **tenants** —
independent populations that share a topology and share no data.

The requirement, stated as a decision rather than a preference:

- **The entire graph is per tenant.** Not one shared graph whose rows carry a
  tenant column, but N instances of the same topology.
- **A tenant graph is single-threaded; tenants run concurrently.**
- **No cell spans tenants** in the motivating case. The design should not
  preclude one, but nothing here depends on supporting it.
- **A dashboard switches tenant at the top level**, before the existing funnel
  (question → cell → view).
- **One tenant dimension**, not several. Decided; a second structural dimension
  is not in scope.

### Why a tenant column is not enough

The motivating case has keys minted by the upstream a source polls — a vendor id
from a third-party site, unique within that site and meaningless across sites.
Two tenants will produce the same key for different things.

That is not unusual; it is the normal situation whenever a leaf's keys come from
outside. So tenant cannot be only a filter column on a row: it has to reach the
**frontier**, where units of work are identified, not just the reads.

### The concurrency model, stated exactly

- **Within one graph: single-threaded.** This is a correctness requirement the
  library already documents (`drain.ex:51-58`): the per-cell claim is atomic, but
  the pick-then-claim PAIR is not serialized, so two drains over one graph can
  select the same cell and both recompute it. `with_lock/2` enforces one at a
  time; `:busy` is documented as not an error, because the frontier is a set and
  the loser's work is still there for the winner.
- **Across graphs: concurrent.** Three tenants, three locks, three drains in
  flight. The shape is
  `with_lock(fn -> Drain.run(tenant_plan) end, scope: tenant)`.

Two existing properties make this composable rather than a rewrite: the
transaction/savepoint is **per cell within one drain** (`drain.ex:238-259`), so it
assumes nothing about a global drain; and `Insights` is already concurrency-safe —
its counter is `:erlang.unique_integer([:monotonic, :positive])` and its ETS table
is `:public`, so two tenants' reports coexist untouched.

### What is part of which graph

Today, membership is **the resource list passed to `graph/2`**:

    ReactiveDag.Node.graph([Docs, Shell, Meeting, …], for_each: &fetch/1)

There is no named or registered graph. A plan is just the cells lowered from
those resources, and `Graph.build/1` computes `parents`/`depths` over exactly
that set. That is a good property for this design, because membership is
**enforced loudly**: `validate_inputs!/2` (`graph.ex:91`) raises
`cell "x" references unknown input "y"` if a cell's edge points outside the plan.
A per-tenant plan missing a cell fails at assembly, not silently at drain time.

So "a graph per tenant" means: `plan(tenant)` returns the cells for that tenant
and nothing else. The question is how each resource says which tenant its cells
belong to — which is where the blocker below lives.

### Superseded framing

An earlier draft of this ADR designed **scope-per-key inside one shared graph**:
a `scope` column on the dirty table, a scoped `claim/1`, scoped retirement. That
was the wrong shape for the requirement above, and it was strictly more work.
The corrected design is below; the rejected one is recorded at the end because
its failure modes are instructive.

## What the library already provides — and where it stops

`for_each` looks like graph-per-member (`node.ex:1729-1737`):

- A node declaring `for_each :tenants` builds **no template cell**.
  Instead one instance per member, with ids `"<base>.<member.id>"` —
  `docs.tenant_a`, `docs.tenant_b`.
- Each member's `meta` map is **stamped onto every cell** in that instance's
  sub-tree, so an instance carries its own tenant id with no per-cell
  declaration.

And because **the dirty table is keyed by `cell_id`** (`migration.ex:60`),
distinct instance ids give real isolation for free:

| concern | with instances |
|---|---|
| claim isolation | `claim/1`'s cell-wide `DELETE` is *correct* — claiming all of `docs.tenant_a`'s keys is exactly right |
| retirement | already per-cell, so no cross-tenant `retire_vanished` risk exists |

No `scope` column. No migration. No change to claim or retirement.

**But `for_each` does not compose, and that is the blocker.** Probed: two
per-tenant nodes, a leaf and a derived node reading it —

```elixir
reactive do: (id :docs;  for_each :tenants; leaf? true)
reactive do: (id :shell; for_each :tenants; depends_on [:docs])

graph([Docs, Shell], for_each: fetch)
#=> ** (ArgumentError) cell "shell.tenant_a" references unknown input "docs"
```

`for_each` rewrites the **cell id** but not the **input references** of a sibling
instance. `shell.tenant_a` still points at the bare `docs`, which no longer exists,
so `validate_inputs!` raises.

That is not a bug in `for_each` — it is what `for_each` was built for. Its
existing use (`test/node_test.exs:355`) is a per-member fan-out **under a shared
upstream**: `rule_concern.r1` and `.r2` both read the one `edr_agents` leaf. It
gives a per-member *layer*, not a per-member *subgraph*.

So **a whole graph per tenant is not expressible today.** This is the central
change, and it is larger than the scanner relaxation this ADR's previous draft
identified as the only blocker.

### Change 0 — partition the frontier by plan

Probed: `next_cell/2` is
`Enum.min_by(dirty_cells(), &Map.get(depths, &1, 1_000_000))` where
`dirty_cells()` is a **global** `SELECT DISTINCT cell_id`. A cell absent from
`depths` gets depth 1,000,000 but **remains a candidate**:

| frontier state | tenant A's plan picks |
|---|---|
| both tenants dirty | `docs.tenant_a` — its own, wins on depth |
| **only tenant B dirty** | **`docs.tenant_b`** — a foreign cell |

`empty?/0` is global too, so A's drain loop does not terminate while B has work.

So instance ids alone do **not** give concurrency, and `with_lock(scope:)` does
not fix it — a lock prevents *simultaneity*, it does not *partition* the
frontier. Those are different things.

Fix: filter candidates to the cells in this plan (`Map.keys(depths)`), and give
`empty?/0` the same filter. No schema change, no scope column. This is the
prerequisite for everything else here.

### Change 1 — `tenant`, borrowed from Ash's shape

The problem is that `shell.tenant_a` must read `docs.tenant_a` rather than bare
`docs`. The first instinct is to annotate the edge — `depends_on [{:docs, per:
:tenants}]` — one chance to forget per node, on a mistake whose symptom is one
tenant reading another tenant's data.

**Ash already solves this problem, and not by annotating edges.** A resource
declares once that it is tenant-scoped; the tenant then travels in **context**,
and every relationship traversal inherits it. A resource that is not tenant-
scoped is global, and mixing the two is a declared property rather than something
inferred per-edge.

Borrow that shape:

```elixir
reactive do
  id :shell
  tenant :tenants            # this node's cells are per-tenant
  depends_on [:docs]         # resolves to docs.<tenant> BECAUSE both are tenanted
end
```

Edge resolution follows from the two nodes' own declarations:

- target is tenanted → resolve to `<target>.<tenant>` (instance-local)
- target is global → keep the bare id (a shared upstream)
- neither node is tenanted → today's behaviour, untouched

Declared once per node, and the ambiguous case that made per-edge inference
dangerous — a generator over a *different* population — cannot arise, because
tenancy is a single named dimension rather than an arbitrary population.

**Borrow the shape, do not delegate to Ash.** Two reasons:

- The frontier is raw SQL (`Frontier.query!/2`) and never sees Ash context, so
  Ash tenancy cannot provide claim isolation. Change 0 is required either way.
- The cross-node join revert (`b93c69e`) is explicit about the failure mode of
  over-borrowing: "has_many was the wrong borrowing… it asserts cardinality,
  loadability, writability and a public API surface; a DAG edge wants exactly one
  fact from it." Reading an Ash `multitenancy` block and inferring DAG structure
  from it would repeat that: it asserts query-layer behaviour the DAG does not
  want. Ash's own tenancy stays a read-path convenience, adopted separately.

#### `tenant` is NOT `for_each` — separate graphs, not more cells

**Decided.** These are different mechanisms, and the difference is the whole
point:

| | question | effect |
|---|---|---|
| `tenant` | which GRAPH does this cell belong to? | N separate plans, drained concurrently |
| `for_each` | how many cells within ONE graph? | more cells in one plan, one drain |

`for_each` expands a node into several cells inside a single plan, sharing an
upstream and drained by one single-threaded drain — the within-tenant case.
`tenant` does not expand anything: it says which plan a node's cell belongs to.

**This simplifies Change 1 considerably.** If a tenant is a separate plan, then
within that plan there is only one `shell` and one `docs` — so the cell id needs
no tenant suffix at all, and `shell` reads `docs` exactly as it does today. No
instance-local edge rewriting, no `<base>.<tenant>` ids, and the `for_each`
composition problem disappears because the two features never touch the same
mechanism.

What `tenant` then needs is much smaller:

- **`graph/2` builds one plan per tenant.** The host asks for a tenant's plan;
  the library lowers the tenanted resources for that tenant and the global ones
  alongside them. Cell ids stay as authored.
- **Cell ids must be unique per FRONTIER, not per plan.** This is the one real
  constraint. The dirty table is keyed by `(cell_id, key)` and is shared across
  tenants, so two tenants' `shell` cells would collide there. Either the frontier
  gains the tenant (a `tenant` column, which is the first draft's `scope` idea
  arriving from the other direction and now justified), or cell ids carry the
  tenant after all.

That trade — tenant in the frontier vs tenant in the cell id — is the design
question this change turns on, and it should be measured rather than guessed.
Carrying it in the frontier keeps authored ids clean and every plan identical;
carrying it in the id keeps the frontier untouched and makes a dirty row
self-describing. Both are viable; they were conflated in earlier drafts.

A node declaring both `tenant` and `for_each` is now unproblematic: `for_each`
expands within whichever plan the node belongs to, and the two do not interact.

### Change 2 — a scanner on a generator node is per-instance

`verify_one_node_per_source!` (`node.ex:1481`) raises when two cells name one
scan module. Probed with a generator leaf declaring a scanner:

```
reactive_dag: Crawler is declared by 2 nodes (docs.tenant_a, docs.tenant_b).
... so the upstream is polled 2 times.
```

The check is right for the case it was written for — one crawl producing rows for
several leaves, where N cells means N redundant polls of one upstream — and
**inverted for this one**. N tenants have N upstreams; polling N times is the
requirement. It should group by `(module, generator)`
so cells differing only by member are not a violation, while cells sharing a
module *without* being instances of one generator still raise.

### Change 3 — the member stamp reaches `poll/1`

An instance knows which tenant it is (its `meta` stamp), and a scanner needs that
to poll the right upstream. `scan_args` is currently declared statically in the
DSL (`args: [recent: true, …]`).

An instance's poll options should be its declared `args` merged with its member
stamp — so `Source.refresh(plan, "docs.tenant_a")` reaches `poll/1` with the
tenant named, without the host wiring it per instance.
The precise merge key wants deciding (all of `meta`, or a declared subset) and is
the main open design question below.

### Change 4 — scheduling: sweep or per-tenant?

`scan_jobs/1` already emits one entry per cell with a `scan` (`source.ex:528`),
so three instances are three units of work automatically. `crontab/2` is the
decision point: it defaults to **one entry per distinct cadence** — a sweep that
runs every source sequentially inside one job, followed by one drain
(`source.ex:631-636`) — and takes `per_cell: true` for one entry per cell.

Under the default sweep, N tenants poll **sequentially in one job** and then drain
once. That is the opposite of the requirement: one tenant's slow upstream should
not hold up the others. So per-tenant scheduling means either `per_cell: true`,
or a new per-tenant grouping — one sweep per tenant rather than one per cadence.

One sweep per tenant looks right: it keeps the "several sources then one drain"
shape that makes a sweep worth having, while making the unit of serialization the
tenant. That is a genuine addition to `crontab/2`, not a flag it already has.

## Dashboard: the tenant switch

A host graph of ~30 cells becomes ~90 dotted ids across three tenants in one flat
list — the funnel's cell picker (`rdd-starts`) would be unusable, and
`Tree.starting_points/2` would offer one copy of every source per tenant.

The plan already arrives as a host-configured MFA, applied on every `load/1`. So a
tenant switch is a **different argument to the same MFA**, not new plumbing:

- the host's `plan/1` takes a tenant and returns that tenant's graph — its
  instances only, so the rendered cell count stays as it is today.
- The switch renders **above** `rdd-ask`, outside the funnel. The page is already
  a narrowing sequence (question → cell → view) and tenant sits before all of
  it — the same reasoning that moved `runs` into the page header rather than
  leaving it in the view bar.
- Tenant rides in the URL like `direction` does, so a link to a cell is a link to
  *that tenant's* cell.

Open: whether the library should expose "the tenants in this plan" so the
dashboard can render the switch generically, or whether the host supplies the
list. The library knows the member ids (it expanded them); the human-readable
names are the host's.

## Consequences

**Wanted.** Three tenants drain concurrently, each with its own lock, claim set
and `%Drain.Report{}`. One tenant's slow upstream does not block another's. Cell
ids carry the tenant, so upstream-minted key collisions are structurally
impossible. A dashboard shows one tenant at a time at its current scale.

**Costs, stated plainly.**

- `Drain.transaction/1` holds a connection at `timeout: :infinity`, justified in
  its own docs by "drains are SERIALIZED… one connection, not one per concurrent
  drain" (`frontier.ex:183`). N tenants draining concurrently means N long-held
  connections. Pool sizing is a deliberate decision, and there should be a cap.
- `DrainWorker`'s Oban uniqueness must become per-tenant or three tenants' jobs
  collapse into one.
- Three tenants means three times the scheduled crawling against three
  third-party sites. Intended, but a real increase in outbound load — and the
  default sweep would serialize them, so this needs the scheduling decision in
  Change 4 rather than falling out for free.
- Every host resource that should be per-tenant must declare it — a broad,
  mechanical change across the whole graph, and one that is easy to apply
  incompletely. A cell that *should* be per-tenant and isn't becomes a shared
  cell three tenants write to, which is the collision this design exists to
  prevent. Worth a check that enumerates cells lacking `for_each`.

**Out of scope.** Whether a host's ROW keys also become composite. Instance ids
isolate the *cells*; whether `docs.tenant_a`'s rows live in one table with a
tenant column or a table per tenant is a host schema decision with its own
measurement.

## Open questions

1. **How does the member stamp reach `poll/1`?** All of `meta`, or a declared
   subset (`tenant :tenants, poll_args: [:tenant]`)? Passing all
   of `meta` is convenient and leaks internal stamps into a third-party API call.
2. **Per-tenant cadence?** `every:` is declared once on the template. If every
   tenant can be polled on one schedule, nothing is needed.
3. **Does `slice` still apply to the tenant dimension?** Under this design, no —
   you pick a tenant by choosing which graph to look at, so a tenant is not a
   slice. Any remaining slice becomes per-tenant by virtue of reading a tenant's
   own instance rather than by taking an argument, so the DEPENDENT-slice feature
   an earlier draft proposed is not needed for this.
4. **Who lists the tenants for the dashboard switch?** Library (it knows member
   ids) or host (it knows names)?
5. **Tenant in the frontier, or in the cell id?** The live question (see Change
   1). The dirty table is shared across tenants and keyed by `(cell_id, key)`, so
   one of the two must carry the tenant. A `tenant` column keeps authored ids
   clean and every plan identical; a suffixed id keeps the frontier untouched and
   makes a dirty row self-describing.
6. **How does a resource say it is NOT per-tenant?** A global node is the
   exception rather than the default, so the declaration should probably be
   opt-out (`global? true`, as Ash spells it) — and a cell that should be
   per-tenant and is not becomes a shared cell every tenant writes to. Worth a
   check that enumerates non-tenanted cells in a tenant plan rather than trusting
   a mechanical sweep.

## Rejected alternatives

- **Scope-per-key in one shared graph** (this ADR's first draft). A `scope`
  column on the dirty table, scoped `claim/1`, scoped retirement. Rejected: it
  does not match "the entire graph is per tenant", and it is strictly more work
  than instances. Two failure modes it would have had are worth recording,
  because both are absent under instances: a scoped whole-cell pass reconciling
  against `Rows.all/1` — an unscoped `Ash.read!()` — would have **retired every
  other tenant's rows** (the partial-read-vs-total-baseline shape of the two-node
  join bug in #183, except destroying rows rather than nil-ing columns); and a
  nullable `scope` would have broken `mark_dirty`'s `ON CONFLICT` coalescing,
  since Postgres treats nulls as distinct in a unique index.
- **A dirty table per tenant.** `dirty_table` is already runtime-configurable
  (`frontier.ex:268`), so this works today with no library change. Rejected: the
  table name is global application config, so it cannot vary per concurrent
  drain — two tenants draining at once would need two application environments.
- **Ash `context` multitenancy (schema per tenant).** Rejected: it makes every
  cross-tenant question N queries with no joins. With no cross-tenant cells in
  scope this costs little *today*, which is exactly why it is a trap — the first
  cross-tenant feature would require unwinding it.
- **Ash `attribute` multitenancy.** Not rejected, but not load-bearing: it is a
  convenience on the read path. Ash tenancy lives in query context and the
  frontier is raw SQL through `query!/2` that never sees it, so it cannot provide
  claim isolation. Instance ids already do.

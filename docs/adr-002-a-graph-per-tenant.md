# ADR-002 — A graph per tenant

**Status:** proposed — no code written. This document exists to be argued with
before the library is touched.
**Date:** 2026-08-21

## Context

Cascade covers one municipality. It is about to cover three — Red Hook Village,
Town of Red Hook, Village of Tivoli. The requirement, stated as a decision
rather than a preference:

- **The entire graph is per municipality.** Not one shared graph whose rows carry
  a municipality column, but three instances of the same topology.
- **No cell is cross-municipality.** Confirmed; there is no "compare all three"
  roll-up in scope. Every cell belongs to exactly one municipality.
- **The dashboard switches tenant at the top level**, before the existing funnel
  (question → cell → view). You pick a municipality, then ask questions of that
  municipality's graph.

### The concurrency model, stated exactly

- **Within one graph: single-threaded.** This is a correctness requirement the
  library already documents (`drain.ex:51-58`): the per-cell claim is atomic, but
  the pick-then-claim PAIR is not serialized, so two drains over one graph can
  select the same cell and both recompute it. `with_lock/2` enforces one at a
  time; `:busy` is documented as not an error, because the frontier is a set and
  the loser's work is still there for the winner.
- **Across graphs: concurrent.** Three tenants, three locks, three drains in
  flight. The shape is
  `with_lock(fn -> Drain.run(tenant_plan) end, scope: "tivoli")`.

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

So "a graph per tenant" means: `plan(municipality)` returns the cells for that
municipality and nothing else. The question is how each resource says which
tenant its cells belong to — which is where the blocker below lives.

### The fact that forces per-tenant identity

`meeting_id` looks like `_07252024-459` and is assigned by **AgendaCenter's own
routing** (`sources/agenda_center_crawler.ex:422` builds it as `"_" <> id` from
the listing page). It is unique within one municipality's site and says nothing
across sites. Two municipalities will collide.

### Superseded framing

An earlier draft of this ADR designed **scope-per-key inside one shared graph**:
a `scope` column on the dirty table, a scoped `claim/1`, scoped retirement. That
was the wrong shape for the requirement above, and it was strictly more work.
The corrected design is below; the rejected one is recorded at the end because
its failure modes are instructive.

## What the library already provides — and where it stops

`for_each` looks like graph-per-member (`node.ex:1729-1737`):

- A node declaring `for_each :municipalities` builds **no template cell**.
  Instead one instance per member, with ids `"<base>.<member.id>"` —
  `meeting_docs.tivoli`, `meeting_docs.red-hook-village`.
- Each member's `meta` map is **stamped onto every cell** in that instance's
  sub-tree, so an instance carries its own municipality id with no per-cell
  declaration.

And because **the dirty table is keyed by `cell_id`** (`migration.ex:60`),
distinct instance ids give real isolation for free:

| concern | with instances |
|---|---|
| claim isolation | `claim/1`'s cell-wide `DELETE` is *correct* — claiming all of `docs.tivoli`'s keys is exactly right |
| retirement | already per-cell, so no cross-tenant `retire_vanished` risk exists |

No `scope` column. No migration. No change to claim or retirement.

**But `for_each` does not compose, and that is the blocker.** Probed: two
per-municipality nodes, a leaf and a derived node reading it —

```elixir
reactive do: (id :docs;  for_each :municipalities; leaf? true)
reactive do: (id :shell; for_each :municipalities; depends_on [:docs])

graph([Docs, Shell], for_each: fetch)
#=> ** (ArgumentError) cell "shell.tivoli" references unknown input "docs"
```

`for_each` rewrites the **cell id** but not the **input references** of a sibling
instance. `shell.tivoli` still points at the bare `docs`, which no longer exists,
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
| both tenants dirty | `docs.tivoli` — its own, wins on depth |
| **only tenant B dirty** | **`docs.redhook`** — a foreign cell |

`empty?/0` is global too, so A's drain loop does not terminate while B has work.

So instance ids alone do **not** give concurrency, and `with_lock(scope:)` does
not fix it — a lock prevents *simultaneity*, it does not *partition* the
frontier. Those are different things.

Fix: filter candidates to the cells in this plan (`Map.keys(depths)`), and give
`empty?/0` the same filter. No schema change, no scope column. This is the
prerequisite for everything else here.

### Change 1 — instance-local edges

When `X` declares `for_each :pop` and `depends_on [:y]`, and `Y` is **also** a
generator over the same population, `x.member`'s edge must resolve to `y.member`.

The rule needs care, because both behaviours must survive:

- `:y` is a generator over the same population → resolve to `y.<member>`
  (instance-local: the per-tenant subgraph).
- `:y` is not a generator → keep the bare `y` (shared upstream: the existing
  fan-out shape, which the portal depends on).
- `:y` is a generator over a *different* population → ambiguous, and should raise
  rather than guess.

That last case is why this wants to be explicit rather than inferred. An
alternative worth weighing: an opt-in marker on the edge
(`depends_on [{:docs, per: :municipalities}]`) so instance-locality is declared
rather than derived from whether the target happens to be a generator. The
library's own history favours the declared form — a silently-wrong edge here
means a tenant reading another tenant's data, which nothing downstream detects.

### Change 2 — a scanner on a generator node is per-instance

`verify_one_node_per_source!` (`node.ex:1481`) raises when two cells name one
scan module. Probed with a generator leaf declaring a scanner:

```
reactive_dag: Crawler is declared by 2 nodes (docs.red-hook-village, docs.tivoli).
... so the upstream is polled 2 times.
```

The check is right for the case it was written for — one crawl producing rows for
several leaves, where N cells means N redundant polls of one upstream — and
**inverted for this one**. Three municipalities have three AgendaCenter sites;
polling three times is the requirement. It should group by `(module, generator)`
so cells differing only by member are not a violation, while cells sharing a
module *without* being instances of one generator still raise.

### Change 3 — the member stamp reaches `poll/1`

An instance knows which municipality it is (its `meta` stamp), and the crawler
needs that to crawl the right site. `scan_args` is currently declared statically
in the DSL (`args: [recent: true, year: &…/0]`).

An instance's poll options should be its declared `args` merged with its member
stamp — so `Source.refresh(plan, "meeting_docs.tivoli")` reaches
`poll/1` with `municipality: "tivoli"` without the host wiring it per instance.
The precise merge key wants deciding (all of `meta`, or a declared subset) and is
the main open design question below.

### Change 4 — scheduling: sweep or per-tenant?

`scan_jobs/1` already emits one entry per cell with a `scan` (`source.ex:528`),
so three instances are three units of work automatically. `crontab/2` is the
decision point: it defaults to **one entry per distinct cadence** — a sweep that
runs every source sequentially inside one job, followed by one drain
(`source.ex:631-636`) — and takes `per_cell: true` for one entry per cell.

Under the default sweep, three municipalities crawl **sequentially in one job**
and then drain once. That is the opposite of the requirement: the whole point is
that Tivoli's slow crawl should not hold up Red Hook. So per-tenant scheduling
means either `per_cell: true`, or a new per-tenant grouping — one sweep per
municipality rather than one per cadence.

One sweep per tenant looks right: it keeps the "several sources then one drain"
shape that makes a sweep worth having, while making the unit of serialization the
tenant. That is a genuine addition to `crontab/2`, not a flag it already has.

## Dashboard: the tenant switch

Cascade has **33 cells** today (measured). Three municipalities is 99 cells with
dotted ids in one flat list — the funnel's cell picker (`rdd-starts`) would be
unusable, and `Tree.starting_points/2` would offer three copies of every source.

The plan already arrives as an MFA — `plan_mfa: {MuniWatch.RedHook.Nodes, :plan, []}`
in cascade's config, applied on every `load/1` (`dag_live.ex:47,478`). So a
tenant switch is a **different argument to the same MFA**, not new plumbing:

- `plan/1` takes a municipality and returns that municipality's graph — one
  tenant's instances only, so the dashboard's cell count stays ~33.
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
and `%Drain.Report{}`. A slow Tivoli crawl does not block Red Hook. Cell ids
carry the tenant, so `meeting_id` collisions are structurally impossible. The
dashboard shows one tenant at a time at its current scale.

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
- Every cascade resource that should be per-municipality needs `for_each` — a
  broad, mechanical change across ~33 cells, and one that is easy to apply
  incompletely. A cell that *should* be per-tenant and isn't becomes a shared
  cell three tenants write to, which is the collision this design exists to
  prevent. Worth a check that enumerates cells lacking `for_each`.

**Out of scope.** Whether cascade's row keys also become composite. Instance ids
isolate the *cells*; whether `meeting_docs.tivoli`'s rows live in one table with
a `municipality_id` column or three tables is a cascade schema decision with its
own measurement.

## Open questions

1. **How does the member stamp reach `poll/1`?** All of `meta`, or a declared
   subset (`for_each :municipalities, poll_args: [:municipality]`)? Passing all
   of `meta` is convenient and leaks internal stamps into a third-party API call.
2. **Per-member cadence?** `every:` is declared once on the template. If all
   three sites can be crawled on one schedule, nothing is needed.
3. **Does `slice` still apply to municipality?** Under this design, no — you pick
   a municipality by choosing which graph to look at, so only `fiscal_year`
   remains a slice. The dependent-slice work from the earlier draft is therefore
   **not needed for this**, and `Fiscal.meeting_years/0` becomes per-tenant by
   virtue of reading a tenant's own instance rather than by taking an argument.
4. **Who lists the tenants for the dashboard switch?** Library (it knows member
   ids) or host (it knows names)?
5. **Is instance-locality declared or inferred?** (Change 1.) Inferring it from
   "the target is a generator over the same population" needs no new syntax and
   silently does the wrong thing in the ambiguous case; declaring it
   (`depends_on [{:docs, per: :municipalities}]`) is more typing on ~33 cells.
   A wrong answer here means one tenant reading another's data, which nothing
   downstream detects — so this is the decision most worth getting right.
6. **How does a resource say it is NOT per-tenant?** With no cross-municipality
   cells in scope, the answer today is "every resource declares `for_each`". That
   is ~33 mechanical edits and easy to apply incompletely, and a cell that should
   be per-tenant and is not becomes a shared cell three tenants write to. Worth a
   check that enumerates non-generator cells in a tenant plan rather than trusting
   the sweep.

## Rejected alternatives

- **Scope-per-key in one shared graph** (this ADR's first draft). A `scope`
  column on the dirty table, scoped `claim/1`, scoped retirement. Rejected: it
  does not match "the entire graph is per municipality", and it is strictly more
  work than instances. Two failure modes it would have had are worth recording,
  because both are absent under instances: a scoped whole-cell pass reconciling
  against `Rows.all/1` — an unscoped `Ash.read!()` — would have **retired every
  other tenant's rows** (the partial-read-vs-total-baseline shape of the two-node
  join bug in #183, except destroying rows rather than nil-ing columns); and a
  nullable `scope` would have broken `mark_dirty`'s `ON CONFLICT` coalescing,
  since Postgres treats nulls as distinct in a unique index.
- **A dirty table per municipality.** `dirty_table` is already runtime-configurable
  (`frontier.ex:268`), so this works today with no library change. Rejected: the
  table name is global application config, so it cannot vary per concurrent
  drain — two tenants draining at once would need two application environments.
- **Ash `context` multitenancy (schema per tenant).** Rejected: it makes every
  cross-tenant question N queries with no joins. With no cross-municipality cells
  in scope this costs little *today*, which is exactly why it is a trap — the
  first comparison feature would require unwinding it.
- **Ash `attribute` multitenancy.** Not rejected, but not load-bearing: it is a
  convenience on the read path. Ash tenancy lives in query context and the
  frontier is raw SQL through `query!/2` that never sees it, so it cannot provide
  claim isolation. Instance ids already do.

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

## What the library already provides

`for_each` is already graph-per-member (`node.ex:1729-1737`):

- A node declaring `for_each :municipalities` builds **no template cell**.
  Instead one instance per member, with ids `"<base>.<member.id>"` —
  `meeting_docs.tivoli`, `meeting_docs.red-hook-village`.
- Each member's `meta` map is **stamped onto every cell** in that instance's
  sub-tree, so an instance can carry its own municipality id without any
  per-cell declaration.
- `graph/2` takes the member-fetcher: `graph(resources, for_each: &fetch/1)`.
  Proven by `test/node_test.exs:326`.

This matters more than it first appears, because **the dirty table is keyed by
`cell_id`** (`migration.ex:60`, unique index `(cell_id, key)`). Distinct instance
ids therefore give, for free, everything the superseded design was going to
build:

| concern | with instances |
|---|---|
| claim isolation | `claim/1`'s cell-wide `DELETE` is now *correct* — claiming all of `docs.tivoli`'s keys is exactly right |
| retirement | already per-cell, so no cross-tenant `retire_vanished` risk exists |
| locking | `with_lock(fun, scope: …)` already exists (`frontier.ex:155`) and takes any term |
| `next_cell` | already per-cell; a tenant's drain walks its own instances |

No `scope` column. No migration. No change to claim or retirement.

## The one thing that blocks it

**The library actively refuses this shape today.** Verified by probe — a
generator leaf declaring a scanner, assembled through `graph/2`:

```
reactive_dag: Crawler is declared by 2 nodes (docs.red-hook-village, docs.tivoli).
A source is a node, so one scanner feeds one cell — and each of these gets its
own crontab entry, so the upstream is polled 2 times.
```

`verify_one_node_per_source!` (`node.ex:1481`) raises.

The check is right about the case it was written for — one crawl producing rows
for several leaves, where N cells means N redundant polls of one upstream — and
**inverted for this one**. Three municipalities have three AgendaCenter sites;
polling three times is the requirement, not the bug. The check cannot currently
distinguish "N cells, one upstream" from "N cells, N upstreams".

Generator instances are distinguishable by construction: they are the *same
declaration* expanded per member, so a scanner on a generator node is one
scanner per member, each with its own upstream.

### Change 1 — a scanner on a generator node is per-instance

`verify_one_node_per_source!` groups cells by scan module and raises on any group
larger than one. It should instead group by `(module, instance-of)` — cells that
differ only by generator member are not a violation. Cells sharing a module
*without* being instances of one generator still raise, with the existing
message.

### Change 2 — the member stamp reaches `poll/1`

An instance knows which municipality it is (its `meta` stamp), and the crawler
needs that to crawl the right site. `scan_args` is currently declared statically
in the DSL (`args: [recent: true, year: &…/0]`).

An instance's poll options should be its declared `args` merged with its member
stamp — so `Source.refresh(plan, "meeting_docs.tivoli")` reaches
`poll/1` with `municipality: "tivoli"` without the host wiring it per instance.
The precise merge key wants deciding (all of `meta`, or a declared subset) and is
the main open design question below.

### Change 3 — scheduling: sweep or per-tenant?

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
  Change 3 rather than falling out for free.
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

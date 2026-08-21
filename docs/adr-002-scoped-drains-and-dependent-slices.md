# ADR-002 — Scoped drains and dependent slices

**Status:** proposed — no code written. This document exists to be argued with
before the frontier is touched.
**Date:** 2026-08-21

## Context

Cascade covers one municipality. It is about to cover three — Red Hook Village,
Town of Red Hook, Village of Tivoli — and the ask is twofold:

1. A human should be able to select **municipality** the way they already select
   fiscal year, and the fiscal-year options should depend on the municipality
   chosen.
2. Scans and the cascade downstream of them should be **separate per
   municipality**, not one global sweep that happens to touch three sites.

That second half is the load-bearing one. It is not a UI request: it says two
municipalities' data should be able to move through the graph at the same time,
without one's drain claiming the other's work.

### The fact that forces the design

`meeting_id` looks like `_07252024-459` and is assigned by **AgendaCenter's own
routing** (`sources/agenda_center_crawler.ex:422` builds it as `"_" <> id` from
the listing page). It is unique within one municipality's site and says nothing
across sites. Two municipalities will collide.

So municipality cannot be only a filter column on a row. It has to be part of
what identifies a unit of work — which means it has to reach the **frontier**,
not just the reads.

### What already exists (verified, not assumed)

- `Frontier.with_lock/2` **already takes a `:scope` option**:
  `key = :erlang.phash2(Keyword.get(opts, :scope, dirty()))`
  (`frontier.ex:155`). Per-scope advisory locking is already the primitive.
- **Nothing passes it.** `:scope` has zero callers in the library. The two real
  callers — `DrainWorker` (`drain_worker.ex:137`) and `ScanWorker`
  (`scan_worker.ex:178`) — both call `with_lock/2` with no options, so every
  drain in a cluster contends for one global lock.
- Slices are **already a list per node** (`node.ex:2090` `slices/1`), and
  `Rows.keys_where/2` already takes an arbitrary filter — so multi-dimension
  *filtering* needs no change.
- `Rows.poll_opts/2` already translates a multi-key selection from column names
  to `poll_as` names. A two-dimension scan request needs no library change.

### What blocks it (verified)

**The claim is cell-wide.** `Frontier.claim_with_priors/1`:

```sql
DELETE FROM dirty WHERE cell_id = $1 RETURNING key, prior
```

Every key for the cell, regardless of which municipality it belongs to. Two
per-municipality drains reaching the same cell steal each other's keys: A's drain
deletes B's dirty keys, recomputes them under A's scope, and B's drain finds
nothing to do. Silent, and wrong in the direction that reads as "nothing
changed".

Three consequences follow from that one line:

- `next_cell/2` asks `SELECT DISTINCT cell_id FROM dirty` (`frontier.ex:82`), so
  a scoped drain cannot ask *which cells are dirty for me*.
- The dirty table's unique index is `(cell_id, key)` (`migration.ex:60`).
- `Rows.all/1` is a bare `Ash.read!()` (`rows.ex:74`), so `retire_vanished`
  reconciles a scoped pass against **every** tenant's rows.

That last one is the dangerous one and is treated as a first-class risk below.

## Decision

Three changes, independently shippable, in this order. Each is useful alone.

### Part 1 — dependent slices (no frontier changes)

`values:` resolution today is `resolve_values({m, f, a}) -> apply(m, f, a)`
(`rows.ex:248`) — a fixed MFA, so a child dimension's options cannot narrow to
the parent's selection.

Add a declared dependency:

```elixir
slice :municipality_id, values: {Municipalities, :all, []}

slice :fiscal_year,
      values: {Fiscal, :meeting_years, []},
      depends_on: :municipality_id
```

- `depends_on:` names another slice **on the same node**. Checked at assembly:
  the named slice must exist, and the dependency graph must be acyclic. A typo is
  a compile error, not a control that silently offers everything.
- When a slice declares `depends_on:`, its `values:` MFA is applied with the
  current selection appended — `Fiscal.meeting_years(%{municipality_id: "…"})`.
  Declaring the dependency is what makes the arity change safe to expect.
- `Rows.slices/1` returns slices in **dependency order**, so a UI renders parents
  before children.
- A child whose parent has no selection resolves to `values: nil` — the existing
  "node named no options" case, which the dashboard already renders as free text
  or nothing. No new UI state.

Why declared rather than inferred from arity: an inferred version fails only at
runtime, in the UI, as an empty control. This library's own history says a
silently-inert declaration is worse than no declaration (`verify_reactive.ex`
carries two checks that exist for exactly that reason).

### Part 2 — a `scope` column on the frontier

Municipality becomes a **first-class scope on the dirty row**, not a substring of
the key.

```
dirty(cell_id, scope, key, reason, enqueued_at, prior)
unique_index(cell_id, scope, key)
```

A cross-municipality roll-up (compare Tivoli to Red Hook) is one cell whose work
is global and must not be forced to invent a tenant. The obvious spelling for
that is a nullable `scope`, and **it is a trap**:

`mark_dirty` coalesces via `ON CONFLICT (cell_id, key) DO NOTHING`
(`frontier.ex:63`), backed by that unique index. In Postgres **nulls are distinct
in a unique index**, so with a nullable `scope` two marks of the same global key
would both insert instead of coalescing — the queue grows a row per mark, and the
one property the index exists to provide is silently gone for exactly the cells
that carry no tenant.

So `scope` is `null: false` with a **sentinel default** for "global":

```
add(:scope, :text, null: false, default: "*")
unique_index(cell_id, scope, key)
```

`"*"` because the library already uses it for "the whole cell" in claim sets
(`scope(["*"]) -> nil` in `recompute.ex`), so the vocabulary is not new. The
public API still takes `scope: nil` and normalises to `"*"` at the boundary —
callers never write the sentinel, and `IS NOT DISTINCT FROM` is then unnecessary:
plain `=` works because there are no nulls.

API changes, all backward compatible by defaulting scope to `nil`:

| today | scoped |
|---|---|
| `mark_dirty(cell, keys, reason)` | `mark_dirty(cell, keys, reason, scope: id)` |
| `claim(cell)` | `claim(cell, scope: id)` → `WHERE cell_id = $1 AND scope = $2` |
| `next_cell(depths, except)` | `next_cell(depths, except, scope: id)` |
| `with_lock(fun)` | `with_lock(fun, scope: id)` — **already supported** |

Omitting `scope:` means `"*"`, so every existing call site keeps working and
keeps meaning what it means today.

**Why a column and not a key prefix.** Prefix-matching (`key LIKE 'tivoli|%'`)
needs no migration and is wrong twice: it makes the scope unreadable to
`SELECT DISTINCT`, and it collides with the `"|"`-joined composite keys the
library already produces (`recompute_by [a: :x, b: :y]`). A key that already
contains `"|"` cannot be prefix-parsed unambiguously.

### Part 3 — scoped retirement (the risk that must be closed first)

`retire_vanished/3` subtracts what a pass produced from a baseline, and destroys
the difference. For a whole-cell pass the baseline is `current_keys(cell)` —
`Rows.all/1`, an unscoped `Ash.read!()`.

**A scoped whole-cell pass would therefore destroy every other municipality's
rows in that cell.** This is the same shape as the two-node join bug fixed in
#183: a partial read reconciled against a total baseline. There it wrote nils;
here it issues `Payload.retire/5`.

So Part 2 must not ship without: `Rows.all/1` gaining a scope filter, and
`vanished_baseline/2` passing the drain's scope into it. A cell with no scope
column on its resource, drained under a scope, must **refuse to reconcile**
(return `nil`, the existing "cannot enumerate — do not reconcile" path at
`recompute.ex:452`) rather than reconcile wide.

The test written first, before any of this: seed two municipalities into one
cell, drain one under its scope, assert the other's rows still exist.

## Ash native multitenancy

Worth a straight answer, since the option was raised.

**Use `attribute` multitenancy for reads; do not rely on it for the frontier.**

- The `attribute` strategy is exactly a `municipality_id` filter Ash applies for
  you, and it composes with everything above: if the tenant attribute and the
  frontier `scope` carry the same value, the read side and the claim side agree
  by construction. That removes the asymmetry that makes scoped drains dangerous.
- The `context` strategy (schema per tenant) is the wrong fit: the interesting
  questions here are cross-municipality comparisons, which under
  schema-per-tenant become N queries with no joins.
- Tenancy cannot *replace* Part 2. Ash tenancy lives in query context; the
  frontier is raw SQL through `query!/2` and never sees it. Tenant-scoped reads
  with tenant-blind claims is precisely the failure mode above.

So: Ash tenancy is a convenience on the read path, adopted after Part 2, not a
substitute for it.

## Consequences

**Wanted.** Two municipalities drain concurrently, each with its own lock, claim
set and `%Drain.Report{}`. A slow Tivoli crawl stops blocking Red Hook. The
dashboard gains a per-scope view for free — reports are already per-run.

**Costs, stated plainly.**

- `Drain.transaction/1` holds one connection at `timeout: :infinity`, justified
  in its own docs by "drains are SERIALIZED… one connection, not one per
  concurrent drain" (`frontier.ex:183`). N concurrent scopes means N long-held
  connections. The pool must be sized deliberately, and there should be a cap on
  concurrent scoped drains.
- A migration on a live frontier table. It is additive (nullable column, new
  index) and the old index must be dropped only after the new one exists.
- `DrainWorker`'s Oban uniqueness must become per-scope, or the queue will
  collapse three municipalities' jobs into one.
- Every `mark_dirty` call site in every host must decide its scope. Defaulting to
  `nil` keeps them compiling, which is also the risk: a forgotten scope silently
  marks global work. Worth a host-side audit, not a library check — the library
  cannot know which cells are per-municipality.

**Explicitly out of scope here.** Whether cascade's cell keys become composite
`(municipality_id, meeting_id)`. Part 2 makes the *frontier* tenant-aware; the
AgendaCenter id collision is a cascade schema question, and the library already
has an identity-keyed path (`Payload.upsert_identity/5`) for it. Separate
decision, separate measurement.

## Open questions

These change the design and are worth answering before Part 2:

1. **One AgendaCenter site or three?** If three, each municipality needs its own
   base URL and the crawler takes a municipality argument — scans are genuinely
   separate units of work, and `for_each` (which already exists) may generate the
   per-municipality cells. If one site serves all three, municipality may be
   derivable from the listing page and the scan stays single.
2. **Is `fiscal_year` genuinely per-municipality?** Do the three have different
   fiscal calendars, or the same years with different data? If the same, the
   dependency is presentational (which years have *this* municipality's meetings)
   rather than structural — Part 1 still applies, but `Fiscal.meeting_years/1`
   is a filter, not a different calendar.
3. **Which cells are cross-municipality?** Every such cell is a null-scope cell
   and drains under the global lock, so a graph where most cells are global gets
   little parallelism. Worth counting before building for it.

## Rejected alternatives

- **Scope as a key prefix.** No migration, but unreadable to `SELECT DISTINCT`
  and ambiguous against existing `"|"`-joined composite keys.
- **A dirty table per municipality.** `dirty_table` is already configurable, so
  this works today with zero library change — and fails on the cross-tenant
  cells, which would need a fourth table and a rule for which to read.
- **Schema-per-tenant (Ash `context`).** Kills cross-municipality queries, which
  are much of the point of covering three municipalities.
- **Inferring slice dependency from MFA arity.** Fails at runtime as an empty
  control rather than at compile time.

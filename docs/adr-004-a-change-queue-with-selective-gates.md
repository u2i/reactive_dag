# ADR-004 — A change queue, with selective gates

**Status:** proposed. Nothing implemented. Reasoned from first principles in
conversation and priced against a real host (cascade, 33 cells, 39 edges) at
every step; the measurements below are from that host's dev data, not estimates.
**Date:** 2026-08-23

> This ADR proposes replacing the dirty queue's *contents*, not the loop that
> drains it. Depth ordering, per-cell savepoints, claim-as-delete and the tenant
> column all stay exactly as they are. What changes is what a queue row says.

## The question that started it

> Is dirtiness tracking updatedness?

No — and the answer exposed that the queue stores **what needs to change**, not
what changed. A changed `fiscal_line_rows` row produces three queue entries,
none of which names that row:

```
I CHANGED:   fiscal_line_rows / dc_budget_fy23_24_combined|84
QUEUE HOLDS: account_totals   / budget|A141046|FY23/24
QUEUE HOLDS: budget_rollups   / combined|FY23/24|appropriation|General Government Support
QUEUE HOLDS: cost_allocation  / FY23/24
```

The mapping from change to affected units happens at **mark time**, in the
producing job, via each consumer's declared key rule. The queue receives the
result.

That is a real design with real benefits (below). It also has one structural
consequence: the queue row has lost the row that caused it. Anything the mapping
needs from that row must be captured at mark time or it is gone — which is what
the existing `prior` column is for, and why the deleted-row and moved-row cases
exist as special cases at all.

## What is proposed

One table, appended to inside the writer's transaction:

```
change_queue
  tenant     text       -- which graph
  cell_id    text       -- which node's rows changed
  key        text       -- which row (its cell key)
  before     jsonb      -- the row as it was; NULL for a creation
  after      jsonb      -- the row as it is;  NULL for a deletion
  state      text       -- pending | approved | rejected  (gated cells only)
  at         timestamptz
```

The write path knows nothing about consumers. Each consumer derives its own
affected units at claim time by applying its declared grain to **both** sides:

```
units(entry) = grain(before) ∪ grain(after)
```

| entry shape | means | units claimed |
|---|---|---|
| `before` nil | creation | `grain(after)` |
| `after` nil | deletion | `grain(before)` |
| both, same grain | update in place | one |
| both, different grain | the row **moved** | **two** — both repriced |

## What this fixes

**The `"*"` degradations disappear.** Today a `:group` claim resolves by reading
the changed row, so:

* a **deleted** row cannot be read → degrade to `"*"`, reprice the whole cell;
* a **moved** row reads as where it landed, never where it came from → the group
  it left is stranded, so the safe answer is again `"*"`.

Both are the biggest current source of over-claiming, and both become ordinary
shapes above. No live read, so nothing to fail.

**`"*"` stops being a stored sentinel.** It is a claim about one consumer's
grain, and with before/after each consumer can decide for itself — a set too
large to be worth enumerating, or an entry it cannot grain. Completeness moves to
where the judgement belongs.

**Retirement becomes explicit.** `"*"` currently doubles as "this pass saw the
whole set, so retire anything absent". `after = nil` says "this key is gone"
directly.

**Marking gets cheap and consumer-blind.** One append, no derivation, no query of
the target cell. That matters for the transactional-subgraph idea below: a writer
should not need to know the graph.

## What it costs

Measured on 300 real changed lines sharing one fiscal year — the actual fan-in in
`derived_fiscal_line`, not a constructed case:

| | queue rows | queries | mapping time |
|---|---|---|---|
| **today** (resolved units) | **322** | 1 at mark | 28ms |
| **proposed** (changes) | **900** | 1 per consumer at claim | 11ms |

2.8× the rows, because entries are per change × per consumer edge rather than per
resolved unit. The collapse that produces 322 is:

| consumer | 300 lines → units | collapse |
|---|---|---|
| `cost_allocation` | 1 | 300× |
| `budget_rollups` | 21 | 14× |
| `account_totals` | 300 | **1×** |

So the eager mapping's whole benefit is the first two rows of that table. Where
a consumer's grain is near-unique per input row (`account_totals` groups by
`(side, normalized_code, fiscal_year)`), it collapses nothing and the mapping is
pure overhead — which suggests that node's grain is worth revisiting
independently of this ADR.

Dedup also moves from a unique index to per-consumer in-memory work. Same final
recompute count; different place.

## Selective gates

A change may need **approval before it propagates**. Not universally — declared
per node:

```
reactive do
  id :agenda_items
  gated true          # entries stay `pending` until approved
end
```

The gate holds **propagation, not the write**. The row is written as now, so the
derived tables stay coherent and readable — which matters because they are the
served product: `MuniWatchWeb.Public.Data` reads them directly at ~19 sites, and
a meeting page renders from `MeetingEvents` and `AgendaItems` live. Deferring the
write (a command pattern) would put a serialised job queue between a user's write
and the page that displays it, and would invert the crash guarantee: today a
committed change always leaves a mark, whereas a committed *command* leaves the
row correct-but-old.

`before`/`after` are already exactly the diff a reviewer needs, which is why the
two ideas fit together rather than merely coexisting.

**Where a gate belongs.** On the extraction boundary — nodes making claims about
what a public body did, from a machine transcript. Not on arithmetic over
already-approved inputs; gating a sum adds a human step to addition.

**What each gate would hold back**, measured:

| gated node | downstream cells held |
|---|---|
| `meeting_events` | **11** |
| `agenda_items` | 4 |
| `transcript_record` | 4 |
| `meeting_summaries` | 0 |

That is the honest cost: gating `meeting_events` stalls eleven cells until a
human acts. It is also the point — those eleven are all downstream of a claim
about a meeting.

## Open questions

**One entry per change, or coalesced per row?** Five writes to a row before a
drain gives five entries (a true log; the first entry's `before` is the real old
value) or one (`before` of the first, `after` of the last). The log is honest;
the coalesced form is what a consumer needs and keeps the queue smaller. Not
resolved.

**Does the change queue replace the dirty queue or sit beside it?** A change log
is useful in its own right — provenance, audit, "what happened to this row". But
some claims are not row changes: a reprocess button, a source re-scanning
everything, a code change invalidating a whole cell. Those need a home, and
"whole cell" is not expressible as a before/after pair.

**Unbounded pending state.** A change never reviewed stays forever, and a
*second* change to the same row arrives with a `before` reflecting the
unapproved first. Either coalesce (approve the net effect) or block further
writes to a row with a pending change. Neither is obviously right.

**Cascade depth of approval.** If an `agenda_items` change is approved, do the
four cells downstream need their own approval? If yes, one document revision
needs many approvals. If no, approving at the boundary implicitly approves
everything derived from it — defensible, and it means gates are only useful near
leaves.

## Alternatives considered and rejected

**A `dirty` flag, or the changed attributes, on the row itself.** Fails on
measurement: `cost_allocation` claims units like `"FY23/24"` while holding 27
rows keyed by `alloc_key` — the unit is not a row of its own table. A row that
was **deleted** also cannot carry a flag saying it needs retiring, and retirement
is a first-class outcome here.

**Queue the changed row's UUID and let consumers map it.** Coherent, needs no
lookup table (each consumer reads the row and applies its own grain — verified
working). Rejected because it is the proposal above minus `before`/`after`: every
consumer then hits the deleted/moved problem and degrades to `"*"`, losing the
precision that is this ADR's main benefit. If you are storing a snapshot anyway,
store it once per change rather than once per consumer edge.

**A surrogate UUID per unit of work.** Requires the same unit to get the same
UUID on every claim, so either a lookup table keyed by the grain tuple (which is
the string key, one indirection deeper) or a deterministic hash of that tuple
(which is the string key, unreadable). Only viable if groupings are reified as
entities — a `fiscal_years` table, a `funds` table — which is a different and
larger change.

**Doing nothing.** Still the honest default. Note that the existing `prior`
mechanism already implements the "where it was" half of this design, including
the union with the live read — and cascade populates it on **zero** nodes, so the
snapshot path never runs. The trigger for wanting any of this is a writer that
moves a row between groups from outside the graph. Cascade has none today; its
one candidate (`Correction`) deliberately writes *through* a derived row instead.

## Related

* A separate observation from the same conversation, not part of this ADR: the
  transactional subgraph. Cascade's financial/WWTP cells (`fiscal_docs` reaches
  6) are pure arithmetic over rows in one database — milliseconds, no external
  calls. That subgraph could recompute synchronously in the writer's transaction
  as a depth-ordered fixpoint over an in-memory change set, needing no queue at
  all. The meeting chain cannot: `meeting_docs` reaches 14 cells and most are LLM
  calls, which no transaction survives. The dividing line is whether a cell's
  recompute can complete inside the writer's transaction — and it is a better
  split than "which mechanism should everything use".
* The drain's `drain: 1` Oban concurrency plus a graph-wide advisory lock
  predates the tenant column. Two municipalities' drains share no state, so a
  per-tenant lock is now correct and needs no design change.

# ADR-004 — A change queue, with selective gates

**Status:** proposed. Nothing implemented. Reasoned from first principles in
conversation and priced against a real host (cascade, 33 cells, 39 edges) at
every step; the measurements below are from that host's dev data, not estimates.
**Date:** 2026-08-23

> This ADR replaces the dirty queue with ONE table that says what CHANGED rather
> than what to do about it. The drain loop is untouched: depth ordering, per-cell
> savepoints and the tenant column all stay exactly as they are. Whether an entry
> is claimed-and-deleted or kept behind a watermark is the one open question that
> changes the shape — see **Lifecycle** below.

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

ONE table, appended to inside the writer's transaction:

```
change_queue
  tenant     text       -- which graph
  cell_id    text       -- which node's rows changed
  key        text       -- which row (its cell key), or "*"
  kind       text       -- changed | recalculate
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

| kind | before | after | means | units claimed |
|---|---|---|---|---|
| `changed` | ✓ | ✓ same grain | update in place | one |
| `changed` | ✓ | ✓ different grain | the row **moved** | **two** — both repriced |
| `changed` | ✗ | ✓ | creation | `grain(after)` |
| `changed` | ✓ | ✗ | deletion | `grain(before)` |
| `recalculate` | ✗ | ✗ | redo this key | the key as given |

### Why `recalculate` is the same table

An earlier draft of this ADR split these — a change log beside a work queue —
on the grounds that a table of diffs should not carry rows with no diff. That was
wrong on two counts.

**The derivation needs no branch on `kind`.** It already handles a nil on either
side, so "neither side" falls out of the same rule: *derive from whichever sides
you have; with neither, take the key at face value.* `kind` is documentation of
WHY, not a switch.

**And `recalculate` is not the rare case.** "Code changed, not data" is what a
reprocess is — a new extraction prompt, a fixed fold, a backfill for a capability
that did not exist. Measured on cascade's drain log, 7 of 16 recorded steps were
externally triggered rather than propagated. That sample is small and
dev-shaped — which is the point: it is the shape of the loop a developer spends
their time in. Splitting it out would put the most-used origin in the
less-developed mechanism, with its own view, its own tooling and its own
reasoning.

So: one table, four shapes, one rule.

The claims that have no `before`/`after` are exactly the ones with nothing to say
about data:

| origin | kind |
|---|---|
| a derived row was written | `changed` |
| a reprocess button | `recalculate` |
| a prompt or model change | `recalculate` |
| a backfill for a new capability | `recalculate` |
| a consumer that cannot localise (`:all`) | `recalculate`, key `"*"` |

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

**Lifecycle — the one question that changes the shape.** Two coherent answers:

* **claim-and-delete**, as today. Simple, and an audit trail already exists in
  `derived_drain_step`. But then this table is a queue, not a log: processing
  destroys it, and the `state` column governs rows that are about to disappear.
* **keep, with a watermark.** The table becomes the log too, and a
  `kind: recalculate` row records "I reprocessed `agenda_items` at 14:32". Given
  how much of the working loop is reprocessing, that is plausibly the more useful
  artefact — and it gives the gate somewhere durable to live rather than a
  `state` on rows being deleted.

The second is more appealing than it first looks, precisely because
`recalculate` is common. It also costs a retention policy, which the first does
not.

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

**Two tables — a change log beside a work queue.** Argued for at length in the
conversation that produced this ADR, on the grounds that the two have different
lifecycles: a change is immutable and worth keeping, a work item is transient and
claimed-and-deleted. Rejected because the split puts `recalculate` — the origin a
developer hits most, every prompt change and every backfill — in a second
mechanism with its own view and its own tooling. The lifecycle difference is real
but it is a question about retention, not about how many tables there are; see
**Lifecycle** above.

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

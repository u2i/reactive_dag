# ADR-004 — Changes as the propagation source

**Status:** proposed. Nothing implemented. Reasoned from first principles in
conversation and priced against a real host (cascade, 33 cells, 39 edges) at
every step; the measurements below are from that host's dev data, not estimates.
**Date:** 2026-08-23

> **Scope, third revision.** This ADR started out proposing a new `change_queue`
> table. It does not any more. Both halves already exist:
>
> * **changes** — `ash_paper_trail`, in `:full_diff` mode, with
>   `primary_key_type :uuid_v7`
> * **recalculations** — `ReactiveDag.ReprocessWorker`, an Oban job, already
>   built
>
> What is left to build is a *reader*: derive affected units from a version's
> diff, and advance a watermark. The revisions are kept visible below rather than
> smoothed over, because two of the discarded designs were mine and the reasons
> they failed are the useful part.

## The question that started it

> Is dirtiness tracking updatedness?

No — and the answer exposed that the dirty queue stores **what needs to change**,
not what changed. A changed `fiscal_line_rows` row produces three queue entries,
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

That is a real design with real benefits. It has one structural consequence: the
queue row has lost the row that caused it. Anything the mapping needs from that
row must be captured at mark time or it is gone — which is what the existing
`prior` column is for, and why the deleted-row and moved-row cases exist as
special cases at all.

## What is proposed

**Two sources, each in the mechanism that suits it.** Neither is new.

```
ash_paper_trail versions ──►  changes: before/after, in-transaction, v7-ordered
ReprocessWorker (Oban)   ──►  recalculations: cell + optional where/keys
                                       │
                                       ▼
                                dirty queue ──► drain
```

The drain keeps one input. The dirty queue keeps depth ordering, per-cell
savepoints, claim-as-delete and the tenant column, exactly as they are. What goes
away is the **eager mapping at mark time** and the **`prior` column**.

### Changes: `ash_paper_trail`

`change_tracking_mode :full_diff` records precisely the shape this design needs:

```elixir
%{subject: %{from: "subject", to: "new subject"}, body: %{unchanged: "body"}}
```

Each consumer derives its own affected units from a version by applying its
declared grain to **both** sides:

```
units(version) = grain(from) ∪ grain(to)
```

| `version_action_type` | means | units claimed |
|---|---|---|
| `:create` | creation | `grain(to)` |
| `:destroy` | deletion | `grain(from)` |
| `:update`, same grain | update in place | one |
| `:update`, different grain | the row **moved** | **two** — both repriced |

Three properties make it the right dependency rather than a convenient one:

**It writes inside the transaction.** `Ash.Changeset.after_action`, verified in
`lib/resource/changes/create_new_version.ex`. That is the same choice
`ReactiveDag.Node.Changes.MarkDirty` made, for the reason its own moduledoc
gives: a notifier fires after commit, so a crash between commit and dispatch
loses the record, and a lost mark is silent staleness. `ash_paper_trail` reached
the same conclusion independently.

**`primary_key_type :uuid_v7` is the watermark ordinate.** Ash's `UUIDv7`
delegates to Ecto's monotonic generator, which "guarantees strictly increasing
values per node — even for UUIDs generated within the same millisecond". A
lexicographically sortable PK is all a watermark needs.

That last point is worth dwelling on, because this repo has already been bitten
by the alternative. The dashboard's live drain log needed a `seq` counter added
because millisecond `at` timestamps could not order a fast cascade — and the
first two tests of that fix passed by luck, because map iteration order happened
to agree with the right answer. A watermark on `version_inserted_at` would be the
same mistake with the same failure mode.

**The log, audit, actor and retention concerns stop being ours.** Versions carry
actor relationships already — which is *who made the change*, not who approved
it. Those are different people at different times; the approver is a field the
gate owns. See **Selective gates** below, where the distinction turns out to
matter more than it first appears.

### Recalculations: the Oban job that exists

`ReactiveDag.ReprocessWorker` already takes `%{"cell" => id}` with an optional
`"where"` or `"keys"`, and is unique on args with `period: :infinity`. That is
the whole of `recalculate`:

| origin | mechanism |
|---|---|
| a derived row was written | a version |
| a reprocess button | `ReprocessWorker` |
| a prompt or model change | `ReprocessWorker` |
| a backfill for a new capability | `ReprocessWorker` |
| a consumer that cannot localise | `"*"` in the dirty queue, as now |

**This is the revision worth recording.** An earlier draft put both kinds in one
table with a `kind: changed | recalculate` column, because "code changed, not
data" is not rare — it is what a reprocess *is*, and it is the loop a developer
spends their time in (7 of 16 recorded drain steps on cascade were externally
triggered rather than propagated).

That reasoning was right and the conclusion was wrong. The two origins want
different *mechanisms*, not different columns: a change is a log entry read
forward through with a watermark; a recalculation is a job wanting retry,
uniqueness and a queue. Oban does the second properly and a table would not. So
the `kind` column disappears, along with the objection that a table of diffs
should not hold rows with no diff — the non-change case never enters the change
table at all.

## What this fixes

**The `"*"` degradations disappear.** Today a `:group` claim resolves by reading
the changed row, so:

* a **deleted** row cannot be read → degrade to `"*"`, reprice the whole cell;
* a **moved** row reads as where it landed, never where it came from → the group
  it left is stranded, so the safe answer is again `"*"`.

Both are the biggest current source of over-claiming, and both become ordinary
shapes above. No live read, so nothing to fail.

**Retirement becomes explicit.** `"*"` currently doubles as "this pass saw the
whole set, so retire anything absent". `version_action_type :destroy` says "this
key is gone" directly.

**Marking gets consumer-blind.** A writer appends a version and needs to know
nothing about the graph. That matters for the transactional-subgraph idea under
**Related**.

## What it costs

Measured on 300 real changed lines sharing one fiscal year — the actual fan-in in
`derived_fiscal_line`, not a constructed case:

| | queue rows | queries | mapping time |
|---|---|---|---|
| **today** (resolved units) | **322** | 1 at mark | 28ms |
| **proposed** (per change) | **900** | 1 per consumer at claim | 11ms |

2.8× the entries, because they are per change × per consumer edge rather than per
resolved unit. The collapse that produces 322 is:

| consumer | 300 lines → units | collapse |
|---|---|---|
| `cost_allocation` | 1 | 300× |
| `budget_rollups` | 21 | 14× |
| `account_totals` | 300 | **1×** |

So the eager mapping's whole benefit is the first two rows. Where a consumer's
grain is near-unique per input row (`account_totals` groups by
`(side, normalized_code, fiscal_year)`), it collapses nothing and the mapping is
pure overhead — which suggests that node's grain is worth revisiting
independently of this ADR.

Dedup also moves from a unique index to per-consumer in-memory work. Same final
recompute count; different place.

**And a cost this revision adds: a version row per write.** Every derived table in
cascade is written by the graph itself, so versioning all 33 doubles write volume
on tables where the graph already knows the resolved unit. The precision win only
exists where something writes rows from *outside* the graph. So versioning should
be opt-in per resource, and cascade starts with none — see **Doing nothing**.

## Selective gates

A change may need **approval before it propagates**. Not universally — declared
per node:

```elixir
reactive do
  id :agenda_items
  gated true          # versions stay unapproved until a human acts
end
```

The gate holds **propagation, not the write**. The row is written as now, so the
derived tables stay coherent and readable — which matters because they are the
served product: `MuniWatchWeb.Public.Data` reads them directly at ~19 sites, and a
meeting page renders from `MeetingEvents` and `AgendaItems` live. Deferring the
write (a command pattern) would put a serialised job queue between a user's write
and the page that displays it, and would invert the crash guarantee: today a
committed change always leaves a mark, whereas a committed *command* leaves the
row correct-but-old.

A version's diff is already exactly what a reviewer needs to see, and approval
state lives on the version — durable, rather than on a queue row about to be
deleted.

### Actor and approver are different people

`ash_paper_trail`'s actor is **who made the change**: the LLM extractor, the
crawler, a user. The **approver** is a human deciding later, possibly never. The
version gives the first for free; the second is the gate's own field.

That distinction is not bookkeeping. It decides what a gate means:

> **A gated cell's MACHINE changes wait for approval. A change a human made
> propagates immediately.**

A person editing a row should not queue for approval of their own edit. An
extractor claiming a meeting had seven motions is exactly what wants review. The
actor answers "was this a person", so the library can tell them apart without the
host declaring anything extra.

**Declared on the node, evaluated on the actor.** The node says *whether* to gate;
the actor decides whether *this* change needs it:

```elixir
reactive do
  id :agenda_items
  gated true    # machine changes here wait; a human's edit passes
end
```

Three scopes were considered:

| scope | behaviour | verdict |
|---|---|---|
| **actor only** | every machine change in the graph gates | wrong scope — `budget_rollups` sums already-approved lines, and gating a sum adds a human step to addition |
| **node only** | a declared cell gates everything through it | correct today, and simplest |
| **node + actor** | a declared cell gates only machine changes | same behaviour today, plus one property that matters later |

On cascade today, **node-only and node+actor are indistinguishable**: nothing but
the extractor writes `agenda_items` or `meeting_events` — grepped, there is no
human write path into either. The actor check never fires.

It is still the right semantics, for the day one appears. An operator correcting a
mis-extracted motion count by hand is a plausible path, and the alternative is
that the first such edit silently queues for its own author's approval — a
confusing bug to meet later rather than a rule to state now.

### The correction loop is the awkward case

A human writes a `Correction`; that triggers a re-read; the re-read produces a
**machine** change to a gated cell; so it waits for approval.

Two steps for one intent. Defensible — they said "you misheard NYCOM", and
checking whether the re-read fixed it is the point of the exercise — but worth
naming rather than discovering. If it proves annoying, the escape is for a
correction to carry its author forward as the version's approver, which makes the
re-read self-approving. That is a decision for when someone has used it, not now.

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

That is the honest cost: gating `meeting_events` stalls eleven cells until a human
acts. It is also the point — those eleven are all downstream of a claim about a
meeting.

## Open questions

**UUIDv7 is monotonic "per node".** Strictly increasing on one BEAM node. Cascade
deploys as a single machine — its own Oban config says the deploy replaces one
machine — so a watermark is safe there today. On a multi-node deploy a watermark
could skip an entry written by a node with a slightly-behind clock. This should be
a documented constraint of the reader, not something discovered later.

**One version per write, or coalesced?** Five writes to a row before a drain gives
five versions. `ash_paper_trail` will record all five (it is a paper trail); the
reader can coalesce — first `from`, last `to` — which is what a consumer needs.
Whether it should is unresolved.

**Unbounded unapproved state.** A version never reviewed stays forever, and a
*second* change to the same row arrives with a `from` reflecting the unapproved
first. Either coalesce (approve the net effect) or block further writes to a row
with an unapproved version. Neither is obviously right.

**Cascade depth of approval.** If an `agenda_items` change is approved, do the
four cells downstream need their own approval? If yes, one document revision needs
many approvals. If no, approving at the boundary implicitly approves everything
derived from it — defensible, and it means gates are only useful near leaves.

## Alternatives considered and rejected

**A `dirty` flag, or the changed attributes, on the row itself.** Fails on
measurement: `cost_allocation` claims units like `"FY23/24"` while holding 27 rows
keyed by `alloc_key` — the unit is not a row of its own table. A row that was
**deleted** also cannot carry a flag saying it needs retiring, and retirement is a
first-class outcome here.

**Queue the changed row's UUID and let consumers map it.** Coherent, needs no
lookup table (each consumer reads the row and applies its own grain — verified
working). Rejected because it is the proposal above minus the diff: every consumer
then hits the deleted/moved problem and degrades to `"*"`, losing the precision
that is this ADR's main benefit. If you are storing a snapshot anyway, store it
once per change rather than once per consumer edge.

**Two tables — a change log beside a work queue.** Argued for at length, on the
grounds that the two have different lifecycles. Rejected at the time because it
put `recalculate` in a second, less-developed mechanism. The final design *is*
two mechanisms — but the second is Oban, which a host already runs, so the
objection does not apply.

**One table with a `kind` column.** The next draft, and closer. Superseded because
the two origins want different mechanisms rather than different columns: retry,
uniqueness and queueing for one, forward-reading with a watermark for the other.

**Gating on the actor alone, with no node declaration.** Rejected: it gates the
whole graph, so `budget_rollups` would wait for a human to approve a sum over
already-approved inputs. The node declaration is what confines the gate to the
extraction boundary; the actor is what decides whether a given change there needs
review.

**A surrogate UUID per unit of work.** Requires the same unit to get the same UUID
on every claim, so either a lookup table keyed by the grain tuple (which is the
string key, one indirection deeper) or a deterministic hash of that tuple (which
is the string key, unreadable). Only viable if groupings are reified as
entities — a `fiscal_years` table, a `funds` table — which is a different and
larger change.

**Writing our own change table.** What the first two drafts of this ADR proposed.
Superseded by `ash_paper_trail`, which has the diff mode, the in-transaction
guarantee, the sortable key, the actor, and the retention story already — and is
maintained by the Ash team rather than by us.

**Doing nothing.** Still the honest default, and the recommendation until a
trigger appears. The existing `prior` mechanism already implements the "where it
was" half of this design, including the union with the live read — and cascade
populates it on **zero** nodes, so the snapshot path never runs. The trigger for
wanting any of this is a writer that moves a row between groups from *outside* the
graph. Cascade has none today; its one candidate (`Correction`) deliberately
writes *through* a derived row instead. The gate, by contrast, has independent
value and does not need this ADR.

## Related

* A separate observation from the same conversation, not part of this ADR: the
  transactional subgraph. Cascade's financial/WWTP cells (`fiscal_docs` reaches 6)
  are pure arithmetic over rows in one database — milliseconds, no external calls.
  That subgraph could recompute synchronously in the writer's transaction as a
  depth-ordered fixpoint over an in-memory change set, needing no queue at all.
  The meeting chain cannot: `meeting_docs` reaches 14 cells and most are LLM
  calls, which no transaction survives. The dividing line is whether a cell's
  recompute can complete inside the writer's transaction — a better split than
  "which mechanism should everything use".
* The drain's `drain: 1` Oban concurrency plus a graph-wide advisory lock predates
  the tenant column. Two municipalities' drains share no state, so a per-tenant
  lock is now correct and needs no design change.

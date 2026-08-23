# ADR-004 — Changes as the propagation source

**Status:** accepted & implemented. Reasoned from first principles in
conversation and priced against a real host (cascade, 33 cells, 39 edges) at
every step; the measurements below are from that host's dev data, not estimates.
**Date:** 2026-08-23 (proposed); implemented same day.

> This ADR keeps its decision-time framing. Two of its predictions were wrong and
> are annotated inline rather than rewritten — see **What shipped** below, which
> is the section to read first if you want the outcome rather than the reasoning.

## What shipped

| planned | actual |
|---|---|
| `changes_from` — read an `ash_paper_trail` version table | **no version table.** The WRITER hands its diff forward; `ash_paper_trail` never became a dependency |
| a watermark per `(tenant, cell)`, ordinate `uuid_v7` | **nothing to watermark** — no table is read forward |
| `gated` + actor-aware semantics | as designed, `Frontier.approve/reject/awaiting` |
| retire `prior` and the eager mapping | the eager mapping is gone; the `prior` COLUMN kept its name |

**The version table was the wrong shape, and the reason is worth keeping.** The
plan was to version every reactive resource and have the drain derive claims from
the versions its own write just produced. That is a read-after-write on every
drain step, to recover something the writer had in hand: `payload.ex` already
compares against the existing row to decide `:created`/`:changed`/`:unchanged`,
so the diff is that comparison KEPT rather than discarded. One
`collecting_diffs/1` around the recompute covers every rung — a declarative fold,
a `per_key` action and a `compute` op all reach the same payload write.

`ash_paper_trail` earned its keep differently. Reading its source gave the
`:full_diff` VOCABULARY — `from`/`to`/`unchanged`, and the exact four-clause
contract in `ChangeBuilders.FullDiff.Helpers` — which is now what every producer
speaks. A host already keeping a paper trail can feed `ReactiveDag.Node.Diff`
directly.

**Two producers, not one.** The design assumed changes come from one place. They
come from two, and missing that cost a debugging cycle: the payload loop (a write
inside a recompute) AND `dirties_on` (a host writing a row itself, which the graph
never sees). Both emit the same shape, and the drain merges them — a cell's own
writes win on key collision, and a claimed key it did not rewrite keeps the diff
it arrived with.

**What it cost, measured.** A row moved between funds on cascade's real data now
claims two units where it previously degraded to `"*"` and repriced all 114
rollups.

## What is still open

* **The frontier's tests are weak where it matters most.** 24 fake repos
  pattern-match SQL strings rather than executing them, so two mutations of the
  `ON CONFLICT` clause survived the suite — verified detectable against real
  Postgres, so the implementation is right and the test is not. The same class of
  gap hid a `?`-operator bug during this work (`?` is Postgres's jsonb-exists
  operator AND Postgrex's parameter marker, so two `CASE` branches were dead).
  Wants a shared fake, or a subset of these tests against a real database.
* **Four routes still read the live row**, each a real case rather than an
  unfinished edge: a `{:calc, _}` grain (an Ash `expr` the datastore evaluates), a
  `%Join{}` spec, a key with no diff (a source-fed leaf has no Ash row), and a
  diff yielding no unit. Measured: 3 of cascade's 4 `:group` cells take the diff
  path; `account_totals` needs the first.
* **The gate's open questions**, below, are unchanged by implementation:
  unbounded held state, approval depth, and the correction loop.

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

> **Annotated.** The left column below is what shipped, with one correction: the
> changes come from the WRITER, not from a version table. See **What shipped**.

**Two sources, each in the mechanism that suits it.** Neither is new.

```
the writing side ──►  changes: before/after, in-transaction
  · payload write  (a recompute's own rows)
  · dirties_on     (a host writing a row itself)
ReprocessWorker  ──►  recalculations: cell + optional where/keys
                                       │
                                       ▼
                                dirty queue ──► drain
```

The drain keeps one input. The dirty queue keeps depth ordering, per-cell
savepoints, claim-as-delete and the tenant column, exactly as they are. What goes
away is the **eager mapping at mark time** and the **`prior` column**.

### Changes: the diff, in `:full_diff` shape

> The vocabulary is `ash_paper_trail`'s; the DATA is the writer's. This section
> reads as though a version table were the source — it is not, and the reasoning
> below about why the shape is right holds either way.

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

**~~UUIDv7 is monotonic "per node"~~ — MOOT.** There is no watermark: nothing
reads a table forward, so there is no ordinate to advance and no clock to skew.

**~~One version per write, or coalesced?~~ — RESOLVED: coalesced.**
`Frontier.merge_diffs/2` merges on conflict — the earliest prior side and the
latest `to`, per attribute. A row moving meals → travel → lodging before a drain
claims `meals` and `lodging`, never `travel`: an intermediate no settled state
held. `DO NOTHING` would have stranded lodging and overwriting would have
stranded meals, so this merges rather than picking a side.

**~~Unbounded unapproved state~~ — HALF-RESOLVED.** A second change to a held key
merges into it, so a reviewer sees the net effect rather than a queue of steps,
and the held row does not multiply. What is still open is the other half: a change
nobody ever reviews stays held indefinitely, and nothing surfaces or expires it.
`Frontier.awaiting/2` makes it visible; noticing is still a host's job.

**Cascade depth of approval — ANSWERED by construction, and worth stating.** A
gate is per NODE, and a downstream cell that declares none does not gate. So
approving at the boundary implicitly approves everything derived from it: one
document revision, one approval. Gating a downstream cell too would need its own
`gated`, and the numbers above say what that costs.

That is the right default — but it means the reviewer approves a diff of the
GATED cell, not of what the cascade will produce from it. A reviewer approving an
`agenda_items` extraction is not previewing the `meeting_summary` that follows.

**The correction loop.** A human writes a correction; that triggers a re-read; the
re-read is a machine change to a gated cell, so it waits for that same human. Two
steps for one intent. Defensible — checking whether the fix worked is the point —
and the escape, if it grates, is for the correction to carry its author forward as
the change's approver. Not built.

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

**Version every reactive resource and read the versions back.** This ADR's own
plan, and wrong. The drain would read the versions its own write just produced —
a read-after-write to recover what the writer had in hand, since `payload.ex`
already compares against the existing row to decide its verdict. It also doubles
write volume on tables the graph itself writes, and makes `ash_paper_trail` a hard
dependency of every host. Superseded by the writer handing its diff forward; the
`:full_diff` vocabulary is kept, with attribution.

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

**Doing nothing.** Considered and rejected. The existing `prior` mechanism
implements the "where it was" half already, including the union with the live
read, and cascade populates it on **zero** nodes — so today's design has a
correctness story it never exercises, which is worse than not having one. The
argument for waiting was that no cascade writer moves a row between groups from
outside the graph yet. That is an argument for the *precision* win being latent,
not for the design being wrong, and it does not apply to the gate at all.

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

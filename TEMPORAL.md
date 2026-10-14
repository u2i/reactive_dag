# Temporal: replacing the version machinery

## What is being replaced

A suspension records `(resource, row_uuid, version_id)`. Resuming means reading
that version, deriving the units it touched, recomputing only those. Two
declarations make it work, on different nodes:

  * the SUSPENDING node declares `version_diff` — how to read a version back
  * each of its INPUTS declares `version_id` — how to record one when it writes

Neither is checkable by a Spark verifier (one sees a single resource and cannot
know what feeds it), so assembly raises instead. The failure it guards against
is silent and expensive: a missing version does not error, the resumption
simply recomputes the whole cell — an LLM call over every row instead of the
three that moved. "The cost shows up as a bill."

That is ~70 references across nine modules, plus a host-side version resource
and table per application.

## What replaces it

A temporal resource records each row's validity period natively:

    temporal do
      attribute :valid_at        # what was true, and when
      recorded_at :recorded_at   # when WE wrote it — wall clock
    end

A suspension then needs only `(resource, row_uuid, recorded_at)` — a timestamp
it already has. Resuming reads the row as-of that instant and as-of now, and
diffs them. The diff becomes DERIVED rather than stored, so it cannot drift
from the data, and the "unreadable version reference silently widens the claim"
failure disappears with the reference.

## The two clocks, and why both are needed

`valid_at` is a property of the data: "the minutes said X from June 2019".
`recorded_at` is a property of our knowledge: "we learned it in October 2026".

Staleness must use `recorded_at`. A backdated correction has a `valid_at` in
2019 and would never appear newer than a child derived last week — a silent
under-claim, which is the failure mode the whole key-rule ladder exists to
avoid. Ash stamps `recorded_at` from the wall clock precisely because
`now()` would resolve to the write's `as_of`.

## Staleness becomes a query

    stale(child) ⟺ ∃ parent : parent.recorded_at > child.recorded_at

Per edge, generated from `cell.inputs`. It works on one row or a million
because it is a join rather than a traversal, and nothing can be missed the way
a dropped propagation can — the next query sees it.

The key-rule ladder maps onto joins rather than disappearing:

  * `:identity`      → `ON parent.key = child.key`
  * `:all`           → `max(parent.recorded_at) > child.recorded_at`
  * `{:group, ...}`  → join on the grain columns, typed and indexable rather
                       than parsed out of a `|`-delimited key
  * diff-derived     → range overlap. A row that moved from group A to B still
                       has a period in A overlapping the window, so A is
                       correctly stale — the MOVE case, which is the one thing
                       the stored diff was uniquely for, falls out of the data
                       model instead of needing to be carried.

## What this does NOT replace

  * the walk: no-pending-ancestor ordering, diamond merging, savepoint per cell
  * suspensions: slow work still has to stop and resume
  * `moved?/3`: the only thing in the system that knows which columns constitute
    a change for a given node. Postgres compares every column; Ash's temporal
    layer keys on identity and period, never on values. A node declaring
    `compare: [:total]` means a shifted `ordinal` is not news, and only the node
    knows that.

## Precondition: writes must be gated on the verdict

Verified in the Ash Postgres source: the temporal upsert matches on identity
and period only —

    UPDATE t SET <cols> = src.<cols>, valid_at = t.valid_at * [as_of, inf)
    FROM src WHERE t.<keys> = src.<keys> AND t.valid_at @> $as_of

— with no `IS DISTINCT FROM` anywhere in the path. Writing unchanged values
splits the period anyway, producing two identical-valued periods either side of
an instant when nothing happened. History fills with noise.

`Payload.resolve_write/6` already gates its update this way. `upsert/6` and
`write/6` do not: they compute the verdict and write regardless, using it only
to decide propagation.

**But the gate is not simply "skip the write when unchanged".** Four tests pin
the opposite: a re-crawl that finds the same document must still record
`last_seen_at` and a re-issued `etag` — "the row is current, it just isn't
news". Attempted and reverted; see the leaf split below.

## The leaf split

A leaf row holds two kinds of field:

  * RESULT — `md5`, `title`, `url`, `filed_as`: what the document is
  * OBSERVATION — `last_seen_at`, `etag`, `last_modified`, `status`: what we did

Only the first should move a period. In cascade the observation columns turn
out to have no readers outside the leaf at all — `etag` and `last_modified` are
written and never used for conditional GETs — so they can go rather than being
split out.

`tombstoned_at` is the exception: it is load-bearing, and temporal replaces it
properly with a closed period.

Crawl health (`CrawlObservation`, keyed `source_id|url`) is a SEPARATE concern
and stays. It answers "is our knowledge current at all", which staleness cannot:
a crawl that 404s on every page produces no new upstream rows, so every child
looks fresh and the derivation layer is silent. `status_since` is what
distinguishes a URL that has 404'd for months (expected) from one that was 200
last week (a regression).

## MEASURED, against PostgreSQL 18.4

`spike/temporal_behaviour.exs`. Run it with a PG18 on 5545; it needs
`btree_gist` for a `text` column in a `WITHOUT OVERLAPS` primary key.

    after first write:      1 period    Budget/abc      [t0 .. inf)
    after UNCHANGED write:  2 periods   Budget/abc      [t0 .. t1)
                                        Budget/abc      [t1 .. inf)   <- noise
    after CHANGED write:    3 periods   Budget/abc      [t0 .. t1)
                                        Budget/abc      [t1 .. t2)
                                        Budget(rev)/def [t2 .. inf)

**An unchanged write splits the period.** Two adjacent periods with identical
values, divided at an instant when nothing happened. So the write gate is a
precondition, not an optimisation: without it a cascade that merely passes
through a cell fragments its history permanently.

**`recorded_at` needs a default** — `default: &DateTime.utc_now/0` plus
`update_default:`. It is not stamped merely by naming it in the `temporal`
block, which the DSL docs do not spell out. It came back NULL on the first run
of this spike.

**The earlier period keeps its original `recorded_at`.** The `__leftovers`
reinsertion copies the old row's value rather than restamping, so an earlier
period still records when WE first wrote it. That is what makes it a genuine
transaction-time axis rather than a touch counter.

## Still open

  * Can a temporal write update WITHOUT splitting, for bookkeeping fields? If
    so the leaf split is unnecessary even where observation columns are live.
  * What does `as_of` return for an instant inside a closed period — the
    staleness query needs the prior boundary, not just point-in-time.

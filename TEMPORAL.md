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

## A write CAN update in place — which changes the design

`spike/temporal_in_place.exs`. Pinning `as_of` to the current period's
`lower(valid_at)` updates that period rather than splitting it:

    bookkeeping, as_of = period's lower bound   -> 1 period  (in place)
    real change,  as_of = now()                 -> 2 periods (split)
    bookkeeping,  as_of = new period's lower    -> 2 periods (no new split)

The mechanism: the update's portion is `[lower, inf) * [lower, inf)`, the whole
existing period, so `__leftovers` has nothing to reinsert.

`recorded_at` still moves on every write, so "when did we last touch this"
survives even when `valid_at` does not change. Both clocks behave correctly and
independently.

### So the leaf split is NOT needed

Result and observation fields can share a resource. The distinction moves from
schema to write-time `as_of` selection:

    as_of = if verdict == :changed, do: DateTime.utc_now(),
                                    else: current_period_start(prior)

And that is the gate `Payload` already computes. `moved?/3` decides `:changed`
vs `:unchanged`; it now also decides whether the write splits a period or
updates one in place. The four tests that failed the naive "skip the write"
gate keep passing, because the write still happens — it just lands in the
period that is already open.

## Still open

  * What does `as_of` return for an instant inside a closed period? The
    staleness query needs the prior boundary, not just point-in-time.
  * ~~Does the in-place write need the prior row, and is that read already on
    the write path?~~ **Yes, and it is free.** `spike/temporal_prior_period.exs`:
    an ordinary `Ash.read_one!` — the exact call `Payload.existing/4` makes —
    returns the period as `%Ash.Range{lower: ~U[...], upper: nil}`. No extra
    query, no field selection, no API change. `prior.valid_at.lower` is the
    `as_of` an in-place write needs.

## The three consumers of `version_id`

Not one mechanism — three, with different needs. Mapping each before cutting.

### 1. Resumption narrowing (`resumption_worker.ex`, `cascade.ex`)

A suspension carries `version_id`; resuming reads it back through the
suspending node's `version_diff` and derives the units that moved.

`version_map/2` takes the EARLIEST version for a key: "the change that
succeeded the last settled state. A later one describes a state the graph never
saw settled."

**Replacement:** `recorded_at`. Strictly better — earliest-is-smallest is
intrinsic to a timestamp, where today it relies on `at/1` returning rows in
insertion order. Resuming reads the row `as_of` that instant and `as_of` now,
and diffs them. The diff becomes DERIVED rather than stored, so it cannot drift
from the data.

### 2. The `"*"` escalation (`resumption_worker.ex`, `suspension.ex`)

`version_id == "*"` means "this change could not be attributed", and one such
suspension escalates the whole cell. That is a sentinel, not a version.

**Replacement:** a nullable `recorded_at`. `NULL` means unattributable, which
is more honest than a magic string — and it cannot collide with a real value.

### 3. Approval (`approval.ex`)

    approved?(row) = row.<reference>.version_id == row.version_id

The argument is that an equality which stops holding cannot be forgotten,
unlike a flag that needs clearing on every write path.

**Replacement:** the same comparison on `recorded_at`. The property is
preserved exactly — content moves, `recorded_at` moves, the equality lapses.
Arguably improves it: `approval.recorded_at >= row.recorded_at` also answers
"approved at or after this state", which an opaque id cannot.

The module's stated constraint stands: the library reads two columns and
nothing else, so a host's approval scheme stays the host's business.

### What goes

  * `version_id` and `version_diff` node declarations, and the assembly check
    that they agree across an edge
  * `Versions`-style host resources and their tables (cascade's
    `derived_row_versions`)
  * `read_version/2`, `resolve_versions/3`, and the MFA-reader contract whose
    failure mode is a silent widening to whole-cell recompute

## ETS supports temporal — the test graphs need no database

`Ash.DataLayer.Ets` answers `can?(_, :temporal) -> true` and implements period
handling. I had assumed temporal was Postgres-only because `strategy :context`
"defers to native data layer support"; it is not.

So the library's own test graphs can be temporal without standing up Postgres,
and `spike/temporal_on_ets.exs` exercises the read path a resumption actually
takes:

    as_of(t0).title = "Budget"
    now.title       = "Budget (revised)"
    DIFF: %{"title" => %{"from" => "Budget", "to" => "Budget (revised)"}}

That is the `:full_diff` shape `Diff.groups/3` consumes, derived from history
with nothing stored. The path removed with `version_id` is replaced and
measured, not merely compiled.

## Staleness, asked of the data — `ReactiveDag.Stale`

    stale(child) <=> exists parent : parent.recorded_at > child.recorded_at

Edges come from the plan (`cell.inputs`), so there is no list to keep in step.
The two rules that translate cleanly are done:

  * `:identity` — a join on the key.
  * `:all` — NEWEST parent against OLDEST child. Comparing newest-to-newest
    would miss a child derived before a change a LATER child already saw.

`{:group, …}` returns `{:error, {:unsupported_rule, _}}` rather than falling
back to `:identity`, which would silently under-claim. A grain join is the
right shape and better than today's key-string parsing — typed columns,
indexable — but it needs the grain's columns on both sides and is not built.

Every answer is `{:ok, keys}` / `:all` / `{:error, _}`, never a bare list, so a
caller cannot mistake "I could not look" for "nothing is stale". `unaskable/1`
names the cells the question cannot be asked of at all.

### `recorded_at` must be in `upsert_fields`

MEASURED, `spike/temporal_upsert_recorded_at.exs`: an upsert that changes a
value leaves `recorded_at` at its CREATE value unless the attribute is named in
`upsert_fields`. `update_default` alone does not fire on the conflict path.

That silently breaks staleness — the stamps compare equal and nothing is ever
reported stale. It is a constraint on every temporal node, not a test detail.

## The grain join: what `range_overlaps` can and cannot do

MEASURED, `spike/temporal_overlap_limits.exs`.

`Ash.Query.range_overlaps/2` exists and its own docs name this use case —
"relating two temporal resources". It works. But it filters WITHIN a read, and
a read is pinned to ONE instant:

    plain read:     [{"FY25", t2}]     <- pinned to now
    as_of(create):  [{"FY24", t1}]     <- pinned to then
    overlap only:   [{"FY25", t2}]     <- still only the pinned period

`Ash.Temporal.resolve_read_as_of/1` raises `AsOfNotAnInstant` on anything that
is not a `DateTime`. This is the blog's stated limitation — "no read across a
range of times" — and it is load-bearing here.

So the shape I sketched for the MOVE case does not work:

    -- cannot express: rows EVER in this group since T
    JOIN line l ON l.fy = t.fy AND l.valid_at && tstzrange(t.recorded_at, NULL)

### What works instead

TWO pinned reads, which is what `resolve_versions/3` already does for a single
row. For a grain join:

    before = read as_of (t - 1us), grouped by the grain columns
    now    = read as_of now,       grouped by the grain columns
    stale  = units present in EITHER whose membership or values moved

That finds both sides of a move — the unit a row joined AND the one it left —
because the `before` read sees the old membership. It is two queries per edge
rather than one, which for a sweep is acceptable.

Worth stating plainly: this is the same two-reads-and-diff shape the single-row
path uses. The grain join is not a different mechanism, just the same one
applied to a projection rather than a key.

## Testing against real Postgres

`mix test.postgres` — starts a PG18 container, creates the database and the
`btree_gist` extension, runs the suite with the `:postgres` tag included.
Plain `mix test` excludes that tag and is unaffected.

### The DDL is GENERATED from the DSL

`PostgresCase.migrate!/1` hands the test's own resources to
`AshPostgres.MigrationGenerator`, runs what it generates, and drops the tables
after. Nothing about the schema is written twice.

I first wrote these tables by hand, on the assumption that the generator needs
a compiled application's resource list and so has nothing to work from for
resources declared inline in a test body. That was WRONG, and checking took
one command: `generate/2` takes DOMAINS and asks
`Ash.Domain.Info.resources/1`. Inline resources are fine. The one real
requirement is a domain that LISTS them — `allow_unregistered?` reports an
empty resource list, so the generator finds nothing to generate.

Generating matters more here than it would elsewhere. These tests exist to
catch what ETS cannot answer, and hand-written DDL that drifts from what the
generator emits hides exactly that class of bug behind a table shaped
differently from the real one. A test running against a PLAIN primary key
would pass every assertion while testing nothing about history — the table
holds one version per key, exactly as ETS does. (There is still an assertion
that the generated PK really is `WITHOUT OVERLAPS`: it proves the DSL's
`temporal` block reached the schema rather than being silently ignored.)

### Why both substrates, rather than porting the suite

The 64 resource files run on ETS and should stay there — but the reason is
NOT that porting is impractical. With `migrate!/1` it is now mechanical. The
reason is that those files test the DSL, and ETS answers DSL questions
correctly and faster. Postgres earns its place only where the substrates
disagree.

What Postgres is for is the places where ETS CANNOT give a trustworthy answer:

  * `WITHOUT OVERLAPS` primary keys are PG18 with no ETS analogue. ETS stores
    `valid_at` as a plain field and never enforces non-overlap — a write
    Postgres would REJECT succeeds there.

  * The temporal upsert is entirely different code. Postgres cannot
    `INSERT ... ON CONFLICT` against an exclusion PK, so AshPostgres emits
    data-modifying CTEs that split the period by hand
    (`valid_at * tstzrange($1, NULL)`, with
    `multirange(old) - multirange(new)` preserving the leftover). None of that
    machinery exists on ETS.

  * `Ash.Query.as_of` lowering to a SQL period predicate. The historical read
    is the whole mechanism of the grain join's move case.

MEASURED, `test/stale_grain_postgres_test.exs`: all three hold, and the move
case claims both units through real SQL history.

### Three things that cost time

`setup_all` cannot skip a module. ExUnit treats `:skip` as a TAG, and a value
returned from `setup_all` lands in the context where nothing acts on it — every
test body then ran on into a missing repo and reported as FAILING. A
compile-time `@moduletag skip:` is what actually skips. This matters because
"no database configured" must not read as "the code is broken".

DDL must bypass the sandbox. It has to outlive the per-test transaction that
gets rolled back, so `exec/2` opens its own short-lived connection while
`exec_sandboxed/2` uses the test's. Isolation was then verified directly
rather than assumed: one test writes a row, the next asserts it sees zero.

The MIGRATOR needs its own repo. It runs through whatever repo it is given and
cannot take a connection of its own, so against the sandboxed test repo it
either finds no owner (nothing holds a checkout in `setup_all`) or wraps the
DDL in a transaction the first test rolls back. `ReactiveDag.MigrationRepo` is
the same database with an ordinary pool, started on demand and stopped
straight after.

And the migration VERSION must be the file's own timestamp, not a counter. A
counter collides with versions `schema_migrations` already holds from an
earlier module, and the migrator then SKIPS the migration silently — leaving
the test to fail on a table that was never created, which reads like a bug in
the code under test rather than in the harness.

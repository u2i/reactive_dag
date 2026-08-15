# Sources and scanning

A **source** reads external state — a fleet API, a git host, an LLM, a human's
table — and writes a leaf cell. This guide covers the contract, the two-phase
design invariant behind it, and the one discipline that keeps a graph honest
when the outside world is unreachable.

## The two phases: poll, then drain

```
1. POLL   — each source fetches → writes its leaf's rows → returns changed keys.
            Effectful, non-deterministic, fallible. OUTSIDE the drain.
2. DRAIN  — the engine recomputes everything downstream of the dirty frontier.
            Pure set/graph computation over rows already written.
            Deterministic, re-runnable, never fails on a network outage.
```

This split is a design invariant, not an accident. Because no source runs
inside the drain, a failure is contained to its own leaf: one vendor being down
cannot wedge the recompute of everything else, and the drain can always be
re-run without re-touching the world.

## The contract

```elixir
defmodule MyApp.Sources.FleetScan do
  @behaviour ReactiveDag.Source

  @impl true
  def id, do: :fleet_scan

  @impl true
  def leaf_cells(_graph), do: ["machines"]

  @impl true
  def origin, do: %{label: "Fleet · endpoint inventory", store: "Fleet MDM"}

  @impl true
  def poll(_opts) do
    with {:ok, hosts} <- Fleet.hosts() do
      keys = Enum.map(hosts, & &1.serial)

      {:ok, changed} =
        ReactiveDag.Node.Rows.reconcile(cell, keys,
          upsert: fn key -> write_row(key) end
        )

      {:ok, %{changed: changed, unreachable: []}}
    else
      {:error, reason} -> {:ok, %{changed: [], unreachable: [{"fleet", reason}]}}
    end
  end
end
```

`poll/1` returns the keys that **actually changed** — the caller marks exactly
those dirty, which is what keeps the cascade proportional to real change rather
than to scan size.

## Binding a source to its leaf

The scanner↔leaf binding has one home, chosen by cardinality:

- **1:1 (the common case)** — inline on the leaf: `source :fleet_scan` /
  `driver MyApp.Sources.FleetScan` in the leaf's `reactive` block. The leaf and
  its scanner travel together; `ReactiveDag.Source.drivers/2` reads the
  bindings off the graph.
- **fan-out (rare)** — a scanner that writes cells no single leaf owns names
  them via its own `leaf_cells/1` and is passed to the host as an *extra*
  driver.

Either way, `ReactiveDag.Source.verify!/2` confirms at startup that every
declared leaf is a real cell in the built plan — a renamed cell fails loudly
instead of a scanner silently writing rows nothing reads.

## Reconcile: the leaf-write skeleton

`ReactiveDag.Node.Rows.reconcile/3` is the one algorithm every leaf driver
otherwise hand-rolls:

```
current  = the cell's current keys (read from its own resource)
want     = what the scan found
upsert   each want key    → the row you observed; the library writes it
vanished = current − want → retired (destroyed, or a host tombstone policy)
⇒ changed_upserts ++ vanished   (the keys to propagate)
```

With a `fingerprint` on the leaf, a poll is fetch → build rows → reconcile:

```elixir
def poll(_opts) do
  with {:ok, docs} <- Crawler.fetch() do
    {:ok, changed} =
      ReactiveDag.Node.Rows.reconcile(cell, Enum.map(docs, & &1.url),
        upsert: fn url -> Map.get(by_url, url) end     # the row, or nil
      )

    {:ok, %{changed: changed, unreachable: []}}
  end
end
```

Returning `nil` for a key means *I could not observe this one*: nothing is
written and the key is not reported. That is the honest gap (below) expressed in
a return value rather than a rule you have to remember.

`upsert:` also accepts `(key -> boolean)` — write the row yourself and say
whether it moved — for a leaf whose write is not an upsert into its own
resource. `retire:` is destroy by default; a retain-if-vanished host passes a
tombstone function. Vanished keys always propagate: something disappearing is a
change.

A leaf written by ordinary Ash actions rather than a scan needs none of this:
declare `dirties_on [:create, :update, :destroy]` and each write marks its own
key dirty, inside the write's transaction.

## `fingerprint`: what counts as the same observation

A leaf's row carries fields that move on **every** observation without the
observation having changed anything — a `last_seen_at` by definition, an `etag`
a server may re-issue for identical bytes. The library's default change
detection compares every attribute, so those fields report a change on every
poll, and everything downstream recomputes. For a graph whose downstream work is
LLM extraction over PDFs, that is the entire cost the engine exists to avoid.

Name the one value that decides instead:

```elixir
reactive do
  id :agenda_docs
  leaf? true
  scan MyApp.Sources.AgendaCenter

  fingerprint [:content_md5]        # hashed and stored on the row
end
```

Or compute it, when "the same observation" is not a plain field comparison:

```elixir
  # a re-titled meeting re-fires its shell, even though the PDF has not moved
  fingerprint fn row -> "#{row.content_md5}|#{:erlang.phash2(row.title)}" end
  fingerprint_attribute :digest     # default is :fingerprint
```

The value is written to the row, so the next pass has something to compare
against — the resource needs that column, and you get a raise naming it if it is
missing rather than a fingerprint that silently never matches.

**What counts is yours to decide.** Usually it is the content digest;
deliberately not always. The library only needs somewhere to put the answer.

This is the same `fingerprint` vocabulary `per_key` uses to skip an expensive
action when its inputs have not moved — one concept, one implementation, at two
rungs of the ladder.

## When a key stops being returned

One decision, three answers. Pick by what the row is worth once the upstream
stops listing it:

| you want | you write | the row | the key propagates? |
|---|---|---|---|
| **destroy** it | nothing — the default | destroyed | yes |
| **keep** it | `retain_if_vanished true` | untouched | no |
| **mark** it | `retain_if_vanished mark: &tombstone/1` | yours to write | yes |

Keep and mark are the same operation with one question between them: **do we
write something to say it is gone?** Propagation follows from the answer rather
than being a separate switch —

- **destroying** removes a unit downstream was counting, so it is a change;
- **keeping** writes nothing, so nothing changed — reporting it would be a lie,
  *and* would report it again on every poll forever, since nothing marks it as
  handled;
- **marking** writes something, so downstream hears about it.

```elixir
reactive do
  leaf? true
  scan MyApp.DocCrawler

  retain_if_vanished true                        # keep, silent
  # retain_if_vanished mark: &MyApp.tombstone/1  # ...or mark, and propagate
end
```

`mark:` receives the vanished keys and does whatever your policy is — set a
status, stamp a timestamp, write an audit row. The library never learns what it
means, which is why the column names stay yours.

The rest of this section is the second and third rows.

### Keeping what the upstream dropped

By default a key the scan stops returning has its row **destroyed**. For a
derived node that is right: a row whose inputs are gone is stale, and a stale
derived row is indistinguishable from a live one.

For a leaf it is often wrong. The listing dropped the document, but the PDF you
fetched is still yours — and may not be re-fetchable. Declare it:

```elixir
reactive do
  leaf? true
  scan MyApp.DocCrawler
  retain_if_vanished true
end
```

The row stays, untouched, with everything on it.

**A retained key is not reported as changed.** The row is still there and
nothing about it moved, so from a consumer's side nothing happened — a rollup
over this leaf still counts it, correctly, because it is still a row. That also
keeps polling idempotent: reporting it would report it *again* on every
subsequent poll, forever, since nothing marks the key as already handled.

```
poll 1: [a, b]   → changed: ["a", "b"]
poll 2: [a]      → changed: []          ← b's row kept; nothing to report
poll 3: [a]      → changed: []          ← and it stays quiet
poll 4: [a, b]   → changed: []          ← b never left
```

Real changes still propagate: if `b` comes back with different content, its
fingerprint has moved and it is reported. Retention hides a disappearance, not
an edit.

For anything beyond keeping the row — a tombstone column, an audit trail —
`:retire` still takes a `(keys -> any)` fun, and those keys **do** propagate,
because the host did something.

### Marking: when the row records that it is gone

`retain_if_vanished mark: &tombstone/1` keeps the row and hands you the vanished
keys to write whatever your policy is — a status, a timestamp, an audit row. The
library never learns what it means, which is why the column names stay yours.

Because something was written, the keys propagate.

**Revival is handled for you.** A marked-retired row that comes back carries the
fingerprint it left with — its content did not move, its liveness did — so a
fingerprint comparison alone would report "unchanged" and the return would never
reach downstream. The library reports it instead: the key was in the scan, and
absent from the baseline you supplied, which is exactly what coming back looks
like.

```
poll 1: [a, b]   → changed: ["a", "b"]
poll 2: [a]      → changed: ["b"]        ← marked; you wrote the tombstone
poll 3: [a]      → changed: []           ← already marked, and out of your baseline
poll 4: [a, b]   → changed: ["b"]        ← REVIVED, though b's bytes never moved
```

This needs your `:current` to be the **live** set — the keys your marking left
alone. That is the baseline the library subtracts from, so it is also how it
recognises a return.

## Partial observations

Retiring a key is an **inference**: the upstream no longer lists it, so it is
gone. That inference is only valid from a *complete* observation.

A scoped poll (`only:`), a windowed one (`recent:`), or a crawl whose index page
failed all produce a want-set that is real but incomplete. Absence from it means
"not looked at", not "gone" — so say so:

```elixir
Rows.reconcile(cell, observed_keys, observed: :partial, upsert: &fetch/1)
```

Nothing vanishes, nothing is retired. The keys you *did* see are written and
reported exactly as usual, so a partial poll still drives the cascade for the
slice it covered.

**Why this has a name.** The failure is asymmetric. Getting `:partial` wrong
under-retires — rows linger that should have gone, and the next full scan
cleans them up. Getting `:all` wrong tombstones everything the scan did not
happen to look at, which for an archival consumer is a mass-deletion wave from
one upstream 500. One direction is untidy; the other is unrecoverable.

If your scanner narrows itself — and `scan … args: [recent: true]` means it
does — decide the mode from the same condition:

```elixir
def poll(opts) do
  scoped? = opts[:only] != nil or opts[:recent] == true

  with {:ok, docs, failures} <- fetch(opts) do
    Rows.reconcile(cell, Map.keys(docs),
      observed: if(scoped? or failures != [], do: :partial, else: :all),
      upsert: &Map.get(docs, &1)
    )
  end
end
```

A failed index page belongs in that condition too: a crawl that could not read
part of its own index observed less than it meant to, whether or not it was
scoped.

## The honest-gap discipline

The single most important rule for a source:

> **An upstream you could not reach writes NOTHING.**

If the fleet API is down and the scan writes an empty set, `reconcile` will
dutifully retire every machine — and every downstream guarantee will see an
estate with no members, which typically rolls up as *vacuously green*. A scan
that couldn't look must never render as a scan that found nothing.

A total outage and a partial observation are not the same thing, though both
mean "do not retire". An outage writes **nothing** — so it marks nothing dirty,
and the drain correctly does no downstream work; the rows you already have stand
as the last true thing you knew. A partial observation writes what it *did* see,
and simply must not conclude anything from what it did not.

So on failure: write nothing, retire nothing, and report the outage in the poll
result (`unreachable:`) so the host can surface it. Within a partially-successful
scan, returning `nil` from `upsert:` for the keys you could not observe does the
same thing per-key — but a scan that failed entirely must not reach `reconcile`
with an empty want-set at all, because *every* key would then read as vanished. The stale rows that remain
are the *truthful* state: last known, and aging — put a `last_seen_at` column on
the leaf's resource if you want that visible.

Corollary: when a source feeds several leaves and only some upstreams fail,
write the leaves you could observe and skip the ones you couldn't — never let
one vendor's outage discard another's (or a human's) data. If two kinds of
evidence keep ending up in one poll, that is usually the signal they are two
sources.

## Humans are a source too

A human edit — a managed list, an approval, a claim — enters the graph the same
way a scan does: write the leaf, mark dirty, drain. The only differences are
timing (human-initiated rather than scheduled), latency (sub-second: it reads
your own database), and failure mode (none — no vendor round-trip). None of
those differences need machinery; they are properties of the source, not of
the propagation.

For human assertions that carry *accountability* — who confirmed what, when,
and whether it still holds — carry a `ReactiveDag.Basis` digest beside it rather than a bare
leaf write: it adds the signer, the content basis, and read-time force
evaluation on top of exactly this propagation path.

## The refresh loop

A typical host wraps poll + propagate + drain in one function:

```elixir
def refresh(source, plan) do
  {:ok, result} = source.poll([])

  for leaf <- source.leaf_cells(plan) do
    ReactiveDag.Graph.dirty_parents(plan, leaf, result.changed, MyApp.KeyRule)
  end

  ReactiveDag.Drain.run(plan, recompute: MyApp.Recompute, key_rule: MyApp.KeyRule)
  {:ok, result}
end
```

Order sources so that ones which only observe the world run before any source
that derives from other cells' results — a deriving source that runs first
computes against a stale model.

## Cadence, and running a scanner on demand

Scanners differ enormously in what a full check costs. A directory listing is
free; a crawler whose discovery is one request per board per year is not. The
routine check should be cheap, and the expensive pass should be something you
ask for — one scanner with two ways to call it, not two scanners.

Both options go on the `scan` declaration, so everything about a leaf reads in
one place: what feeds it, what a routine check costs, how often it should
happen, and what counts as a change.

```elixir
defmodule MyApp.Docs do
  use Ash.Resource, data_layer: AshPostgres.DataLayer, extensions: [ReactiveDag.Node]

  attributes do
    attribute :url, :string, primary_key?: true
    attribute :body, :string
    attribute :content_md5, :string
    attribute :last_seen_at, :utc_datetime_usec   # moves on EVERY poll
  end

  actions do
    defaults [:read, :destroy]
    create :upsert do upsert?(true); accept([:url, :body, :content_md5, :last_seen_at]) end
  end

  reactive do
    leaf? true

    scan MyApp.DocCrawler,
      args: [recent: true],       # the standing default for a routine poll
      every: "0 * * * *"          # how often a routine poll SHOULD run

    fingerprint [:content_md5]    # what counts as a changed observation
  end
end
```

`args:` merges into the poll's opts with **the caller winning**:

```elixir
poll_all(plan)                  # recent: true — cheap, and no call site had to remember
poll_all(plan, recent: false)   # the deliberate deep pass
poll_all(plan, only: [2019])    # narrower still; recent: true still applies
```

That is the point of declaring it: a forgotten bound at one call site would
quietly issue every request the cheap path exists to avoid.

**A scanner cheap enough to run whole declares nothing** and is polled with
whatever the caller passes. No ceremony — and no misleading range control on
something that has no range.

### Running one scanner on demand

`poll_all/2` is the routine sweep. `poll_cell/3` is the "refresh this" button: a
UI has a *cell* in hand, not a source module, and a human asking to refresh is
asking about one leaf.

```elixir
Source.poll_cell(plan, "docs")                  # the cheap default
Source.poll_cell(plan, "docs", recent: false)   # the deep pass
```

`{:error, :no_scanner}` comes back for a cell that has none — a derived node, or
a leaf fed by ordinary writes. Render that as *no refresh available* rather than
as a failure.

### Building the control

The library describes; your UI renders. `controls/1` reports every cell that has
a scanner, and what it declared:

```elixir
Source.controls(plan)
#=> %{"docs" => %{source: MyApp.DocCrawler,
#                 args: [recent: true],
#                 every: "0 * * * *",
#                 origin: %{label: "City agenda center"}}}
```

A cell with no scanner is absent. A scanner with no `args:`/`every:` reports
them empty — so a cheap leaf gets a plain *refresh* and an expensive one can be
offered its deep pass, without the UI knowing which scanners are costly.

### The library does not schedule

`every:` is a declaration, not a job. `crontab/2` collects them into entries you
hand to your own scheduler:

```elixir
plugins: [
  {Oban.Plugins.Cron, crontab: ReactiveDag.Source.crontab(plan, MyApp.ScanWorker)}
]
#=> [{"0 * * * *", MyApp.ScanWorker, args: %{"source" => "doc_crawler"}}]
```

Emitting data rather than inserting jobs keeps the library out of your
supervision tree and your deploy story — and lets you filter, rewrite or ignore
what it produces, which you could not do if it had already scheduled. Your
worker receives `%{"source" => "doc_crawler"}` and polls that one scanner.

## Seeing whether it worked

A poll returns `%{changed: […], unreachable: […]}`, and both halves matter. An
empty `changed` is ambiguous on its own — nothing moved, or nothing was looked
at? — which is why `unreachable` exists. Log it, alert on it, or surface it; a
gap that nobody sees is the failure mode this whole discipline exists to prevent.

For the state of a leaf after the fact, read the cell:

```elixir
cell = plan.cells["machines"]

ReactiveDag.Node.Rows.all(cell)               # what the leaf currently holds
ReactiveDag.Insights.cell_status(plan, "machines")
#=> %{key_count: 412, statuses: %{…}, failing_sample: […], …}
```

A leaf whose rows cannot be read at all reports `key_count: 0` rather than
raising, so a health check can tell "the scan found nothing" from "I could not
look" — the same distinction the honest-gap rule turns on.
[reactive_dag_dashboard](https://github.com/u2i/reactive_dag_dashboard) renders
this, including a per-leaf key count and the drain trace each poll produced.

Put a `last_seen_at` on the leaf's resource (excluded from the `fingerprint`, so
it does not fire the cascade) and staleness becomes an ordinary query:

```elixir
MyApp.Machines
|> Ash.Query.filter(last_seen_at < ago(2, :day))
|> Ash.read!()
```

## Declaring the scanner: `scan`

A leaf says which scanner feeds it:

```elixir
reactive do
  id :agenda_docs
  leaf? true
  scan MuniWatch.Crawler
end
```

That makes the scanner↔leaf pairing **a fact of the graph**, which buys two
things:

- **`ReactiveDag.Node.graph/2` verifies it.** The module must implement `ReactiveDag.Source`,
  and its own `leaf_cells/1` must claim this leaf. A scanner refactored to feed
  `"agenda_docs_v2"` while a resource still declares `scan` fails at assembly,
  rather than polling into a cell nobody reads. No `verify!/2` call needed.
- **`Source.poll_all/2` finds scanners from the plan**, not from a list kept
  alongside it:

```elixir
plan = MyApp.Dag.plan()
{:ok, _results} = ReactiveDag.Source.poll_all(plan)   # poll phase
{:ok, report} = ReactiveDag.Drain.run(plan, opts)     # then drain
```

A source feeding many leaves is polled **once**, however many leaves name it.
One scanner failing is reported (`{:error, [{module, reason}]}`) rather than
cancelling the others or looking like success.

Polling still happens **outside the drain** — external I/O has no business
inside a depth-ordered recompute. `scan` changes where the binding is
*declared*, not when fetching happens.

### A scanner is not required to be `leaf? true`…

…but it must not be a node that **computes**. Nothing about `Source` inspects
`leaf?`, and hosts legitimately direct-write cells that aren't strictly leaves
(a companion store cell, for instance). What is a contradiction is declaring a
scanner *and* a computation on one node:

```elixir
reactive do
  id :category_totals
  scan MyApp.Crawler                 # writes this cell's rows from outside…
  reduce into: [sum: [amount: :total]]   # …and derives them from inputs
end
```

A scanner writes this cell's rows from outside the graph; a combinator derives
them from its inputs. Declared together, the poll and the drain overwrite each
other — the drain reprices from inputs and discards whatever the poll wrote,
which surfaces as data that mysteriously reverts. `graph/2` raises on it.

### When not to use it

`scan` is the **single-leaf** spelling. A source that feeds many leaves — one per
discovered kind, or a generator's expanded instances — implements
`leaf_cells/1` and is passed to `verify!/2` directly. That callback takes the
lowered graph precisely because those leaves come from live data, which no
declaration can name ahead of time.

## `dirties_on` vs a `Source`: which trigger?

Two ways a leaf becomes dirty, and they are not alternatives — they cover
different kinds of state.

| | use | why |
|---|---|---|
| **`dirties_on`** | state written **through Ash** | the write itself is the trigger; nothing to poll, nothing to miss |
| **`Source`** | state the datastore **doesn't own** | an S3 bucket, an API, another system's table — only a poll can notice |

```elixir
reactive do
  id :expenses
  leaf? true
  dirties_on [:create, :update, :destroy]   # writes here trigger the cascade
end
```

A create/update/destroy marks that record's key dirty on its own cell, so the
next drain picks it up. Without it, a host must call
`ReactiveDag.Frontier.mark_dirty/3` at every write site — and a missed call is
silent staleness, which is the failure this removes.

**The mark is inside the write's transaction.** It runs as an `after_action`
hook, so a rolled-back write leaves no dirty key, and a committed write always
leaves one. (An `Ash.Notifier` looks like the natural fit and is not: Ash
dispatches notifications *after* commit, so a crash in between would lose the
mark.)

It is **opt-in and not implied by `leaf?`** — a leaf fed by a `Source` poll
would otherwise double-trigger, marking itself on the write the poll just made.

**The mark carries a snapshot of the row as it was.** `after_action` fires after
the change is applied, so the *result* names where a row went — but a parent
also needs to know where it came FROM. The changeset's pre-change data is
recorded alongside the key, which is the only thing that survives:

| case | without a snapshot | with one |
|---|---|---|
| a row is **deleted** | nothing to read → the claim degrades to whole-cell | the snapshot still names its unit |
| a row **moves** between units | only the destination is claimed; the origin silently keeps counting it | both units are claimed |

Coalescing keeps the **first** snapshot (`ON CONFLICT DO NOTHING`): if a row is
written twice before a drain, the oldest prior state is the one that names the
unit it was in when the graph last settled.

Keys derive exactly as the payload loop's do: a composite primary key
serializes in primary-key order (`"gf|2025"`), otherwise the payload key
attribute. A record with no derivable key escalates to a whole-cell claim
rather than silently marking nothing.


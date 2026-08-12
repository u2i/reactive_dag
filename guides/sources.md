# Sources and scanning

A **source** reads external state — a fleet API, a git host, an LLM, a human's
table — and writes a leaf cell. This guide covers the contract, the two-phase
design invariant behind it, and the one discipline that keeps a graph honest
when the outside world is unreachable.

## The two phases: poll, then drain

```
1. POLL   — each source fetches → writes its leaf tuples → returns changed keys.
            Effectful, non-deterministic, fallible. OUTSIDE the drain.
2. DRAIN  — the engine recomputes everything downstream of the dirty frontier.
            Pure set/graph computation over tuples already present.
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
        ReactiveDag.Tuple.reconcile("machines", keys,
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

`ReactiveDag.Tuple.reconcile/3` is the one algorithm every leaf driver
otherwise hand-rolls:

```
current  = the cell's current keys
want     = what the scan found
upsert   each want key    → host writes the row; returns true iff CHANGED
vanished = current − want → retired (delete, or a host tombstone policy)
⇒ changed_upserts ++ vanished   (the keys to propagate)
```

The host supplies the two variation points — `upsert:` (what a row contains and
what counts as changed is domain logic) and `retire:` (`:delete` by default; a
retain-if-vanished host passes a tombstone function). Vanished keys always
propagate: something disappearing is a change.

## The honest-gap discipline

The single most important rule for a source:

> **An upstream you could not reach writes NOTHING.**

If the fleet API is down and the scan writes an empty set, `reconcile` will
dutifully retire every machine — and every downstream guarantee will see an
estate with no members, which typically rolls up as *vacuously green*. A scan
that couldn't look must never render as a scan that found nothing.

So on failure: write no tuples, retire nothing, and report the outage in the
poll result (`unreachable:`) so the host can surface it. The stale rows that
remain are the *truthful* state: last known, aging, and visibly so through the
spine's `observed_at`.

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

- **`Node.graph/2` verifies it.** The module must implement `ReactiveDag.Source`,
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
  scan MyApp.Crawler                 # writes tuples from outside…
  reduce into: [sum: [amount: :total]]   # …and derives them from inputs
end
```

A scanner writes this cell's tuples from outside the graph; a combinator derives
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


# One engine, and where the domain enters

There is one recompute engine. You declare relationships with the DSL, and it
runs.

The substrate decides *when* and *in what order* cells recompute; it never
decides *what a value means*. A node's `reactive` block declares both halves of
what the drain needs — what the node computes, and what a change to its input
invalidates — and the drain reads those declarations. Nothing is supplied at the
call site:

```elixir
plan = ReactiveDag.Node.graph([MyApp.FiscalLines, MyApp.BudgetRollups])
{:ok, report} = ReactiveDag.Cascade.run(plan, [%{cell: "expenses", keys: ["e1"]}])
```

`run/2`'s only option is `:max_passes`, a runaway guard. Everything else the
drain needs, it reads off the plan.

## What the drain reads

| declaration | what the drain does with it | authored as |
|---|---|---|
| the combinator — `reduce` / `join` / `aggregate` / `union` / `per_key` / `run` / `compute Mod` | how this cell recomputes | one entity in `reactive do` |
| `recompute_by` (or `key_rule` on a node with no combinator) | what unit a change invalidates, hence what propagates | one declaration in `reactive do` |
| `ref` / `context` / `over:` / `recompute_by to:` | the input edges, hence the scheduling order | edges in `reactive do` |
| `poll Mod` | which module fetches from outside, in the poll phase | an edge to a `ReactiveDag.Source` |

The combinators are covered in [Authoring nodes](authoring-nodes.md); this guide
is about the boundary itself — what the library will and will not let you
replace, and how to run the engine over cells you built yourself.

`op :map` is not on that list. It reads like the field that decides how a node
recomputes, and it is not: recompute dispatches on the **entity**, and `op` is a
free atom for documentation. Nothing in the library reads it.

## What is still a seam

Two things, and they have a property in common: both are **declared in the
node**, so `graph/2` can check them.

### `ReactiveDag.Source` — how the world gets in

A host implements `poll/1`. Covered in depth in
[Sources and scanning](sources.md): `id/0`, `poll/1 → changed keys`, with
polling deliberately **outside** the drain. The seam exists because fetching is
effectful and fallible while the drain must stay pure and re-runnable — an
unreachable vendor is one cell staying dirty, not a wedged cascade.

It is a real seam because there is nothing generic to say about fetching. An
API's pagination, a repo's diffing, a crawler's rate limit: none of that is
schedule or identity, and no combinator will ever express it.

```elixir
reactive do
  id :agenda_center
  poll MuniWatch.Sources.AgendaCenter, every: "0 12 * * *"
end
```

`graph/2` checks the module implements the behaviour, and `Source.poll_all/2`
finds every scanner from the plan rather than from a list kept beside it.

### `compute Mod` — work Ash cannot express

The in-DSL escape hatch: a `ReactiveDag.Op` module that receives
`(cell, dirty_keys)`, reads its inputs however it likes, writes its rows however
it likes, and returns the keys that **actually changed**. For an LLM call, a PDF
parse, a bespoke multi-input recompute — recompute that outgrows Ash entirely.

```elixir
reactive do
  compute MyApp.Ops.EventsExtract   # implements ReactiveDag.Op
  ref :transcripts
end
```

The changed-key return is the contract that keeps a cascade O(real changes)
rather than O(graph size). Returning every key is always correct, just less
efficient. An op with something worth reporting about the work returns
`{:ok, changed, meta}` instead of `{:ok, changed}` — an arbitrary map that rides
on the `%Report{}` step (`Report.total/2` rolls one key up across steps). The
library never interprets it: token and cost counts, cache hits, retries and rows
scanned are all just keys.

An op **raises** to fail. Returning `{:error, reason}` from an op is not the
containment mechanism — the drain has already claimed those keys, and a
swallowed failure marks them clean over work that did not happen.

Before reaching for `compute`, note the two rungs above it: `run :action` keeps
the computation a first-class Ash action, and `per_key :action` additionally lets
the library see what each row depends on, so `fingerprint:` can skip rows that
have not moved. `compute` buys arbitrary power at the cost of the library
knowing nothing about the work.

## Why the pluggable engine went

Earlier versions took `recompute:` and `key_rule:` module options on
`Drain.run/2`, with a `RecomputeStrategy` behaviour a host implemented and a
`SetOp` strategy that dispatched `cell.op` to a SQL template looked up in
application config. Both hosts — a per-key Elixir pipeline calling LLMs, and a
set-based SQL compliance model — ended up passing the library's own modules,
because what actually varied between them was **data the DSL can declare** (a
module named in `compute`, a combinator, a key rule), not control flow. A
pluggable engine everyone plugs the same thing into is an indirection.

That is the line between the two lists above. `Source` and `compute Mod` are
named **in the node**, so assembly resolves them, the verifier checks them, and
the plan carries them. A strategy looked up in config is invisible to `graph/2`
and can contradict every node it dispatches over.

## Where results live

There is no shadow table. A node's results are rows in that node's own
resource, with that resource's own columns, types, policies and migration.

This was not always so. The library used to write a coordination tuple — a row
per `(cell_id, key)` in a side table — carrying a `status` and freshness
alongside every result, with a `CoordinationWriter` seam so a host could stamp
its own extension columns into the same upsert. It existed because a node could
be *tableless*: a verdict node had nowhere else to put its answer.

Once every node had a resource, that table had nothing left to record that the
resource did not already say, and the seam had nothing left to write. A host
that wants a `source_ref`, a `last_seen_at` or a tombstone flag puts it on the
node's resource, where the rest of the row already is.

What remains of that machinery is `ReactiveDag.Node.Rows.reconcile/3` — the
set math a leaf driver needs (`current − want → retired`), reading the leaf's
own rows.

## Hand-assembled graphs

The `Node` extension is one authoring surface, not the substrate. A host can
build `ReactiveDag.Cell` structs directly — or lower its own DSL — and run the
same engine:

```elixir
cells = [
  %ReactiveDag.Cell{id: "machines", op: :leaf, leaf?: true},
  %ReactiveDag.Cell{id: "verdict", op: :reconcile, inputs: ["machines"],
                    meta: %{compute: MyApp.ReconcileOp, key_rule: :all}}
]

plan = ReactiveDag.Graph.build(cells)
ReactiveDag.Cascade.run(plan, origins)
```

The cells are hand-built; the engine is the same one. `meta.compute` is where
`ReactiveDag.Node` puts a `compute` module, so a hand-built cell reaches the
`ReactiveDag.Op` seam by putting it in the same place — and `meta.key_rule`
(`:identity | :all`) is read the same way. `Cell.meta` is otherwise an open map
the substrate passes through untouched, with an `Access` impl so `cell[:field]`
reads meta transparently; carry whatever your ops need.

For a host with its own *nested* expression DSL, `ReactiveDag.Lowering.walk/3`
is the shared recursion (parameterized by id grammar, ref resolution, and cell
construction), and `ReactiveDag.Dsl.compile/2` adds structural validation plus
a domain-validation hook.

**Marking from outside the drain.** A host that writes a leaf's rows itself — a
scanner, a migration, a re-run triggered by an operator — marks the change
upward with `ReactiveDag.Graph.dirty_parents/5`. Its fourth argument is a module
exporting `rule/3`, defaulting to `ReactiveDag.Node.KeyRule`, so the ordinary
call omits it:

```elixir
ReactiveDag.Graph.dirty_parents(plan, "fiscal_lines", changed)
```

The parameter survives because the caller is outside the drain and may know
something the drain cannot: a host pre-marking a re-run states the propagation
it means, rather than the one the node declares. Inside a drain, propagation
always comes from the node's own declaration.

`ReactiveDag.KeyRule` is where the vocabulary lives — the `result()` type
(`:all | {:keys, keys}`) and the trivial `identity/3`. It is not a behaviour to
implement; getting the `:all`-versus-keys distinction right is the whole
content, and the moduledoc is worth reading before writing a rule, because
escalating to `:all` is always correct and sometimes wasteful, while passing
keys through when the grain actually changed strands rows that needed
repricing.

## The design law

One sentence governs what goes where: **if it mentions the domain, it is the
host's; if it decides scheduling or identity, it is the library's.** The full
argument — including what was deliberately *removed* (a command frontier whose
queue never actually queued anything, and the pluggable strategy layer above) —
is in the repository's ADR-001.

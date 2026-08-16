# The seams

The substrate decides *when* and *in what order* cells recompute; it never
decides *how*, or *what a value means*. Everything domain-shaped enters through
three named seams — the same three that let two very different hosts (a per-key
Elixir pipeline calling LLMs, and a set-based SQL compliance model) share one
engine without forking it.

This guide is for hosts going beyond the `ReactiveDag.Node` authoring surface:
custom recompute strategies, custom key propagation, and hand-assembled graphs.

## Seam 1: `ReactiveDag.RecomputeStrategy` — how a cell recomputes

```elixir
@callback recompute(cell, dirty_keys :: [key] | :all) :: {:ok, changed :: [key]}
```

The drain claims a cell's dirty keys and hands them to the strategy; the
strategy does the work — per-key Elixir, one set-based SQL statement, an LLM
call — and returns the keys whose output **actually changed**. Only those
propagate: the contract that keeps a cascade O(real changes) instead of
O(graph size).

Two strategies ship:

- `ReactiveDag.Node.Recompute` — dispatches on the `reactive` block's
  combinator (`reduce`/`join`/`aggregate`) or `compute` module. What
  `Node`-authored graphs use.
- `ReactiveDag.SetOp` — dispatches on `cell.op` to a host-supplied SQL
  template. What a set-based host uses; this is the one place `Cell.op` is
  load-bearing rather than documentation.

## Seam 2: `ReactiveDag.KeyRule` — how a change propagates

```elixir
@callback rule(parent_cell, child_id, changed_keys) :: {:keys, [key]} | :all
```

When child `c` reports changed keys, the rule decides what that means for each
parent: the same keys (`:identity` — same-grain pipelines), a whole-cell
recompute (`:all` — grain-changing folds), or any host remapping (prefix
grammars, one-to-many expansions). `ReactiveDag.Node.KeyRule` reads
`:identity | :all` off the authored block; bring your own for a real key
grammar.

## Seam 3: `ReactiveDag.Source` — how the world gets in

Covered in depth in [Sources and scanning](sources.md): `id/0`,
`poll/1 → changed keys`, with polling deliberately outside the
drain. The seam exists because fetching is effectful and fallible while the
drain must stay pure and re-runnable.

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
                    meta: %{compute: MyApp.ReconcileOp}}
]

plan = ReactiveDag.Graph.build(cells)
ReactiveDag.Drain.run(plan, recompute: MyStrategy, key_rule: MyKeyRule)
```

A strategy with something worth reporting about the work returns
`{:ok, changed, meta}` instead of `{:ok, changed}` — an arbitrary map that rides
on the `%Report{}` step (`Report.total/2` rolls one key up across steps). The
library never interprets it: token/cost counts, cache hits, retries and rows
scanned are all just keys.

```elixir
```

For a host with its own *nested* expression DSL, `ReactiveDag.Lowering.walk/3`
is the shared recursion (parameterized by id grammar, ref resolution, and cell
construction), and `ReactiveDag.Dsl.compile/2` adds structural validation plus
a domain-validation hook. `Cell.meta` is an open map the substrate passes
through untouched — with an `Access` impl so `cell[:field]` reads meta
transparently; carry whatever your strategy needs.

## The design law

One sentence governs what goes where: **if it mentions the domain, it is the
host's; if it decides scheduling or identity, it is the library's.** The full
argument — including what was deliberately *removed* (a command frontier whose
queue never actually queued anything) — is in the repository's ADR-001.

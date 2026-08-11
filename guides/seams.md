# The seams

The substrate decides *when* and *in what order* cells recompute; it never
decides *how*, or *what a value means*. Everything domain-shaped enters through
four named seams — the same four that let two very different hosts (a per-key
Elixir pipeline calling LLMs, and a set-based SQL compliance model) share one
engine without forking it.

This guide is for hosts going beyond the `ReactiveDag.Node` authoring surface:
custom recompute strategies, custom key propagation, extension columns, and
hand-assembled graphs.

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

## Seam 3: `ReactiveDag.CoordinationWriter` — how tuples are written

The spine (`cell_id, key, status, freshness`) is shared, but a host's
coordination write often touches its **extension columns in the same atomic
upsert** — a `strength` modality, a `source_ref`, a tombstone policy. That is
host policy, so the write is a seam:

```elixir
@callback put(cell_id, key, opts) :: :ok | boolean()
@callback delete(cell_id, keys) :: :ok
@callback tombstone(cell_id, keys) :: :ok    # optional
```

Ops write through `ReactiveDag.Op.put/3`, which routes here. Two things worth
exploiting:

- **The changed signal.** A writer's `put` may return a boolean — *did the
  row's verdict actually flip?* — which ops use as their changed-key signal.
  The spine-only default reports it (`ReactiveDag.Tuple.put_changed/3`: `true`
  for a new row or a status flip). A writer that returns bare `:ok` is treated
  as "assume changed": correct, just less scoped.
- **Opts are the extension channel.** Machinery above the seam passes host
  fields in opts (`strength:`, `source_ref:`, …); the default writer takes
  only spine keys and drops the rest, a host writer stamps what it knows. This
  is how, e.g., attested rows carry `strength: "attested"` without the library
  ever writing a column it doesn't own.

Configure with `config :reactive_dag, coordination_writer: MyApp.Writer`.

## Seam 4: `ReactiveDag.Source` — how the world gets in

Covered in depth in [Sources and scanning](sources.md): `id/0`,
`leaf_cells/1`, `poll/1 → changed keys`, with polling deliberately outside the
drain. The seam exists because fetching is effectful and fallible while the
drain must stay pure and re-runnable.

## Spine vs. extension columns

The library owns the spine columns of the tuple table and is their only
reader/writer:

```
cell_id, key                      (composite identity)
status                            (string — the HOST defines the vocabulary)
observed_at, stale_after, updated_at
```

The host's physical table may carry anything else beside them; the library
never reads or writes those columns. `status` deserves emphasis: it is an open
vocabulary. `"present"` is only a default — a compliance host writes
`covered/failing/pending`, and rollups (`ReactiveDag.Verdict`) take the
host's meaning of each status as configuration, not assumption.

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

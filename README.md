# reactive_dag

A domain-agnostic **reactive DAG engine** for Elixir/Ash apps: a dirty frontier
+ depth-ordered incremental drain + change propagation, plus the coordination
tuple, leaf-reconcile, and nested-expression lowering that go with it. Extracted
from two apps that independently grew the same engine (the Red Hook `cascade`
pipeline and the u2i compliance portal's `model_eval`), and now shared by both.

The substrate decides *when* and *in what order* cells recompute; it never
decides *how* or *what a value means*. Each host brings its domain at the seams:

- **`ReactiveDag.RecomputeStrategy`** — how a cell recomputes (cascade: per-key
  Elixir that may call an LLM / parse a PDF; the portal: one set-based SQL join).
  Returns the keys that actually changed.
- **`ReactiveDag.KeyRule`** — how a change propagates to a parent (identity, a
  remap, or `:all` for a whole-cell recompute).
- **`ReactiveDag.CoordinationWriter`** — how a cell's coordination tuples are
  written (the host writes its spine + extension columns in one atomic upsert).
  A default spine-only writer ships; hosts with extension columns supply their own.

## What the library owns

| Layer | Module | What it provides |
|---|---|---|
| Node IR | `ReactiveDag.Cell` | domain-neutral node; `op` is a **free atom** the library never interprets; app fields ride in `meta` (with an `Access` impl so `cell[:field]` reads meta transparently). |
| Compiled plan | `ReactiveDag.Plan` | pure data: `cells / parents / depths`. |
| Graph math | `ReactiveDag.Graph` | `build/1` (validate + parent edges + longest-path depths + cycle check); `dirty_parents/4` (propagation via the host `KeyRule`). |
| Dirty frontier | `ReactiveDag.Frontier` | claim-as-delete over the host's dirty table; `mark_dirty / next_cell / claim / empty?`. |
| Drain loop | `ReactiveDag.Drain` | depth-ordered incremental propagation; `run/2` parameterized by the two seams + an `:on_step` trace hook carrying `triggered_by` + `duration_us`. |
| Coordination tuple | `ReactiveDag.Tuple` | the shared `(cell_id, key, status, freshness)` spine over the host's tuple table: `put / present_keys / all_keys / keys_by_status / status_histogram / reconcile / …` + a `:key_scope` selector. Payload stays in the host's typed resources, joined by `key`. |
| Nested-expr lowering | `ReactiveDag.Lowering` | `walk/3` — the nested op-expression → flat-cell recursion both DSLs grew, parameterized by host callbacks (id grammar, ref resolution, cell construction). |
| Compile pipeline | `ReactiveDag.Dsl` | `compile / validate_cells` — resolve → structural-validate, with a domain-validation hook. |
| Op contract | `ReactiveDag.Op` | the behaviour a cell's compute module implements (`recompute(cell, keys) -> {:ok, changed}`) + the write API ops call (`put / tombstone / delete`, routed to the `CoordinationWriter`). |
| **Node authoring** | `ReactiveDag.Node` | an **Ash resource extension**: a resource declares its own op + dependencies + computation in a `reactive do … end` block — the resource *is* the node. `ReactiveDag.Node.graph/2` assembles the whole `Plan` from the node resources. See below. |

The host owns its **physical tables** (dirty + tuple, named via config), its
**op algebra**, its **recompute executor**, and any **extension columns** on the
tuple (the portal's `strength` modality, cascade's tombstone/fingerprint
policy). The library owns the spine and the schedule; the domain differences sit
on named seams, not forks.

## Authoring a node

A node is an Ash resource with the `ReactiveDag.Node` extension; its `reactive`
block declares the op, its dependencies, and *how it computes* — the resource
carries both the node definition and (via its own attributes) the payload. The
computation is declared inline for the common shapes, with an escape hatch to a
module for anything bespoke — the Ash calculation model:

```elixir
defmodule MyApp.BudgetRollups do
  use Ash.Resource, extensions: [ReactiveDag.Node]

  reactive do
    op :fold
    key_rule :all
    # a REDUCE (fold): read → group_by → reduce each group to one row.
    reduce over: :fiscal_lines,
           read:     fn :fiscal_lines -> FiscalDoc |> Ash.read!() end,
           group_by: fn line -> {line.fund, line.fy} end,
           key:      fn {fund, fy} -> "#{fund}|#{fy}" end,
           into:     fn {fund, fy}, lines -> %{total: sum(lines)} end,
           upsert:   fn key, row -> write_payload(key, row) end   # → changed?
  end
end
```

Three declarative combinators cover the common map/reduce shapes; each writes the
result set through the coordination seam and `Op.put`s only the changed keys:

- **`reduce`** — a fold: `into` returns one row per group.
- **`expand`** — a `reduce` whose `into` returns a **list** (group → many rows).
- **`join`** — a two-input left join: index `over` into `left`/`right` sides, emit
  one row per left key joined to its right (right may be absent).

Anything the combinators can't express — an LLM call, a PDF/Tigris fetch, a
bespoke multi-input recompute — uses the module escape hatch: `compute: MyOp`
where `MyOp` implements `ReactiveDag.Op`. Both coexist on the same block.

```elixir
# assemble + run a Node-authored graph (no host-written dispatch):
plan = ReactiveDag.Node.graph([BudgetRollups, FiscalLines, …], for_each: &fetch/1)
{:ok, passes} =
  ReactiveDag.Drain.run(plan,
    recompute: ReactiveDag.Node.Recompute,   # runs reduce/join/expand or compute:
    key_rule:  ReactiveDag.Node.KeyRule)       # reads :identity | :all from the block

# config
config :reactive_dag,
  repo: MyApp.Repo,
  dirty_table: "my_dirty",
  tuple_table: "my_tuple",
  coordination_writer: MyApp.Writer   # optional; a spine-only default ships
```

A host can also assemble cells by hand and bring its own strategy/key_rule —
`ReactiveDag.Graph.build(cells)` + `ReactiveDag.Drain.run(plan, recompute:,
key_rule:)` — which is how both apps ran before adopting the `Node` surface.

Status: **both hosts fully migrated** onto the substrate — the shared engine
spans a per-key Elixir recompute (cascade) and a set-based SQL recompute (the
portal), all coordination writes routed through the seam, proven by both suites
green. Cascade additionally authors several ops via the `Node` `reduce`/`join`/
`expand` combinators. Consumed today as a `path:` dep by each app; publish/pin is
the remaining step. See
[docs/adr-001-reactive-dag-library.md](docs/adr-001-reactive-dag-library.md)
for the boundary, the seams, and the design law behind them.

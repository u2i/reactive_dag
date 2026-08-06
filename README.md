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
| **Graph authoring** | `ReactiveDag.Graph.Dsl` / `ReactiveDag.Dsl.Spine` | the **shared graph-level DSL**: one `graph do … end` block declares scanners (`source`), source-fed leaves (`observed`), and derived nodes (`node`/`ref`/`compose`) — the whole DAG in one module. `op` is an open atom; domain fields ride in `meta`. See "Authoring a graph" below. |
| **Node authoring** | `ReactiveDag.Node` | an **Ash resource extension** (the per-resource alternative): a resource declares its own op + dependencies + computation in a `reactive do … end` block — the resource *is* the node. `ReactiveDag.Node.graph/2` assembles the whole `Plan` from the node resources. |
| **Scanner seam** | `ReactiveDag.Source` | the behaviour a scanner implements (`id / leaf_cells / poll`) — reads external state into a leaf in a *poll* phase outside the drain; `verify!/2` checks every declared leaf resolves to a real cell. |

The host owns its **physical tables** (dirty + tuple, named via config), its
**op algebra**, its **recompute executor**, and any **extension columns** on the
tuple (the portal's `strength` modality, cascade's tombstone/fingerprint
policy). The library owns the spine and the schedule; the domain differences sit
on named seams, not forks.

## Authoring a graph

The primary surface is one **graph-level DSL** (`ReactiveDag.Graph.Dsl`, backed by
the `ReactiveDag.Dsl.Spine` extension): a single module declares the whole DAG —
the scanners that feed it, the source-fed leaves, and the derived nodes over them.

```elixir
defmodule MyApp.Pipeline do
  use ReactiveDag.Graph.Dsl

  graph do
    # a SCANNER — reads external state into a leaf, out-of-band (phase-1 poll,
    # NOT a drain op). `driver` must implement `ReactiveDag.Source`, checked at
    # compile time.
    source :fleet_scan, driver: MyApp.Sources.FleetScan

    # a source-fed LEAF — the substrate a scanner writes. `fed_by` names a declared
    # `source`; an unknown id fails the build (compile-time leaf↔scanner check).
    observed :machines, grain: :machine, strength: :measured, fed_by: :fleet_scan

    # a derived NODE — an op over input cells. Options are set INSIDE the block
    # (like Ash's `attributes do attribute … end`). `op` is an OPEN atom: the
    # library schedules the graph; the host's recompute interprets the op-kind.
    node :fleet_health do
      op :reduce
      meta grain: :machine     # open host binding; the library never reads it
      ref :machines            # an input edge (by name)
    end

    # nested COMPOSE — an anonymous intermediate op-expression, so the algebra
    # reads as a tree rather than a pile of named nodes.
    node :variance do
      op :join
      ref :machines

      compose :fold do
        as :rolling
        ref :fleet_health
      end
    end
  end
end
```

Introspect + run it:

```elixir
plan    = ReactiveDag.Dsl.Spine.Info.plan(MyApp.Pipeline)     # → %ReactiveDag.Plan{}
sources = ReactiveDag.Dsl.Spine.Info.sources(MyApp.Pipeline)  # → [driver modules]

# poll (phase 1): each source writes its leaf, out-of-band.
# then verify the leaf↔scanner binding against the built graph:
:ok = ReactiveDag.Source.verify!(sources, plan)

# drain (phase 2): the engine recomputes downstream from the dirty frontier.
{:ok, _passes} = ReactiveDag.Drain.run(plan, recompute: MyApp.Recompute, key_rule: MyApp.KeyRule)
```

**What's checked at compile time:** `driver` implements `ReactiveDag.Source`
(`{:behaviour, _}`); every `observed.fed_by` names a declared `source`; the graph
is structurally sound (refs resolve, ids unique, acyclic). A typo fails the build
with a located `Spark.Error.DslError`, not a silent dead edge.

**How a node computes** — two ways, mixable in one graph:

- **op-kind dispatch** — `node :recon do op :reconcile end` carries only its op
  atom; the host's `ReactiveDag.RecomputeStrategy` (e.g. a set-based-SQL template
  registry keyed by op-kind) supplies the computation. Centralized per op-kind.
- **a per-node executor** — `node :rollups do op :fold; reduce over: …, … end`
  carries its own computation via a `reduce`/`join` combinator or a `compute
  Module` escape hatch. These are the **same** entities the per-resource
  `ReactiveDag.Node` surface uses, lowering to the same `meta.reduce`/`meta.join`/
  `meta.compute` — so `ReactiveDag.Node.Recompute` runs a spine node exactly like
  a resource-authored one.

**Domain vocabulary** lives on the open `op` atom + `meta:` — a compliance host
writes `node :g, do: (op :guarantee; meta claim: "…", addresses: [:CC6_1])`, a
pipeline host `node :d, do: (op :map; meta compute: MyOp)`. A host that wants
*typed* domain fields (compile-checked `{:one_of}` enums, cross-referenced id
lists) composes its own Spark entities alongside the spine — see
[docs/adr-002-unified-dsl.md](docs/adr-002-unified-dsl.md) and the worked
before/after in [docs/adr-002-dsl-sketches.md](docs/adr-002-dsl-sketches.md).

## Authoring a node (per-resource)

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
bespoke multi-input recompute — uses the module escape hatch, declared as an
entity in the same block: `compute MyOp` where `MyOp` implements
`ReactiveDag.Op`. (Mirrors Ash's `calculate :x, :type, MyModule` — the arbitrary
case is an entity too, not a schema key beside the declarative ones.) The
combinators and the escape hatch coexist in the block.

```elixir
reactive do
  op :map
  compute MyApp.Ops.EventsExtract   # arbitrary recompute (LLM, fetch, …)
end
```

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

## Mixing both surfaces — the unified model

The two authoring surfaces are not islands: both lower to the same
`ReactiveDag.Cell`, so `ReactiveDag.assemble/1` builds **one plan from both** —
a graph authored mostly in a `graph do … end` block, with specific nodes broken
out as Ash resources (a typed payload table + a `reduce`/`join` combinator), or
vice-versa.

```elixir
plan =
  ReactiveDag.assemble(
    spine:     [MyApp.Pipeline],                     # `graph do … end` module(s)
    resources: [MyApp.BudgetRollups, MyApp.FlowSeries],
    for_each:  &MyApp.Populations.fetch/1            # generator member-fetcher
  )
```

Cells merge **by id**. A resource **overrides** a spine node of the same id (the
resource is the more specific definition) — which is exactly the "graft a resource
cell over the DSL cell" pattern cascade hand-rolled, now a first-class call. Both
`:spine` and `:resources` default to `[]`, so `assemble/1` is the superset of
`ReactiveDag.Dsl.Spine.Info.plan/1` and `ReactiveDag.Node.graph/2`.

Status: **both hosts fully migrated** onto the substrate — the shared engine
spans a per-key Elixir recompute (cascade) and a set-based SQL recompute (the
portal), all coordination writes routed through the seam, proven by both suites
green. Cascade additionally authors several ops via the `Node` `reduce`/`join`/
`expand` combinators. Consumed today as a `path:` dep by each app; publish/pin is
the remaining step. See
[docs/adr-001-reactive-dag-library.md](docs/adr-001-reactive-dag-library.md)
for the boundary, the seams, and the design law behind them.

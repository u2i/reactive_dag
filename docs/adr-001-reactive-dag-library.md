# ADR-001 — Extract the reactive-DAG engine as `reactive_dag`

**Status:** proposed (skeleton built; neither host migrated yet)
**Date:** 2026-08-05

## Context

Two Elixir/Ash apps independently grew the same reactive-DAG engine:

- **cascade** (`~/dev/cascade`) — the Red Hook data pipeline. Ops are a
  data-pipeline algebra (`map/fold/join/project/union/assemble/sink`); recompute
  is **per-key Elixir** (each op a behaviour module that may call an LLM, parse a
  PDF, hit Tigris). Frontier `cascade_dirty`, ledger `cascade_tuple`.
- **u2i-compliance-portal** (`~/dev/u2i_compliance/u2i-compliance-portal`) — the
  compliance model_eval. Ops are a set-combination algebra
  (`product/relation/fold/bind/reconcile/analysis/union`); recompute is
  **set-based SQL** (one join per (cell, dirty-keys) writing `model_tuple`).
  Frontier `model_dirty`.

They share a **skeleton** and diverge on the **algebra** and the **recompute
model**. This ADR extracts the shared skeleton as a standalone library both
depend on, with clean seams at the two divergence points.

Both apps are on aligned toolchains — `ash ~> 3.5` (3.27 / 3.31), `ash_postgres
~> 2.0`, `spark 2.7.2` (identical) — so the library can be an **Ash extension**
that *owns* the substrate tables and (eventually) the Spark DSL, rather than an
ORM-agnostic core that reimplements storage per host.

## Decision

Extract `reactive_dag`: the reactive-DAG **substrate** + a **pluggable
op-algebra layer**. A new standalone repo; cascade and the portal both add it as
a dependency.

### What the library owns (shared)

| Concern | Module | Notes |
|---|---|---|
| Node IR | `ReactiveDag.Cell` | `op` is a **free atom** — the library never interprets it. `meta` carries app data. |
| Compiled plan | `ReactiveDag.Plan` | pure data: `cells / parents / depths`. |
| Graph build | `ReactiveDag.Graph` | `build/1` (validate + parents + longest-path depths, cycle-checked); `dirty_parents/4` (propagation via the host KeyRule). |
| Dirty frontier | `ReactiveDag.Frontier` | `reactive_dag_dirty` table; `mark_dirty / next_cell / claim / empty?`; claim-as-delete. Host supplies its repo via config. |
| Drain loop | `ReactiveDag.Drain` | depth-ordered incremental propagation; `run/2` parameterized by the two seams + an `:on_step` trace hook. |
| Substrate migration | `ReactiveDag.Migration` (todo) | creates `reactive_dag_dirty` (+ optionally a generic tuple ledger). |
| Spark DSL extension | `ReactiveDag.Dsl` (todo, phase 2) | the shared `source/observed/derive/ref/compose/target` vocabulary lowering to `Cell`s. |

### The two seams (stay app-side)

1. **`ReactiveDag.RecomputeStrategy`** — `recompute(cell, keys) -> {:ok, changed}`.
   *How* a cell recomputes. cascade dispatches to `cell.meta.compute` (Elixir per
   key); the portal dispatches by `cell.op` to a SQL template. The substrate
   only decides *when/order*, never *how* — this is the SQL-vs-Elixir split.

2. **`ReactiveDag.KeyRule`** — `rule(parent, child, changed) -> :all | {:keys, mapped}`.
   *How* a change propagates. cascade's uniform `:identity | :all` field is the
   trivial case; the portal's per-op, per-which-input rules (product/relation
   escalate to `:all` when their *fn* leg changed, pass keys when their *members*
   leg changed) are expressible because the callback sees which child changed.
   `KeyRule.identity/3` is the default.

The **tuple ledger** (materialized values) is deliberately NOT in the library:
its storage is exactly what differs (per-key Elixir writes vs set-based SQL), so
it stays owned by each host's RecomputeStrategy.

## Why not more, why not less

- **Not less** (bare graph, host does storage): both hosts are AshPostgres, so
  making them each reimplement the frontier is needless — the library owning it
  removes real duplication (cascade's `Frontier` + the portal's `model_dirty`
  access are ~the same raw SQL).
- **Not more** (unify the algebra + recompute into the library): the algebra and
  recompute model are precisely where the apps diverge on purpose. Forcing one
  algebra would either cripple cascade's LLM-per-key ops or cripple the portal's
  set-based scale. The seams keep both first-class.

## Migration cost

**cascade** (low — it was built as this generalization):
- Replace `Cascade.Engine.{Cell,Plan,Graph,Frontier,Drain}` with `ReactiveDag.*`.
- `Cascade.Engine.Op` → implement `ReactiveDag.RecomputeStrategy` (a thin
  dispatcher to `cell.meta.compute`); the existing per-op modules are unchanged.
- Cascade's `key_rule` field → a small `KeyRule` module (`:identity`→`{:keys}`,
  `:all`→`:all`). Move the field into `cell.meta`.
- Rename `cascade_dirty` → `reactive_dag_dirty` (or alias). Keep `cascade_tuple`
  (host-owned ledger).
- Trace/PullDetail/scans stay in cascade via the `:on_step` hook.

**portal** (moderate):
- `ModelEval.Graph.build/1` currently takes a **module**; split into
  "lower DSL → cells" (stays portal-side, phase 1) + `ReactiveDag.Graph.build(cells)`.
- `ModelEval.Recompute` SQL templates → wrap behind a `RecomputeStrategy` that
  dispatches by `cell.op`.
- The per-op `dirty_parents` rules → a `KeyRule` module.
- `model_dirty` → `reactive_dag_dirty`; `model_tuple` stays portal-owned.

## Phasing

1. **Phase 0 (this ADR):** skeleton repo with the interfaces (done; compiles).
2. **Phase 1:** migrate **cascade** onto `reactive_dag` (path dep), prove the
   full suite green. De-risks the seam on the app that fits most naturally.
3. **Phase 2:** lift the Spark DSL into the library; migrate the portal.
4. **Phase 3:** publish (hex or git), pin both apps.

## Open questions

- Should the library own a **generic tuple ledger** for hosts that want one, or
  stay frontier-only? (Cascade has its own `cascade_tuple` shape; the portal
  joins to first-class tables. Leaning frontier-only + optional ledger helper.)
- DSL unification (phase 2): cascade's `derive/ref/compose` vs the portal's
  `guarantee/control/register/op` — is there one vocabulary, or does the library
  provide primitives each app skins? (Likely primitives + per-app skin.)

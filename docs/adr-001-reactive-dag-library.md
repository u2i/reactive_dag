# ADR-001 — Extract the reactive-DAG engine as `reactive_dag`

**Status:** accepted & implemented — both hosts fully migrated (path deps); the
substrate, coordination tuple, reconcile, DSL lowering, and drain loop are all
shared, both suites green. Only publish/pin (phase 3) remains.
**Date:** 2026-08-05 (proposed); implemented same cycle.

> This ADR keeps its decision-time framing. Where the build went beyond the
> original proposal (the tuple ledger DID move into the library as a *spine*;
> the DSL WAS lifted via `Lowering`/`Dsl`), the sections below are annotated
> with what actually shipped rather than rewritten.

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
| Coordination tuple | `ReactiveDag.Tuple` ✅ | the shared `(cell_id, key, status, freshness)` **spine** over the host's tuple table (`put / present_keys / all_keys / keys_by_status / status_histogram / max_observed_at / reconcile` + a `:key_scope` selector). See "what shipped beyond the proposal". |
| Nested-expr lowering | `ReactiveDag.Lowering` ✅ | `walk/3` — the nested op-expr → flat-cell recursion both DSLs grew, via host callbacks. |
| Compile pipeline | `ReactiveDag.Dsl` ✅ | `compile / validate_cells` — resolve → structural-validate with a domain hook. |

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

The **tuple ledger** was originally scoped OUT of the library. What actually
shipped is more precise (see "what shipped beyond the proposal"): the *spine* —
`(cell_id, key, status, freshness)` + the read/reconcile operators over it — IS
shared (`ReactiveDag.Tuple`), because both hosts had the identical spine spelled
two ways. What stays host-side is the *payload* (each host's typed resources,
joined by `key`) and the *extension columns* (the portal's `strength` modality,
cascade's tombstone/fingerprint policy). The write executor is still the
RecomputeStrategy seam.

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

## Phasing — as executed

1. **Phase 0:** skeleton repo + interfaces. ✅
2. **Phase 1:** cascade migrated (Frontier/Graph/Drain delegated), full suite
   green. ✅
3. **Phase 2:** portal migrated onto the shared Drain (proving the boundary
   spans set-based SQL recompute too); DSL refs unified **by-name** in both apps;
   `Lowering.walk` + `Dsl` extracted and adopted by both. ✅
4. **Beyond the original phase 2** (the shared substrate turned out deeper than
   just frontier + graph): the coordination-tuple **spine** (`ReactiveDag.Tuple`)
   + the leaf-`reconcile` skeleton + spine reads with a `:key_scope` selector;
   and the Drain loop enriched (`on_step` carries `triggered_by` + `duration_us`)
   so cascade dropped its bespoke trace loop and the portal lowers through
   `walk`. ✅
5. **Phase 3 (remaining):** publish (hex or a git tag) and pin both apps off the
   `path:` deps.

## What shipped beyond the proposal

The proposal scoped the library at frontier + graph + two seams, with the tuple
ledger and DSL left host-side. Migrating both apps revealed the shared surface
was larger, and in exactly the places the proposal was unsure about:

- **The coordination tuple is shared (spine), not host-only.** Both hosts had a
  thin `(cell_id, key, status, freshness)` row keyed identically — cascade's
  `cascade_tuple`, the portal's `model_tuple` — plus a typed *payload* resource
  joined by `key`. The spine + its read/reconcile operators became
  `ReactiveDag.Tuple` (host owns the physical table via
  `config :reactive_dag, tuple_table:`, extension columns and all). This is the
  resolution of the first open question: **not** frontier-only; the spine is a
  genuine shared layer, the payload and extension columns are not.
- **The DSL lowering is shared (primitives), not per-app.** Resolving the second
  open question: refs are **by-name** in both apps, and the nested-expr → flat-
  cell recursion is one `Lowering.walk` primitive both DSLs call with their own
  callbacks. Each app keeps its *vocabulary* (`source/dataset/target` vs
  `guarantee/control/register`) as a thin skin — **primitives + per-app skin**,
  as guessed.
- **`reconcile/3` unified the leaf-write algorithm.** The portal's ~12 leaf
  drivers and cascade's `refresh_leaf` were the same skeleton (upsert a desired
  set, retire the vanished); the one real divergence — delete vs tombstone — is
  now a one-line `:retire` seam.

## The design law behind the seams

Why `RecomputeStrategy` is a seam and not shared code, stated as the principle we
converged on: **the reactive substrate is shared because *scheduling* is
purpose-neutral; recompute is not shared because it encodes the app's *purpose*.**
The portal is a *proof engine* — every node's codomain collapses to a status
lattice (`present/failing` × strength), which is exactly what set-based SQL is
total over. Cascade is a *data pipeline* — nodes produce open-typed values (a
dollar variance, an LLM-extracted record), which needs the BEAM. Purpose →
codomain (status-lattice vs open value) → executor (SQL vs BEAM). The one
domain difference that is *not* an executor choice is **modality** (`strength` —
how well-established a fact is), which the portal computes and cascade has no use
for; it rides as a tuple extension column, never forced onto cascade.

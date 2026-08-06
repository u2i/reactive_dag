# ADR-002 — A unified authoring DSL, with domain vocabulary as extensions

**Status:** proposed (design only — no code yet). Supersedes the ADR-001 stance
that "each host keeps its own graph-level Spark DSL; only the compile pipeline is
shared."
**Date:** 2026-08-06

## Context

ADR-001 extracted the engine, the coordination tuple, the recompute seams, and the
DSL *compile pipeline* (`ReactiveDag.Dsl.compile/2` with `lowering` + `validate`
hooks). It deliberately left the **Spark authoring entities** app-side: cascade
authors in `Cascade.Dsl` (`pipeline do source…; observed…; dataset… end`), the
portal in `U2iPortal.Model` (`sources do…; values do…; guarantees do… end`). Both
lower to the same `Cell` IR via the shared pipeline.

Building "scanners as first-class nodes" in the portal exposed the cost of that
seam: a `source` concept that both apps had **independently converged on** (same
`id`/`leaf_cell`/`poll→changed-keys` contract — cascade's own moduledoc says it
"mirrors the portal's Source contract") had **nowhere shared to live**. Each app
re-declared the `source` entity, the `observed.fed_by`/`observed.source` edge, and
the leaf↔source validation. The duplication is not incidental — it is the
predictable result of two DSLs expressing the same graph substrate.

Inventory of the two authoring vocabularies:

| Entity | cascade | portal | kind |
|---|---|---|---|
| `source` | ✓ | ✓ | **spine** (read the substrate) |
| `observed` | ✓ | ✓ | **spine** |
| `ref` | ✓ | ✓ | **spine** (by-name edge) |
| `compose` | ✓ | ✓ | **spine** (anonymous sub-op) |
| `dataset`/`target`/`sink` | ✓ | — | pipeline domain |
| `declared`/`workflow`/`op` | — | ✓ | compliance domain |
| `guarantee`/`control`/`register`/`value`/`scenario`/`realized_by` | — | ✓ | compliance domain |

Four spine entities are shared; the rest is domain. The compile pipeline is
already shared. **The gap is the authoring surface.**

## Decision

Adopt **one canonical `ReactiveDag` graph DSL** that both apps author in. Domain
vocabulary is expressed through **extension points** — registered op-kinds + typed
meta — not as parallel bespoke entity sets per app. cascade's `dataset` and the
portal's `guarantee` both become **the same `node`/op-expression entity** carrying
a domain op-kind and domain meta; the domain-specific *validation* and *lowering*
plug in through the hooks that `compile/2` already exposes.

### Shape

```
# the ONE shared authoring DSL (reactive_dag)
graph do
  source :app_scan, driver: Drivers.AppScan          # spine: first-class
  node :catalog, op: :product, source: :app_scan      # a node; op-kind is open
  node :g_store_encrypted, op: :guarantee,            # "guarantee" is an OP-KIND,
       inputs: [:catalog],                            #   not a bespoke entity
       meta: [claim: "…", addresses: [:C1_1], shape: :delta]
end
```

- **`source` / `observed` / `ref` / `compose`** — first-class in the lib DSL. One
  definition. `source`'s `driver:` is typed `{:behaviour, ReactiveDag.Source}`.
- **op-kind** (`:product` / `:guarantee` / `:fold` / `:dataset`…) — an **open
  atom**, not a closed `{:one_of}`. The lib validates graph structure (inputs
  resolve, acyclic, source↔leaf); the *host* validates op-kind semantics via the
  `validate` hook and lowers op meaning via the `lowering` hook.
- **domain fields** (`claim`, `addresses`, `conformance`, `dataset`'s `compute`) —
  ride in `meta:`.

## The hard tension (why this ADR exists rather than a patch)

The portal's `guarantee` entity is **not** a thin wrapper. It carries **9+ typed,
validated fields**: `claim` (required string), `addresses` (`{:list, :atom}`,
cross-checked against real control ids), `shape` (`{:one_of, @shapes}`),
`population` (`{:one_of, [:sites,:events,:dated]}`), `conformance` (keyword bars),
`for_each` (generator directive), `rests_on`/`depends_on` (assurance-ladder id
lists, cross-checked), `over`, `layer`. cascade's `dataset` carries `compute`
(a `{:behaviour, Op}` module), `key_rule`, `grain`.

Reducing these to "`meta:` — an untyped blob" **loses two things that matter**:

1. **Per-field compile-time validation.** Today `shape: :dleta` (typo) fails at
   compile via the Spark `{:one_of}`. In a meta blob it fails at runtime, or never.
   `addresses: [:CC6_1]` is checked against declared controls in `Resolve`. This is
   the SAME class of guarantee the scanner work just *added* (fed_by resolves) — we
   would be regressing it for every other domain field.
2. **Authoring ergonomics.** `guarantee :x do claim "…"; addresses [:CC6] end` reads
   as compliance; `node :x, op: :guarantee, meta: [claim: "…", addresses: [:CC6]]`
   reads as graph plumbing — the exact readability the portal's DSL was built to
   preserve (ADR-001 kept the domain skin for this reason).

**So "meta blob" is the wrong reading of "domain via extension points."** The
right reading: the lib provides the spine + a **domain-entity registration
mechanism**, so a host declares `guarantee` as a *typed extension entity* (keeping
its 9 field schemas) that the lib's DSL **composes in** and routes through the
shared lowering/validate hooks. The host still writes the field schemas (they're
domain — only the host knows `shape` has those five values); the lib owns the
*graph structure, the spine entities, and the composition*.

This is the difference between:

- ❌ **flatten** domain entities into `node + meta` (loses typing/ergonomics), and
- ✅ **compose** host domain entities onto a shared spine via a Spark DSL fragment
  + a registration hook (keeps typing; unifies structure).

The unification is REAL (one spine, one `source`, one compile path, one graph
grammar) without pretending compliance vocabulary and pipeline vocabulary are the
same thing — which they are not.

## Mechanism

Spark supports this directly via **DSL fragments / extension composition**:

1. **`ReactiveDag.Dsl.Spine`** — a `Spark.Dsl.Extension` fragment defining the
   `graph`/`pipeline` section with the four spine entities (`source`, `observed`,
   `ref`, `compose`) + the `ReactiveDag.Source` behaviour + `fed_by`/leaf verify.
2. **Host extends it.** `U2iPortal.Model.Dsl` and `Cascade.Dsl` each `use
   Spark.Dsl.Extension` composing the spine and **adding their own domain
   entities** into the same section's `entities:` list — `guarantee`/`control`/…
   keep their full typed schemas. Spark merges the entity lists; the section is one.
3. **Lowering/validate unchanged.** Domain entities still lower + validate through
   the ADR-001 hooks. Nothing about the recompute seams or the tuple changes.

Net: `source`/`observed`/`ref`/`compose` are DEFINED ONCE (the spine); domain
entities stay typed and app-side but now plug into a shared grammar instead of a
parallel one. `source` finally has its shared home — the original itch.

## Consequences

- **Both apps re-point their DSL at the spine.** Mechanical for the 4 spine
  entities (delete the local copies, compose the fragment); domain entities are
  untouched except for the section they attach to. Two green suites are the gate.
- **`ReactiveDag.Source` becomes the 4th seam** (alongside `RecomputeStrategy`,
  `KeyRule`, `CoordinationWriter`): the behaviour + generic `verify/2` live in the
  lib; `poll`'s fetch + app UI extras (`label`/`origin`) stay host-side.
- **Bundle with the `Node → Reactive` rename** (the deferred v0.2.0 item) — both
  are breaking DSL changes; ship them as one v0.2.0 so consumers absorb one break.
- **Reverses ADR-001's "keep DSLs app-side."** Justified: ADR-001 correctly kept
  the *domain vocabulary* app-side, but overshot by keeping the *spine* app-side
  too. This ADR splits that hair: spine shared, domain composed.
- **Risk:** if the domain-entity registration mechanism ends up needing per-host
  escape hatches for lowering order or section nesting, the "one grammar" claim
  weakens. Mitigation: prototype the portal's `guarantee` (the richest entity) on
  the spine FIRST; if it composes cleanly with full typing, the rest follow.

## Rollout

1. Build `ReactiveDag.Dsl.Spine` (4 entities + Source behaviour + verify) in the lib.
2. **Prototype-gate:** compose the portal's `guarantee` (9 typed fields) onto the
   spine in a lib spike; prove per-field validation + lowering survive. If not,
   fall back to ADR-002-B ("shared spine fragment, domain stays fully separate" —
   the smaller option) and stop.
3. Re-point `Cascade.Dsl` (path dep — moves with the lib).
4. Re-point `U2iPortal.Model.Dsl`; bump the portal's pin `v0.1.0 → v0.2.0`.
5. Tag `reactive_dag v0.2.0` (spine DSL + `Node→Reactive` rename together).

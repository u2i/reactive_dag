# Attestations

Some facts no scanner can establish. *Whose is this machine?* — Fleet can see
which account is signed in (evidence of use), but assignment is something only
the holder can assert. *Is this list still current?* — editing it on the 3rd
does not mean anyone confirmed the other 39 entries that day. A human signing
off on data is one of the most common shapes in a compliance- or
correctness-oriented graph, and `ReactiveDag.Attestation` makes it a substrate
concern: records, content-addressed bases, eligibility, quorum, and gated
edges, with propagation identical to any other input.

## The model in five words each

- **Record** — an immutable fact: *who* affirmed or rejected *what scope* of a
  cell's data, *what it looked like* (a content digest), *when*, and — for a
  rejection — *why*. Never updated, never deleted.
- **Stance** — a signer's latest record for a scope. History stays; only the
  latest word per signer has force.
- **Scope** — what was signed: one row (`{:key, k}`) or the set a filter
  selects (`{:filter, key_scope}`).
- **Basis** — a digest of the rows the scope selected at signing. The
  signature binds to *what was there*, not to the key.
- **Force** — whether a stance currently counts: a read-time predicate, never
  a stored status.

## Declaring a requirement

Policy is declared **once**, on the node that owns the raw data:

```elixir
defmodule MyApp.Machines do
  use Ash.Resource, data_layer: Ash.DataLayer.Simple, extensions: [ReactiveDag.Node]

  reactive do
    op :source
    leaf? true

    attestation :machine_ownership do
      signers :machine_holders          # an ELIGIBILITY CELL's id — see below
      join &MyApp.Policy.holder?/2      # (scope, eligibility_key) -> who | nil
      quorum :any                       # :any | :all | {:n_of, k}
      tolerance days: 180               # how long a signature holds
    end
  end
end
```

`signers` names a **cell**, not a list and not a callback. Who may sign is
data — the machine's holder, the members of a role — and that data changes. As
a cell, eligibility is a real input edge of every attested view: a role
revocation propagates through the drain like any other change, and the lineage
of a green verdict shows *the roles data* among the things it rests on. (An
opaque callback would hide exactly that join.)

`join` interprets the eligibility cell's key grammar, which is yours: given a
scope and one eligibility key, it returns the identity licensed to sign, or
nil.

## Consuming it: two spellings, one shape

**A declared attested view** — both cells exist in the graph, the raw list and
the signed list, and a consumer picks per edge:

```elixir
defmodule MyApp.ConfirmedMachines do
  use Ash.Resource, data_layer: Ash.DataLayer.Simple, extensions: [ReactiveDag.Node]

  reactive do
    id :confirmed_machines
    attested over: :machines, requirement: :machine_ownership
  end
end
```

**A gated edge** — signing as a property of the *dependency*:

```elixir
reactive do
  id :ownership_verdict
  verdict? true
  op :reconcile
  compute MyApp.Reconcile
  depends_on [:machines, {:machines, gate: :machine_ownership}]
  #            ^ denominator: raw     ^ numerator: signed rows only
end
```

`gate:` is sugar, not a second mechanism: assembly interposes an anonymous
attested cell (id `machines@machine_ownership`) between the raw cell and the
consumer — the same shape a declared `attested` node lowers to, deduped across
consumers. The Cell IR is unchanged; the graph, drain, and lineage see ordinary
cells with `inputs: [raw, eligibility, store]`.

### The denominator is never gated

Data that "requires attestation" must still flow *somewhere*. If every leg of a
verdict consumes through the gate, an unsigned machine is simply **absent** —
and the guarantee goes vacuously green, having swallowed its own denominator.
Assembly enforces this: a verdict cell whose every evidence path passes through
one requirement's *blocking* gate **raises at graph build** as structurally
vacuous. Keep one leg on the raw cell; the join between the legs is what makes
the shortfall visible.

### Non-blocking: best effort, distinguished

Not every consumer should *withhold* unsigned data — a report, a metric, a
downstream computation often wants the best available value while keeping
signed and unsigned distinguishable. That is a **mode** of the view, declared
where it is consumed:

```elixir
ref :machines, gate: :machine_ownership                    # blocking (default)
ref :machines, gate: :machine_ownership, mode: :annotate   # non-blocking
depends_on [{:machines, gate: :machine_ownership, mode: :annotate}]
attested over: :machines, requirement: :machine_ownership, mode: :annotate
```

Force evaluation is **identical** in both modes — who signed, whether it still
counts, the three lapse predicates. The mode changes only what a
not-yet-signed row projects to: `:require` writes it as `pending` (withheld
from consumers of the signed set); `:annotate` writes it as `unsigned` — it
flows, best effort, and stays distinguishable from `covered`.

Two consequences:

- **A rejection bites in both modes.** Unsigned means nobody has vouched;
  refused means someone said the data is *wrong* — passing that through as
  best-effort would launder the objection. `refused` stays `refused`.
- **The vacuity lint ignores annotate views.** They withhold nothing, so they
  cannot swallow a denominator; an all-annotate-gated verdict is legitimate.

The two modes are two projections, so a graph consuming both gets two
interposed cells (`machines@machine_ownership` and
`machines@machine_ownership~annotate`) over the same records — signing once
moves both.

## What the view computes

For each row of the raw cell, an **admission**, projected per the view's mode:

| state | `:require` writes | `:annotate` writes | meaning |
|---|---|---|---|
| affirmed | `covered` | `covered` | in-force affirmations meet the quorum |
| pending | `pending` | `unsigned` | nobody has signed — or every signature lapsed |
| refused | `refused` | `refused` | an in-force rejection exists |

The status vocabulary is overridable per requirement (`statuses:`); the
defaults compose with `ReactiveDag.Verdict` unchanged. Affirmed rows are put
with `strength: "attested"` in the writer opts — the spine-only default writer
drops it; a host writer with a strength column stamps it.

### Force: three ways a signature lapses

A record is immutable history; whether it *counts* is evaluated at read time,
and can fail three independent ways:

| lapse | meaning | remedy |
|---|---|---|
| `:basis` | the **world** moved — what was signed is not what is there | re-present, re-ask |
| `:tolerance` | **time** passed — the assertion aged out | re-affirm |
| `:eligibility` | **authority** moved — the licence to sign was withdrawn | a different signer |

All three read as *pending* — never as green, never silently as rejected — but
the evaluation names the failed predicate per lapsed signer, because a UI must
say which remedy it is asking for.

The basis is what makes this self-maintaining. It digests the selected rows'
`(key, status)` at signing; at evaluation the digest is recomputed from current
data. The world moving lapses coverage with **no revocation bookkeeping**: a
serial reused by a rebuilt host, a corrected row, a filter selecting a new
member — all simply stop matching.

### Withdrawal: clearing your word

Between affirming and rejecting sits a third act: **withdraw** — "I no longer
vouch" (handing the machine back, leaving the team), with no claim that
anything is wrong. `Attestation.withdraw/4` supersedes the signer's stance
like any record but carries **no force in either direction**: the scope
returns to *pending* (unaffirmed, re-askable), never to refused, and other
signers' affirmations are untouched. Its reason is optional — withdrawing
asserts nothing about the data, so nothing must be explained.

### Rejection is sticky, and reasoned

A rejection **requires a reason** (`reject/5` raises without one): an
affirmation asserts the data as presented, but a rejection asserts it is
*wrong*, and a bare "no" leaves whoever must act with nothing to fix.

Refused is deliberately not out-voted by other signers' affirmations. Under a
matching basis it stays refused; what clears it is the world changing (the
data is corrected → the rejection's basis lapses like anything else) or the
rejector's own later affirmation (stance = latest record per signer). A system
that re-asks until it gets a yes is laundering attestations, not collecting
them.

### Quorum

`:any` (one in-force affirmation), `:all` (every currently-eligible signer —
dual control), `{:n_of, k}` (four-eyes counting). Quorum is evaluated over the
**currently-eligible** set, and an empty eligible set never affirms:
nobody-may-sign is a gap in the eligibility data, not a satisfied quorum.

## Signing, and how it propagates

```elixir
{:ok, record, changed} =
  ReactiveDag.Attestation.affirm("machines", {:key, "AAA111"}, "alice@u2i.com")

{:ok, record, changed} =
  ReactiveDag.Attestation.reject("machines", {:key, "AAA111"}, "bob@u2i.com",
    "this serial is a rebuild; the machine was recycled")
```

The store surfaces in the graph as **one leaf cell**
(`ReactiveDag.Attestation.leaf_cell/0`, default `"attestations"`, injected
automatically into any graph that uses the vocabulary). Signing is therefore a
leaf write like any other: mark the returned keys dirty on that leaf, drain,
and every attested view downstream re-evaluates — structurally identical to a
scan finishing. Sub-second, and it cannot fail on a vendor being down: it
reads only your own database.

Reads: `stances/1` (latest per scope × signer — what evaluation consumes) and
`history/2` (the full append-only trail — what an auditor consumes).

## Storage: a host-defined Ash resource

Records live in an **Ash resource the host defines** — the same pattern as
`ash_authentication`'s token resource. The `ReactiveDag.Attestation.Record`
extension stamps the required shape (the record attributes, a `:sign` create,
a primary read); the host chooses repo, table, and domain, and the library
reaches the resource via config:

```elixir
defmodule MyApp.Attestation.Record do
  use Ash.Resource,
    domain: MyApp.Attestations,
    data_layer: AshPostgres.DataLayer,
    extensions: [ReactiveDag.Attestation.Record]

  postgres do
    table "attestation_records"
    repo MyApp.Repo
  end

  attestation_record do
    who_from_actor fn actor -> to_string(actor.email) end
  end
end
```

```elixir
config :reactive_dag, attestation_resource: MyApp.Attestation.Record
```

Migrations are generated (`mix ash.codegen add_attestation_records`), not
hand-written. And because it is an ordinary resource of yours, everything Ash
composes onto it:

- **`who_from_actor`** — with an actor on the `:sign` action, the signer is
  **forced from the actor**. Impersonation is prevented at the write, not
  merely discounted at read time by the eligibility check. (`affirm`/`reject`
  pass `actor:` through.)
- **Policies** — signing authorization in the same framework as the rest of
  your app.
- **Notifications** — pub_sub a signing straight into your refresh.

Two invariants are enforced by the extension rather than left to convention:

- **append-only** — a verifier fails compilation if the resource declares any
  update or destroy action. Rows are never mutated; stance is a read; force is
  a predicate; the history is the audit trail.
- **reasoned rejection** — the `:sign` action errors a `"reject"` with a blank
  `reason`, so even writes that bypass `ReactiveDag.Attestation` obey the
  rule.

`basis_version` pins each record to the digest scheme it was signed under, and
an unknown (future) version evaluates as a basis mismatch — *re-ask*, never a
crash. This is what lets the canonicalization evolve without a deploy lapsing
every attestation in the estate.

## Set-level scopes: signing the boundary

`{:filter, key_scope}` signs **the set a filter selects** — which is a
different claim from signing each member. "These are all of my machines" is
about the subset *and its boundary*: a member appearing inside the filter
changes the basis, and the completeness claim lapses, correctly, even though
every previously-signed member is untouched.

The store and the evaluation support filter scopes fully
(`ReactiveDag.Attestation.Evaluation.evaluate_scope/6`); the DSL wiring for
set-level requirements is not yet built — today's `attestation` declarations
are `scope :key`.

## Design rationale

The machinery/policy split follows what is domain-independent: records, bases,
force, quorum counting, and propagation mention no domain; *which* cells
require attestation, *who* is eligible, and the tolerances are irreducibly the
host's. The full design record — including why eligibility must be an edge,
why refusal is sticky, and why the basis is content-addressed — lives in the
host project's ADR-002 (attestation), building on its ADR-001
(freshness/tolerance).

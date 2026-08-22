# ADR-003 — UUID identity, and one kind of cell key

**Status:** proposed. No code written.
**Date:** 2026-08-22

## Context

Three things came together.

**1. External ids are not identity.** A vendor's id — AgendaCenter's
`_07252024-459`, a DocumentCenter doc number — is another system's primary key.
It is not inherent in the data. `_07252024-459` decomposes as MMDDYYYY plus
*their* row counter, and a host already regex-parses the date back out of it. The
date is a fact about the world; the counter is an artifact of their database.

**2. Ash tenancy is unexpressible without a UUID primary key.** Verified by
probe, and this is a hard constraint rather than a preference:

  * Spark refuses a nullable primary key.
  * `Ash.Changeset.for_create` validates required attributes BEFORE `Ash.create`
    applies the tenant (`Ash.Actions.Create.handle_attribute_multitenancy/1`) —
    probed: the changeset carries `tenant: "a"` while the tenant attribute is
    still `nil` at validation time.

  So a tenant attribute can be neither nullable nor non-nullable if it is part
  of the primary key. It cannot be in the primary key at all. With a UUID PK the
  problem dissolves: the tenant is an ordinary column and "unique per tenant"
  becomes an `identity` that `upsert_identity` points at.

**3. A cell key is a reference to rows in another resource.** This is what the
code does with it, not an interpretation: `:identity` propagation is documented
as "a changed input key maps to the same output key (pass through)", and
`key_rule.ex:198` looks changed keys up with
`Ash.Query.do_filter([{source.payload_key, [in: changed]}])`. The key is an
upstream row reference.

### Vocabulary, which this ADR fixes

The library has been overloading one word. After this:

| term | meaning |
|---|---|
| **cell** | a resource, plus the computation it declares. One per resource (`compose` and `for_each` fan out deliberately) |
| **row** | resource + UUID |
| **cell key** | the name of a unit of work — a reference to the upstream rows that unit covers |

## Decision

### UUID primary keys; natural and external ids demoted

Every node's resource gets `uuid_primary_key :id`. What was the primary key
becomes an ordinary column, and uniqueness moves to an `identity`:

```elixir
uuid_primary_key :id
attribute :key, :string                 # was the primary key
attribute :municipality_id, :string     # the tenant, if any

identity :by_tenant_key, [:municipality_id, :key]

create :upsert do
  upsert? true
  upsert_identity :by_tenant_key
end
```

The UUID is a stable internal handle. The natural key is a *lookup*, so refining
what makes two observations "the same thing" does not touch anything that
references the row. That matters concretely: a host's meeting table already
contains two rows sharing `(date, board)`, so the natural key there is not
settled and will need refining.

**External ids become source references, many per row.** An upstream id is kept
because it says *what the upstream system thinks it did*:

  * same vendor id, changed content → upstream revised an existing record
  * new vendor id, same natural key → upstream created a new record for
    something we consider the same thing
  * one natural key, two vendor ids → a question, not a duplicate

Nothing else can distinguish those. So the id is stored as evidence, and never
overwritten — overwriting destroys exactly that signal. Interpretation is the
entity's, not the library's: only the node's own op knows whether two upstream
ids mean "double-filed" or "two sessions". The library records; it does not
adjudicate.

### One kind of cell key

Today a key is one of three things depending on what a node declared:

  * a `"*"` sentinel (whole-cell),
  * an upstream row reference, or
  * a **group label** like `"gf|FY24"`, parsed by the library.

Group labels go. A claim becomes UUIDs (or `"*"`), uniformly.

**Why labels existed:** a label inverts to a query predicate —
`"gf|FY24"` → `WHERE fund = 'gf' AND fiscal_year = 'FY24'`
(`Recompute.group_scope/2`) — so a fold claiming one group reads exactly that
group in one query.

**Why they go anyway.** The label's only unique capability is
`{:group, from: :key}`: deriving a parent's claim by *parsing* a child key rather
than querying for it. Measured — a host graph of 17 nodes uses it **zero** times
(`:all` × 13, `:identity` × 4, `recompute_by` × 0). And the query it avoids is
already being run: `group_claims/3` reads the changed rows to derive group labels
on the propagation path. The label is a cache of a derivation the library
performs anyway, for a fast path nobody uses.

With UUID claims a fold does what it already does, in a different order:

1. read the changed rows by id (`WHERE id IN (…)`)
2. derive their groups from column values (`Enum.group_by`, already present)
3. read those groups whole, because a `sum` needs every row in the group, not
   only the ones that moved

Step 3 is the cost — a second query where a label needed one. Accepted for
uniformity: one kind of key, no grammar, nothing to parse, no `"|"` convention
for a host to collide with.

### How a cell key maps to a row: a declared ladder

A cell key names a unit of work. Writing that unit's row means answering "which
row is this?" — and today the library ANSWERS BY INFERENCE, which is what breaks
under a UUID primary key:

  * `payload_key` names a column, and the key is written into it;
  * a composite primary key means the row IS its identity, so the key is the
    identity serialised in primary-key order;
  * otherwise `derived_payload_key/1` returns the single-attribute primary key —
    which under a UUID PK is `:id`, so the library writes cell keys into the
    UUID column. Silently.

Two fixes were considered and both are worse than declaring the mapping.
*Refuse a UUID PK without an explicit `payload_key`* only guards the ambiguity:
change detection would still look up `WHERE key = …` while the insert conflicts
on `(tenant, key)` — two definitions of "the same row" in one function, which is
exactly the bug that made a second tenant's first write report `:changed` rather
than `:created`. *Read `upsert_identity` off the action* removes that drift but
is still inference: it has nothing to say when an action declares no identity,
or when several exist.

So the mapping becomes a declaration, on the same ladder shape the library uses
for computation — declarative rungs first, an escape hatch last:

| the key is… | rung | the author writes |
|---|---|---|
| the row's own id (often an upstream UUID passed through) | **uuid** | the key maps straight to the PK |
| the values of some columns, looked up | **simple** | the columns that identify the row |
| a judgement about whether two observations are the same thing | **function** | a resolver, one per entity |

**Rung 1 (uuid)** is the pass-through case. Upstream hands us a UUID, or the
node mints one; the key IS the id and no lookup is needed.

**Rung 2 (simple)** is the common case and what the working fixture already
does by hand: `identity :by_tenant_key, [:municipality_id, :key]` plus
`upsert_identity` on the action. Declaring it here means change detection and
the insert use the same fields BY CONSTRUCTION rather than by the author keeping
two places in step.

**Rung 3 (function)** exists because the hard case is real, not hypothetical. A
host's meeting table already holds two rows sharing `(date, board)`, so that
tuple does not identify a meeting — it needs a time, or a dedup rule, and
"is this the same meeting?" is a judgement. Under rungs 1–2 that logic lives in
each op and drifts. As a named resolver it is one place per entity, and it is
where the source-reference story pays off: a new upstream id for something we
already hold is precisely what the resolver adjudicates.

This retires `payload_key` as the mechanism. Rung 1 says what
`payload_key`-plus-single-attribute-PK said; rung 2 says what a composite PK
said. Keeping the key in a column becomes a separate, optional question
("store the unit name for debugging?") rather than the means of finding the row.

#### Open: the resolver's contract

Rungs 1 and 2 are pure — `(cell_key, attrs) -> identity_map` — and need nothing
from the database. Rung 3 may not be: deciding "do I already have a meeting for
this board on this date, within an hour of this time?" means CONSULTING the
stored rows, so the honest return is a row or nil rather than a map.

Those are different contracts, and picking one shapes the feature:

  * `(cell_key, attrs) -> identity_map` keeps all three rungs uniform and pure,
    and cannot express the fuzzy case.
  * `(cell_key, attrs) -> row | nil` expresses it, and makes rung 3 responsible
    for its own query — which the library then cannot scope by tenant, so the
    resolver must be handed the tenant and trusted with it.

Unresolved. Worth deciding before the naming, since the second shape makes the
rung an escape hatch in the `compute Mod` sense — the host takes over and the
library stops checking.

#### Open: the name

`identity` is the obvious word and collides with Ash's own `identities` block,
which this would often be read alongside. `row_key`, `key_maps_to`, `keyed_by`
are candidates. It is authored on every resource, so it is worth getting right.

### What this deliberately does NOT do

`"*"` stays. It is a sentinel compared by string equality
(`drain.ex:282,376`) and never parsed, so it coexists with UUIDs trivially.

## Consequences

**Gained.**

  * Ash `attribute` multitenancy becomes expressible, which is what prompted
    this. A tenanted resource declares it once, in Ash's vocabulary, and the
    library sets the tenant on the changeset and the query — Ash reads the
    attribute name and applies its own `parse_attribute`. The library never
    learns the column name, so a host using a non-identity `parse_attribute`
    still works.
  * One key grammar. `key_prefix`, the `"|"` join convention, and the
    identity-serialisation rules stop being things an author must know.
  * A fixed bug: without tenant scoping, a second tenant's first write returned
    `:changed` rather than `:created` — `existing/4` looked up by key alone,
    found the other tenant's row, and compared against it. A wrong `:changed`
    propagates a change that never happened.

**Costs, stated plainly.**

  * A fold's scoped recompute becomes two queries rather than one.
  * `payload_key` needs rethinking. `derived_payload_key/1` returns the
    single-attribute primary key, which under a UUID PK is `:id` — so the library
    would write cell keys into the UUID column. Silently. Either the verifier
    refuses a UUID PK without an explicit `payload_key`, or the payload loop
    stops keying rows off the cell key and upserts by identity instead. The
    second is more honest: it already exists as `upsert_identity/5`, and a cell
    key identifies *upstream* rows, so writing it into a column here was always a
    conflation.
  * Four parsing sites go or change: `key_rule.ex:298`, `recompute.ex:264,297`,
    `payload.ex:338`.
  * Every host resource changes shape, plus a data migration to mint UUIDs and
    move the old key into a column. Not backward compatible, by decision.

## Open questions

1. **Does the cell key still get stored on the row?** Useful for debugging
   ("which unit produced this row") even when identity does the lookup. A plain
   non-identity column, or dropped? Under the declared ladder this is a separate
   question from finding the row, which is the point.
2. **The resolver's contract, and the declaration's name** — see the two
   subsections above. Both block implementation.
3. **What is a fold's row identity?** `budget_rollups`' unit is
   `(fund, fiscal_year, section, category)`, all already columns. `identity` over
   those with a UUID PK is the natural shape — worth confirming it is what we
   want for derived nodes and not only for leaves.
4. **Sequencing.** UUID PKs and dropping group labels are separable: the first
   unblocks tenancy, the second is uniformity. Ship together or in order?

## Rejected alternatives

  * **Tenant attribute in the primary key.** Impossible — see Context 2. This is
    not a preference, it is a constraint discovered by probe.
  * **Synthetic (name-based) UUIDs.** A UUIDv5 over the natural key is
    derivable without a lookup, which sounds attractive. Rejected: it *is* the
    compound key, encoded, so it inherits the compound key's brittleness —
    refining what identifies a thing changes every id. Everything here writes
    through one repo, so a lookup is always available. Use synthetic ids only
    where something specifically needs a lookup-free id.
  * **Refusing a UUID PK without an explicit `payload_key`.** Guards the
    ambiguity without removing it: the insert would conflict on the identity
    while change detection looked up a single column, so the two could disagree
    — which is the bug that made a second tenant's first write report `:changed`.
    Also ~40 per-resource edits in the adopting host.
  * **Inferring the mapping from the action's `upsert_identity`.** Introspectable
    (`Ash.Resource.Info.action/2` carries it, and `identity/2` resolves the
    fields), so it needs no new DSL and was tempting. Rejected because it is
    still inference: silent when an action declares no identity, ambiguous when
    a resource declares several. This library's own history is against
    inferring what an author can declare.
  * **Keeping group labels for folds.** The uniformity is worth one extra query,
    and the capability labels uniquely provide is unused.

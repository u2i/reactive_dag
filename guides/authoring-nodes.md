# Authoring nodes

A node is an Ash resource with the `ReactiveDag.Node` extension. **The resource
is the node and its own payload table**: the `reactive do … end` block declares
the computation; the resource's `attributes` are the rows it materializes. This
guide covers every shape that block can take.

## The four node shapes

| shape | data_layer | attributes | `reactive` block | result lives in |
|---|---|---|---|---|
| **payload** | AshPostgres/Ets | the payload columns + an `:upsert` action | a combinator, no `upsert:` | the resource itself |
| **verdict** (`verdict? true`) | `Ash.DataLayer.Simple` | none | a combinator; rows carry `:status` | the coordination tuple |
| **write-elsewhere** | Simple | none | a combinator + a custom `upsert:` | wherever `upsert:` writes |
| **escape hatch** | Simple | none | `compute MyOp` | up to the op |

The line between payload and verdict is exactly whether the result fits the
tuple's fixed schema. A verdict node that declares payload attributes raises at
compile time — the attributes would silently never be written.

## Combinators

Declarative combinators cover the common shapes. Each reads its input, computes
the result set, writes it (into the node's own resource by default), and
`Op.put`s **only the changed keys** — so downstream work is proportional to
real change.

### `reduce` — an in-BEAM fold

```elixir
reduce over: :fiscal_lines,
       read: fn :fiscal_lines -> FiscalDoc |> Ash.read!() end,
       group_by: fn line -> {line.fund, line.fy} end,
       key: fn {fund, fy} -> "#{fund}|#{fy}" end,
       into: fn {fund, _fy}, lines -> %{key: ..., fund: fund, total: sum(lines)} end
```

Two variations worth knowing:

- **expand** — `into` may return a **list** of rows (one group → many outputs);
  each returned row must carry its own `:key`. There is no separate `expand`
  entity; it is this list-returning shape of `reduce`.
- **scoped reads** — `read` may be arity-2 (`(over, dirty_keys) -> items`;
  `dirty_keys` is `nil` for whole-cell) to narrow the datastore read to the
  claimed keys. Important for large inputs.

### `join` — a left join (one input, two sides)

```elixir
join over: :observations,
     read: fn _over -> Observation |> Ash.read!() end,
     left: fn item -> item.declared_id end,       # nil = not on this side
     right: fn item -> item.observed_id end,
     key: fn join_key -> join_key end,
     into: fn _key, declared, observed_or_nil -> %{key: ..., status: ...} end
```

One row per **left** key, right side optional — the declared-vs-observed
reconcile shape. A missing right is information (a gap), not an error.

### `aggregate` — a pure-datastore fold

```elixir
aggregate over: :dmr_reports,          # a has_many on THIS resource
          count: :day_count,
          avg: [flow: :avg_flow],
          max: [flow: :peak_flow]
```

Postgres does the `GROUP BY`; **no rows cross into the BEAM**. Only expressible
as a relationship aggregate (the group must be a resource with a relationship
to the input) — for anything else, use `reduce`.

### `compute` — the escape hatch

```elixir
compute MyApp.Ops.EventsExtract   # implements ReactiveDag.Op
```

For anything the combinators can't express: an LLM call, an external fetch, a
bespoke multi-input recompute. The op receives `(cell, dirty_keys)`, reads its
inputs however it likes, writes via `ReactiveDag.Op.put/3`, and returns the
keys that actually changed.

## Input edges

```elixir
ref :transcripts                  # recompute edge: a change dirties this node
reference :people                 # read-as-context: consulted, never triggers
depends_on [:a, :b]               # flat sugar — one ref per id
reduce over: :x, ...              # a combinator's `over:` implies a ref
ref :machines, gate: :ownership   # gated: consume through the attested view
```

**`ref` vs `reference`** is the load-bearing distinction. A `reference` edge is
a real input — validated, depth-ordered so the target settles first, read at
recompute — but the target's changes do **not** re-trigger this node. Use it
when recompute is expensive or non-deterministic and consults mutable context
it shouldn't be re-run by:

```elixir
reactive do
  op :map
  compute MyApp.EnhanceMinutes   # an LLM pass
  ref :transcripts               # a transcript change RE-RUNS the LLM
  reference :people              # a people edit does NOT — the LLM just reads
                                 # current people the next time it runs
end
```

One boundary: a `reference` edge still participates in depth ordering (that is
what guarantees the target settles first), so it cannot form a cycle —
`Graph.build` raises. It reads settled upstream context; it is not a feedback
mechanism.

`gate:` is covered in the [Attestations](attestations.md) guide — it interposes
an attested view on the edge, admitting only signed rows.

## Nested expressions: `compose`

A leg can be an inline anonymous cell rather than a named node — the op-algebra
expression-tree form:

```elixir
reactive do
  id :meeting_shell
  op :union
  compute ShellOp
  ref :agenda_docs

  compose :fold do
    as :projected_meetings         # explicit id; else positional "<parent>/<i>"
    compute ProjectOp
    ref :resolutions
    ref :meeting_events
  end
end
```

Each `compose` lowers to its own intermediate cell (addressable, depth-ordered,
recomputed like any other); `compose` legs nest.

## Cell ids

A node's id defaults to its module's short name, snake-cased
(`MyApp.FlowMonth` → `:flow_month`); override with `id :name`. **This id is the
vocabulary of every edge** — `ref`, `depends_on`, `over:`, and the ids passed
to `graph/2`. When an edge fails to resolve at assembly, it is almost always an
id mismatch.

## Key rules

`key_rule` declares how a child's changed keys map onto this node's recompute:

- `:identity` (default) — child key `k` changed → recompute my key `k`. For
  same-grain pipelines.
- `:all` — any child change → whole-cell recompute. For folds whose output
  grain differs from the input's.

A custom mapping (prefix-remap, expansions) means bringing your own
`ReactiveDag.KeyRule` — see [The seams](seams.md).

## Generators: one sub-tree per member

A node with `for_each:` is a **template**: it builds no cell of its own.
Instead, `graph/2` expands one instance sub-tree per member of a population:

```elixir
reactive do
  id :rule_concern
  for_each :rules                  # a population atom
  op :probe
  compute ProbeOp
  ref :edr_agents
end

plan = ReactiveDag.Node.graph(resources, for_each: fn :rules -> fetch_rules() end)
# → cells "rule_concern.r1", "rule_concern.r2", … each with the member's meta stamped on
```

A member is any map with an `:id` (plus optional `:meta`, merged onto every
instance cell — the per-member stamp, e.g. a probe filter). Without a fetcher,
a generator node is skipped.

## Companion cells

`companion op: …` builds a **two-cell node**: the op-tree roots at
`<id>/<suffix>` and a companion cell at `<id>` is a derived view over it — the
node PLUS a projection of it, both addressable. This is the shape a
three-valued verdict historically needed (all evaluated members in one cell,
only the problem rows in the other).

Note that **first-class coverage** — keeping `covered` rows in the guarantee
cell itself, so green vs never-evaluated falls out of one histogram — makes the
companion unnecessary for that use. Reach for `companion` only when you
genuinely want two addressable views of one computation.

## Assembly

```elixir
plan = ReactiveDag.Node.graph([NodeA, NodeB, ...], for_each: &fetch/1)
```

Assembly is where cross-resource resolution happens: refs are checked against
real cells, ids must be unique, cycles are rejected, attestation requirements
are resolved and their interposed cells manufactured. A broken graph fails
here, with the offending id in the message.

# ADR-002 companion — the unified DSL in practice

Concrete before/after for the two stress cases (a portal `guarantee`, a cascade
`join`/`dataset`), plus the spine + registration mechanism that makes them share a
grammar. These are faithful translations of REAL modules
(`catalog.ex:501`, `pipeline.ex:52+`), not toys.

The whole point of these sketches: prove the **compose** reading (domain entities
stay typed, plug onto a shared spine) reads well, and show exactly what the
literal **flatten** reading (`node op:, meta:`) would cost. Judge the design on
these, not the prose in ADR-002.

---

## 0. The spine (defined ONCE, in the lib)

```elixir
defmodule ReactiveDag.Dsl.Spine do
  @moduledoc "The shared graph grammar: source/observed/ref/compose + the Source seam."
  use Spark.Dsl.Extension.Fragment          # a composable fragment, not a full extension

  @source %Spark.Dsl.Entity{
    name: :source, target: ReactiveDag.Node.Source, args: [:id],
    schema: [
      id:     [type: :atom, required: true],
      driver: [type: {:behaviour, ReactiveDag.Source}, required: true]
    ]
  }

  @observed %Spark.Dsl.Entity{
    name: :observed, target: ReactiveDag.Node.Observed, args: [:id],
    schema: [
      id:     [type: :atom, required: true],
      grain:  [type: :atom],
      strength: [type: :atom, default: :measured],
      fed_by: [type: :atom, doc: "a declared `source :id` — validated at compile"]
    ]
  }

  @ref     %Spark.Dsl.Entity{name: :ref, target: ReactiveDag.Node.Ref, args: [:id], schema: [id: [type: :atom, required: true]]}
  @compose %Spark.Dsl.Entity{name: :compose, target: ReactiveDag.Node.Compose, args: [:op], recursive_as: :legs, schema: [...]}

  # a host EXTENDS this section by adding its own entities to `entities:`.
  # `graph` is the one canonical section both apps author in.
  @graph %Spark.Dsl.Section{
    name: :graph,
    entities: [@source, @observed, @ref, @compose],
    # ↑ hosts append domain entities here via the registration hook (§3)
  }

  spine_section @graph
  spine_verify  ReactiveDag.Source            # fed_by resolves + leaf_cell resolves
end
```

---

## 1. PORTAL — a guarantee (the richest domain entity)

### 1a. Today (`catalog.ex:501`, `U2iPortal.Model`)

```elixir
guarantee :hire_screened do
  claim("every hire is screened BEFORE they start")
  addresses([:CC1_4])
  shape(:queue)

  op :relation do
    label("joiners (Started ∈ [now−w, now+w])")
    grain(:person)
    ref :active_people

    workflow "background screen" do
      grain(:person)
      attest_for("CC1_4")
      attest_kind("background_screen")
      cadence(:none)
    end
  end
end
```

### 1b. After — COMPOSE (recommended): identical authoring, now on the shared spine

```elixir
# U2iPortal.Model still owns `guarantee`/`op`/`workflow` (typed, domain), but they
# attach to the LIB's `graph` section. `ref`/`source`/`observed` are the spine's.
# The author sees NO difference — that's the goal.
graph do
  guarantee :hire_screened do
    claim("every hire is screened BEFORE they start")
    addresses([:CC1_4])            # still {:list,:atom}, still cross-checked in validate hook
    shape(:queue)                  # still {:one_of, @shapes} — typo still fails at COMPILE

    op :relation do
      label("joiners (Started ∈ [now−w, now+w])")
      grain(:person)
      ref :active_people           # ← spine entity (was app-local, now shared)

      workflow "background screen" do
        grain(:person)
        attest_for("CC1_4")
        attest_kind("background_screen")
        cadence(:none)
      end
    end
  end
end
```

**What changed:** the enclosing section is now the lib's `graph`; `ref` is the
spine's `ref`. `guarantee`/`op`/`workflow` keep every typed field. Zero authoring
churn, full validation preserved.

### 1c. After — FLATTEN (what the literal "op-kind + meta" reading costs)

```elixir
graph do
  node :hire_screened,
    op: :guarantee,
    inputs: [:hire_screened_set],
    meta: [                                   # ← everything demoted to an untyped blob
      claim: "every hire is screened BEFORE they start",
      addresses: [:CC1_4],                    # no {:list,:atom}; a typo'd :CC_14 slips through
      shape: :queue                           # no {:one_of}; :qeue compiles, fails at runtime
    ]

  node :hire_screened_set,
    op: :relation,
    inputs: [:active_people, :"hire_screened_set/1"],
    meta: [label: "joiners …", grain: :person]

  node :"hire_screened_set/1",
    op: :workflow, leaf?: true,
    meta: [grain: :person, attest_for: "CC1_4",
           attest_kind: "background_screen", cadence: :none]
end
```

**What flatten loses, concretely:**
- `shape: :qeue` and `addresses: [:CC_14]` no longer fail at compile — the Spark
  `{:one_of}` / `{:list,:atom}` typing is gone. This is the SAME guarantee the
  scanner work just *added* for `fed_by`, thrown away for every other field.
- the nested op-expression (`op :relation do … workflow … end`) is manually
  flattened into three `node`s with hand-written `inputs:` + `/1` sub-ids the
  author now maintains by hand — the DSL's whole job.
- it reads as graph plumbing, not compliance.

→ **Flatten is rejected.** Compose (1b) is the design.

---

## 2. CASCADE — a join dataset (the richest pipeline entity)

### 2a. Today (`pipeline.ex`, `Cascade.Dsl`)

```elixir
dataset :meeting_shell do
  op(:union)
  compute(Ops.Shell)
  ref(:agenda_docs)

  compose :fold do
    as(:projected_meetings)
    compute(Ops.ProjectedMeetings)
    key_rule(:all)
    ref(:resolutions)
    ref(:meeting_events)
  end
end
```

### 2b. After — COMPOSE: `dataset` stays cascade's, on the shared spine

```elixir
graph do
  source :agenda_scan, driver: Sources.Agenda      # spine
  observed :agenda_docs, fed_by: :agenda_scan       # spine (was cascade-local)

  dataset :meeting_shell do                         # cascade's own entity, still typed
    op(:union)
    compute(Ops.Shell)                              # {:behaviour, ReactiveDag.Op} — preserved
    ref(:agenda_docs)                               # spine ref

    compose :fold do                                # spine compose
      as(:projected_meetings)
      compute(Ops.ProjectedMeetings)
      key_rule(:all)
      ref(:resolutions)
      ref(:meeting_events)
    end
  end
end
```

**What changed:** `source`/`observed`/`ref`/`compose` are now the spine's (one
definition, shared with the portal). `dataset` + `compute`/`key_rule` stay
cascade's typed domain entity. Same authoring.

Note the payoff visible here: `observed :agenda_docs, fed_by: :agenda_scan` uses
the SAME spine `observed`+`fed_by` the portal uses — the convergent `source`
concept that started this whole thread now has exactly one definition.

---

## 3. The registration mechanism (how a host adds a typed domain entity)

```elixir
defmodule U2iPortal.Model.Dsl do
  use ReactiveDag.Dsl.Spine,                    # ← get graph + source/observed/ref/compose
    domain_entities: [                          # ← append host entities to the `graph` section
      @guarantee,   # the full 9-field typed entity, UNCHANGED from today
      @op, @declared, @workflow, @control, @register, @value, @scenario
    ],
    lowering: U2iPortal.Model.Lowering,         # ADR-001 hook (classify/legs/to_cell) — unchanged
    validate: &U2iPortal.Model.Resolve.validate/1   # ADR-001 hook (addresses/rests_on cross-checks)
end

defmodule Cascade.Dsl do
  use ReactiveDag.Dsl.Spine,
    domain_entities: [@dataset, @target, @sink],
    lowering: Cascade.Lowering,
    validate: &Cascade.Resolve.validate/1
end
```

- The lib owns the `graph` **section**, the **four spine entities**, the
  **structural checks** (inputs/refs resolve, acyclic, `fed_by`→source,
  `source.leaf_cells`→cell), and the **Source seam**.
- The host supplies its **typed domain entities** (Spark merges them into the
  section's `entities:`) + the two ADR-001 hooks. Nothing about lowering,
  recompute, or the tuple changes.

This is the whole claim in one screen: `source`/`observed`/`ref`/`compose` defined
once; `guarantee` and `dataset` still first-class + typed + app-side; both authored
inside one `graph do … end` grammar routed through one compile path.

---

## 4. What the prototype gate (ADR-002 step 2) must prove

The sketches ASSERT that `@guarantee` (with its `{:one_of}` / `{:list,:atom}` field
schemas + nested `set:`/`extra:` entity lists) can be appended to the spine's
`graph` section via `domain_entities:` and keep:

1. per-field compile-time validation (`shape: :qeue` fails at compile), and
2. nested-entity lowering (`op :relation do … workflow … end` → the right cells).

If Spark's fragment/entity-merge composes that cleanly → build it (§3 is the API).
If it needs per-host escape hatches for section nesting or lowering order → fall
back to ADR-002-B (spine as a shared *fragment* apps `import`, domain sections stay
fully separate) — a smaller win, but `source` still gets one home.

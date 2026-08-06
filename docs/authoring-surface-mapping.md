# Mapping between the two authoring surfaces

`reactive_dag` has two ways to author a node, and they are **information-equivalent
at the cell level** — both lower to the same `ReactiveDag.Cell`, which is the pivot
for translation. To convert either direction: *read the lowered cell, re-emit it in
the other surface.*

- **Resource surface** — `ReactiveDag.Node`: an Ash resource IS one node; its
  `reactive do … end` block is the definition. One module per node.
- **Graph surface** — `ReactiveDag.Dsl.Spine`: one `graph do … end` block declares
  many nodes as `observed` / `node` entities. One module for the whole graph.

`ReactiveDag.assemble/1` merges cells from both, so a node need not commit to one
surface — but the mapping below is what a per-node translation does.

## The pivot: `ReactiveDag.Cell`

Every field of the cell, and which surface expresses it how:

| `Cell` field | Resource (`reactive do`) | Graph (`node`/`observed`) |
|---|---|---|
| `id` (string) | `id :x` (defaults to the module's snake short-name) | `node :x` / `observed :x` (required — no module to default from) |
| `op` (atom) | `op :fold` | `op :fold` (in-block setter) |
| `inputs` ([string]) | `depends_on [:a]` / `dep :a` / `ref :a` / a combinator's `over:` | `ref :a` / nested `compose` / a combinator's `over:` |
| `leaf?` (bool) | `leaf? true` | `observed` ⇒ always `true`; `node`/`compose` ⇒ `leaf? true` |
| `meta.key_rule` | `key_rule :identity` | `key_rule :identity` |
| `meta.compute` | `compute Mod` (entity) | `compute Mod` (entity) |
| `meta.reduce` / `meta.join` | `reduce …` / `join …` (entity) | `reduce …` / `join …` (entity) |
| `meta.grain` / domain fields | `meta grain: …` (+ `observed`'s `grain`/`strength`) | `meta grain: …` (+ `observed`'s `grain`/`strength`) |
| `meta.scan` (leaf's scanner) | `source :id` + `driver Mod` on the leaf | `observed :x, scan: Mod` |
| `meta.resource` | the resource module (auto) | `nil` (no backing resource) |

Everything else a host puts in `meta:` is carried verbatim by both.

## Worked example A → B: a real resource, rewritten as a graph node

This is `Cascade.RedHook.Nodes.BudgetVsActual` — a real `ReactiveDag.Node` resource
with a `join` combinator. Below, the exact same node authored in the graph surface.

**A (resource) — the reactive block that matters (helper fns elided, they don't move):**

```elixir
defmodule Cascade.RedHook.Nodes.BudgetVsActual do
  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false
    resources do allow_unregistered?(true) end
  end

  use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Simple,
    extensions: [ReactiveDag.Node]

  reactive do
    id :budget_vs_actual
    op :fold
    key_rule :all
    join over: :fiscal_lines,
         read:  &__MODULE__.read_lines/1,
         left:  &__MODULE__.budget_key/1,
         right: &__MODULE__.actual_key/1,
         key:   &__MODULE__.cell_key/1,
         into:  &__MODULE__.into/3,
         upsert: &__MODULE__.upsert/2
  end

  def read_lines(:fiscal_lines), do: …   # all the fns stay exactly as-is
  def budget_key(_), do: …
  # …
end
```

**B (graph) — the same node inside a `graph do … end`:**

```elixir
node :budget_vs_actual do
  op :fold
  key_rule :all
  join over: :fiscal_lines,
       read:  &MyPipeline.read_lines/1,
       left:  &MyPipeline.budget_key/1,
       right: &MyPipeline.actual_key/1,
       key:   &MyPipeline.cell_key/1,
       into:  &MyPipeline.into/3,
       upsert: &MyPipeline.upsert/2
end
```

**The mechanical A→B edits (this is the whole recipe):**

1. **Drop the module scaffolding** — `defmodule`, the `Domain`, `use Ash.Resource`,
   `extensions: [ReactiveDag.Node]`. The graph node is not a module.
2. **`id :budget_vs_actual` → `node :budget_vs_actual do`** — the `id` becomes the
   node's positional name (it was optional on the resource, required here).
3. **`op` / `key_rule` / `join` copy VERBATIM** — the `join` entity is the *same
   struct*; nothing inside it changes.
4. **Re-home the fn captures.** `&__MODULE__.read_lines/1` referred to the resource
   module; in the graph the fns live in the graph module (or any module), so
   `__MODULE__` becomes that module. The fn *bodies* don't move or change — only
   where they're defined.
5. **`meta.resource` is lost.** The resource cell had `resource: BudgetVsActual`
   (its payload table, `Shadow.BudgetVsActual`). The graph node is tableless. Here
   it's fine because the payload is written by `upsert` via `Ash.create` on
   `Shadow.BudgetVsActual` directly — the node never reads its *own* resource. If it
   HAD (e.g. `Ash.read!(__MODULE__)`), you'd keep `Shadow.BudgetVsActual` as the
   payload module and point the fns at it. **This is the one judgment call.**

That's it — for a derived node the translation is "unwrap the module, rename `id`
to the node head, re-home the fn captures." The computation is byte-identical.

## Worked example B → A: a real graph node, rewritten as a resource

This is the portal's `hire_screened` guarantee. Its native DSL is `U2iPortal.Model`,
but it lowers to the same cells the spine `node` produces, so here it is in the
spine graph surface (B) and then as a `ReactiveDag.Node` resource (A).

**B (graph) — a guarantee op over a nested relation set with a workflow leaf:**

```elixir
node :g_hire_screened do
  op :guarantee
  meta claim: "every hire is screened BEFORE they start",
       addresses: [:CC1_4],
       shape: :queue

  compose :relation do
    as :"g_hire_screened/set"
    meta grain: :person
    ref :active_people

    compose :leaf do
      as :"g_hire_screened/set/screen"
      leaf? true
      meta grain: :person, attest_kind: "background_screen", cadence: :none
    end
  end
end
```

**A (resource) — the same node + nested set as a resource:**

```elixir
defmodule MyApp.Guarantees.HireScreened do
  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false
    resources do allow_unregistered?(true) end
  end

  use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Simple,
    extensions: [ReactiveDag.Node]

  reactive do
    id :g_hire_screened
    op :guarantee
    meta claim: "every hire is screened BEFORE they start",
         addresses: [:CC1_4],
         shape: :queue

    compose :relation do
      as :"g_hire_screened/set"
      meta grain: :person
      ref :active_people

      compose :leaf do
        as :"g_hire_screened/set/screen"
        leaf? true
        meta grain: :person, attest_kind: "background_screen", cadence: :none
      end
    end
  end
end
```

**The mechanical B→A edits:**

1. **Wrap in a module** — invent a module name (here `MyApp.Guarantees.HireScreened`)
   and the `Domain` + `use Ash.Resource, extensions: [ReactiveDag.Node]` scaffold.
   This is the inverse of A→B step 1, and it's the one thing B→A *adds*.
2. **`node :g_hire_screened do` → `id :g_hire_screened`** inside `reactive do`. The
   node head becomes the explicit `id`.
3. **`op` / `meta` / `compose` copy VERBATIM** — `compose`/`ref`/`leaf?`/`as`/`meta`
   are the same entities; the nested op-expression tree is unchanged.
4. **No scanner here**, so nothing to fold. (If a leaf had `scan: Mod`, it would
   become `source: <id>` + `driver Mod` on that leaf; see below.)

## The leaf case (the only field-shaped difference)

A source-fed leaf is where the two surfaces differ in *shape*, not just wrapping:

```elixir
# B (graph): scanner inlined as one module
observed :machines, grain: :machine, strength: :measured, scan: MyApp.FleetScan

# A (resource): a leaf resource; scanner splits into a source: ID + a driver MODULE
defmodule MyApp.Machines do
  use Ash.Resource, extensions: [ReactiveDag.Node]
  reactive do
    id :machines
    op :leaf
    leaf? true
    source :fleet_scan        # synthesized id (B→A) — use the driver's id/0
    driver MyApp.FleetScan     # the module carries over 1:1
  end
end
```

- **B→A:** `scan: Mod` → `source: <id>` + `driver Mod`. Synthesize the id — the
  driver's `id/0` is the natural choice. Add `op :leaf` + `leaf? true` (an
  `observed` was implicitly a leaf; a resource must say so).
- **A→B:** `source: id` + `driver Mod` → `scan: Mod`. The `source:` id is dropped
  (graph discovery reads the module off `meta.scan`, no id needed).

## The asymmetries (where it is NOT 1:1)

Three fields don't round-trip cleanly — translation must handle them:

1. **`id`.** Resource `id` is optional (defaults to the module's short-name);
   graph `node`/`observed` id is required. Resource→graph: use `ReactiveDag.Node.
   cell_id/1` to get the effective id. Graph→resource: the id becomes the `id :x`
   (and you must invent a module name to host it).

2. **`meta.resource`.** A resource cell carries `resource: MyModule` (used for
   payload table access); a graph cell has `resource: nil`. Graph→resource gains a
   backing module; resource→graph loses it (the node becomes tableless — fine
   unless the recompute reads the resource's own Ash payload).

3. **Scanner binding (`source:`+`driver` ↔ `scan:`).** Resource = id + module;
   graph = module only. See the leaf example above. Also: a graph `observed` is
   *always* a leaf; a resource leaf must say `leaf? true` explicitly.

## Direction summary

- **Resource → Graph:** for each resource, `cell = ReactiveDag.Node.to_cell(res)`;
  emit `observed`/`node` from the cell (leaf? ⇒ `observed`, else `node`), copying
  op/inputs/combinator/meta; drop `meta.resource`; turn `source:`+`driver` into
  `scan:`.
- **Graph → Resource:** for each `observed`/`node` cell, generate a resource module
  named for the id, `reactive do` with the same op/inputs/combinator/meta; add
  `leaf? true` for leaves; turn `scan:` into `source:`+`driver` (synthesize an id).

Because the cell is the shared pivot, a mechanical translator in either direction is
just "lower to `ReactiveDag.Cell`, then render the other surface's syntax from it" —
the only judgment calls are the three asymmetries above (id source, backing module,
scanner id).

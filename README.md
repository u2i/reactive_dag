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
| Node IR | `ReactiveDag.Cell` | domain-neutral node; `op` is an **optional free-atom label** (load-bearing only for an op-dispatching `RecomputeStrategy` like `SetOp`); app fields ride in `meta` (with an `Access` impl so `cell[:field]` reads meta transparently). |
| Compiled plan | `ReactiveDag.Plan` | pure data: `cells / parents / depths`. |
| Graph math | `ReactiveDag.Graph` | `build/1` (validate + parent edges + longest-path depths + cycle check); `dirty_parents/4` (propagation via the host `KeyRule`). |
| Dirty frontier | `ReactiveDag.Frontier` | claim-as-delete over the host's dirty table; `mark_dirty / next_cell / claim / empty?`. |
| Drain loop | `ReactiveDag.Drain` | depth-ordered incremental propagation; `run/2` parameterized by the two seams + an `:on_step` trace hook carrying `triggered_by` + `duration_us`. |
| Coordination tuple | `ReactiveDag.Tuple` | the shared `(cell_id, key, status, freshness)` spine over the host's tuple table: `put / present_keys / all_keys / keys_by_status / status_histogram / reconcile / …` + a `:key_scope` selector. Payload stays in the host's typed resources, joined by `key`. |
| Nested-expr lowering | `ReactiveDag.Lowering` | `walk/3` — the nested op-expression → flat-cell recursion both DSLs grew, parameterized by host callbacks (id grammar, ref resolution, cell construction). |
| Compile pipeline | `ReactiveDag.Dsl` | `compile / validate_cells` — resolve → structural-validate, with a domain-validation hook. |
| Op contract | `ReactiveDag.Op` | the behaviour a cell's compute module implements (`recompute(cell, keys) -> {:ok, changed}`) + the write API ops call (`put / tombstone / delete`, routed to the `CoordinationWriter`). |
| **Node authoring** | `ReactiveDag.Node` | the authoring surface — an **Ash resource extension**: a resource declares its op + dependencies + computation in a `reactive do … end` block. The resource *is* the node **and** its own payload table. `ReactiveDag.Node.graph/2` assembles the `Plan` from the node resources. |
| **Payload loop** | `ReactiveDag.Node.Payload` | writes a combinator's row into the node's own resource (the default; omit `upsert:`). A `verdict? true` node stores nothing of its own — its result is the coordination tuple. |
| **Scanner seam** | `ReactiveDag.Source` | the behaviour a scanner implements (`id / leaf_cells / poll`) — reads external state into a leaf in a *poll* phase outside the drain; `verify!/2` checks every declared leaf resolves to a real cell. |

The host owns its **physical tables** (dirty + tuple, named via config), its
**op algebra**, its **recompute executor**, and any **extension columns** on the
tuple (the portal's `strength` modality, cascade's tombstone/fingerprint
policy). The library owns the spine and the schedule; the domain differences sit
on named seams, not forks.

## Authoring a node

A node is an Ash resource with the `ReactiveDag.Node` extension. **The resource IS
the node and its own payload table** — its `reactive` block is the computation, its
`attributes` are the rows it materializes. The library **closes the payload loop**:
`into` returns a row and the lib writes it into *this* resource — no `upsert:`
needed for the common case.

```elixir
defmodule MyApp.BudgetRollups do
  use Ash.Resource, data_layer: AshPostgres.DataLayer,   # its OWN payload table
    extensions: [ReactiveDag.Node]

  attributes do
    attribute :key, :string, primary_key?: true          # the payload columns
    attribute :fund, :string
    attribute :total, :float
  end
  actions do
    create :upsert do upsert?(true); upsert_identity(:key); accept([:key, :fund, :total]) end
  end

  reactive do
    op :fold
    key_rule :all
    # read → group_by → reduce each group to one row. `into`'s row is written into
    # THIS resource (keyed by :key) by the library; it Op.puts only changed keys.
    reduce over: :fiscal_lines,
           read:     fn :fiscal_lines -> FiscalDoc |> Ash.read!() end,
           group_by: fn line -> {line.fund, line.fy} end,
           key:      fn {fund, fy} -> "#{fund}|#{fy}" end,
           into:     fn {fund, _fy}, lines -> %{key: …, fund: fund, total: sum(lines)} end
  end
end
```

`upsert:` is an **optional override** — supply it only to write somewhere *other*
than the node's own resource (e.g. an existing shadow table). A tableless node
(`data_layer: Ash.DataLayer.Simple`, no attributes) either supplies `upsert:` or
uses the `compute Module` escape hatch.

Declarative combinators cover the common shapes; each writes the result set (into
the node's resource, or a custom `upsert:`) and `Op.put`s only the changed keys:

- **`reduce`** — a fold: read `over` into the BEAM, group, `into` returns one row
  per group. (`into` may instead return a **list** of rows — a group → many-rows
  "expand"; each returned row must carry its own `:key`. There is no separate
  `expand` entity; it's this list-returning shape of `reduce`.)
- **`join`** — a two-input left join: index `over` into `left`/`right` sides, emit
  one row per left key joined to its right (right may be absent).
- **`aggregate`** — a **pure-Ash-query** fold: the datastore groups + aggregates a
  relationship (`avg`/`sum`/`count`/…) in ONE query — no rows cross into the BEAM.
  The node's resource is the group's resource (one row per group); `over` is its `has_many`. Only for
  relationship aggregates (Ash has no arbitrary `GROUP BY … → rows`); use `reduce`
  for in-BEAM folds. Example: `aggregate over: :readings, avg: [flow: :avg_flow], count: :day_count`.

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
    recompute: ReactiveDag.Node.Recompute,   # runs reduce/join/aggregate or compute:
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

## Verdict nodes (no payload of their own)

A node whose computed result fits the coordination tuple — a status (and, if the
host extends the tuple, a strength) — needs **no payload table**. Mark it `verdict
true`: its `reduce`/`join` rows carry `:status`/`:strength`, which the library
writes straight into the tuple via `Op.put`. No `data_layer`, no attributes, no
`upsert:`.

```elixir
defmodule MyApp.StoreEncrypted do
  use Ash.Resource, data_layer: Ash.DataLayer.Simple, extensions: [ReactiveDag.Node]

  reactive do
    op :reconcile
    key_rule :all
    verdict? true                       # result lives in the tuple, not a table
    reduce over: :stores,
           read: …, group_by: …, key: …,
           into: fn store, [r | _] -> %{key: store, status: (if r.enc, do: "present", else: "failing")} end
  end
end
```

This is the "purely calculated" node: it computes a verdict per key and persists
nothing beyond the coordination row. A **payload-bearing** node (above) computes a
typed value that doesn't fit the tuple, so it materializes rows into its own
resource. The line between them is exactly whether the result fits the tuple's
fixed schema.

## Human input — the command frontier

Scanners feed leaves out-of-band; but a **human** edit (a managed list, an
approval) must enter the graph *in order* and *atomically with its consequences*.
That's `ReactiveDag.Commands` — a second frontier, for INTENTS instead of dirty
keys. It's the drain pattern one layer up:

- a human-managed list / an approval is a **leaf** a command's executor writes;
- a **command** is the ordered, transactional intent to write it;
- the processor claims commands in `seq` order (**serialized** — no interleaving),
  runs each via its host `ReactiveDag.CommandExecutor` (dispatched by `kind`),
  and on success kicks the model drain — so the leaf write and its downstream
  propagation happen in one pass.

```elixir
# a host executor: apply one intent (write leaves), return an outcome
defmodule MyApp.ApproveExec do
  @behaviour ReactiveDag.CommandExecutor
  def execute(cmd, _ctx) do
    # … write the approval leaf via Op.put / an Ash upsert …
    {:done, %{approved: cmd["payload"]["thing"]}}
  end
end

config :reactive_dag, command_executors: %{"approve" => MyApp.ApproveExec}

ReactiveDag.Commands.enqueue!(%{kind: "approve", scope: "app-7", payload: %{"thing" => "x"}})
ReactiveDag.Commands.run(on_settled: fn _cmd, _r -> MyApp.kick_drain() end)
```

**Human-in-the-loop is first-class.** An executor that returns `{:blocked, needs}`
parks the command as a pending question and **freezes its scope** — later
same-scope commands wait rather than racing ahead — without stranding the queue.
The answer arrives as another command (freeze-exempt, `answers_id` back-pointing)
that settles it and thaws the scope. `{:error, _}` is contained the same way (one
bad command freezes only its scope). Storage is a seam
(`ReactiveDag.Commands.Store`, default Postgres `seq`-ordered + `FOR UPDATE SKIP
LOCKED`); the Oban worker that triggers `run/1` stays host-side, like the drain's.

Status: **both hosts run on the substrate** — the shared engine spans a per-key
Elixir recompute (cascade) and a set-based SQL recompute (the portal), all
coordination writes routed through the seam, proven by both suites green. Cascade
authors several ops via the `Node` `reduce`/`join` combinators. Consumed today as
a `path:` dep by each app; publish/pin is the remaining step. See
[docs/adr-001-reactive-dag-library.md](docs/adr-001-reactive-dag-library.md)
for the boundary, the seams, and the design law behind them.

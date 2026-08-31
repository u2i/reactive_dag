# Authoring nodes

A node is an Ash resource with the `ReactiveDag.Node` extension. **The resource
is the node and its own payload table**: the `reactive do … end` block declares
the computation; the resource's `attributes` are the rows it materializes. This
guide covers every shape that block can take.

## The node shapes

| shape | data_layer | attributes | `reactive` block | result lives in |
|---|---|---|---|---|
| **derived** | AshPostgres/Ets | the payload columns + an `:upsert` action | a combinator, or `compute MyOp` | the resource itself |
| **leaf** | AshPostgres/Ets | the observed columns + an `:upsert` action | `leaf? true` + `poll MyScan` | the resource itself |
| **compose** | Simple | none | `compose … do … end` | its legs' resources |

A cell owns the rows it computes. Only a `compose` node is tableless, and it is
not a cell — its nested legs are, and each of those owns its own. The verifier
refuses a node that computes something with nowhere to put the answer.

An earlier version allowed a **write-elsewhere** shape: no attributes, plus a
custom `upsert:` closure writing into another resource's table. It is gone. Such
a node could not use the payload loop, so it got no change detection and reported
a change on every recompute; the library could not see it held rows, so every
question about them answered empty; and vanished units were never reconciled
away. If rows belong in another resource's table, that resource's node is where
they should be computed.

Every node emits **rows**. A node whose answer is one word emits a row with a
`:status` column — see below.

## Declaring the computation

> #### `op` is a label, not a selector {: .info}
>
> `op :map` reads like the field that decides how a node recomputes. It is not.
> Recompute dispatches on the **entity** — `aggregate` / `reduce` / `join` /
> `union` / `per_key` / `run` / `compute` — and never on `op`, which is a free
> atom for documentation. Nothing in the library reads it.
>
> A block with an `op` and no entity declares no computation at all, and is
> rejected at compile time.

Authoring is **Ash-first**: start from what Ash can express declaratively and
work outward — each step down the ladder trades declarativeness for power, and
you take only the steps your shape needs. Every form reads its input, computes
the result set, writes it (into the node's own resource by default), and
reports **only the changed keys**, so downstream work is proportional to
real change.

| rung | you write | when |
|---|---|---|
| `aggregate` | attribute atoms only | the fold is a datastore aggregate over a relationship |
| `recompute_by` + `reduce` | the unit a change invalidates, then the fold | you know what a change should re-do (the common case) |
| declarative `reduce`/`join` | attributes + fold keywords | grouping/joining by attributes; the library reads Ash for you |
| per-slot escapes | a fn for the one slot that outgrew attributes | `query:`, computed groups/keys/rows, `expand:` |
| `per_key :action` | an action called once per row | expensive per-row work — an LLM, an embedding, a fetch |
| `run :action` | a generic Ash action on this resource | arbitrary recompute that should stay a first-class action |
| `compute Module` | a `ReactiveDag.Op` | recompute that outgrows Ash entirely |

### `aggregate` — the datastore does it

```elixir
aggregate over: :dmr_reports,          # a has_many on THIS resource
          count: :day_count,
          avg: [flow: :avg_flow],
          max: [flow: :peak_flow]
```

Postgres does the `GROUP BY`; **no rows cross into the BEAM**. Only expressible
as a relationship aggregate (the group must be a resource with a relationship
to the input) — for anything else, step down to `reduce`.

**Same vocabulary as the in-BEAM fold.** `aggregate` and `reduce into:` take
the same kinds (`count`/`sum`/`avg`/`min`/`max`/`first`), the same
`[src: dest]` spelling, the same SQL nil semantics, and the same key rules
(a composite primary key makes either node identity-keyed). One list backs
both, so they cannot drift — moving a fold between the datastore and the BEAM
does not change the answer, only who computes it:

| | who aggregates | rows into BEAM | recompute unit |
|---|---|---|---|
| `aggregate` | **Postgres**, in one query | none | whole cell, always |
| `reduce into:` | the BEAM | the scoped slice | whatever `recompute_by` says |

The trade is real in both directions: `aggregate` reads nothing into the BEAM
but reprices every group; `reduce` + a tight `recompute_by` reads only the
claimed slice, which often wins for a big table with fine-grained changes.

### `reduce` — an in-BEAM fold, declared

```elixir
reduce over: :fiscal_lines,
       group_by: [:fund, :fy],                    # group by attributes
       into: [sum: [amount: :total], count: :n]   # fold each group
```

No `read:` — the library reads the over node's resource (its primary read
action), **automatically scoped to the claimed dirty keys** by filtering the
over's payload key. No `key:` — the group's values join with `"|"`
(`"gf|2025"`); `key_prefix: "roll"` namespaces (`"roll|gf|2025"`). The row is
the group's attributes plus the fold results (`count`/`sum`/`avg`/`min`/`max`/
`first`, nil sources excluded, SQL-style), written into this resource by the
payload loop. `read: :recent` names a `:read` action on the over resource
instead of its primary — same auto-scoping.

**Keys are Ash keys.** A single-attribute primary key is the payload key,
derived — `payload_key` exists only for non-PK key columns. Better: declare a
**composite primary key** (`fund` + `fy`) and drop the key column entirely —
the row IS its identity, the upsert conflicts on the primary key, and the cell
key is the identity's serialization in primary-key order. The verifier checks
every identity field is produced by the row (group columns ∪ fold dests). And
the DAG edge reads as a **relational join**: a `group_by` entry may be the
pair `parent_column: :child_field` (`group_by: [fund: :fund_code, fy: :fy]` —
"this node's `fund` = the child's `fund_code`").

### The recompute unit: `recompute_by`

The declaration the engine actually cares about is **what unit a change
invalidates**. Everything else — `group_by`, `into`, key derivation — is
mapping data into shape once you already know what to recompute.

```elixir
reactive do
  recompute_by :category, to: :expenses, from: :expense_cat

  reduce into: [sum: [amount: :total], count: :n]
end
```

Read it as a sentence: *recompute by category, from the input's
`expense_cat`.* A change to a row's `expense_cat` invalidates my `category`
unit, so redo it whole. That one fact supplies the **input edge**, the
**grouping**, the **claim resolution** and the **read scope** — which is why it
replaces `key_rule` entirely. The same unit used to be stated twice, once as
the grouping and once as the claim rule, and the two had to agree.

Four answers to the one question:

| declaration | the unit a change invalidates |
|---|---|
| *(omitted)* | key-for-key — a changed input key maps to the same output key |
| `recompute_by :cat, from: :field` | per unit, resolved by **reading** the changed rows; a key the lookup can't find (a deleted row) degrades to whole-cell |
| `recompute_by [fund: :fund_code, fy: :fy]` | a **composite** unit — the grain IS the grouping, so `group_by:` is not restated |
| `recompute_by :month, from_key: true` | per unit, resolved **purely** from the changed key's `\|`-segments — no query at all, at the price of the key-grammar contract. Rarely needed: a change's diff resolves the unit without either a query or a grammar |
| `recompute_by :cell` | the whole cell — any change re-does everything |

### Composite grain

A unit can be several columns. State the pairs once and the grouping follows:

```elixir
recompute_by [fund: :fund_code, fy: :fy], to: :lines
reduce into: [sum: [amount: :total], count: :n]
```

Reads as `rollups.fund = lines.fund_code AND rollups.fy = lines.fy`. The cell
key serializes the columns in order (`"gf|2025"`), and with a composite primary
key the row is identity-keyed — no key column at all.

The read is **scoped per column**: a claim of `"gf|2025"` becomes
`fund_code IN ("gf") AND fy IN ("2025")`. For several claims that admits a
cross-product superset (`"gf|2025"` + `"water|2026"` also matches `"gf|2026"`)
— still sound, since a superset read stays closed over unit boundaries, and far
tighter than reading the whole table. Columns that aren't plain strings don't
invert; the fold sorts them out.

**It is the recompute unit, not the output's grain.** They coincide for a plain
rollup and diverge the moment one unit emits many rows: percentile
distributions `recompute_by :day` (touch one reading and the whole day is
re-derived) while the rows themselves are keyed day+percentile via `expand:`.

```elixir
recompute_by :day, to: :readings, from: :date
reduce expand: fn day, rows -> percentiles(day, rows) end
```

The unit is consumed at **compile time** — it lowers to `over:` + `group_by:` +
the claim rule, and nothing traverses it at recompute. One declaration per
node, so a combinator reads exactly one input: one unit, one claim translation.
A node reads its input, materializes rows, and downstream consumers query
*those rows* rather than re-deriving them back up the chain.

### Retirement: units that stop existing

A fold writes the units it produced — and **reconciles** the ones it didn't. A
unit whose input rows have all gone produces nothing, so without this its last
computed value would linger forever, and a stale derived row is
indistinguishable from a live one.

Retirement destroys the **payload row**, so the derived table stops showing the
unit, and reports its key as changed, so the retirement propagates downstream.
The row is the unit — there is no second place a stale copy could survive. A
node that can retire therefore needs a destroy action — `defaults [:destroy]`,
or name one with `payload_destroy`.

What a pass may retire is bounded by its **claim**: a whole-cell pass reconciles
everything the node holds, a scoped pass only the units it claimed. Reconciling
wider would retire live units that simply weren't visited.

A node with a custom `upsert:` owns its own writes, so the library does not
reconcile on its behalf.

**Limit — a row moving between units.** The claim names where the row *landed*;
the unit it *left* is invisible, because nothing records which unit an input key
previously fed. The origin is repriced by the next whole-cell pass rather than
the scoped claim. Fixing it exactly needs input-key → unit provenance.

A combinator read is **always an Ash read** — to shape it, stay in the query:

```elixir
reduce over: :fiscal_lines,
       query: fn q, _dirty -> Ash.Query.filter(q, posted == true) end,
       group_by: fn line -> {line.fund, line.fy} end,                    # computed group
       key: fn {fund, fy} -> "#{fund}|#{fy}" end,
       into: fn {fund, _fy}, lines -> %{fund: fund, total: sum(lines)} end
```

`query:` receives the base query and the claimed dirty keys (`nil` =
whole-cell) and returns a query — filter, sort, load, without leaving Ash's
pipeline (policies still apply); the library executes it and applies the
dirty-key scope afterwards, so scoping stays the substrate's job. The other
slots resolve independently — a declarative `group_by` with an `into:` fn is
fine (the fn receives the group tuple exactly as the fn idiom always has). A
read that isn't Ash at all belongs on the `run`/`compute` rungs.

Two shapes are worth calling out:

- **verdict** — a verdict is a **column**, written with `into:` like any other:

  ```elixir
  attributes do
    attribute :key, :string, primary_key?: true
    attribute :status, :string
    attribute :headroom, :float      # why a table is worth having
  end

  reduce over: :category_totals, group_by: :key,
         into: fn _k, [r | _] ->
           %{status: if(r.total < 1000.0, do: "present", else: "failing"),
             headroom: 1000.0 - r.total}
         end
  ```

  "What is failing?" is then `filter(status == "failing")` — an ordinary Ash
  read, with policies, joins and loads.

  There used to be a tableless shape for this (`verdict? true`, writing the
  status straight into a coordination table). It saved a migration when the
  answer was one word, and cost a ceiling: that table's schema was fixed, so the
  moment a verdict wanted a `headroom` the shape had nothing to offer. A row
  costs a migration and answers every later question.

- **expand** — declare `expand:` (`(group, items -> [row])`, each row carrying
  its own `:key`, since one group fans out to many keys).

#### The classic: date-bucketed rollups

A `group_by:` entry may name a **calculation** as well as an attribute — so a
derived grouping value (the classic being a calendar bucket) is declared where
Ash puts derived values: on the resource that owns the data. The library loads
it in the read; the bucket label becomes the group column *and* the derived
cell key.

```elixir
# on the data's resource — usable by ANY Ash consumer, not just the DAG
calculations do
  calculate :month, :string, expr(fragment("to_char(?, 'YYYY-MM')", date))
end

# the rollup node: daily readings → monthly totals, keys like "2026-08"
reduce over: :readings,
       group_by: [:month],
       into: [sum: [value: :total], count: :n]
```

**Prefer a plain ATTRIBUTE for the grain where you can.** A calculation groups
correctly, but its propagation is coarser: a change's claim is derived from the
change's own DIFF, and a diff holds attributes rather than evaluated
expressions — so a calculation grain falls back to a whole-cell claim while an
attribute grain claims only the units the change actually touched. Storing the
bucket as a column on write costs one string and buys per-unit propagation.

And the mid-granularity claims come from the same declaration — the unit:

```elixir
recompute_by :category, to: :expenses, from: :category
reduce into: [sum: [amount: :total], count: :n]
```

A changed child key is resolved by **reading** the changed rows and evaluating
the unit's `from:` field (one scoped query per propagation; a key the lookup
can't find — a deleted row — degrades to whole-cell, since vanish must reprice
everything it might have left). When the unit is one plain string attribute,
the library also scopes the read to the claimed units (`category in claims`).

Except that it usually is not read at all. A change carries its own DIFF — both
sides of what moved — so the unit is derived from that, with no query and nothing
to fail on a deleted row. The lookup above is the fallback for the cases a diff
cannot answer: a calculation grain, a `%Join{}`, or a key that arrived without
one (a source-fed leaf has no Ash row behind it).

`from_key: true` trades the lookup for PURE resolution when keys carry the unit's
input fields as leading `|`-segments (`"2026-08-11|r4"`): no query, and
deletion-safe. It predates the diff and is now the narrow case — a host that
wants resolution with no read whatsoever, and controls its own key grammar to get
it. For anything else the diff is both cheaper and more precise, because it names
the unit a moved row LEFT as well as the one it entered.

The general soundness rule behind all of it: a scoped read must be **closed
over unit boundaries** — the omitted (identity) case is entry-closure,
`recompute_by :cat` (either resolution) is unit-closure, `:cell` is the
universe. The read auto-scope inverts claims through the same group plan: a
plain string attribute filters by equality, a composite unit by every column's
seen values. `key_rule` at block level remains for nodes with **no** combinator
(`run`/`compute`/leaves); declaring it alongside `recompute_by` is a compile
error, since they are the same fact.
`test/group_rule_test.exs` is the worked demo: touch one expense, watch exactly
one category recompute and propagate. `test/payload_diff_propagation_test.exs`
shows the move — one row changing group claims BOTH units.

### `join` — a left join (one input, two sides), declared

```elixir
join over: :entries,
     left:  [key: :acct, where: [kind: "budget"]],   # side = discriminator + key
     right: [key: :acct, where: [kind: "actual"]],
     into:  [left: [amount: :budget], right: [amount: :actual]]
```

One row per **left** key, right side optional; an absent side yields `nil`
columns, so the declared-vs-observed gap is information, not an error. A plain
attribute is the two-column case (`left: :declared_id` — a nil value means
"not on this side"); `[key:, where:]` splits ONE input into sides by a
discriminator field. `outer: true` also emits right-only keys (an undeclared
member is a finding). The fn escapes: `left: fn item -> ... end` for computed
side keys, `into: fn jk, l, r -> ... end` for computed columns
(variance = budget − actual). `query:` shapes the read exactly as on `reduce`.

### `join` — two inputs, two nodes

```elixir
join left_over:  :budgets,          # one node
     right_over: :actuals,          # ...and a different one
     left:  :account_code,          # each side's JOIN KEY column
     right: :acct,
     outer: true,                   # a right-only key is a row too
     into:  [left: [amount: :budget], right: [amount: :actual]]
```

Two nodes correlated into one row per join key. `left:`/`right:`/`into:`/`outer:`
mean exactly what they do in the one-input form; what changes is that each side
is a **separate node**, read and scoped independently, and each contributes its
own input edge — so a change on either propagates through it. No `depends_on` is
needed: naming the two sides names the two edges.

The claim rule defaults to `:group` rather than `:identity`, because an input's
changed keys are **its own** keys. An `Actuals` row keyed `"a1"` joins on
`acct: "5000"`, so `"a1"` names no join key; the rule translates a changed row to
the join key it belongs to, through the side that propagated.

That translation is the whole reason this shape works, and getting it wrong is
what reverted the first attempt: it scoped each side by the side's own
`payload_key` while the claim carried join keys — different columns, different
values — so the scoped read matched nothing, the side came back empty, and the
join emitted `nil` for its columns. The payload upsert then wrote those nils over
good data. Each side is now scoped by the column it is **indexed by**.

> #### A fn side reads whole {: .info}
>
> `left: fn row -> ... end` computes its join key in the BEAM, so there is no
> column to push a filter into and that side reads whole. Correct, and no worse
> than the one-input form, which also reads whole for a fn side — but it means a
> named-attribute or `[key: …]` side is the one that scopes.

An absent side yields `nil` columns, as with one input: nil means "no row on that
side", which is information rather than an error. A node that needs to
distinguish "no row" from "that source has not reported yet" wants a column of
its own recording which sides have been seen.

### `union` — the graph-wide roll-up as a node

```elixir
attributes do
  attribute :check, :string, primary_key?: true      # which input
  attribute :subject, :string, primary_key?: true    # its key
  attribute :status, :string
end

reactive do
  union from: [:category_health, :fund_balance, :machine_ownership],
        into: [check: :cell, subject: :key, status: :status]
end
```

One row per `(input cell, key)`, across several inputs.

The point is the question a graph of verdict nodes cannot otherwise answer
cheaply: **what is failing anywhere?** Each verdict node knows about its own
cell, so the roll-up means scanning every cell separately
(`Insights.summary/1` does exactly that, one query per cell). A union makes it a
node — one indexed table, and `filter(status == "failing")` is an ordinary Ash
read.

It is maintained **incrementally**: a verdict flips, that key propagates, one
row updates. The composite primary key makes it identity-keyed, so cell keys are
`"category_health|travel"` — the key carries its own provenance.

Source fields available to `into:` are `:cell` (which input the row came from),
`:key`, `:status` and `:observed_at`.

#### Union vs a two-input join

A union does not **correlate**: each input contributes rows independently, one
row per `(input cell, key)`, so a scoped claim reads only the input that moved.
That is why the shapes differ — a union *stacks* rows, a join *matches* them.

A cross-node join was built and reverted once, on the reasoning that "a claim
naming one side leaves the other unread, and the fold writes nulls over good
data". The failure was real. The diagnosis was one level too shallow: the unread
side was a **mis-scoped read**, not an inherent consequence of correlating. See
[`join` with two inputs](#join-two-inputs-two-nodes) — each side is scoped by
its own join-key column, and both sides read the rows the claim is about.

Reach for a union when the inputs produce **comparable rows** and stacking them
is meaningful on its own — `fiscal_line` is a line item you can read. Reach for a
two-input join when two sources describe the **same entity** and one contributes
columns to it; unioning those keyed by provenance makes two half-rows per entity
purely so a one-input join can put them back together.

### `per_key` — one action call per row

```elixir
per_key :summarise,
  args: [text: :body],          # the row's :body becomes the `text` argument
  fingerprint: [:body],         # SKIP the call when :body has not moved
  into: [summary: :summary],    # the result's "summary" → this :summary
  max_concurrency: 8            # bounded Task.async_stream over the rows
```

For work that is expensive **per row** rather than per cell — an LLM call, an
embedding, a document fetch. The library drives the loop: scope to the claimed
keys, read those rows, call the action once each, write each result through the
payload loop.

`fingerprint:` is why this is a rung of its own rather than a shape of `run`.
The library can see what a `per_key` node depends on, so it partitions **before**
calling: a row whose fingerprint has not moved costs nothing at all. It reports
`%{called: n, skipped: n}` in the drain step's meta, so the saving is visible
rather than assumed.

```
first pass                    → %{called: 40, skipped: 0}
second pass, two rows edited  → %{called: 2,  skipped: 38}
```

`max_concurrency:` bounds a `Task.async_stream` over the rows, since the latency
is a remote call rather than CPU. Results apply in row order regardless, so the
changed-key list stays deterministic. Rows skipped by `fingerprint:` never enter
the stream, so slots are spent only on real calls.

An **LLM node** is this rung with an [ash_ai](https://hexdocs.pm/ash_ai)
prompt-backed action behind it — no library code required. See
[LLM nodes](llm-nodes.html) for the shape, the cost discipline, and how to test
one without a model.

### `run` — a generic Ash action as the recompute

```elixir
actions do
  action :extract, {:array, :string} do
    argument :keys, {:array, :string}, allow_nil?: true   # nil = whole-cell
    argument :cell_id, :string
    run fn input, _ctx ->
      changed = MyApp.Extract.run(input.arguments[:keys])
      {:ok, changed}                                      # the CHANGED keys
    end
  end
end

reactive do
  run :extract
  ref :transcripts
end
```

The Ash-native escape hatch — one step less escape than a module, because the
computation stays a first-class action: arguments, policies, testable with
`Ash.run_action`. The library passes only the arguments the action declares
(`keys`, `cell_id` — declare neither for a whole-cell recompute), the action
does its own domain writes, and the keys it returns are what propagates. The
action must exist and be generic — verified at compile time.

Reach for `run` over `per_key` when the recompute is genuinely **whole-cell** —
one action that rewrites the node in a pass. The trade is that an action is
opaque: the library cannot see what it depends on, so it cannot fingerprint it,
and a whole-cell claim re-runs everything. Per-row work that costs money belongs
on `per_key` for exactly that reason.

### `compute` — the outermost escape hatch

```elixir
compute MyApp.Ops.EventsExtract   # implements ReactiveDag.Op
```

For recompute that outgrows Ash entirely: an LLM call, an external fetch, a
bespoke multi-input recompute. The op receives `(cell, dirty_keys)`, reads its
inputs however it likes, writes its rows however it likes, and returns the
keys that actually changed.

### `slice` — the dimension a person selects by

`recompute_by` says what unit a *change* invalidates. `slice` says what unit a
*person* picks, and they are rarely the same:

```elixir
reactive do
  recompute_by :category, to: :expenses, from: :category
  reduce group_by: :category, into: [sum: [amount: :total]]

  slice :fiscal_year, values: {MyApp.Osc, :available_years, []}
end
```

That node recomputes per category. An operator still asks about a *year* —
"reprocess just FY25", "the prompt changed, re-run last year's documents" — and
nothing generic can find the year in a row: a cell key is one column or a
`"|"`-joined identity, so `fiscal_year` on one node and `published_on` on
another are equally invisible until the node names one.

With it declared, selection is a read and reprocessing is a mark:

```elixir
keys = ReactiveDag.Node.Rows.keys_where(cell, fiscal_year: "FY25")
ReactiveDag.CascadeWorker.enqueue("budget_rollups", keys)
```

`values:` is what makes a control a choice rather than a text box — only the host
knows which fiscal years exist, and usually already has the function that says
so. Omit it and a UI must take free text. `ReactiveDag.Node.Rows.slices/1`
reports the declarations with their options resolved, which is what a dashboard
renders from.

Nothing stops a caller filtering on any column with `keys_where/2`; the
declaration is what makes a *UI* possible, not what makes a filter legal.

**Not time-shaped, deliberately.** The obvious first guess is a date range, and
it fits almost nothing here: the dimension in practice is a `"FY22"` string, and
a processing version is not temporal at all. Time is one instance of slicing —
a node declaring `slice :published_on` gets a date control — rather than its
shape.

#### Reprocessing a slice

Selecting is a read; doing it is a job:

```elixir
%{"cell" => "budget_rollups", "where" => %{"fiscal_year" => "FY25"}}
|> ReactiveDag.ReprocessWorker.new()
|> Oban.insert()
```

That marks exactly those keys, drains, and reports `claimed` against `changed`
in `[:reactive_dag, :reprocess, :stop]`. Pass `"keys"` instead when a UI has
already chosen them. Omit both and it claims the whole cell — which propagates
`:all`, so everything beneath it re-derives too. That is usually what "the code
changed" means, and occasionally much more work than intended.

**It invalidates the fingerprint first.** A `per_key` node skips rows whose
declared inputs have not moved, and after a prompt change they have not — so
marking alone would skip exactly the rows you asked it to redo.

So the stored fingerprint is cleared on the selected keys before they are marked.
That is not a bypass: a null fingerprint means *"no valid prior result"*, which is
precisely true once the code that produced it has changed. The recompute then runs
for the ordinary reason and stores a fresh fingerprint as it always would — so a
reprocess is a one-shot, not a mode. The telemetry's `invalidated` says how many
rows that touched, and is 0 on a node with no fingerprint column, which recomputes
regardless.

#### Polling a slice — `poll_as:`

A slice narrows two different things, and reprocessing is only one of them.
`keys_where/2` filters rows a node already **holds** — *"re-derive FY25 from the
documents I have"*. But a source whose upstream is addressable by the same
dimension can be asked to fetch just that part: a crawler that takes
`fiscal: "FY25/26"` walks twelve months instead of the whole corpus.

`poll_as:` names the dimension as the **scanner** spells it:

```elixir
reactive do
  id :agenda_center
  poll MuniWatch.Sources.AgendaCenter, every: "0 12 * * *"

  slice :fiscal_year, values: {MuniWatch.Fiscal, :years, []}, poll_as: :fiscal
end
```

```elixir
ReactiveDag.Source.refresh(plan, "agenda_center", fiscal: "FY25/26")
```

`Rows.poll_opts/2` does the translation, taking the selection a UI has (keyed by
**column**, since that is what it rendered buttons under) and returning what the
scanner wants:

```elixir
Rows.poll_opts(cell, %{"fiscal_year" => "FY25/26"})
#=> [fiscal: "FY25/26"]
```

Caller opts override the declared `args:`, so this composes with a standing
`args: [recent: true]` rather than replacing it.

**Two names because they are two names.** A scanner's option belongs to whatever
it wraps — an API query parameter, a CLI flag — and the column belongs to this
node's schema. Requiring them to match would make every scanner rename its
arguments after a storage decision. It defaults to the column, so the second name
is written only when it differs.

A column the node never declared as a slice is **ignored** rather than passed
through: an unrecognised option would otherwise reach `poll/1` as if the node had
offered it, and a scanner that pattern matches its arguments would crash on a typo
the DSL could not vouch for.

`poll_as:` on a node that declares no `poll` raises at assembly — it names the
option a poll is asked with, and a node nothing polls will never be asked. That
usually means the slice landed on the derived node instead of the source feeding
it.

## Input edges

```elixir
ref :transcripts                  # recompute edge: a change dirties this node
context :people                   # read-as-context: consulted, never triggers
depends_on [:a, :b]               # flat sugar — one ref per id
reduce over: :x, ...              # a combinator's `over:` implies a ref
recompute_by :x, to: :xs, ...     # ...as does `recompute_by to:`
```

**`ref` vs `context`** is the load-bearing distinction — a change to a `ref`
target propagates; a `context` target is read as settled context and never
triggers. A `context` edge is still a real input — validated, depth-ordered so
the target settles first, read at recompute. Use it when recompute is
expensive or non-deterministic and consults mutable context it shouldn't be
re-run by:

```elixir
reactive do
  op :map
  compute MyApp.EnhanceMinutes   # an LLM pass
  ref :transcripts               # a transcript change RE-RUNS the LLM
  context :people                # a people edit does NOT — the LLM just reads
                                 # current people the next time it runs
end
```

One boundary: a `context` edge still participates in depth ordering (that is
what guarantees the target settles first), so it cannot form a cycle —
`Graph.build` raises. It reads settled upstream context; it is not a feedback
mechanism.

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

## Which row is this key? — `row_key`

A cell key names a unit of work. Writing that unit means deciding which ROW it
is, and by default the library derives that from the primary key: a
single-attribute PK is the key's column, a composite PK means the row is its own
identity. `payload_key` overrides the column.

That derivation has nothing to say about a UUID primary key — it would resolve
to `:id` and write cell keys into it. So a node whose identity is not its primary
key declares the mapping:

```elixir
row_key :uuid                        # the key IS the row's id
row_key [:fund, :fiscal_year]        # the columns that identify the row
row_key &MyApp.Meetings.resolve/3    # sameness is a judgement
```

**`:uuid`** — the pass-through case. An upstream hands us a UUID, or the node
mints one; no lookup is needed.

**A column list** — the row is found by those values, taken off the row being
written. Change detection and the upsert then agree by construction, rather than
by you keeping a `payload_key` and an `upsert_identity` in step. The cell key is
not stored.

**A resolver** — `(cell_key, attrs, opts) -> row | nil`. For when no tuple
identifies the thing: a board that can meet twice in a day is not identified by
`(board, date)`, and "is this the same meeting?" is a decision. The resolver
makes its own read, so it receives `opts` and should scope by `opts[:tenant]` —
the library cannot scope a query it does not make.

> #### A resolver updates in place {: .info}
>
> Rungs 1 and 2 upsert, and the host's conflict target reaches the row. A
> resolver may name a row the upsert would NOT reach — a meeting matched "within
> an hour" has a different `starts_at` — so the matched row is revised with an
> update action. That is `:update` by default; name another with
> `payload_update:`. A resolver rung on a resource with no update action raises
> at the write, saying so.

Omit `row_key` and nothing changes: `payload_key` and the derived-primary-key
path apply exactly as before.

## Key rules

A combinator node declares its recompute unit with
[`recompute_by`](#the-recompute-unit-recompute_by), which subsumes this. For a
node with **no** combinator (`run`/`compute`/leaves), the block-level
`key_rule` still declares how a child's changed keys map onto this node's
recompute:

- `:identity` (default) — child key `k` changed → recompute my key `k`. For
  same-grain pipelines.
- `:all` — any child change → whole-cell recompute. For folds whose output
  grain differs from the input's.

Those are the rules the drain applies, read off the block. A host marking the
frontier *itself* — pre-marking a re-run, say — can state a different
propagation for that one call, by passing a module exporting `rule/3` to
`ReactiveDag.Graph.dirty_parents/5`; see
[One engine, and where the domain enters](seams.md).

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
real cells, ids must be unique, cycles are rejected
are resolved and their interposed cells manufactured. A broken graph fails
here, with the offending id in the message.

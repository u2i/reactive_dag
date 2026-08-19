defmodule ReactiveDag.Node do
  @moduledoc """
  An **Ash resource extension** that makes a resource a node in a reactive DAG.
  The resource IS the node **and** its own payload table: its `reactive` block
  defines the computation, its `attributes` are the rows the node materializes.
  This is the intended shape — one resource, both roles.

      defmodule MyApp.FlowMonth do
        use Ash.Resource,
          domain: MyApp.Domain,
          data_layer: AshPostgres.DataLayer,     # the node's OWN payload table
          extensions: [ReactiveDag.Node]

        attributes do                             # the payload columns; the row
          attribute :plant, :string, primary_key?: true   # IS its identity — the
          attribute :month, :string, primary_key?: true   # cell key "north|2024-01"
          attribute :avg_flow, :float                     # is its serialization
        end

        actions do
          create :upsert do upsert?(true); accept([:plant, :month, :avg_flow]) end
        end

        reactive do
          op :fold
          # ASH-FIRST: `recompute_by` names the UNIT a change invalidates (and
          # supplies the edge + the claim rule); the library reads :dmr_rows,
          # folds each group, and upserts the row by its Ash IDENTITY with the
          # payload write. No `read:`, no `key:`, no key column, no
          # `upsert:` — each slot has an escape hatch when the shape outgrows
          # attributes.
          recompute_by :plant, to: :dmr_rows, from: :plant
          reduce group_by: [:plant, :month],
                 into: [avg: [flow: :avg_flow]]
        end
      end

  The library closes the payload loop: a `reduce`/`join` whose `into` returns a row
  has that row written into the node's **own** resource
  (`ReactiveDag.Node.Payload`) with change-detection. There is nowhere else for it
  to go — a node owns its rows, and one that would write into a neighbour's table
  should be that neighbour's node.

  Keys work like Ash keys. A SINGLE-attribute primary key is the payload key
  (derived — declare `payload_key` only for a non-PK key column); a COMPOSITE
  primary key means the row IS its identity: no key column at all, the upsert
  conflicts on the primary key, and the cell key is the identity's
  serialization in primary-key order (`"gf|2025"`). The `payload_action`
  upsert defaults to `:upsert`.

  ## Which computation? (the Ash-first ladder)

  Start from what Ash expresses declaratively; each step outward trades
  declarativeness for power:

  | you want to… | use | rows into BEAM? | you write |
  |---|---|---|---|
  | group + `avg`/`sum`/`count` a relationship | `aggregate` | **none** (datastore GROUP BY) | attribute atoms |
  | fold, with the recompute UNIT declared | `recompute_by` + `reduce` | the scoped slice | the unit + the field it comes from + an `into:` fold |
  | fold one input's rows into per-group summaries | `reduce` | the scoped slice of `over` | `group_by:` attrs + an `into:` fold |
  | reconcile one input's two sides by key | `join` | the scoped slice of `over` | side attrs/`[key:, where:]` + picks |
  | correlate TWO nodes by a shared key | `join left_over:/right_over:` | each side's own scoped slice | a node + join-key column per side + picks |
  | a slot the attributes can't express | the slot's escape | same | `query:` (shape the Ash read), fn group/key/into, `expand:`, `status:` |
  | arbitrary recompute, kept Ash-native | `run :action` | up to the action | a generic action on THIS resource |
  | recompute beyond Ash (LLM, fetch, bespoke) | `compute Mod` | up to the module | a `ReactiveDag.Op` |

  ## Node shapes (what scaffolding a node needs)

  | shape | data_layer | attributes | actions | `reactive` |
  |---|---|---|---|---|
  | **derived** (materializes typed rows) | AshPostgres/Ets | the payload columns | an `:upsert` action | a combinator, or `compute Mod` |
  | **leaf** (a source writes its rows) | AshPostgres/Ets | the observed columns | an `:upsert` action | `leaf? true` + `poll Mod` |
  | **compose** (its legs are the cells) | Simple | none | none | `compose … do … end` |

  Only a `compose` node is tableless, and it is not a cell — its nested legs are.
  Every cell that computes something owns the rows it computes, which the verifier
  enforces; a `compute Mod` escape hatch is no exception, since its op writes its
  node's own resource like any other.

  There used to be a **write-elsewhere** shape — `Simple`, no attributes, a
  combinator plus a custom `upsert:` closure writing into another resource. It is
  gone (rc.39). It could not use the payload loop, so it got no change detection
  and reported a change on every recompute; the library could not see it held rows,
  so every question about them answered empty; and `retire_vanished` skipped it
  outright, so its stale units lingered forever. Three of them accumulated in one
  host, each causing a bug that took reading dispatch code to find.

  A payload node's `into` row is written into the resource itself (the payload
  loop, `ReactiveDag.Node.Payload`); the cell key maps to the `payload_key`
  attribute (default `:key`) via the `payload_action` upsert (default `:upsert`).

  ## Cell ids (the vocabulary of every edge)

  A node's cell id defaults to the resource module's short name, snake-cased
  (`MyApp.FlowMonth` → `:flow_month`); set `id:` to override. **This id is what
  every edge names** — `depends_on [:flow_month]`, `ref :flow_month`, and the ids
  passed to `graph/2`. If an edge doesn't resolve, it's almost always an id that
  doesn't match a node's (defaulted or explicit) id.

  ## Assembling + running

  `ReactiveDag.Node.graph/2` builds a `ReactiveDag.Plan` from a list of node
  resources; the substrate reads only the `reactive` block. Then:

      plan = ReactiveDag.Node.graph([FlowMonth, FiscalLines, …], for_each: &fetch/1)
      ReactiveDag.Drain.run(plan)

  Nothing else to wire: the drain reads what each node declared.

  ## Config

      config :reactive_dag,
        repo:        MyApp.Repo,   # REQUIRED (raises if unset)
        dirty_table: "my_dirty"    # frontier table (must match your migration)

  `dirty_table` defaults silently, so a name that doesn't match your migration
  yields empty results with no error — set it explicitly.
  """

  defmodule Ref do
    @moduledoc """
    A by-name input edge to another named node (`ref :id`). The general form —
    nestable inside `compose`. The flat `depends_on: [:a, :b]` schema key is sugar
    that lowers to one `%Ref{}` per id.

    """
    defstruct [:to, :__identifier__, :__spark_metadata__]
  end

  defmodule Compose do
    @moduledoc """
    An anonymous nested op-expression leg: composes inline as an intermediate
    cell (its `as` id, or a positional id derived from the parent). Its own legs
    are `ref`/`compose`, so the algebra reads as an expression tree.
    """
    defstruct [
      :op,
      :compute,
      :as,
      :key_rule,
      :leaf?,
      legs: [],
      meta: [],
      __identifier__: nil,
      __spark_metadata__: nil
    ]
  end

  @ref %Spark.Dsl.Entity{
    name: :ref,
    target: Ref,
    args: [:to],
    describe: "A by-name input edge to another node (a RECOMPUTE edge — changes propagate).",
    schema: [
      to: [type: :atom, required: true, doc: "the referenced node's id"]
    ]
  }

  defmodule Context do
    @moduledoc """
    A by-name CONTEXT input edge (`context :people`): the node READS the target
    as settled context but is NOT recomputed when the target changes. Still a
    real input (validated, ordered by depth so the target settles first, read
    at recompute) — it just doesn't propagate. For a node whose recompute is
    expensive/non-deterministic and consults mutable context it shouldn't be
    re-triggered by (an LLM step that looks up a human-curated
    people/positions table). Contrast `ref`, which dirties this node on change.
    """
    defstruct [:to, :__identifier__, :__spark_metadata__]
  end

  @context %Spark.Dsl.Entity{
    name: :context,
    target: Context,
    args: [:to],
    describe: "A by-name CONTEXT edge: read the target as settled context; its changes do NOT recompute this node.",
    schema: [
      to: [type: :atom, required: true, doc: "the target node's id (read-only context)"]
    ]
  }

  defmodule RecomputeBy do
    @moduledoc """
    THE declaration the engine cares about: **what unit does a change
    invalidate?** Everything else a combinator declares — `group_by`, `into`,
    key derivation — is mapping data into shape once you already know what to
    recompute.

        recompute_by :category, from: :expense_cat

    Read: "recompute by category, from the input's `expense_cat`" — a change to
    a row's `expense_cat` invalidates my `category` unit, so redo it whole.
    That one fact supplies the input edge, the grouping, the claim resolution
    and the read scope, so it replaces the `key_rule` vocabulary entirely.

    Four answers to the one question:

      * `recompute_by :cell`                        — the whole cell; any change
        re-does everything (a fold whose every output depends on every input).
      * `recompute_by :cat, from: :child_field`     — per unit, resolved by
        READING the changed rows and evaluating the field. A key the lookup
        can't find (a deleted row) degrades to whole-cell: vanish must reprice
        everything it might have left.
      * `recompute_by :month, from_key: true`       — per unit, resolved PURELY
        from the changed key's leading `|`-segments. No query and deletion-safe,
        at the price of the key-grammar contract.
      * (omitted)                                   — key-for-key: a changed
        input key maps to the same output key.

    NOTE this is the RECOMPUTE unit, not the output's grain. They coincide for a
    plain rollup, and diverge the moment one unit emits many rows: percentiles
    per day `recompute_by :day` (touch one reading, redo that day) while the
    rows are keyed day+percentile via `expand:`.

    The unit is consumed at COMPILE time — it lowers to the combinator's `over:`
    + `group_by:` pair and is never traversed at recompute. One declaration per
    node, so a combinator reads exactly one input: one unit, one claim
    translation.
    """
    defstruct [
      :unit,
      :to,
      :from,
      :from_key,
      :read,
      :__identifier__,
      :__spark_metadata__
    ]

    @doc """
    The unit as a `group_by` entry list: `[unit: :from]` — this node's column on
    the left, the input's field on the right. `nil` when the unit does no
    grouping.

    Three shapes, and code that handles only some of them is the bug this
    function exists to prevent: the compile-time verifier once re-derived this
    with the composite clause missing, so a composite unit read as "groups by
    nothing" and the `into:` check it feeds reported `would carry [nil]` on
    every build. A warning that fires on a correct declaration trains readers to
    ignore the one that matters.

      * `:cell` — the whole cell is one unit, so nothing is grouped.
      * `[fund: :fund_code, fy: :fy]` — the COMPOSITE form IS the grouping, and
        passes straight through (which is why `group_by:` is not restated).
      * `:month` + `from: :read_on` — one pair. Without `from:` the grouping is
        left to the combinator.
    """
    @spec group_by(struct()) :: keyword() | nil
    def group_by(%__MODULE__{unit: :cell}), do: nil
    def group_by(%__MODULE__{unit: pairs}) when is_list(pairs), do: pairs
    def group_by(%__MODULE__{from: nil}), do: nil
    def group_by(%__MODULE__{unit: u, from: f}), do: [{u, f}]
  end

  @recompute_by %Spark.Dsl.Entity{
    name: :recompute_by,
    target: RecomputeBy,
    args: [:unit],
    describe:
      "The unit a change invalidates — `recompute_by :category, from: :expense_cat` " <>
        "(per category, by looking the changed rows up), the COMPOSITE form " <>
        "`recompute_by [fund: :fund_code, fy: :fy]` (a multi-column unit, stated once), " <>
        "`from_key: true` (resolved purely from the key's segments), or `:cell` (redo " <>
        "everything). Omit it for key-for-key. Subsumes `key_rule` AND the grouping.",
    schema: [
      unit: [
        type: {:or, [:atom, :keyword_list]},
        required: true,
        doc:
          "THIS node's column naming the unit (`:category`, `:month`), the reserved " <>
            "`:cell` (the whole cell, for a computation whose every output depends on " <>
            "every input), or the COMPOSITE grain as `[this_column: :input_field, …]` " <>
            "(`[fund: :fund_code, fy: :fy]` — the unit IS the grouping, so `group_by:` " <>
            "is not restated; the cell key serializes the columns in order). With the " <>
            "composite form, `from:` is carried per entry and must not also be given."
      ],
      to: [
        type: :atom,
        required: false,
        doc:
          "the input node id, when it isn't already named by the combinator's `over:`. " <>
            "Declared on one or the other, never both."
      ],
      from: [
        type: :atom,
        required: false,
        doc:
          "the INPUT's field the unit is computed from (`:expense_cat`) — what is read " <>
            "and grouped, and what a claim traverses back through. Resolution READS the " <>
            "changed rows; a key it can't find (a deleted row) degrades to whole-cell. " <>
            "For a COMPOSITE unit the pairs carry this instead, so `from:` is omitted."
      ],
      from_key: [
        type: :boolean,
        required: false,
        doc:
          "resolve the unit PURELY from the changed key's leading `|`-segments instead " <>
            "of reading rows — no query and deletion-safe, at the price of the " <>
            "key-grammar contract (a declarative group with default keys only). A key " <>
            "that violates the grammar degrades to whole-cell."
      ],
      read: [
        type: :atom,
        required: false,
        doc:
          "OMIT for the input's primary read; an atom names a different `:read` action " <>
            "on it (same auto-scoping). Equivalent to the combinator's `read:`."
      ]
    ]
  }

  @compose_base %Spark.Dsl.Entity{
    name: :compose,
    target: Compose,
    args: [:op],
    describe: "An anonymous nested op-expression leg; composes inline as an intermediate cell.",
    schema: [
      op: [
        type: :atom,
        required: false,
        doc:
          "OPTIONAL free-atom label for this intermediate cell. Dispatches nothing here — " <>
            "recompute selects on the entity, not on `op` — so it is documentation, " <>
            "pure documentation — nothing dispatches on it. See `ReactiveDag.Cell`."
      ],
      compute: [type: :atom, doc: "the recompute module for this intermediate cell"],
      as: [type: :atom, doc: "an explicit id for this intermediate cell"],
      key_rule: [type: {:one_of, [:identity, :all]}, default: :identity],
      leaf?: [type: :boolean, default: false, doc: "true for a composed LEAF (a source-fed set)"],
      meta: [
        type: :keyword_list,
        default: [],
        doc:
          "OPEN domain binding for this intermediate cell (strength/source/check/…) — merged into its meta, like the root block's `meta:`."
      ]
    ]
  }
  # self-nest a fixed depth so a compose can hold compose legs (mirrors cascade).
  @compose Enum.reduce(1..8, @compose_base, fn _i, child ->
             %{@compose_base | entities: [legs: [@ref, child]]}
           end)

  defmodule Reduce do
    @moduledoc """
    A declarative REDUCE (fold): read an input node's payload, group it, and
    reduce each group to one output row — the common map/fold shape, so the
    author writes the grouping + reduction, not the read/write/changed plumbing.
    Anything the combinator can't express (an LLM call, an external fetch, a
    bespoke join) uses the `compute:` module escape hatch instead.
    """
    defstruct [
      :over,
      :group_by,
      :key,
      :key_prefix,
      :key_rule,
      :into,
      :expand,
      :read,
      :query,
      :__identifier__,
      :__spark_metadata__
    ]
  end

  @reduce %Spark.Dsl.Entity{
    name: :reduce,
    target: Reduce,
    describe:
      "Declarative fold: read the input, group it, reduce each group with `into`. " <>
        "Ash-first: declare `recompute_by` (the unit a change invalidates — it supplies " <>
        "the edge, the grouping and the claim rule), or name `over:` a node id with an " <>
        "explicit `group_by:`; omit `read:` (the library reads the over node's resource, " <>
        "dirty-key scoped) and declare the fold in `into:` — each slot has a fn escape " <>
        "hatch when the shape outgrows attributes.",
    schema: [
      over: [
        type: :atom,
        required: false,
        doc:
          "the input node id whose payload is read + grouped, with `group_by:` declared " <>
            "here. Omit it when `recompute_by` names the input with `to:`, which declares " <>
            "the unit and its edge together."
      ],
      read: [
        type: :atom,
        required: false,
        doc:
          "OMIT for the default: the library reads the OVER node's resource via its " <>
            "primary read action, automatically scoped to the claimed dirty keys by " <>
            "filtering its payload key. An atom names a different `:read` ACTION on the " <>
            "over resource (same auto-scoping). Combinator reads are ALWAYS Ash reads — " <>
            "shape them with `query:`; a non-Ash read belongs on the `run`/`compute` rungs."
      ],
      query: [
        type: {:fun, 2},
        required: false,
        doc:
          "`(Ash.Query.t(), dirty_keys | nil -> Ash.Query.t())` — SHAPE the read (filter, " <>
            "sort, load) without leaving Ash's pipeline; the library still executes it and " <>
            "applies the dirty-key scope afterwards (scoping stays library-owned). " <>
            "`dirty_keys` is nil for a whole-cell recompute."
      ],
      group_by: [
        type: {:or, [{:fun, 1}, :atom, {:list, :any}]},
        required: false,
        doc:
          "REQUIRED with `over:`; implied by a `recompute_by` that declares `from:`. " <>
            "An attribute or CALCULATION on the over resource (`:fund`, or `:month` where " <>
            "the over declares `calculate :month, :string, {ReactiveDag.Calendar, " <>
            "bucket: :month, of: :date}` — derived grouping values are Ash calculations, " <>
            "declared where the data lives; the library loads them). A list groups by " <>
            "the TUPLE of values; an entry may be the RELATIONAL-JOIN pair " <>
            "`parent_column: :child_field` (`[category: :expense_cat]` — group by the " <>
            "child's field, carry it as this node's column: Ash's source/destination " <>
            "attributes, for the DAG edge). The fn escape hatch: `(item -> group_term)`."
      ],
      key: [
        type: {:fun, 1},
        required: false,
        doc:
          "OMIT for the default: the group's values joined with `\"|\"` " <>
            "(`{\"gf\", 2025}` → `\"gf|2025\"`). The fn escape hatch: " <>
            "`(group_term -> cell_key_string)`."
      ],
      key_prefix: [
        type: :string,
        required: false,
        doc:
          "prepend `\"<prefix>|\"` to the DEFAULT key — the `\"va|\" <> acct` namespacing " <>
            "idiom, declaratively. Not combinable with an explicit `key:` fn."
      ],
      into: [
        type: {:or, [{:fun, 2}, :keyword_list]},
        required: false,
        doc:
          "how a PAYLOAD node reduces a group to its ONE row: a declarative FOLD — " <>
            "`[count: :n, sum: [amount: :total], avg: [flow: :avg_flow], min: …, max: …, " <>
            "first: …]` (bare atom = same-named dest; nil sources excluded from numeric " <>
            "folds; the row is the group's attributes + the fold results; requires a " <>
            "declarative `group_by`) — or the fn escape hatch `(group_term, [item] -> row)`. " <>
            "Exactly one of `into`/`expand` on a payload node; a VERDICT node declares " <>
            "`status:` instead."
      ],
      expand: [
        type: {:fun, 2},
        required: false,
        doc:
          "the group → MANY-rows shape: `(group_term, [item] -> [row])`, each returned " <>
            "row carrying its own `:key` (one group fans out to many keys, so keys cannot " <>
            "derive). Mutually exclusive with `into`/`status`. Return `{:skip, key}` in " <>
            "place of a row to DECLINE a claimed key — \"this one is not mine\", as " <>
            "distinct from \"this one is gone\": returning nothing for it retires it, " <>
            "which reports a change and repeats on every pass. A node projecting one kind " <>
            "out of a shared input needs this."
      ]
    ]
  }

  defmodule Join do
    @moduledoc """
    A declarative JOIN: read ONE input's payload, index it into a LEFT and a
    RIGHT side (each a `%{join_key => item}` built from a per-side key fn), then
    emit one row per left key joined to its right item (right may be absent). The
    common declared-vs-observed reconcile/variance shape — the author writes the
    two side keys + the join row, not the read/write/changed plumbing.

    LEFT join by default. `outer: true` makes it a FULL OUTER join: right-only
    keys also emit (`into.(jk, nil, right_item)`), for reconciles where an
    unexpected right-side member is itself a finding (a rogue repo the baseline
    never declared) rather than something to silently drop.

    ## Two inputs

    `left_over:`/`right_over:` read TWO DIFFERENT nodes, each scoped by its own
    keys. That needs `left_owns:`/`right_owns:` — the payload columns each side
    writes — because a claim names one side's keys and leaves the other side
    unread.

    `left_over:`/`right_over:` read TWO DIFFERENT nodes. An earlier attempt at
    this shape was reverted for writing nils over good data — a budget claim
    destroyed the actual and vice versa — and the revert concluded that was "the
    shape's natural failure mode, not an oversight". The failure was real; the
    diagnosis was one level too shallow.

    A claim key is a JOIN key. The reverted version scoped both sides by the one
    claim's keys against each side's own payload key — two different columns,
    usually different values (an `Actuals` row keyed `"a1"` joins on
    `acct: "5000"`). So the scoped read matched nothing, the side came back
    empty, and `into` emitted `nil` for its columns.

    Each side is therefore scoped by the column it is INDEXED BY — its own
    join-key attribute. Both sides then read the rows the claim is actually
    about, `into` sees real values on both, and no nil is ever emitted to be
    written. Nothing about per-column writes is needed; the read was the bug.

    Two consequences worth knowing:

      * A `fn` side computes its join key in the BEAM, so there is no column to
        push a filter into and that side reads WHOLE. Correct, and no worse than
        the one-input form, which also reads whole for a fn side.
      * The claim rule defaults to `:group`, not `:identity`: an input's changed
        keys are its own, so they are translated to join keys through the side
        that propagated — the same edge the read is scoped by.

    So left/right/inner/outer stay ordinary options; `outer: true` is a full
    outer join here exactly as it is with one input.
    """
    defstruct [
      :over,
      :left_over,
      :right_over,
      :read,
      :query,
      :left,
      :right,
      :key,
      :key_prefix,
      :key_rule,
      :into,
      outer: false,
      __identifier__: nil,
      __spark_metadata__: nil
    ]
  end

  @join %Spark.Dsl.Entity{
    name: :join,
    target: Join,
    describe:
      "Declarative join: index `over` into left/right sides, emit a row per left key (per EITHER side's key with `outer: true`). " <>
        "Ash-first: omit `read:`, name each side's attribute (or `[key:, where:]` for a " <>
        "discriminator split), and pick the row's columns in `into:`.",
    schema: [
      over: [
        type: :atom,
        required: false,
        doc:
          "ONE-INPUT form: the input node id whose payload is read + indexed, then split " <>
            "into sides by `left:`/`right:`. Mutually exclusive with `left_over:`/`right_over:`."
      ],
      left_over: [
        type: :atom,
        required: false,
        doc:
          "TWO-INPUT form: the node id feeding the LEFT side, read and scoped INDEPENDENTLY " <>
            "of the right — each by its OWN join-key column, which is what makes two inputs " <>
            "safe here. Requires `right_over:`. Each side is its own input edge, so a change " <>
            "on either propagates through it, and the claim rule translates a changed row to " <>
            "the join key it belongs to."
      ],
      right_over: [
        type: :atom,
        required: false,
        doc: "TWO-INPUT form: the node id feeding the RIGHT side. Requires `left_over:`."
      ],
      read: [
        type: :atom,
        required: false,
        doc:
          "OMIT for the default (the over node's primary read, dirty-key scoped); an " <>
            "atom names a different `:read` action on it. Always an Ash read — see `query:`. " <>
            "ONE-INPUT form only."
      ],
      query: [
        type: {:fun, 2},
        required: false,
        doc:
          "`(Ash.Query.t(), dirty_keys | nil -> Ash.Query.t())` — shape the read inside " <>
            "Ash's pipeline (see `reduce`); the library executes and scopes it."
      ],
      left: [
        type: {:or, [{:fun, 1}, :atom, :keyword_list]},
        required: true,
        doc:
          "the LEFT side: an attribute (`:declared_id` — nil value = not on this side), " <>
            "`[key: :acct, where: [kind: :budget]]` (on this side iff every `where` pair " <>
            "matches; join key = the `key:` attribute), or the fn escape hatch " <>
            "`(item -> join_key | nil)`."
      ],
      right: [
        type: {:or, [{:fun, 1}, :atom, :keyword_list]},
        required: true,
        doc: "the RIGHT side — same shapes as `left`."
      ],
      key: [
        type: {:fun, 1},
        required: false,
        doc:
          "OMIT for the default (`to_string(join_key)`, `key_prefix`-aware). The fn " <>
            "escape hatch: `(join_key -> cell_key_string)`."
      ],
      key_prefix: [
        type: :string,
        required: false,
        doc: "prepend `\"<prefix>|\"` to the DEFAULT key. Not combinable with an explicit `key:` fn."
      ],
      into: [
        type: {:or, [{:fun, 3}, :keyword_list]},
        required: false,
        doc:
          "how a PAYLOAD node emits its ONE row per join key: declarative COLUMN PICKS " <>
            "per side — `[left: [amount: :budget], right: [amount: :actual]]` (bare atom " <>
            "= same-named dest; an absent side yields nils, so gap semantics fall out) — " <>
            "or the fn escape hatch `(join_key, left_item_or_nil, right_item_or_nil -> " <>
            "row)` for computed columns (variance = a − b). left is nil only for " <>
            "`outer: true` right-only keys. A VERDICT node declares `status:` instead."
      ],
      outer: [
        type: :boolean,
        default: false,
        doc: "FULL OUTER: right-only keys also emit, via `into.(jk, nil, right_item)`. Default false (left join)."
      ]
    ]
  }

  defmodule PerKey do
    @moduledoc """
    The PER-ENTRY MAP: for each claimed row of the input, call a generic action
    with that row and write its structured output into this node's attributes.

        recompute_by :key, to: :transcripts, from: :key

        per_key :summarise,
          args: [text: :body],
          fingerprint: [:body],
          into: [summary: :summary]

    The loop every per-row node hand-writes — scope to the claimed keys, read
    the rows, call something once per row, write the result — with the library
    driving it. That matters beyond ergonomics: because the library now SEES
    the input rows, it can fingerprint them and **skip the call** when nothing
    that feeds it has changed. A `run` action is opaque by design (the library
    passes keys and gets keys back), so no amount of declaration outside it
    could do that.

    `fingerprint:` names the input fields the result depends on. Their hash is
    stored on the output row; when a recompute finds it unchanged, the action is
    NOT called and the key is not reported changed. For an expensive or
    non-deterministic action — an LLM call above all — that is the difference
    between a whole-cell claim costing one call and costing all of them.

    The action is an ordinary generic Ash action taking the row's fields as
    arguments and returning a map. That it might be `AshAi.Actions.prompt/2` is
    the library's business not at all.
    """
    defstruct [
      :action,
      :args,
      :fingerprint,
      :into,
      :fingerprint_attribute,
      :max_concurrency,
      :timeout,
      :__identifier__,
      :__spark_metadata__
    ]
  end

  @per_key %Spark.Dsl.Entity{
    name: :per_key,
    target: PerKey,
    args: [:action],
    describe:
      "Per-entry map: call a generic action once per claimed input row and write its " <>
        "structured output into this node's attributes. `fingerprint:` skips the call " <>
        "when the named input fields are unchanged — the point of the rung for " <>
        "expensive or non-deterministic work.",
    schema: [
      action: [
        type: :atom,
        required: true,
        doc:
          "a GENERIC action on THIS resource, taking the row's fields as arguments and " <>
            "returning a map (`:map`, or a typed struct — whatever `into:` can read)."
      ],
      args: [
        type: :keyword_list,
        required: false,
        doc:
          "how the input row becomes the action's arguments: `[text: :body]` passes the " <>
            "row's `:body` as the `text` argument. Omit to pass every action argument " <>
            "whose name matches an input field."
      ],
      fingerprint: [
        type: {:or, [{:fun, 1}, {:list, :atom}]},
        required: false,
        doc:
          "what the result depends on: input fields (their hash), or `(row -> value)` " <>
            "when \"the same input\" is not a plain field comparison. Stored on the " <>
            "output row (`fingerprint_attribute`, default `:fingerprint`); a recompute " <>
            "whose value matches SKIPS the action entirely and reports the key " <>
            "unchanged. Omit to call every time."
      ],
      fingerprint_attribute: [
        type: :atom,
        required: false,
        doc: "the attribute the fingerprint is stored in (default `:fingerprint`)."
      ],
      into: [
        type: :keyword_list,
        required: false,
        doc:
          "how the action's result becomes this node's row: `[summary: :summary]` maps " <>
            "the result's `\"summary\"` onto this resource's `:summary` attribute. Omit " <>
            "to copy every result key whose name matches an attribute."
      ],
      max_concurrency: [
        type: :pos_integer,
        required: false,
        doc:
          "how many rows may be IN FLIGHT at once (default 1 — one call at a time). " <>
            "The drain is sequential per cell by design (depth order is what makes the " <>
            "cascade correct), so this is the only place per-row parallelism can live. " <>
            "Rows skipped by `fingerprint:` never enter the stream, so slots are spent " <>
            "only on real calls. Results are applied in ROW ORDER regardless, so the " <>
            "changed-key list stays deterministic."
      ],
      timeout: [
        type: :timeout,
        required: false,
        doc:
          "milliseconds a single row's action may take before it is killed (default " <>
            "`:infinity`). Only meaningful with `max_concurrency:` — a row that times " <>
            "out FAILS the recompute rather than being silently dropped, since a " <>
            "half-written cell is worse than a loud one."
      ]
    ]
  }

  defmodule Union do
    @moduledoc """
    The N-INPUT shape: one row per `(input cell, key)` across several inputs,
    materialised into this node's own table.

        union from: [:category_health, :fund_balance, :machine_ownership],
              into: [cell_id: :cell, key: :key, status: :status]

    Its purpose is the graph-wide view. A verdict-shaped node answers one
    question about one cell; asking "what is failing ANYWHERE?" today means
    scanning every cell's status separately (`Insights.summary/1` does exactly
    that, one query per cell). A union node makes that roll-up a NODE — so it is
    one indexed table, maintained incrementally: a verdict flips, that key
    propagates, one row updates.

    ## Why N inputs are safe here, when a cross-node JOIN was not

    A join has to CORRELATE its inputs — match a budget row to an actual row —
    so a claim naming one side leaves the other unread, and the fold writes
    nulls over good data. That is not a scoping bug to be fixed; it is what
    correlating independently-claimed inputs means.

    A union does not correlate. Each input contributes its rows independently,
    so a claim scopes to exactly the input that fired and reads nothing else.
    The composite key carries its own provenance (`"category_health|travel"`),
    which is precisely the translation a join could not do.
    """
    defstruct [:from, :into, :__identifier__, :__spark_metadata__]
  end

  @union %Spark.Dsl.Entity{
    name: :union,
    target: Union,
    describe:
      "One row per `(input cell, key)` across SEVERAL inputs — the graph-wide roll-up as " <>
        "a node, maintained incrementally. Safe with N inputs because a union does not " <>
        "correlate them: each contributes rows independently.",
    schema: [
      from: [
        type: {:list, :atom},
        required: true,
        doc: "the input node ids whose keys are unioned. Each becomes an input edge."
      ],
      into: [
        type: :keyword_list,
        required: false,
        doc:
          "how an input's row becomes this node's row: `[cell_id: :cell, key: :key, " <>
            "status: :status]` maps the source fields (`:cell` — which input it came " <>
            "from — plus `:key`, `:status`, `:observed_at`) onto this resource's " <>
            "attributes. Omit to copy every source field whose name matches an attribute."
      ]
    ]
  }

  defmodule Compute do
    @moduledoc """
    The ESCAPE HATCH: declare an arbitrary recompute MODULE (a `ReactiveDag.Op`)
    for a node whose computation the `reduce`/`join` combinators can't express —
    an LLM call, a PDF/Tigris fetch, a bespoke multi-input recompute. `compute
    MyApp.EventsExtract` sits in the block alongside the combinators, mirroring
    Ash's `calculate :x, :type, MyModule` (the arbitrary case is an entity too,
    not a schema key beside the declarative ones).
    """
    defstruct [:module, :__identifier__, :__spark_metadata__]
  end

  @compute %Spark.Dsl.Entity{
    name: :compute,
    target: Compute,
    args: [:module],
    describe: "Escape hatch: this node's recompute is `module` (a ReactiveDag.Op).",
    schema: [module: [type: :atom, required: true, doc: "a module implementing ReactiveDag.Op"]]
  }

  defmodule Slice do
    @moduledoc """
    A dimension a human may select this node by: `slice :fiscal_year`.

    `recompute_by` declares the unit a CHANGE invalidates. This declares the unit
    a PERSON picks — "reprocess just FY25", "re-run last year's documents" — and
    they are rarely the same. A node recomputing per `:category` is still sliced
    by `:fiscal_year`, because that is the question an operator asks.

    Without it the library can offer no such control. A cell key is one column or
    a `"|"`-joined identity, so nothing generic can find the year in a row —
    `fiscal_year` on one node and `published_on` on another are equally invisible
    until the node says which it is.

    `values:` makes the control renderable rather than a text box. Only the host
    knows which fiscal years exist, and it usually already has the function:

        slice :fiscal_year, values: {MyApp.Osc, :available_years, []}

    Deliberately NOT time-specific. The obvious first guess is a date range, and
    it fits almost nothing: the real dimension here is a `"FY22"` string, and a
    version is not temporal at all. Time is one instance of slicing, not its
    shape.

    ## Selecting a slice from the SOURCE, not just from stored rows

    A slice narrows two different things, and only one of them was reachable.
    `column` filters rows this node already HOLDS — the reprocess case, "re-derive
    FY25 from documents I have". But a source whose upstream is addressable by
    the same dimension can be asked to fetch only that part: a crawler that takes
    `fiscal: "FY25/26"` walks twelve months instead of the whole corpus.

    `poll_as:` is what the dimension is called when asking the SCANNER for it:

        poll MuniWatch.Sources.AgendaCenter, every: "0 12 * * *"
        slice :fiscal_year, values: {MuniWatch.Fiscal, :years, []}, poll_as: :fiscal

    Then `Source.refresh(plan, "agenda_center", fiscal: "FY25/26")` reaches
    `poll/1` with the scanner's own vocabulary, while the same slice still
    filters `fiscal_year` for a reprocess.

    Two names because they are genuinely two names. A scanner's option belongs to
    whatever it wraps — an API query parameter, a CLI flag — and the column
    belongs to this node's schema; requiring them to match would make every
    scanner rename its arguments after a storage decision. Defaults to `column`,
    which is the common case, so the second name is written only when it differs.

    Declared here rather than mapped in a host because a translation table off to
    one side drifts from the DSL that needs it, and because the source has to
    reach `poll/1` and the crontab sweep identically — both read this entity.
    """
    defstruct [:column, :values, :label, :poll_as, :__identifier__, :__spark_metadata__]

    @doc """
    What to call this dimension when asking the SCANNER for it — `poll_as` when
    given, the column otherwise.

    One place decides, because a default spelled at each call site is a default
    that disagrees with itself eventually.
    """
    @spec poll_key(%__MODULE__{}) :: atom()
    def poll_key(%__MODULE__{poll_as: nil, column: column}), do: column
    def poll_key(%__MODULE__{poll_as: poll_as}), do: poll_as
  end

  @slice %Spark.Dsl.Entity{
    name: :slice,
    target: Slice,
    args: [:column],
    describe:
      "A dimension a human may select this node by — `slice :fiscal_year` — so a UI can " <>
        "offer 'reprocess just this year'. Distinct from `recompute_by`, which is the unit " <>
        "a CHANGE invalidates; this is the unit a PERSON picks, and they are rarely the " <>
        "same. `values:` enumerates the options so the control is a choice rather than a " <>
        "text box.",
    schema: [
      column: [
        type: :atom,
        required: true,
        doc: "an attribute on THIS node's resource, filtered with `==` to select rows."
      ],
      values: [
        type: {:or, [{:list, :any}, {:tuple, [:atom, :atom, {:list, :any}]}]},
        required: false,
        doc:
          "the selectable options: a literal list, or an `{module, function, args}` " <>
            "returning one. Only the host knows which values exist — omit it and a UI " <>
            "must take free text."
      ],
      label: [
        type: :string,
        required: false,
        doc: "what to call this dimension in a UI (default: the column name)."
      ],
      poll_as: [
        type: :atom,
        required: false,
        doc:
          "what to call this dimension when asking the SCANNER for it (default: the " <>
            "column name). A scanner's option is its own vocabulary — a crawler taking " <>
            "`fiscal:` over a `:fiscal_year` column — so a source can be asked for one " <>
            "slice without renaming its arguments after a storage decision."
      ]
    ]
  }

  defmodule Lapse do
    @moduledoc """
    What a machine recompute does to a HUMAN's mark: `lapse :approved_at,
    when_changed: :any`.

    The default needs no declaration and is SURVIVAL. The payload write only sets
    what the computation emits, so a column the upsert action does not `accept`
    is never touched — a transcript correction is about the *recording*, and
    re-extracting it does not make "you misheard that name" any less true.

    Survival is wrong for a sign-off. "I checked this" is a claim about content,
    and when the content moves the claim is stale. That is what this declares:

        lapse :approved_at,   when_changed: :any
        lapse :signed_off_by, when_changed: [:total, :vote_count]
        lapse MyApp.Correction, key: :meeting_id, when_changed: [:speaker_ids]

    ## Its own comparison, not the propagate verdict

    `when_changed:` runs a SECOND content comparison, narrowed to the fields
    named. This is not a convenience — the two grains are genuinely independent.
    A recompute can be `:changed` overall (so it propagates) while the lapse
    fields sat still, and the mark must then survive: a spelling fix that moves
    `:label` leaves an approval of `:total` standing. Reusing the propagate
    verdict would clear every approval on every cosmetic edit, and an approval
    that lapses constantly stops being read as information.

    ## Its own write, ordered after the payload

    A lapse is a SEPARATE write, made after the payload create and only when it
    fires. That ordering is what keeps survival free. Folding the nulling into
    the payload upsert's attrs would require the payload action to `accept` the
    human column — and it would then null that column on EVERY pass, destroying
    the default the feature is built around. So lapse needs an action of its own
    (`lapse_action:`, default `:lapse`) accepting the lapsing attributes; for a
    child resource, a destroy action.

    A failure to clear is LOGGED, never raised: the payload write runs inside the
    drain's per-cell savepoint, and a raise in a nested transaction aborts the
    outer one — a mark that could not be cleared must not cost the recompute that
    moved the content. Everything checkable without writing (a missing attribute,
    a child resource with no destroy action, an `over:` naming a unit this node
    does not declare) raises at compile time or assembly instead, which is off
    the hot path.

    ## `:created` never lapses

    No prior record means no mark. A lapse is a comparison against what was
    there, and on the pass that first creates the row there is nothing to
    compare and nothing to clear.
    """
    defstruct [
      :target,
      :when_changed,
      :key,
      :over,
      :lapse_action,
      :__identifier__,
      :__spark_metadata__
    ]
  end

  @lapse %Spark.Dsl.Entity{
    name: :lapse,
    target: Lapse,
    args: [:target],
    describe:
      "What a machine recompute does to a HUMAN's mark — `lapse :approved_at, when_changed: " <>
        "[:total]`. The default (no declaration) is SURVIVAL, and costs nothing: the payload " <>
        "action never accepts the human column, so the normal path cannot touch it. Declare " <>
        "this when the mark is a CLAIM ABOUT CONTENT — a sign-off is stale the moment the " <>
        "figures move. `when_changed:` runs its OWN comparison, narrowed to the fields named: " <>
        "a recompute can be `:changed` overall while those fields sat still, and the mark " <>
        "then survives, so a spelling fix does not clear an approval of the totals.",
    schema: [
      target: [
        type: {:or, [:atom, :module]},
        required: true,
        doc:
          "WHAT is cleared: an attribute on this resource (nulled), or a child RESOURCE " <>
            "whose rows attached to the lapsing key are destroyed (then `key:` is required). " <>
            "The two are indistinguishable at parse time — both are atoms — so which one " <>
            "this is resolves at assembly, against the resource's attributes."
      ],
      when_changed: [
        type: {:or, [{:one_of, [:any]}, {:list, :atom}]},
        required: true,
        doc:
          "WHEN it is cleared: `:any` whenever the computed content moves at all, or a list " <>
            "of the fields the mark was actually ABOUT. The narrow form is worth the thought " <>
            "it takes — `:any` is safe and will clear approvals for reasons nobody considers " <>
            "meaningful (a re-ordered label, a rounding change), and a mark that lapses " <>
            "constantly stops being read as information. Required rather than defaulted: the " <>
            "grain is the whole decision, and a default would be made silently."
      ],
      key: [
        type: :atom,
        required: false,
        doc:
          "for a child RESOURCE: the child's column holding this node's cell key. Required " <>
            "rather than inferred, because a resource may reference a node by more than one " <>
            "column and guessing wrong here DELETES THE WRONG ROWS."
      ],
      over: [
        type: :atom,
        required: false,
        doc:
          "SET-GRAIN: the mark lives once over a whole unit — `over: :fy` for \"I approve " <>
            "fiscal year 2026\" — rather than on each of nine hundred rows. Must name the " <>
            "unit this node declares with `recompute_by`, verified at compile time: the graph " <>
            "knows how to invalidate a `recompute_by` unit, so \"what exactly did I approve\" " <>
            "has an answer the substrate can also act on. A sign-off over a set the graph has " <>
            "no name for is a promise nobody can keep. A set-grain mark lapses when ANY " <>
            "member moves, which is correct (you approved a total that no longer holds) and " <>
            "broad — pair it with a narrow `when_changed:` list."
      ],
      lapse_action: [
        type: :atom,
        required: false,
        doc:
          "the action the clearing write goes through: an `update` accepting the lapsing " <>
            "attributes (default `:lapse`), or for a child resource a `destroy` (default " <>
            "`:destroy`). It is deliberately NOT the payload action — that one must never " <>
            "accept the human column, or every recompute would null it and survival would " <>
            "stop being the default."
      ]
    ]
  }

  defmodule Poll do
    @moduledoc """
    This node's rows come from OUTSIDE the graph: `poll MuniWatch.Crawler`.

    A source is an ordinary node whose rows a scanner writes rather than a
    combinator computing them. `ReactiveDag.Source` is the behaviour that
    fetches them, and it deliberately runs outside the drain — external I/O must
    not sit inside a depth-ordered recompute.

    Everything downstream is then an ORDINARY EDGE. A leaf holding one kind of
    what the crawl found reads the source like any other input:

        # the source
        reactive do
          id :agenda_center
          poll MuniWatch.Sources.AgendaCenter, every: "0 12 * * *", args: [recent: true]
        end

        # a consumer of it
        reactive do
          id :agenda_docs
          reduce over: :agenda_center, group_by: :key, expand: &agenda_only/2
        end

    This replaced `scan Mod` on each fed leaf plus `leaf_cells/1` on the module
    plus `verify_scan!/3` to police the two agreeing — the same fact written
    twice, with a verifier for when the copies drifted. The cells a source feeds
    are now `plan.parents[id]`, which cannot disagree with anything.

    `every:` and `args:` belong here rather than on the fed leaves, because they
    describe the POLL: one crawl has one cadence and one bound. Spread across
    leaves they had to be reassembled, and a source feeding two leaves could
    silently lose the args one of them declared.
    """
    defstruct [:module, :args, :every, :__identifier__, :__spark_metadata__]
  end

  @poll %Spark.Dsl.Entity{
    name: :poll,
    target: Poll,
    args: [:module],
    describe:
      "The `ReactiveDag.Source` that fetches this node's rows. Makes the node a source: " <>
        "its rows come from outside the graph, and everything reading it is an ordinary " <>
        "edge. `Source.poll_all/2` and `crontab/2` find sources from the plan.",
    schema: [
      module: [
        type: :atom,
        required: true,
        doc:
          "a module implementing `ReactiveDag.Source`. Verified at assembly — it must " <>
            "implement the behaviour."
      ],
      args: [
        type: :keyword_list,
        required: false,
        doc:
          "the STANDING options for a routine poll, merged into `Source.poll_all/2`'s opts " <>
            "with the caller's winning. This is where a cheap default lives: a crawler whose " <>
            "full pass costs a request per board per year declares `args: [recent: true]`, so " <>
            "the routine call stays `poll_all(plan)` and no call site can forget the bound.\n\n" <>
            "A VALUE may be a zero-arity function, which `poll_all/2` and `Source.scan/3` " <>
            "call at poll time: `args: [recent: true, year: &MyApp.Clock.year/0]`. This list " <>
            "is DSL data frozen at compile time, so a bound that depends on the clock has to " <>
            "be deferred or it is correct on the day of the build and quietly wrong after. " <>
            "`Source.controls/1` and `Source.scan_jobs/1` report the function itself — " <>
            "describing a graph does not run the host's code."
      ],
      every: [
        type: :string,
        required: false,
        doc:
          "how often a routine poll SHOULD run, as a cron expression. The library never " <>
            "schedules anything: `Source.crontab/2` collects these into data the host hands " <>
            "to its own scheduler. One poll, one cadence."
      ]
    ]
  }

  defmodule Run do
    @moduledoc """
    The ASH-NATIVE escape hatch: `run :recompute_keys` declares that this
    node's recompute is a GENERIC action on its own resource — one step less
    escape than a `compute` module, because the computation stays a first-class
    Ash action (arguments, policies, `Ash.run_action` testability).

    The contract: the action returns the CHANGED keys (`{:array, :string}`;
    `[]` for none). The library passes only the arguments the action declares —
    `keys` (`{:array, :string}`, allow_nil — nil means whole-cell) and
    `cell_id` (`:string`; generator instances need it, since one resource
    expands to many cells). The returned keys are what propagates. The action owns its
    DOMAIN writes.
    """
    defstruct [:action, :__identifier__, :__spark_metadata__]
  end

  @run %Spark.Dsl.Entity{
    name: :run,
    target: Run,
    args: [:action],
    describe:
      "Ash-native escape hatch: this node's recompute is the named GENERIC action on its " <>
        "own resource — `(keys, cell_id) -> changed keys`; the action writes its domain, " <>
        "the returned keys are what propagates.",
    schema: [
      action: [
        type: :atom,
        required: true,
        doc:
          "a generic action on THIS resource returning the changed keys " <>
            "(`{:array, :string}`); it may declare `keys` and/or `cell_id` arguments"
      ]
    ]
  }

  defmodule Aggregate do
    @moduledoc """
    A PURE-ASH-QUERY reduce: the datastore does the grouping via a RELATIONSHIP
    aggregate. The node's own resource is the group's resource — ONE row per group
    — and `over` names its `has_many` to the rows being aggregated. The library loads
    the aggregates in ONE Ash query — Postgres computes the `GROUP BY` — and each
    parent row's aggregate values are its payload. No rows cross into the BEAM; no
    `into`/`read`/`upsert` (contrast the in-BEAM `reduce`, which loads every row).

    Only expressible as a relationship aggregate: the group must be a resource with
    a relationship to the input. Arbitrary attribute `GROUP BY` is NOT in Ash 3.x's
    read API — use the in-BEAM `reduce` (or a `compute` module) for that.

    Each aggregate maps a kind (`:count | :sum | :avg | :min | :max | :first`) to a
    `{source_field, dest_attribute}` (or, for `:count`, just a dest attribute):

        aggregate over: :dmr_reports,          # has_many on THIS resource
                  count: :day_count,           # count(dmr_reports) → :day_count
                  avg: [flow: :avg_flow],      # avg(dmr_reports.flow) → :avg_flow
                  max: [flow: :peak_flow]
    """
    defstruct [
      :over,
      :count,
      :sum,
      :avg,
      :min,
      :max,
      :first,
      :__identifier__,
      :__spark_metadata__
    ]
  end

  # ONE list, shared with the in-BEAM fold: `aggregate` and `reduce into:` speak
  # the SAME vocabulary (same kinds, same `[src: dest]` spelling, same SQL nil
  # semantics), so moving a fold between the datastore and the BEAM cannot
  # change the answer. Declared once so the two can never drift.
  @agg_kinds ReactiveDag.Node.Recompute.Declarative.fold_kinds()

  @aggregate %Spark.Dsl.Entity{
    name: :aggregate,
    target: Aggregate,
    describe:
      "Pure-Ash relationship aggregate: the datastore groups + aggregates the `over` relationship in one query; each parent row IS a payload row.",
    schema:
      [
        over: [
          type: :atom,
          required: true,
          doc: "a `has_many`/`has_one` relationship on THIS resource — the rows to aggregate per group."
        ]
      ] ++
        Enum.map(@agg_kinds, fn kind ->
          {kind,
           [
             type: {:or, [:atom, :keyword_list]},
             doc:
               "#{kind}: an attribute name (for :count) or `[source_field: dest_attribute]` mapping the aggregate onto this resource's attributes."
           ]}
        end)
  }

  @reactive %Spark.Dsl.Section{
    name: :reactive,
    describe: "Declares this resource as a reactive-DAG node: its op + dependencies.",
    # PATCHABLE: a host extension may add its own domain entities into this section
    # via `Spark.Dsl.Patch.AddEntity{section_path: [:reactive], entity: …}` — the
    # composition seam that lets a domain vocabulary (e.g. a compliance
    # `guarantee`/`control`) be authored INSIDE `reactive do … end` alongside the
    # lib's combinators, rather than as a separate fork DSL. Domain entities the
    # lowering doesn't recognise ride in the node's `meta`; the host reads them back
    # via `Spark.Dsl.Extension.get_entities/2` (its own transformers/introspection).
    patchable?: true,
    # Computation is declared with an ENTITY: `reduce`/`join`/`aggregate`
    # (declarative) or `compute Module` (the escape hatch). legs (ref/compose) are
    # the nested dependency form; dep is the flat form.
    entities: [
      @ref,
      @context,
      @recompute_by,
      @compose,
      @reduce,
      @join,
      @per_key,
      @union,
      @aggregate,
      @compute,
      @run,
      @poll,
      @slice,
      @lapse
    ],
    schema: [
      id: [
        type: :atom,
        doc: "the cell id; defaults to the resource module's short name, snake_cased"
      ],
      op: [
        type: :atom,
        doc:
          "OPTIONAL free-atom label. Recompute dispatches on the `reduce`/`join`/`aggregate`/`compute` entity + `meta` shape, NEVER on `op` — so `op` is documentation. See `ReactiveDag.Cell`."
      ],
      key_rule: [
        type: {:one_of, [:identity, :all]},
        default: :identity,
        doc:
          "how a child key maps to this cell's key on propagation: `:identity` (same " <>
            "key) or `:all` (whole-cell). Group-grain claims (`:group`, " <>
            "`{:group, from: :key}`) are declared ON the combinator — the claim grain " <>
            "and the computation it must agree with belong in one unit."
      ],
      leaf?: [type: :boolean, default: false, doc: "true for a source-fed leaf (no compute)"],
      retain_if_vanished: [
        type: {:or, [:boolean, :keyword_list]},
        default: false,
        doc:
          "keep the row when a scan stops returning its key, instead of destroying it. For a " <>
            "leaf whose upstream WITHDRAWS items but whose artifacts you keep — the listing " <>
            "dropped the document, the PDF you fetched is still yours. " <>
            "`true` keeps the row untouched, and the key is NOT reported as changed: nothing " <>
            "about the row moved, so from a consumer's side nothing happened — and reporting " <>
            "it would report it again on every poll forever, since nothing marks it handled. " <>
            "`mark: fun` keeps the row AND records that the upstream dropped it " <>
            "(`retain_if_vanished mark: &MyApp.tombstone/1`, receiving the vanished keys); " <>
            "because something was written, the keys DO propagate. That is the whole " <>
            "decision — *do we write something to say it is gone?* — with propagation " <>
            "following from the answer rather than being a separate switch. Leave it false " <>
            "on a DERIVED node, where a row whose inputs are gone is stale, not archival."
      ],
      dirties_on: [
        type: {:list, {:one_of, [:create, :update, :destroy]}},
        required: false,
        doc:
          "make ordinary Ash writes trigger the cascade: a `:create`/`:update`/`:destroy` " <>
            "on THIS resource marks the written record's key dirty on this cell, so the " <>
            "next drain picks it up. Without it a host must call " <>
            "`ReactiveDag.Frontier.mark_dirty/3` at every write site, and a missed call " <>
            "is silent staleness. Wired as an `after_action` change, so the mark runs " <>
            "INSIDE the write's transaction — a rolled-back write leaves no dirty key, " <>
            "and a committed one always leaves one. (A NOTIFIER cannot promise that: " <>
            "Ash dispatches notifications after commit.) Opt-in, and not implied by " <>
            "`leaf?` — a leaf fed by a `ReactiveDag.Source` poll would double-trigger. " <>
            "Marks but does not SCHEDULE: pair with `schedule_drain: true` unless " <>
            "something else already drains on a cadence you are happy to wait for. " <>
            "For a SOURCE-FED LEAF, where every write is an observation; a COMPUTED " <>
            "node whose rows the library writes itself needs `augmented_by`, which " <>
            "names actions rather than types and so cannot catch the payload upsert."
      ],
      augmented_by: [
        type: {:list, :atom},
        required: false,
        doc:
          "make a HUMAN EDIT on a COMPUTED node trigger the cascade: a write through any " <>
            "of these named ACTIONS on this resource marks the written record's key dirty " <>
            "on this cell, so the next drain re-runs everything downstream of the " <>
            "correction. The human edit attaches to the node's key by construction — the " <>
            "write goes through the node's own action, on the node's own row.\n\n" <>
            "`dirties_on` cannot do this job. It names action TYPES and wires ONE GLOBAL " <>
            "change, which is right for a source-fed LEAF (every write is an observation, " <>
            "and no write site can be forgotten) and unusable on a computed node: the " <>
            "library writes that node's rows itself through `payload_action`, which is an " <>
            "ordinary Ash write, so a global change would make every recompute re-dirty " <>
            "the cell it just computed — an infinite drain. Naming ACTIONS excludes the " <>
            "payload upsert by construction rather than by a filter someone must " <>
            "maintain, and naming it here is rejected at compile time.\n\n" <>
            "Wired as an `after_action` change on each named action, so the mark runs " <>
            "INSIDE the write's transaction, exactly as `dirties_on` does — a rolled-back " <>
            "correction leaves no dirty key, a committed one always leaves one. Composes " <>
            "with `schedule_drain: true` (same meaning: enqueue the drain in the same " <>
            "transaction) and with `dirties_on` on a leaf that is ALSO human-augmented; " <>
            "an action covered by both marks ONCE."
      ],
      schedule_drain: [
        type: :boolean,
        required: false,
        default: false,
        doc:
          "with `dirties_on` or `augmented_by`, enqueue a `ReactiveDag.DrainWorker` job in the SAME " <>
            "transaction as the mark — so a write is promptly reflected rather than " <>
            "merely durable. Without it the mark waits for whatever drains next (in " <>
            "practice the hourly sweep), which for a write-fed leaf with no polling " <>
            "source on that cadence may be no drain at all (u2i/reactive_dag#142). " <>
            "The enqueue is one INSERT and joins the write's transaction, so a " <>
            "rolled-back write schedules nothing; the DRAIN itself runs later, out " <>
            "of the request. A burst of N writes coalesces to one pending job. " <>
            "Requires Oban (an optional dependency) — without it the option raises " <>
            "at compile time rather than silently marking and never draining. " <>
            "Default false: existing hosts keep today's behaviour."
      ],
      fingerprint: [
        type: {:or, [{:fun, 1}, {:list, :atom}]},
        required: false,
        doc:
          "for a SOURCE-FED LEAF: the one value that decides whether an observation " <>
            "moved — input fields (their hash), or `(row -> value)` for a computed " <>
            "digest. `ReactiveDag.Node.Rows.reconcile/3` compares it instead of every " <>
            "attribute, so a row's `last_seen_at`/`etag` moving does not fire the " <>
            "cascade. Stored in `fingerprint_attribute` (default `:fingerprint`). " <>
            "Without it a re-observed row compares on ALL its attributes, which for a " <>
            "leaf reports a change on every poll."
      ],
      fingerprint_attribute: [
        type: :atom,
        required: false,
        doc: "the attribute a leaf's `fingerprint` is stored in (default `:fingerprint`)."
      ],
      compare: [
        type: {:list, :atom},
        required: false,
        doc:
          "which of this node's columns constitute its RESULT — the payload write compares " <>
            "these and no others when deciding whether a key changed. Omit it and every " <>
            "field the row carries is compared, which is right when every field is part of " <>
            "the answer.\n\nDeclare it when the row carries fields that are part of the " <>
            "RECORD but not the result: `doc_id` (which document this came from), `ordinal` " <>
            "(position in the source, which a re-parse shifts without changing anything), a " <>
            "`match_key` a downstream join builds. Comparing those reports a change nothing " <>
            "made, and a spurious change re-runs every fold downstream — the cost that makes " <>
            "a cascade O(graph) instead of O(real changes).\n\nDistinct from `fingerprint`, " <>
            "which stores a digest and is for a LEAF, whose row carries fields that move on " <>
            "every observation (`last_seen_at`, an `etag` a server re-issues). A digest of " <>
            "columns already on the row earns nothing when the comparison can read them, so " <>
            "a derived node wants this and a leaf wants that. `fingerprint` wins if both are " <>
            "declared.\n\nOn an `aggregate` node it is INERT unless the node declares two or " <>
            "more aggregates: that path builds the row from the key column plus each " <>
            "aggregate's `dest` and nothing else, so there is no bookkeeping column on it to " <>
            "narrow past. It bites when one aggregate is the result and another is not — a " <>
            "`count` that moves when a re-parse splits readings without shifting the `avg`."
      ],
      payload_key: [
        type: :atom,
        doc:
          "the resource attribute the cell key writes to when the library closes the " <>
            "payload loop. DERIVED like Ash derives keys: defaults to the resource's " <>
            "single-attribute primary key (else `:key`) — declare it only for a " <>
            "non-primary key column. A COMPOSITE primary key needs no payload_key at " <>
            "all: the row is upserted by its identity and the cell key is the " <>
            "identity's serialization."
      ],
      payload_action: [
        type: :atom,
        default: :upsert,
        doc: "the Ash upsert action used to write the node's own payload (default `:upsert`)."
      ],
      source: [
        type: :atom,
        doc: "convenience: a leaf's source binding id (also merged into meta)"
      ],
      meta: [
        type: :keyword_list,
        default: [],
        doc:
          "OPEN host binding — anything the host's recompute/refresh needs and the library never interprets (cascade: source/driver; portal leaf: source/check/org_kind/strength/attest_for/cadence). Merged into the cell's meta."
      ],
      for_each: [
        type: :atom,
        doc:
          "GENERATOR: expand this node per member of a named population (resolved by the :for_each member-fetcher passed to graph). The template itself builds no cell; each member builds an instance <id>.<member>."
      ],
      over: [
        type: :atom,
        doc:
          "SECOND-ORDER: this node's population is computed from the graph itself (e.g. :findings | :register | :controls). Carried in meta for a host post-build hook; the library does not resolve it."
      ],
      companion: [
        type: :keyword_list,
        doc: """
        TWO-CELL node: emit a COMPANION cell at this node's id (`<id>`) as a derived
        VIEW over the node's op-tree, and re-root the tree at `<id>/<suffix>` (default
        suffix `"set"`). The companion takes the tree root as its sole input and
        carries a host-chosen `op:` as a label (e.g. a status FILTER
        that keeps only the violation rows). Options:

          * `op:` (atom, required) — the companion cell's label.
          * `id_suffix:` (string, default `"set"`) — the tree-root suffix under `<id>`.
          * `meta:` (keyword) — extra meta on the companion cell (e.g. `watched?: true`).

        The general pattern: a node PLUS a derived projection of it, both addressable —
        the shape a THREE-VALUED verdict needs (the tree holds all evaluated members;
        the companion holds only the problem rows; a reader consults both to tell
        "green" from "never evaluated"). The library provides the two-cell STRUCTURE +
        id rooting; the host provides the companion's recompute (via `op:`) and any
        read-side disambiguation. Not compatible with `for_each` (a generator has no
        single companion) or `leaf?`.
        """
      ],
      depends_on: [
        type: {:list, :any},
        default: [],
        doc:
          "input node ids (flat form; equivalent to a `ref` per id). An entry may be " <>
            "an atom."
      ]
    ]
  }

  use Spark.Dsl.Extension,
    sections: [@reactive],
    transformers: [ReactiveDag.Node.Transformers.AddMarkDirty],
    verifiers: [ReactiveDag.Node.Verifiers.VerifyReactive]

  # ── introspection + graph assembly ────────────────────────────────────────

  alias Spark.Dsl.Extension, as: Ext

  @doc "The cell id for a node resource (explicit `id`, else the module's snake short-name)."
  @spec cell_id(module()) :: atom()
  def cell_id(resource) do
    Ext.get_opt(resource, [:reactive], :id, nil) || default_id(resource)
  end

  @doc """
  Assemble a `ReactiveDag.Plan` from a list of node resources. Each resource
  contributes its root cell PLUS an intermediate cell per nested `compose` leg
  (lowered through `ReactiveDag.Lowering.walk`). The union is validated and
  depth-ordered by `ReactiveDag.Graph.build/1`.

  Pass `:for_each` to expand GENERATOR nodes: a `(population_atom -> [member])`
  fun. A node with `for_each: :pop` builds no template cell — instead, for each
  member it builds an instance sub-tree rooted at `<id>.<member.id>`, with the
  member's `meta` merged onto every instance cell (the host's per-member stamp,
  e.g. a probe filter). A member is any map with an `:id` (+ optional `:meta`).
  Without a fetcher, a generator node is skipped (and logged by the caller).
  """
  @spec graph([module()], keyword()) :: ReactiveDag.Plan.t()
  def graph(resources, opts \\ []) do
    fetch = Keyword.get(opts, :for_each)

    resources
    |> Enum.flat_map(&cells(&1, fetch))
    |> resolve_reads()
    |> ReactiveDag.Graph.build()
    |> verify_scans!()
  end

  # `poll Mod` makes a node a SOURCE. Two things are worth failing at assembly;
  # a third used to be here and no longer can be.
  #
  # The one that went: "this scanner disowns this leaf". That error existed
  # because the pairing was written twice — `scan Mod` on the leaf and
  # `leaf_cells/1` on the module — and it caught the copies drifting. The cells
  # a source feeds are now `plan.parents[id]`, so there is one declaration and
  # nothing to disagree with.
  defp verify_scans!(%ReactiveDag.Plan{} = plan) do
    for {id, cell} <- plan.cells, mod = cell.meta[:scan] do
      # A scanner writes a cell's tuples from OUTSIDE the graph; a combinator
      # computes them from its inputs. Declaring both on one node means the poll
      # and the drain overwrite each other — the drain reprices from inputs and
      # discards whatever the poll wrote. That is never intended, so it fails
      # here rather than as data that mysteriously reverts.
      if computation =
           cell.meta[:reduce] || cell.meta[:join] || cell.meta[:per_key] ||
             cell.meta[:aggregate] || cell.meta[:run] || cell.meta[:compute] do
        raise ArgumentError,
              "reactive_dag: cell #{inspect(id)} declares `poll #{inspect(mod)}` AND a " <>
                "computation (#{inspect(kind_of(computation, cell))}). A scanner writes " <>
                "this cell's rows from outside the graph; a computation derives them " <>
                "from its inputs — declared together, the poll and the drain overwrite " <>
                "each other. Keep the poll (making this a source), or drop it and let the " <>
                "node compute."
      end

      ReactiveDag.Source.verify_poll!(mod, id)
    end

    verify_one_node_per_source!(plan)

    plan
  end

  # A source is a NODE, so one scanner belongs to one cell.
  #
  # Before rc.21 the same module on several leaves was the fan-out shape — the
  # scanner declared `leaf_cells/1` and wrote them all. Now everything reading a
  # source is an ordinary edge, so two nodes naming one module means the upstream
  # is polled twice: `crontab/2` emits an entry per node, and a poll writing rows
  # for both cells does it once per entry.
  #
  # Nothing downstream can notice that — `cells_of/2` returns both, the plan
  # assembles, and the duplication only shows up as an upstream complaining about
  # request volume. So it fails here.
  defp verify_one_node_per_source!(%ReactiveDag.Plan{} = plan) do
    plan.cells
    |> Enum.filter(fn {_id, cell} -> cell.meta[:scan] end)
    |> Enum.group_by(fn {_id, cell} -> cell.meta[:scan] end, &elem(&1, 0))
    |> Enum.each(fn
      {_mod, [_one]} ->
        :ok

      {mod, ids} ->
        raise ArgumentError,
              "reactive_dag: #{inspect(mod)} is declared by #{length(ids)} nodes " <>
                "(#{ids |> Enum.sort() |> Enum.join(", ")}). A source is a node, so one " <>
                "scanner feeds one cell — and each of these gets its own crontab entry, " <>
                "so the upstream is polled #{length(ids)} times.\n\n" <>
                "If one crawl produces rows for several cells, keep the `poll` on ONE node " <>
                "and let the others read it:\n\n" <>
                "    reduce over: :#{ids |> Enum.sort() |> hd()}, group_by: :key, expand: &project/2\n\n" <>
                "A consumer declines what is not its own with `{:skip, key}`."
    end)

    plan
  end

  # which computation a cell declares, for the error above
  defp kind_of(_value, cell) do
    Enum.find([:reduce, :join, :per_key, :aggregate, :run, :compute], &cell.meta[&1])
  end

  # ── declarative-read resolution (graph assembly) ────────────────────────────
  #
  # A reduce/join whose `read:` is omitted (Ash-first default) or names a read
  # ACTION needs the OVER node's resource + payload key at recompute time —
  # cross-node facts per-resource lowering cannot know (recompute receives only
  # the cell). Like attestation requirements, they resolve HERE: the over cell's
  # resource/payload_key/read_action are stamped into the consuming cell's meta
  # as `over_source`, and everything checkable against the over resource
  # (action exists + is :read; declarative attribute atoms) is checked now —
  # assembly is the earliest point the over resource is known.
  defp resolve_reads(cells) do
    by_id = Map.new(cells, &{&1.id, &1})
    Enum.map(cells, &resolve_read_cell(&1, by_id))
  end

  # a UNION reads its inputs' ROWS, so like a declarative read it needs each
  # input's resource and key derivation — cross-node facts, resolved here.
  defp resolve_read_cell(%{meta: %{union: %{from: from}}} = cell, by_id) do
    sources =
      Map.new(from, fn input ->
        id = to_string(input)
        source = by_id[id]

        resource = source && source.meta[:resource]

        # a union reads ROWS, so an input needs somewhere to keep them. A node
        # with a resource but no attributes has a table in name only — that is
        # the tableless-verdict shape, and it would silently union nothing.
        if is_nil(resource) || Ash.Resource.Info.attributes(resource) == [] do
          raise ArgumentError,
                "reactive_dag: #{cell.id} unions over #{inspect(input)}, " <>
                  if(is_nil(source),
                    do: "which is not a cell in this graph.",
                    else:
                      "which has no rows to read — a union reads its inputs' rows, " <>
                        "and #{inspect(resource)} declares no attributes."
                  ) <>
                  " Point `from:` at nodes with payload attributes."
        end

        {id,
         %{
           resource: source.meta[:resource],
           payload_key: source.meta[:payload_key],
           identity_fields: source.meta[:identity_fields]
         }}
      end)

    %{cell | meta: Map.put(cell.meta, :union_sources, sources)}
  end

  # a TWO-INPUT join reads two different nodes, so it needs a source PER SIDE —
  # each with its own resource and payload key, because each is scoped by its own
  # claim keys. The one-input clause below stamps a single `over_source`; sharing
  # that between two sides is exactly the defect that got this shape reverted.
  defp resolve_read_cell(%{meta: %{join: %{left_over: l, right_over: r} = j}} = cell, by_id)
       when not is_nil(l) and not is_nil(r) do
    sides =
      Map.new([left: l, right: r], fn {side, input} ->
        {side, join_side_source!(cell, side, input, j, by_id)}
      end)

    %{cell | meta: Map.put(cell.meta, :side_sources, sides)}
  end

  defp resolve_read_cell(cell, by_id) do
    spec = cell.meta[:reduce] || cell.meta[:join] || per_key_read_spec(cell)

    case spec do
      %{read: read} ->
        over_id = to_string(spec.over)
        over = by_id[over_id]
        resource = over && over.meta[:resource]

        cond do
          is_nil(resource) ->
            raise ArgumentError,
                  "reactive_dag: #{cell.id} reads over #{inspect(spec.over)}, " <>
                    "but that cell has no backing resource to read" <>
                    if(is_nil(over), do: " (no such cell in this graph)", else: "") <>
                    " — a combinator read is an Ash read of the over node's resource. " <>
                    "Point `over:` at a resource-backed node, or use `run`/`compute` " <>
                    "for a non-Ash read."

          Ash.Resource.Info.attributes(resource) == [] ->
            raise ArgumentError,
                  "reactive_dag: #{cell.id} reads over #{inspect(spec.over)}, whose " <>
                    "resource #{inspect(resource)} declares no attributes — nothing to " <>
                    "read. Give the over node payload attributes, or use `run`/`compute`."

          true ->
            :ok
        end

        if read && is_nil(read_action(resource, read)) do
          raise ArgumentError,
                "reactive_dag: #{cell.id} names read action #{inspect(read)} on " <>
                  "#{inspect(resource)}, which has no such :read action. " <>
                  "Available: #{inspect(read_action_names(resource))}"
        end

        loads = declarative_loads!(cell, spec, resource)

        source = %{
          resource: resource,
          # nil for an IDENTITY-KEYED over (composite PK): its cell keys are
          # serialized identities, not a column — key scoping then stands down.
          payload_key: over.meta[:payload_key],
          read_action: read,
          load: loads,
          group_key_plan: group_key_plan(spec, resource)
        }

        %{cell | meta: Map.put(cell.meta, :over_source, source)}

      _ ->
        cell
    end
  end

  # One side of a two-input join, resolved against the graph. Mirrors the
  # one-input checks (cell exists, has a resource, the resource has attributes)
  # because a side that reads nothing would silently contribute no columns —
  # and with per-side ownership that reads as "my side saw nothing", which is a
  # much quieter wrong answer than a raise.
  defp join_side_source!(cell, side, input, j, by_id) do
    over_id = to_string(input)
    over = by_id[over_id]
    resource = over && over.meta[:resource]

    cond do
      is_nil(resource) ->
        raise ArgumentError,
              "reactive_dag: #{cell.id} joins #{side}_over: #{inspect(input)}, " <>
                "but that cell has no backing resource to read" <>
                if(is_nil(over), do: " (no such cell in this graph)", else: "") <>
                " — each side of a two-input join is an Ash read of that node's resource."

      Ash.Resource.Info.attributes(resource) == [] ->
        raise ArgumentError,
              "reactive_dag: #{cell.id} joins #{side}_over: #{inspect(input)}, whose " <>
                "resource #{inspect(resource)} declares no attributes — nothing to read."

      true ->
        :ok
    end

    %{
      resource: resource,
      payload_key: over.meta[:payload_key],
      read_action: nil,
      # ONLY this side's attrs, against THIS side's resource. The one-input
      # clause validates `left ++ right` against a single resource, which is
      # right when both sides come from one table and wrong here: the left's key
      # column need not exist on the right's resource.
      load: declarative_loads!(cell, %{j | left: side_spec(j, side), right: nil}, resource)
    }
  end

  defp side_spec(%{left: l}, :left), do: l
  defp side_spec(%{right: r}, :right), do: r

  # a `per_key` node reads its input exactly as a combinator does — it just has
  # no `read:`/`query:` of its own, so it borrows the shape resolve_read_cell/2
  # already understands. (`over` comes from `recompute_by to:`, which lowering
  # has already stamped as the cell's single input.)
  defp per_key_read_spec(%{meta: %{per_key: %PerKey{}}, inputs: [over]}),
    do: %{read: nil, over: String.to_atom(over)}

  defp per_key_read_spec(_cell), do: nil

  # the ONE cross-node fact behind every `:group` capability — an ordered plan
  # of what each group entry IS on the over resource:
  #   {:attr, name, string?}       — a plain attribute (string? gates equality scoping)
  #   {:calendar, kind, of_attr}   — a ReactiveDag.Calendar calculation (pure
  #                                  key resolution + date-range scoping)
  #   {:calc, name}                — an opaque calculation (lookup-resolvable only)
  # Only for a reduce with a declarative group and DEFAULT key derivation —
  # anything richer resolves by lookup and reads whole (or `query:`-scoped).
  defp group_key_plan(%Reduce{group_by: g, key: nil}, resource) when is_atom(g) or is_list(g) do
    g
    |> ReactiveDag.Node.Recompute.Declarative.group_children()
    |> Enum.map(fn name ->
      cond do
        attr = Ash.Resource.Info.attribute(resource, name) ->
          {:attr, name, attr.type == Ash.Type.String}

        calc = Ash.Resource.Info.calculation(resource, name) ->
          case calc.calculation do
            {ReactiveDag.Calendar, opts} -> {:calendar, opts[:bucket], opts[:of]}
            _ -> {:calc, name}
          end

        true ->
          {:calc, name}
      end
    end)
  end

  defp group_key_plan(_spec, _resource), do: nil

  defp read_action(resource, name) do
    case Ash.Resource.Info.action(resource, name) do
      %{type: :read} = action -> action
      _ -> nil
    end
  end

  defp read_action_names(resource) do
    for %{type: :read, name: n} <- Ash.Resource.Info.actions(resource), do: n
  end

  # a declarative group_by / left / right entry names an ATTRIBUTE or a
  # CALCULATION on the over resource (derived grouping values — a calendar
  # bucket, a normalized code — are Ash calculations, declared where the data
  # lives; see `ReactiveDag.Calendar`). Checkable only here, where the over
  # resource is known. Returns the calculations to `Ash.Query.load` at read.
  defp declarative_loads!(cell, spec, resource) do
    names =
      case spec do
        %Reduce{group_by: g} when is_atom(g) or is_list(g) ->
          ReactiveDag.Node.Recompute.Declarative.group_children(g)

        %Join{} = j ->
          side_attrs(j.left) ++ side_attrs(j.right)

        _ ->
          []
      end

    Enum.reduce(names, [], fn name, loads ->
      cond do
        Ash.Resource.Info.attribute(resource, name) ->
          loads

        Ash.Resource.Info.calculation(resource, name) ->
          [name | loads]

        true ->
          raise ArgumentError,
                "reactive_dag: #{cell.id} names #{inspect(name)}, which " <>
                  "#{inspect(resource)} has neither as an attribute nor a calculation. " <>
                  "Attributes: " <>
                  "#{inspect(Enum.map(Ash.Resource.Info.attributes(resource), & &1.name))}; " <>
                  "calculations: " <>
                  "#{inspect(Enum.map(Ash.Resource.Info.calculations(resource), & &1.name))}"
      end
    end)
    |> Enum.uniq()
  end

  defp side_attrs(side) when is_atom(side) and not is_nil(side), do: [side]

  defp side_attrs(side) when is_list(side),
    do: [Keyword.fetch!(side, :key) | Keyword.keys(Keyword.get(side, :where, []))]

  defp side_attrs(_fn_or_nil), do: []

  def cells(resource, fetch \\ nil) do
    case Ext.get_opt(resource, [:reactive], :for_each, nil) do
      nil ->
        base = cell_id(resource) |> to_string()

        case Ext.get_opt(resource, [:reactive], :companion, nil) do
          nil ->
            {_id, cells} = lower(resource, base, %{})
            cells

          companion ->
            # TWO-CELL node: the op-tree roots at `<id>/<suffix>`, and a companion
            # cell at `<id>` is a derived view over it (its sole input is the tree
            # root, carrying the host `op:`).
            suffix = Keyword.get(companion, :id_suffix, "set")
            {tree_root, tree_cells} = lower(resource, "#{base}/#{suffix}", %{})
            [companion_cell(base, tree_root, companion) | tree_cells]
        end

      pop when is_function(fetch, 1) ->
        # GENERATOR: template builds no cell; expand one instance per member.
        base = cell_id(resource) |> to_string()

        fetch.(pop)
        |> Enum.flat_map(fn member ->
          {_id, cells} = lower(resource, "#{base}.#{member.id}", Map.get(member, :meta, %{}))
          cells
        end)

      _pop ->
        # a generator with no fetcher supplied — build nothing (caller decides).
        []
    end
  end

  # lower a resource's reactive block rooted at `root_id`, merging `stamp` (a
  # per-member metadata map, empty for a non-generator) onto every cell.
  defp lower(resource, root_id, stamp) do
    {id, cells} = ReactiveDag.Lowering.walk(root_id, root_node(resource, root_id), walk_cbs())
    {id, Enum.map(cells, &%{&1 | meta: Map.merge(&1.meta, stamp)})}
  end

  # the COMPANION cell of a two-cell node: at `id`, a derived view over the op-tree
  # `tree_root` (its sole input), carrying the host `op:` (its recompute key) + meta.
  defp companion_cell(id, tree_root, companion) do
    %ReactiveDag.Cell{
      id: id,
      op: Keyword.fetch!(companion, :op),
      inputs: [tree_root],
      leaf?: false,
      meta: companion |> Keyword.get(:meta, []) |> Map.new()
    }
  end

  @doc "The root cell a NON-generator node resource lowers to."
  @spec to_cell(module()) :: ReactiveDag.Cell.t()
  def to_cell(resource) do
    id = cell_id(resource) |> to_string()
    resource |> cells() |> Enum.find(&(&1.id == id))
  end

  # ── lowering: the reactive block → a node the walk callbacks understand ─────
  # The root node and every `compose` leg share one internal shape:
  #   {:op, id, op, compute, key_rule, leaf?, resource, legs, extra}
  # where legs are the Ref/Compose entities and `extra` is the open host
  # binding merged into meta. `resource` is nil for a compose (an intermediate
  # cell has no backing resource). `root_id` lets a generator instance re-root.
  defp root_node(resource, root_id) do
    legs =
      Ext.get_entities(resource, [:reactive])
      |> Enum.filter(&(match?(%Ref{}, &1) or match?(%Context{}, &1) or match?(%Compose{}, &1)))

    flat_refs =
      Ext.get_opt(resource, [:reactive], :depends_on, [])
      |> Enum.map(&normalize_dep(&1, resource))

    # a `reduce`/`join over: :x` implies an input edge to :x (the node it reads);
    # so does `attested over: :x` (the raw cell the view attests). The `over`
    # BLOCK names the same edge — one input either way.
    combinator_refs =
      case combinator(resource) do
        nil ->
          []

        %Union{from: from} ->
          Enum.map(from, &%Ref{to: &1})

        # a TWO-INPUT join implies TWO edges — one per side, so a change on
        # either propagates through its own. This is why each side can be scoped
        # by its own keys: the claim arrives via the edge that moved.
        %Join{left_over: l, right_over: r} when not is_nil(l) and not is_nil(r) ->
          [%Ref{to: l}, %Ref{to: r}]

        c ->
          case Map.get(c, :over) || (recompute_by(resource) || %{to: nil}).to do
            nil ->
              raise ArgumentError,
                    "reactive_dag: the combinator on #{inspect(resource)} names no input — " <>
                      "declare `recompute_by :unit, to: :node, from: :field`, or `over: :node` on " <>
                      "the combinator itself."

            over ->
              [%Ref{to: over}]
          end
      end

    all_refs = legs ++ flat_refs ++ combinator_refs

    {:op, root_id, Ext.get_opt(resource, [:reactive], :op, nil), compute_module(resource), effective_key_rule(resource),
     Ext.get_opt(resource, [:reactive], :leaf?, false), resource, all_refs, extra_meta(resource, all_refs)}
  end

  # the combinator's `key_rule:` (declared WITH the computation it must agree
  # with) wins over the block-level one; the block level remains for nodes with
  # no combinator (run/compute/leaves).
  defp effective_key_rule(resource) do
    cond do
      # `recompute_by` IS the claim rule — the unit a change invalidates.
      u = recompute_by(resource) ->
        unit_key_rule(u)

      match?(%{key_rule: kr} when not is_nil(kr), combinator(resource)) ->
        combinator(resource).key_rule

      # A TWO-INPUT join's claim keys are JOIN keys, and an input's changed keys
      # are its OWN payload keys — different columns, usually different values
      # (an `Actuals` row `"a1"` joins on `acct: "5000"`). `:identity` would pass
      # `"a1"` through as a claim, which names no join key at all: the sides read
      # nothing and the row is reconciled away. So the claim rule is `:group` —
      # translate a changed row to the join key it belongs to.
      match?(%Join{left_over: l, right_over: r} when not is_nil(l) and not is_nil(r), combinator(resource)) ->
        :group

      true ->
        Ext.get_opt(resource, [:reactive], :key_rule, :identity)
    end
  end

  # a flat depends_on entry: `:id`.
  # optional `mode: :require | :annotate`).
  # ── `recompute_by` — the unit a change invalidates ──────────────────────────
  #
  # THE declaration the engine cares about. `recompute_by :category, from:
  # :expense_cat` states: a change to the input's `expense_cat` invalidates my
  # `category` unit. From that ONE fact come the input edge, the grouping, the
  # claim rule and the read scope — which is why it replaces `key_rule`
  # (the same unit, previously stated twice and required to agree).
  #
  # It is the RECOMPUTE unit, not the output's grain: they coincide for a plain
  # rollup and diverge when one unit emits many rows (percentiles per day).
  # Consumed at COMPILE time — lowered to `over:` + `group_by:` + `key_rule`,
  # and never traversed at recompute.

  @doc false
  # the node's `recompute_by` declaration, or nil.
  def recompute_by(resource) do
    Ext.get_entities(resource, [:reactive]) |> Enum.find(&match?(%RecomputeBy{}, &1))
  end

  # SUGAR → CORE: fold `recompute_by` into the combinator, so exactly one shape
  # reaches assembly, recompute, the key rules and the verifiers. The unit
  # supplies the edge, the grouping AND the key_rule — nothing downstream knows
  # the declaration existed.
  defp lower_recompute_by(%Reduce{} = r, resource) do
    case recompute_by(resource) do
      nil ->
        r

      %RecomputeBy{} = u ->
        over = edge!(u, r, resource)

        %{
          r
          | over: over,
            group_by: r.group_by || RecomputeBy.group_by(u),
            key_rule: r.key_rule || unit_key_rule(u),
            read: r.read || u.read
        }
    end
  end

  # the input node: named by `recompute_by to:` or by the combinator's `over:`,
  # never both.
  defp edge!(%RecomputeBy{to: nil}, %{over: nil}, resource) do
    raise ArgumentError,
          "reactive_dag: #{inspect(resource)} names no input — give `recompute_by` a " <>
            "`to:` (the input node id), or the combinator an `over:`."
  end

  defp edge!(%RecomputeBy{to: to}, %{over: over}, resource)
       when not is_nil(to) and not is_nil(over) do
    raise ArgumentError,
          "reactive_dag: #{inspect(resource)} names the input TWICE — " <>
            "`recompute_by to: #{inspect(to)}` and `over: #{inspect(over)}`. Declare it once."
  end

  defp edge!(%RecomputeBy{to: to}, %{over: over}, _resource), do: to || over

  # the unit as the claim rule it replaces.
  defp unit_key_rule(%RecomputeBy{unit: :cell}), do: :all
  defp unit_key_rule(%RecomputeBy{from_key: true}), do: {:group, from: :key}
  defp unit_key_rule(%RecomputeBy{}), do: :group

  defp normalize_dep(id, _resource) when is_atom(id), do: %Ref{to: id}

  defp normalize_dep(other, resource) do
    raise ArgumentError,
          "reactive_dag: bad depends_on entry #{inspect(other)} on #{inspect(resource)} — " <>
            "expected an atom id"
  end

  # the escape-hatch `compute Module` entity's module, or nil. An `attested` node
  # defaults to the lib's attestation Op (an explicit `compute` still wins).
  defp compute_module(resource) do
    case Ext.get_entities(resource, [:reactive]) |> Enum.find(&match?(%Compute{}, &1)) do
      %Compute{module: m} -> m
      nil -> nil
    end
  end

  # the node's `attested over: … requirement: …` entity, or nil.
  # the node's `attestation :name do … end` requirement declarations.
  # the node's declarative combinator entity whose `over` is an INPUT NODE (Reduce
  # or Join) — this drives the implicit input edge. NOT Aggregate: its `over` is a
  # relationship on this resource, not another cell, so it adds no edge.
  defp combinator(resource) do
    Ext.get_entities(resource, [:reactive])
    |> Enum.find(
      &(match?(%Reduce{}, &1) or match?(%Join{}, &1) or match?(%PerKey{}, &1) or
          match?(%Union{}, &1))
    )
  end

  # the node's Aggregate entity (a relationship aggregate), or nil.
  defp aggregate(resource) do
    Ext.get_entities(resource, [:reactive]) |> Enum.find(&match?(%Aggregate{}, &1))
  end

  # Normalise the retention declaration to `nil | :keep | {:mark, fun}`. The two
  # forms are one decision — *do we write something to say it is gone?* — and
  # whether the key propagates follows from that rather than being separate.
  defp retain_policy(resource) do
    case Ext.get_opt(resource, [:reactive], :retain_if_vanished, false) do
      true ->
        :keep

      opts when is_list(opts) ->
        case Keyword.get(opts, :mark) do
          fun when is_function(fun, 1) ->
            {:mark, fun}

          other ->
            raise ArgumentError,
                  "reactive_dag: `retain_if_vanished` takes `true` or `mark: fun/1`, got " <>
                    "#{inspect(other)}. `mark:` receives the vanished keys and records that " <>
                    "the upstream dropped them."
        end

      _ ->
        nil
    end
  end

  # The dimensions a human may select this node by, validated at assembly: a
  # slice naming a column the resource lacks would render a control that filters
  # on nothing and silently selects every row.
  defp slices(resource) do
    for %Slice{} = sl <- Ext.get_entities(resource, [:reactive]) do
      cond do
        Ash.Resource.Info.attributes(resource) == [] ->
          # A slice SELECTS rows, and this node has none of its own —
          # `Rows.keys_where/2` queries the node's resource, so every button
          # would select nothing. Worth naming precisely: "no such attribute" is
          # true but misleading when the answer is "no attributes at all".
          raise ArgumentError,
                "reactive_dag: `slice #{inspect(sl.column)}` on #{inspect(resource)}, which " <>
                  "keeps no rows of its own (no attributes). A slice filters this node's " <>
                  "OWN rows, so there is nothing here to select. Declare it on the node " <>
                  "that holds the rows."

        is_nil(Ash.Resource.Info.attribute(resource, sl.column)) ->
          raise ArgumentError,
                "reactive_dag: `slice #{inspect(sl.column)}` on #{inspect(resource)}, which " <>
                  "has no such attribute. A slice filters this node's own rows by that " <>
                  "column, so it must be one of them."

        true ->
          :ok
      end

      # `poll_as` is only meaningful on a node that HAS a scanner: it names the
      # option a poll is asked with, and a node nothing polls will never be
      # asked. Declaring it there is a statement about a source that isn't
      # there — most often the slice landed on the derived node instead of the
      # source feeding it, which is exactly the mistake worth catching at
      # assembly rather than at 3am when the button does nothing.
      #
      # NOT validated against the resource's attributes: it deliberately names
      # the SCANNER's vocabulary, so it usually is not a column here at all.
      polls? = Ext.get_entities(resource, [:reactive]) |> Enum.any?(&match?(%Poll{}, &1))

      if sl.poll_as && not polls? do
        raise ArgumentError,
              "reactive_dag: `slice #{inspect(sl.column)}, poll_as: #{inspect(sl.poll_as)}` on " <>
                "#{inspect(resource)}, which declares no `poll`. `poll_as:` names the option " <>
                "a SCANNER is asked with, so it only means something on a source. Declare it " <>
                "on the polling node, or drop `poll_as:` and keep the slice for reprocess."
      end

      %{
        column: sl.column,
        values: sl.values,
        label: sl.label || to_string(sl.column),
        # what a POLL is asked with, resolved once here rather than at each
        # call site, so the default cannot disagree with itself
        poll_as: Slice.poll_key(sl)
      }
    end
  end

  # What a recompute CLEARS, validated at assembly. Everything checkable without
  # writing is checked here, because the write itself happens inside the drain's
  # per-cell savepoint, where raising would abort the outer transaction — a mark
  # that cannot be cleared must not cost the recompute that moved the content.
  # So the failures that would otherwise surface as a logged warning at 3am
  # (a missing attribute, a child with no destroy action) surface at boot.
  defp lapses(resource) do
    case Ext.get_entities(resource, [:reactive]) |> Enum.filter(&match?(%Lapse{}, &1)) do
      [] ->
        # nil rather than `[]`: extra_meta strips nils, so a node that declares
        # no lapse carries no `:lapse` key at all and the write path's `meta[:lapse]`
        # is a plain nil check rather than an empty-list special case.
        nil

      entities ->
        Enum.map(entities, &lapse_spec!(&1, resource))
    end
  end

  defp lapse_spec!(%Lapse{target: target} = l, resource) do
    validate_when_changed!(l, resource)
    validate_over!(l, resource)

    # An atom is ambiguous at parse time — `:approved_at` and `MyApp.Correction`
    # are both atoms — so the discrimination happens HERE, against the resource's
    # own attributes, which is the only place that can tell them apart.
    if Ash.Resource.Info.attribute(resource, target) do
      attribute_lapse!(l, resource)
    else
      child_lapse!(l, resource)
    end
  end

  # An ATTRIBUTE lapse nulls the column through an update action. The action must
  # ACCEPT the attribute, or the write would succeed and change nothing — a mark
  # silently surviving content it no longer describes, which is the exact failure
  # the declaration exists to prevent.
  defp attribute_lapse!(%Lapse{target: attr} = l, resource) do
    action_name = l.lapse_action || :lapse

    case Ash.Resource.Info.action(resource, action_name) do
      %{type: :update} = action ->
        unless attr in (action.accept || []) do
          raise ArgumentError,
                "reactive_dag: `lapse #{inspect(attr)}` on #{inspect(resource)} clears the " <>
                  "column through the #{inspect(action_name)} action, which does not accept " <>
                  "#{inspect(attr)} — the write would succeed and change nothing, leaving the " <>
                  "mark standing over content it no longer describes. Add it: " <>
                  "`update #{inspect(action_name)} do accept([#{inspect(attr)}]) end`."
        end

      %{type: other} ->
        raise ArgumentError,
              "reactive_dag: `lapse #{inspect(attr)}` on #{inspect(resource)} names " <>
                "#{inspect(action_name)}, a #{inspect(other)} action. Clearing an attribute " <>
                "is an UPDATE (the row stays, the column is nulled) — a destroy would take " <>
                "the computed row with it."

      nil ->
        raise ArgumentError,
              "reactive_dag: `lapse #{inspect(attr)}` on #{inspect(resource)} needs an " <>
                "action to clear the column with, and there is no " <>
                "#{inspect(action_name)} action. A lapse is its OWN write, deliberately: " <>
                "the payload action must never accept #{inspect(attr)}, or every recompute " <>
                "would null it and survival would stop being the default. Add " <>
                "`update :lapse do accept([#{inspect(attr)}]) end`, or name another with " <>
                "`lapse_action:`."
    end

    %{
      kind: :attribute,
      attribute: attr,
      when_changed: l.when_changed,
      over: l.over,
      action: action_name
    }
  end

  # A CHILD lapse destroys the rows attached to the lapsing key. `key:` is
  # required rather than inferred for the reason the guide gives: a resource may
  # reference a node by more than one column, and guessing wrong here deletes the
  # wrong rows — a mistake nothing downstream can detect, since the rows are
  # simply gone.
  defp child_lapse!(%Lapse{target: child} = l, resource) do
    unless Code.ensure_loaded?(child) and function_exported?(child, :spark_dsl_config, 0) do
      attrs = Ash.Resource.Info.attributes(resource) |> Enum.map(& &1.name)

      raise ArgumentError,
            "reactive_dag: `lapse #{inspect(child)}` on #{inspect(resource)} names neither " <>
              "an attribute of this resource nor an Ash resource, so there is nothing to " <>
              "clear. Attributes declared: #{inspect(attrs)}"
    end

    unless l.key do
      raise ArgumentError,
            "reactive_dag: `lapse #{inspect(child)}` on #{inspect(resource)} clears CHILD " <>
              "ROWS, so it needs `key:` — the child's column holding this node's cell key. " <>
              "It is required rather than inferred because a resource may reference a node " <>
              "by more than one column, and guessing wrong here deletes the wrong rows."
    end

    unless Ash.Resource.Info.attribute(child, l.key) do
      raise ArgumentError,
            "reactive_dag: `lapse #{inspect(child)}, key: #{inspect(l.key)}` — " <>
              "#{inspect(child)} has no such attribute, so the destroy would filter on " <>
              "nothing. Its attributes: " <>
              "#{inspect(Ash.Resource.Info.attributes(child) |> Enum.map(& &1.name))}"
    end

    action_name = l.lapse_action || :destroy

    case Ash.Resource.Info.action(child, action_name) do
      %{type: :destroy} ->
        :ok

      %{type: other} ->
        raise ArgumentError,
              "reactive_dag: `lapse #{inspect(child)}` names #{inspect(action_name)}, a " <>
                "#{inspect(other)} action. Clearing child rows DESTROYS them — a lapsed " <>
                "mark is simply gone, and the state afterwards is the state before anyone " <>
                "marked anything."

      nil ->
        raise ArgumentError,
              "reactive_dag: `lapse #{inspect(child)}` needs a #{inspect(action_name)} " <>
                "action on #{inspect(child)} to clear its rows with, for the same reason " <>
                "`retain_if_vanished` demands one: a row that should have gone but silently " <>
                "stayed is indistinguishable from a live one. Add `defaults [:destroy]`, or " <>
                "name another with `lapse_action:`."
    end

    %{
      kind: :child,
      resource: child,
      key: l.key,
      when_changed: l.when_changed,
      over: l.over,
      action: action_name
    }
  end

  # The watched fields must be real columns of the row this node WRITES. A field
  # the payload never carries can never move, so the lapse could never fire — a
  # sign-off that silently outlives every recompute, which reads exactly like a
  # sign-off that is still true.
  defp validate_when_changed!(%Lapse{when_changed: :any}, _resource), do: :ok

  defp validate_when_changed!(%Lapse{when_changed: []} = l, resource) do
    raise ArgumentError,
          "reactive_dag: `lapse #{inspect(l.target)}, when_changed: []` on " <>
            "#{inspect(resource)} watches no fields, so it could never fire. Name the " <>
            "fields the mark is about, or `when_changed: :any` for \"whenever the content " <>
            "moves at all\"."
  end

  defp validate_when_changed!(%Lapse{when_changed: fields} = l, resource) when is_list(fields) do
    attrs = Ash.Resource.Info.attributes(resource) |> Enum.map(& &1.name)

    case Enum.reject(fields, &(&1 in attrs)) do
      [] ->
        :ok

      missing ->
        raise ArgumentError,
              "reactive_dag: `lapse #{inspect(l.target)}, when_changed: #{inspect(fields)}` " <>
                "on #{inspect(resource)} watches #{inspect(missing)}, which this resource " <>
                "has no attribute for. A field the payload never carries can never move, so " <>
                "the lapse could never fire — and a sign-off that silently outlives every " <>
                "recompute reads exactly like one that is still true. " <>
                "Declared: #{inspect(attrs)}"
    end
  end

  # `over:` must name the unit this node declares with `recompute_by`. That
  # constraint is the whole reason set-grain works: the graph knows how to
  # invalidate a `recompute_by` unit, so "what exactly did I approve" has an
  # answer the substrate can also act on.
  defp validate_over!(%Lapse{over: nil}, _resource), do: :ok

  defp validate_over!(%Lapse{over: over} = l, resource) do
    units =
      case recompute_by(resource) do
        %RecomputeBy{unit: :cell} -> [:cell]
        %RecomputeBy{unit: pairs} when is_list(pairs) -> Keyword.keys(pairs)
        %RecomputeBy{unit: u} -> [u]
        nil -> []
      end

    cond do
      units == [] ->
        raise ArgumentError,
              "reactive_dag: `lapse #{inspect(l.target)}, over: #{inspect(over)}` on " <>
                "#{inspect(resource)}, which declares no `recompute_by` — so the graph has " <>
                "no name for the set being signed off, and no way to say when it moved. A " <>
                "sign-off over a set the graph cannot invalidate is a promise nobody can " <>
                "keep. Declare `recompute_by #{inspect(over)}, …`, or drop `over:` for a " <>
                "row-grain mark."

      over not in units ->
        raise ArgumentError,
              "reactive_dag: `lapse #{inspect(l.target)}, over: #{inspect(over)}` on " <>
                "#{inspect(resource)}, whose `recompute_by` declares #{inspect(units)}. " <>
                "`over:` must name a unit this node already recomputes by — that is what " <>
                "makes the set one the substrate can act on rather than a label only the " <>
                "human understands."

      true ->
        :ok
    end
  end

  defp identity_fields(resource) do
    case Ash.Resource.Info.primary_key(resource) do
      pk when is_list(pk) and length(pk) > 1 -> pk
      _ -> nil
    end
  end

  # ASH-DERIVED payload key: the resource's single-attribute primary key (the
  # resource already declares its identity — restating it was duplication).
  # Composite primary keys return nil: those nodes are IDENTITY-KEYED (the row
  # upserts by its identity; the cell key is the identity's serialization, in
  # primary-key order).
  defp derived_payload_key(resource) do
    case Ash.Resource.Info.primary_key(resource) do
      [single] -> single
      _ -> nil
    end
  end

  defp extra_meta(resource, _all_refs) do
    combinator_meta =
      case combinator(resource) do
        %Reduce{} = r -> %{reduce: lower_recompute_by(r, resource)}
        %PerKey{} = pk -> %{per_key: pk}
        %Union{} = u -> %{union: u}
        %Join{} = j -> %{join: j}
        nil -> %{}
      end

    aggregate_meta =
      case aggregate(resource) do
        %Aggregate{} = a -> %{aggregate: a}
        nil -> %{}
      end

    run_meta =
      case Ext.get_entities(resource, [:reactive]) |> Enum.find(&match?(%Run{}, &1)) do
        %Run{action: a} -> %{run: a}
        nil -> %{}
      end

    scan_meta =
      case Ext.get_entities(resource, [:reactive]) |> Enum.find(&match?(%Poll{}, &1)) do
        %Poll{module: m, args: a, every: e} ->
          %{scan: m, scan_args: a || [], scan_every: e}

        nil ->
          %{}
      end

    Ext.get_opt(resource, [:reactive], :meta, [])
    |> Map.new()
    |> Map.merge(
      %{
        source: Ext.get_opt(resource, [:reactive], :source, nil),
        over: Ext.get_opt(resource, [:reactive], :over, nil),
        payload_key: Ext.get_opt(resource, [:reactive], :payload_key, nil) || derived_payload_key(resource),
        payload_action: Ext.get_opt(resource, [:reactive], :payload_action, nil),
        fingerprint: Ext.get_opt(resource, [:reactive], :fingerprint, nil),
        fingerprint_attribute: Ext.get_opt(resource, [:reactive], :fingerprint_attribute, nil),
        compare: Ext.get_opt(resource, [:reactive], :compare, nil),
        retain_if_vanished: retain_policy(resource),
        slices: slices(resource),
        lapse: lapses(resource),
        identity_fields: identity_fields(resource),
        context_inputs: context_inputs(resource)
      }
      |> Enum.reject(fn {_k, v} -> is_nil(v) end)
      |> Map.new()
    )
    |> Map.merge(combinator_meta)
    |> Map.merge(aggregate_meta)
    |> Map.merge(run_meta)
    |> Map.merge(scan_meta)
  end

  defp context_inputs(resource) do
    case Ext.get_entities(resource, [:reactive]) |> Enum.filter(&match?(%Context{}, &1)) do
      [] -> nil
      refs -> Enum.map(refs, &to_string(&1.to))
    end
  end

  defp walk_cbs do
    %{
      classify: fn
        %Ref{} -> :ref
        # a context edge is an input edge too — it just won't propagate (the
        # non-propagation is enforced in Graph.build_parents via context_inputs).
        %Context{} -> :ref
        # a composed LEAF (leaf? true) is terminal — no leg recursion.
        %Compose{leaf?: true} -> :leaf
        %Compose{} -> :op
        {:op, _, _, _, _, _, _, _, _} -> :op
      end,
      legs: fn
        {:op, _, _, _, _, _, _, legs, _} -> legs
        %Compose{legs: legs} -> legs
      end,
      leg_id: fn parent, i, leg ->
        case leg do
          %Compose{as: as} when not is_nil(as) -> to_string(as)
          _ -> "#{parent}/#{i}"
        end
      end,
      ref_id: fn
        %Ref{to: to} ->
          to_string(to)

        %Context{to: to} ->
          to_string(to)
      end,
      to_cell: &build_cell/3
    }
  end

  defp build_cell(id, {:op, _id, op, compute, key_rule, leaf?, resource, _legs, extra}, input_ids) do
    %ReactiveDag.Cell{
      id: id,
      op: op,
      inputs: input_ids,
      leaf?: leaf?,
      meta: Map.merge(%{resource: resource, compute: compute, key_rule: key_rule}, extra)
    }
  end

  defp build_cell(
         id,
         %Compose{op: op, compute: compute, key_rule: key_rule, leaf?: leaf?, meta: meta},
         input_ids
       ) do
    %ReactiveDag.Cell{
      id: id,
      op: op,
      inputs: input_ids,
      leaf?: leaf? || false,
      meta: Map.merge(%{resource: nil, compute: compute, key_rule: key_rule}, Map.new(meta || []))
    }
  end

  defp default_id(resource) do
    resource
    |> Module.split()
    |> List.last()
    |> Macro.underscore()
    |> String.to_atom()
  end
end

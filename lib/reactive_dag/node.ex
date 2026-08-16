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

  The library closes the payload loop: a `reduce`/`join` whose `into` returns a
  row, with **no `upsert:`**, has that row written into the node's own resource
  (`ReactiveDag.Node.Payload`) with change-detection. Writing into a *different*
  resource is the explicit deviation — supply a custom `upsert:` for that.

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
  | a slot the attributes can't express | the slot's escape | same | `query:` (shape the Ash read), fn group/key/into, `expand:`, `status:` |
  | arbitrary recompute, kept Ash-native | `run :action` | up to the action | a generic action on THIS resource |
  | recompute beyond Ash (LLM, fetch, bespoke) | `compute Mod` | up to the module | a `ReactiveDag.Op` |

  ## Node shapes (what scaffolding a node needs)

  | shape | data_layer | attributes | actions | `reactive` |
  |---|---|---|---|---|
  | **payload** (materializes typed rows) | AshPostgres/Ets | the payload columns | an `:upsert` action | a combinator, no `upsert:` |
  | **write-elsewhere** | Simple | none | none | a combinator + a custom `upsert:` |
  | **escape hatch** | Simple | none | none | `compute Mod` |

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
      ReactiveDag.Drain.run(plan,
        recompute: ReactiveDag.Node.Recompute,
        key_rule:  ReactiveDag.Node.KeyRule)

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
    describe:
      "A by-name CONTEXT edge: read the target as settled context; its changes do NOT recompute this node.",
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
            "load-bearing only for a `RecomputeStrategy` that reads it (e.g. " <>
            "`ReactiveDag.SetOp`). See `ReactiveDag.Cell`."
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
      :upsert,
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
      ],
      upsert: [
        type: {:fun, 2},
        required: false,
        doc:
          "OPTIONAL override `(key, row -> boolean)` — write the row's payload + return true iff CHANGED. OMIT it for the common case: the library writes `into`'s row into the node's OWN resource (`ReactiveDag.Node.Payload`). Supply `upsert:` only to write somewhere other than the node itself."
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
    """
    defstruct [
      :over,
      :read,
      :query,
      :left,
      :right,
      :key,
      :key_prefix,
      :key_rule,
      :into,
      :upsert,
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
        required: true,
        doc: "the input node id whose payload is read + indexed"
      ],
      read: [
        type: :atom,
        required: false,
        doc:
          "OMIT for the default (the over node's primary read, dirty-key scoped); an " <>
            "atom names a different `:read` action on it. Always an Ash read — see `query:`."
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
        doc:
          "prepend `\"<prefix>|\"` to the DEFAULT key. Not combinable with an explicit `key:` fn."
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
        doc:
          "FULL OUTER: right-only keys also emit, via `into.(jk, nil, right_item)`. Default false (left join)."
      ],
      upsert: [
        type: {:fun, 2},
        required: false,
        doc:
          "OPTIONAL override `(key, row -> boolean)`. OMIT it → the library writes the row into the node's own resource (see `ReactiveDag.Node.Payload`)."
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
    """
    defstruct [:column, :values, :label, :__identifier__, :__spark_metadata__]
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
            "the routine call stays `poll_all(plan)` and no call site can forget the bound."
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
          doc:
            "a `has_many`/`has_one` relationship on THIS resource — the rows to aggregate per group."
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
      @slice
    ],
    schema: [
      id: [
        type: :atom,
        doc: "the cell id; defaults to the resource module's short name, snake_cased"
      ],
      op: [
        type: :atom,
        doc:
          "OPTIONAL free-atom label. Recompute dispatches on the `reduce`/`join`/`aggregate`/`compute` entity + `meta` shape, NOT on `op` — so `op` is documentation here, load-bearing only for a `RecomputeStrategy` that dispatches on it (e.g. `ReactiveDag.SetOp`). See `ReactiveDag.Cell`."
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
            "`leaf?` — a leaf fed by a `ReactiveDag.Source` poll would double-trigger."
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
        carries a host-chosen `op:` — so `SetOp` recomputes it (e.g. a status FILTER
        that keeps only the violation rows). Options:

          * `op:` (atom, required) — the companion cell's op (its RecomputeStrategy
            dispatch key).
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

    {:op, root_id, Ext.get_opt(resource, [:reactive], :op, nil), compute_module(resource),
     effective_key_rule(resource), Ext.get_opt(resource, [:reactive], :leaf?, false), resource,
     all_refs, extra_meta(resource, all_refs)}
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
            group_by: r.group_by || unit_group_by(u),
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

  # the unit as a `group_by` entry: `[unit: :from]` — this node's column on the
  # left, the input's field on the right. `:cell` groups nothing (the whole cell
  # is one unit); a unit with no `from:` leaves grouping to the combinator.
  defp unit_group_by(%RecomputeBy{unit: :cell}), do: nil

  # the COMPOSITE form IS the grouping: `[fund: :fund_code, fy: :fy]` is already
  # the `group_by` pair list, so it passes straight through.
  defp unit_group_by(%RecomputeBy{unit: pairs}) when is_list(pairs), do: pairs

  defp unit_group_by(%RecomputeBy{from: nil}), do: nil
  defp unit_group_by(%RecomputeBy{unit: u, from: f}), do: [{u, f}]

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
      if is_nil(Ash.Resource.Info.attribute(resource, sl.column)) do
        raise ArgumentError,
              "reactive_dag: `slice #{inspect(sl.column)}` on #{inspect(resource)}, which " <>
                "has no such attribute. A slice filters this node's own rows by that " <>
                "column, so it must be one of them."
      end

      %{column: sl.column, values: sl.values, label: sl.label || to_string(sl.column)}
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
        payload_key:
          Ext.get_opt(resource, [:reactive], :payload_key, nil) || derived_payload_key(resource),
        payload_action: Ext.get_opt(resource, [:reactive], :payload_action, nil),
        fingerprint: Ext.get_opt(resource, [:reactive], :fingerprint, nil),
        fingerprint_attribute: Ext.get_opt(resource, [:reactive], :fingerprint_attribute, nil),
        retain_if_vanished: retain_policy(resource),
        slices: slices(resource),
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

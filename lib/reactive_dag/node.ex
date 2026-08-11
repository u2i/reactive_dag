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
          # ASH-FIRST: the library reads :dmr_rows, groups by the attributes,
          # folds each group, and upserts the row by its Ash IDENTITY with the
          # coordination Op.put. No `read:`, no `key:`, no key column, no
          # `upsert:` — each slot has an escape hatch when the shape outgrows
          # attributes.
          reduce over: :dmr_rows,
                 group_by: [:plant, :month],
                 key_rule: :group,
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
  | fold one input's rows into per-group summaries | `reduce` | the scoped slice of `over` | `group_by:` attrs + an `into:` fold |
  | reconcile one input's two sides by key | `join` | the scoped slice of `over` | side attrs/`[key:, where:]` + picks |
  | a slot the attributes can't express | the slot's escape | same | `query:` (shape the Ash read), fn group/key/into, `expand:`, `status:` |
  | arbitrary recompute, kept Ash-native | `run :action` | up to the action | a generic action on THIS resource |
  | recompute beyond Ash (LLM, fetch, bespoke) | `compute Mod` | up to the module | a `ReactiveDag.Op` |

  ## Node shapes (what scaffolding a node needs)

  | shape | data_layer | attributes | actions | `reactive` |
  |---|---|---|---|---|
  | **payload** (materializes typed rows) | AshPostgres/Ets | the payload columns | an `:upsert` action | a combinator, no `upsert:` |
  | **verdict** (`verdict? true`) | `Ash.DataLayer.Simple` | none | none | a combinator; rows carry `:status` |
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
        repo:                MyApp.Repo,        # REQUIRED (raises if unset)
        tuple_table:         "my_tuple",        # coordination spine table (must match your migration)
        dirty_table:         "my_dirty",        # frontier table (must match your migration)
        coordination_writer: MyApp.Writer       # optional; a spine-only default ships

  `tuple_table`/`dirty_table` default silently, so a name that doesn't match your
  migration yields empty results with no error — set them explicitly.
  """

  defmodule Ref do
    @moduledoc """
    A by-name input edge to another named node (`ref :id`). The general form —
    nestable inside `compose`. The flat `depends_on: [:a, :b]` schema key is sugar
    that lowers to one `%Ref{}` per id.

    `gate:` names an attestation requirement: the edge then consumes the target
    THROUGH its attested view — an interposed cell admitting only rows whose
    attestation currently applies (`ReactiveDag.Attestation`). Signing is a
    property of the EDGE, not of the data: an ungated edge to the same target
    still sees everything, which is what keeps denominators honest.

    `mode:` (with `gate:`) picks how a not-yet-signed row projects: `:require`
    (default) withholds it; `:annotate` lets it FLOW as best effort, written
    with the requirement's `unsigned` status so it stays distinguishable from
    signed. A rejection bites in both modes.
    """
    defstruct [:to, :gate, :mode, :__identifier__, :__spark_metadata__]
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
      to: [type: :atom, required: true, doc: "the referenced node's id"],
      gate: [
        type: :atom,
        doc:
          "an attestation requirement name — consume the target through its ATTESTED view (only rows whose attestation currently applies are admitted; lowered to an interposed attested cell)"
      ],
      mode: [
        type: {:one_of, [:require, :annotate]},
        default: :require,
        doc:
          "with `gate:` — `:require` withholds unsigned rows (blocking); `:annotate` lets them flow as best effort, written with the `unsigned` status so signed stays distinguishable. Rejections bite in both."
      ]
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

  @compose_base %Spark.Dsl.Entity{
    name: :compose,
    target: Compose,
    args: [:op],
    describe: "An anonymous nested op-expression leg; composes inline as an intermediate cell.",
    schema: [
      op: [
        type: :atom,
        required: true,
        doc:
          "free-atom label for this intermediate cell (positional; see `ReactiveDag.Cell` for what `op` means)"
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
      :status,
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
      "Declarative fold: read `over`, group by `group_by`, reduce each group with `into`. " <>
        "Ash-first: omit `read:` (the library reads the over node's resource, dirty-key " <>
        "scoped), name attributes in `group_by:`, and declare the fold in `into:` — " <>
        "each slot has a fn escape hatch when the shape outgrows attributes.",
    schema: [
      over: [
        type: :atom,
        required: true,
        doc: "the input node id whose payload is read + grouped"
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
        required: true,
        doc:
          "an attribute or CALCULATION on the over resource (`:fund`, or `:month` where " <>
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
      key_rule: [
        type:
          {:or,
           [
             {:one_of, [:identity, :all, :group]},
             {:custom, ReactiveDag.Node, :validate_key_rule, []}
           ]},
        required: false,
        doc:
          "this node's claim grain, declared WITH the computation it must agree with. " <>
            "`:group` — a changed child row claims its GROUP; the mapping is what " <>
            "`group_by` (or the join sides) already declares. Resolution: by default " <>
            "the changed rows are READ and the group fields evaluated (one scoped " <>
            "query; a key the lookup can't find — a deleted row — degrades to `:all`); " <>
            "`{:group, from: :key}` resolves PURELY instead — the key's leading " <>
            "`|`-segments carry the group's input fields in `group_by` order (a plain " <>
            "attribute's value; a Calendar calculation's raw date), so no query and " <>
            "deletion-safe, at the price of the key-grammar contract (reduce with a " <>
            "declarative group and default keys only). Overrides the block-level " <>
            "`key_rule` (a non-default in both places is a compile error)."
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
            "derive). Mutually exclusive with `into`/`status`."
      ],
      status: [
        type: {:fun, 2},
        required: false,
        doc:
          "a VERDICT node's result: `(group_term, [item] -> status)` — a status string, " <>
            "or `{status, strength}` where the host's tuple carries strength. The key " <>
            "derives exactly as a payload row's (`key:`/`key_prefix:`/the `\"|\"` default). " <>
            "Required on `verdict? true` nodes; forbidden elsewhere."
      ],
      upsert: [
        type: {:fun, 2},
        required: false,
        doc:
          "OPTIONAL override `(key, row -> boolean)` — write the row's payload + return true iff CHANGED. OMIT it for the common case: the library writes `into`'s row into the node's OWN resource (`ReactiveDag.Node.Payload`) and does the `Op.put`. Supply `upsert:` only to write somewhere other than the node itself."
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
      :status,
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
      key_rule: [
        type:
          {:or,
           [
             {:one_of, [:identity, :all, :group]},
             {:custom, ReactiveDag.Node, :validate_key_rule, []}
           ]},
        required: false,
        doc:
          "this node's claim grain, declared WITH the computation it must agree with. " <>
            "`:group` — a changed child row claims its GROUP; the mapping is what " <>
            "`group_by` (or the join sides) already declares. Resolution: by default " <>
            "the changed rows are READ and the group fields evaluated (one scoped " <>
            "query; a key the lookup can't find — a deleted row — degrades to `:all`); " <>
            "`{:group, from: :key}` resolves PURELY instead — the key's leading " <>
            "`|`-segments carry the group's input fields in `group_by` order (a plain " <>
            "attribute's value; a Calendar calculation's raw date), so no query and " <>
            "deletion-safe, at the price of the key-grammar contract (reduce with a " <>
            "declarative group and default keys only). Overrides the block-level " <>
            "`key_rule` (a non-default in both places is a compile error)."
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
      status: [
        type: {:fun, 3},
        required: false,
        doc:
          "a VERDICT node's result: `(join_key, left_or_nil, right_or_nil -> status)` — " <>
            "a status string or `{status, strength}`. Required on `verdict? true` nodes; " <>
            "forbidden elsewhere."
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
    expands to many cells) — and does the coordination `Op.put` for each
    returned key (an action has no `%Cell{}` to put through; the library
    closes the coordination loop, as in the payload loop). The action owns its
    DOMAIN writes. `coordination_opts` does not apply (there is no row).
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
        "the library Op.puts the returned keys.",
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

  @attestation %Spark.Dsl.Entity{
    name: :attestation,
    target: ReactiveDag.Attestation.Requirement,
    args: [:name],
    describe:
      "A named attestation REQUIREMENT on this node's data: who may sign it (an eligibility " <>
        "CELL + a per-scope join), how many must (`quorum`), and how long a signature holds " <>
        "(`tolerance`). Declared ONCE here; consumed by name from `attested` combinators and " <>
        "`gate:`d edges. Policy has one home, not a copy per consuming edge.",
    schema: [
      name: [
        type: :atom,
        required: true,
        doc: "the requirement's name (what `requirement:`/`gate:` reference)"
      ],
      scope: [
        type: :any,
        default: :key,
        doc:
          "the grain of one assertion: `:key` (per row), `{:filter, key_scope}` (ONE set-level instance — completeness of the whole selected set), or `{:filter_by, (eligibility_key -> {instance_key, key_scope} | nil)}` (one set-level instance per eligibility row — per-person completeness)."
      ],
      instance_key: [
        type: :string,
        default: "all",
        doc: "the view row's key for a `{:filter, _}` single-instance scope"
      ],
      signers: [
        type: :atom,
        required: true,
        doc:
          "the ELIGIBILITY cell's id. Who may sign is DATA — a real input edge of every attested cell, so authority changes propagate and appear in lineage."
      ],
      join: [
        type: {:fun, 2},
        required: true,
        doc:
          "`(scope, eligibility_key -> who | nil)` — does this eligibility row license signing this scope, and as whom? (The eligibility cell's key grammar is the host's; this interprets it.)"
      ],
      quorum: [
        type: :any,
        default: :any,
        doc: "`:any | :all | {:n_of, k}` over the currently-eligible set"
      ],
      tolerance: [
        type: {:custom, ReactiveDag.Attestation.Requirement, :validate_tolerance, []},
        doc:
          "how long a signature holds: seconds, or a keyword of `weeks:`/`days:`/`hours:`/`minutes:`/`seconds:` (units combine; unknown units are rejected at compile time). Omit for no time bound (the host applies its strength-derived default elsewhere)."
      ],
      statuses: [
        type: :keyword_list,
        doc:
          "override the admission-state → spine-status vocabulary (defaults: covered/pending/refused)"
      ]
    ]
  }

  defmodule Attested do
    @moduledoc """
    The ATTESTED VIEW combinator: this node is the derived cell whose rows are
    `over`'s rows joined against currently-applying attestation records under a
    named requirement — both cells exist in the graph (the raw list AND the
    signed list), and a consumer picks per edge. `ref :x, gate: :req` is sugar
    that interposes an anonymous cell of exactly this shape.

    `mode: :annotate` makes the view NON-BLOCKING: unsigned rows flow (best
    effort) under the `unsigned` status instead of being withheld as `pending`.
    """
    defstruct [:over, :requirement, :mode, :__identifier__, :__spark_metadata__]
  end

  @attested %Spark.Dsl.Entity{
    name: :attested,
    target: Attested,
    describe:
      "This node is the ATTESTED VIEW of `over` under `requirement`: raw rows ⨝ applying " <>
        "attestations → covered/pending/refused spine rows (recomputed by ReactiveDag.Attestation.Op).",
    schema: [
      over: [type: :atom, required: true, doc: "the raw cell this view attests"],
      requirement: [
        type: :atom,
        required: true,
        doc: "the attestation requirement's name (declared on the raw node)"
      ],
      mode: [
        type: {:one_of, [:require, :annotate]},
        default: :require,
        doc:
          "`:require` (blocking) withholds unsigned rows as `pending`; `:annotate` (non-blocking) flows them as `unsigned` — best effort, distinguished from signed"
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

  @agg_kinds [:count, :sum, :avg, :min, :max, :first]

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
      @compose,
      @reduce,
      @join,
      @aggregate,
      @compute,
      @run,
      @attestation,
      @attested
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
      coordination_opts: [
        type: {:fun, 2},
        doc:
          "`(key, row -> keyword)` — extra opts for the coordination write when the payload loop `Op.put`s a changed key, so a host `CoordinationWriter` can write EXTENSION COLUMNS (e.g. `source_ref`, a fingerprint) during a `reduce`/`join`. Without it, the loop writes spine columns only. (Retain-if-vanish/tombstone is a LEAF concern — reconcile a source-fed leaf via `ReactiveDag.Tuple.reconcile`, not here.)"
      ],
      verdict?: [
        type: :boolean,
        default: false,
        doc:
          "VERDICT-ONLY node (named `verdict?` to match `leaf?`): its computed result lives entirely in the coordination tuple (`status`/`strength`), with NO payload table of its own. Its `reduce`/`join` declares `status:` — the verdict IS the result, written straight into the tuple via `Op.put`; keys derive as a payload row's would. No resource table, no `into:`, no `upsert:`, no attributes. Use for nodes whose output fits the tuple's fixed schema (e.g. a compliance verdict), as opposed to nodes that materialize typed rows. (Distinct from `ReactiveDag.Verdict`, the read-side status rollup.)"
      ],
      source: [
        type: :atom,
        doc: "convenience: a leaf's source binding id (also merged into meta)"
      ],
      driver: [
        type: :atom,
        doc: "convenience: a leaf's driver module (also merged into meta)"
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
            "an atom, or `{:id, gate: :requirement}` to consume that input through its " <>
            "attested view (see `ref`'s `gate:`)."
      ]
    ]
  }

  use Spark.Dsl.Extension,
    sections: [@reactive],
    verifiers: [ReactiveDag.Node.Verifiers.VerifyReactive]

  # ── introspection + graph assembly ────────────────────────────────────────

  alias Spark.Dsl.Extension, as: Ext

  @doc false
  # the DSL schema's custom validator for the tuple key_rule forms.
  def validate_key_rule({:group, opts}) when is_list(opts) do
    case Keyword.get(opts, :from, :lookup) do
      # :lookup is the default resolution — normalize to the bare atom
      :lookup -> {:ok, :group}
      :key -> {:ok, {:group, from: :key}}
      other -> {:error, "key_rule {:group, from: ...} takes :lookup or :key, got #{inspect(other)}"}
    end
  end

  def validate_key_rule({:bucket, _kind}) do
    {:error,
     "key_rule {:bucket, kind} was unified into the :group rule — declare the Calendar " <>
       "calculation in `group_by` (it already carries the bucket) and use " <>
       "`key_rule: {:group, from: :key}` for the pure key-prefix resolution"}
  end

  def validate_key_rule(other),
    do:
      {:error,
       "key_rule must be :identity, :all, :group, or {:group, from: :key} — got #{inspect(other)}"}

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
    |> resolve_attestations()
    |> ReactiveDag.Graph.build()
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

  defp resolve_read_cell(cell, by_id) do
    spec = cell.meta[:reduce] || cell.meta[:join]

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

          over.meta[:verdict] ->
            raise ArgumentError,
                  "reactive_dag: #{cell.id} reads over #{inspect(spec.over)}, a VERDICT " <>
                    "node — its result lives in the coordination tuple, not a payload " <>
                    "table, so there are no rows to read. Point `over:` at a payload " <>
                    "node, or use `run`/`compute` to read the tuple."

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
        Ash.Resource.Info.attribute(resource, name) -> loads
        Ash.Resource.Info.calculation(resource, name) -> [name | loads]
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

  # ── attestation resolution (graph assembly) ─────────────────────────────────
  #
  # Requirements are declared on the RAW node but consumed by NAME from attested
  # combinators and gated edges on OTHER nodes — so resolution is cross-resource
  # and happens here, at assembly, not in per-resource lowering:
  #
  #   1. index declared requirements (name → %Requirement{on: declaring cell});
  #   2. resolve each `attested` node: swap the requirement NAME for the struct,
  #      add the eligibility cell + the store leaf to its inputs;
  #   3. manufacture the interposed cell for each gated edge (deduped: two
  #      consumers gating the same edge under the same requirement share it);
  #   4. inject the ONE store leaf cell (`ReactiveDag.Attestation.leaf_cell/0`);
  #   5. lint: a verdict cell with NO ungated path to any leaf, where every
  #      gated path shares one requirement, is structurally vacuous — the gate
  #      has swallowed its own denominator — and that is a model defect, so it
  #      raises here rather than rendering as green.
  #
  # A graph with no attestation vocabulary passes through untouched.
  defp resolve_attestations(cells) do
    reqs =
      for c <- cells,
          is_map(c.meta[:attestations]),
          {name, r} <- c.meta[:attestations],
          into: %{} do
        {name, %{r | on: c.id}}
      end

    gated =
      cells
      |> Enum.flat_map(&(&1.meta[:gated_inputs] || []))
      |> Enum.uniq()

    needs? = gated != [] or Enum.any?(cells, &is_map(&1.meta[:attested]))

    if needs? do
      store = ReactiveDag.Attestation.leaf_cell()

      resolved = Enum.map(cells, &resolve_attested_cell(&1, reqs, store))

      interposed =
        Enum.map(gated, fn {over, gate, mode} ->
          interposed_cell(over, gate, mode, reqs, store)
        end)

      store_cell =
        if Enum.any?(cells, &(&1.id == store)) do
          []
        else
          [
            %ReactiveDag.Cell{
              id: store,
              op: :leaf,
              inputs: [],
              leaf?: true,
              meta: %{resource: nil, compute: nil, key_rule: :identity, attestation_store: true}
            }
          ]
        end

      all = resolved ++ interposed ++ store_cell
      lint_vacuous!(all)
      all
    else
      cells
    end
  end

  defp resolve_attested_cell(
         %{meta: %{attested: %{over: over, requirement: name}}} = cell,
         reqs,
         store
       )
       when is_atom(name) do
    req = fetch_requirement!(reqs, name, cell.id)

    if req.on != over do
      raise ArgumentError,
            "reactive_dag: #{cell.id} is `attested over: #{inspect(over)}` under requirement " <>
              "#{inspect(name)}, but that requirement is declared on #{inspect(req.on)} — " <>
              "its records live under that cell's id, so the view must attest the same cell"
    end

    mode = cell.meta.attested[:mode] || :require

    %{
      cell
      | inputs: Enum.uniq(cell.inputs ++ [to_string(req.signers), store]),
        meta:
          cell.meta
          |> Map.put(:attested, %{over: over, requirement: req, mode: mode})
          # any signing (or eligibility change) may move any row, and the Op
          # re-evaluates the whole view — so an attested node is always :all
          # (the schema's :identity default would silently under-propagate).
          |> Map.put(:key_rule, :all)
    }
  end

  defp resolve_attested_cell(cell, _reqs, _store), do: cell

  # the anonymous attested cell a `gate:` lowers to — same shape as a declared
  # `attested` node, at a reserved id (`<over>@<requirement>`, with an
  # `~annotate` suffix for the non-blocking mode: the two modes are two
  # different projections, so a graph using both gets two cells).
  defp interposed_cell(over, gate, mode, reqs, store) do
    req = fetch_requirement!(reqs, gate, "#{over} (gated edge)")

    if req.on != over do
      raise ArgumentError,
            "reactive_dag: an edge gates #{inspect(over)} under requirement #{inspect(gate)}, " <>
              "but that requirement is declared on #{inspect(req.on)} — a gate admits the " <>
              "rows of the cell its requirement attests"
    end

    %ReactiveDag.Cell{
      id: gated_id(over, gate, mode),
      op: :attested,
      inputs: [over, to_string(req.signers), store],
      leaf?: false,
      meta: %{
        resource: nil,
        compute: ReactiveDag.Attestation.Op,
        key_rule: :all,
        attested: %{over: over, requirement: req, mode: mode}
      }
    }
  end

  defp fetch_requirement!(reqs, name, where) do
    reqs[name] ||
      raise(
        ArgumentError,
        "reactive_dag: #{where} names attestation requirement #{inspect(name)}, but no node " <>
          "in this graph declares it (`attestation #{inspect(name)} do … end` on the raw node). " <>
          "Declared: #{inspect(Map.keys(reqs))}"
      )
  end

  @doc """
  The reserved id of the attested view a `gate:` interposes over `over` —
  `<over>@<gate>` for the blocking mode, `<over>@<gate>~annotate` for the
  non-blocking one (two projections → two cells).
  """
  @spec gated_id(String.t(), atom(), :require | :annotate) :: String.t()
  def gated_id(over, gate, mode \\ :require)
  def gated_id(over, gate, :require), do: "#{over}@#{gate}"
  def gated_id(over, gate, :annotate), do: "#{over}@#{gate}~annotate"

  # the vacuity lint (step 5 above). Context edges still count as paths here:
  # they are read paths, and a denominator read through a context edge is honest.
  # An :annotate view is likewise TRANSPARENT to the lint — it withholds
  # nothing (unsigned rows flow, distinguished), so it cannot swallow a
  # denominator; only :require gates block.
  defp lint_vacuous!(cells) do
    by_id = Map.new(cells, &{&1.id, &1})

    for %{meta: %{verdict: true}} = cell <- cells,
        cell.inputs != [],
        # a verdict node that IS the attested view is exempt: first-class
        # coverage retains a row for EVERY raw row (covered/pending/refused),
        # so the view cannot swallow its own denominator — the lint targets
        # CONSUMERS whose only reads pass through a withholding gate.
        not blocking_attested?(cell),
        not reaches_ungated_leaf?(cell.id, by_id, MapSet.new()) do
      case gates_below(cell.id, by_id, MapSet.new()) |> Enum.uniq() do
        [only] ->
          raise ArgumentError,
                "reactive_dag: #{cell.id} is structurally vacuous — every evidence path " <>
                  "reaches its leaves through the #{inspect(only)} gate, so the rows the gate " <>
                  "withholds are invisible to the very join meant to expose them. At least one " <>
                  "leg (the denominator) must consume the raw cell ungated (or through a " <>
                  "non-blocking `mode: :annotate` view)."

        _mixed ->
          :ok
      end
    end

    :ok
  end

  defp reaches_ungated_leaf?(id, by_id, seen) do
    cond do
      MapSet.member?(seen, id) ->
        false

      is_nil(by_id[id]) ->
        # a dangling input — Graph.build raises its own, better error for this.
        false

      blocking_attested?(by_id[id]) ->
        false

      by_id[id].leaf? ->
        true

      true ->
        Enum.any?(by_id[id].inputs, &reaches_ungated_leaf?(&1, by_id, MapSet.put(seen, id)))
    end
  end

  defp gates_below(id, by_id, seen) do
    cond do
      MapSet.member?(seen, id) or is_nil(by_id[id]) ->
        []

      blocking_attested?(by_id[id]) ->
        [by_id[id].meta.attested.requirement.name]

      true ->
        Enum.flat_map(by_id[id].inputs, &gates_below(&1, by_id, MapSet.put(seen, id)))
    end
  end

  defp blocking_attested?(cell) do
    case cell.meta[:attested] do
      %{} = a -> Map.get(a, :mode, :require) == :require
      _ -> false
    end
  end

  @doc """
  The `ReactiveDag.Cell`s a node resource lowers to (no graph math): its root
  cell + one per nested `compose`. Legs are lowered by-name via the shared
  `ReactiveDag.Lowering.walk` — a `ref`/`dep` resolves to an existing cell id
  (no new cell), a `compose` recurses into an intermediate cell.
  """
  @spec cells(module(), (atom() -> [map()]) | nil) :: [ReactiveDag.Cell.t()]
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
    # so does `attested over: :x` (the raw cell the view attests).
    combinator_refs =
      case combinator(resource) do
        nil -> []
        c -> [%Ref{to: c.over}]
      end

    attested_refs =
      case attested(resource) do
        nil -> []
        a -> [%Ref{to: a.over}]
      end

    all_refs = legs ++ flat_refs ++ combinator_refs ++ attested_refs

    {:op, root_id, Ext.get_opt(resource, [:reactive], :op, nil), compute_module(resource),
     effective_key_rule(resource),
     Ext.get_opt(resource, [:reactive], :leaf?, false), resource, all_refs,
     extra_meta(resource, all_refs)}
  end

  # the combinator's `key_rule:` (declared WITH the computation it must agree
  # with) wins over the block-level one; the block level remains for nodes with
  # no combinator (run/compute/leaves).
  defp effective_key_rule(resource) do
    case combinator(resource) do
      %{key_rule: kr} when not is_nil(kr) -> kr
      _ -> Ext.get_opt(resource, [:reactive], :key_rule, :identity)
    end
  end

  # a flat depends_on entry: `:id`, or `{:id, gate: :requirement}` (plus an
  # optional `mode: :require | :annotate`).
  defp normalize_dep(id, _resource) when is_atom(id), do: %Ref{to: id}

  defp normalize_dep({id, opts}, _resource) when is_atom(id) and is_list(opts),
    do: %Ref{to: id, gate: Keyword.fetch!(opts, :gate), mode: Keyword.get(opts, :mode, :require)}

  defp normalize_dep(other, resource) do
    raise ArgumentError,
          "reactive_dag: bad depends_on entry #{inspect(other)} on #{inspect(resource)} — " <>
            "expected an atom id or `{:id, gate: :requirement}`"
  end

  # the escape-hatch `compute Module` entity's module, or nil. An `attested` node
  # defaults to the lib's attestation Op (an explicit `compute` still wins).
  defp compute_module(resource) do
    case Ext.get_entities(resource, [:reactive]) |> Enum.find(&match?(%Compute{}, &1)) do
      %Compute{module: m} -> m
      nil -> if attested(resource), do: ReactiveDag.Attestation.Op, else: nil
    end
  end

  # the node's `attested over: … requirement: …` entity, or nil.
  defp attested(resource) do
    Ext.get_entities(resource, [:reactive]) |> Enum.find(&match?(%Attested{}, &1))
  end

  # the node's `attestation :name do … end` requirement declarations.
  defp attestation_reqs(resource) do
    Ext.get_entities(resource, [:reactive])
    |> Enum.filter(&match?(%ReactiveDag.Attestation.Requirement{}, &1))
  end

  # the node's declarative combinator entity whose `over` is an INPUT NODE (Reduce
  # or Join) — this drives the implicit input edge. NOT Aggregate: its `over` is a
  # relationship on this resource, not another cell, so it adds no edge.
  defp combinator(resource) do
    Ext.get_entities(resource, [:reactive])
    |> Enum.find(&(match?(%Reduce{}, &1) or match?(%Join{}, &1)))
  end

  # the node's Aggregate entity (a relationship aggregate), or nil.
  defp aggregate(resource) do
    Ext.get_entities(resource, [:reactive]) |> Enum.find(&match?(%Aggregate{}, &1))
  end

  # `true` when the node is verdict-only, else nil (so it's dropped from meta and
  # `meta[:verdict]` stays falsy for the common payload-bearing case). Guards the
  # half-state: a verdict node stores nothing but the tuple, so declaring payload
  # attributes on it is a mistake (the verdict write ignores them) — raise rather
  # than silently drop the columns.
  defp verdict_flag(resource) do
    if Ext.get_opt(resource, [:reactive], :verdict?, false) do
      case payload_attributes(resource) do
        [] ->
          true

        extra ->
          raise """
          reactive_dag: node #{inspect(resource)} is `verdict? true` but declares \
          payload attribute(s) #{inspect(extra)}. A verdict node's result lives in \
          the coordination tuple — those attributes would never be written. Drop \
          them (use `data_layer: Ash.DataLayer.Simple`, no attributes), or remove \
          `verdict?` to make it a payload node.
          """
      end
    else
      nil
    end
  end

  # a COMPOSITE primary key means the node is IDENTITY-KEYED: rows upsert by
  # identity, and the cell key is the serialization of these fields in order.
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

  # public, non-primary-key attributes — the payload columns a verdict node must not have.
  defp payload_attributes(resource) do
    resource
    |> Ash.Resource.Info.public_attributes()
    |> Enum.reject(& &1.primary_key?)
    |> Enum.map(& &1.name)
  end

  # the OPEN host binding folded into a cell's meta: the `meta:` keyword list, the
  # source/driver/over conveniences, and the combinator spec under its kind key
  # (`:reduce` | `:join`) — which ReactiveDag.Node.Recompute runs. `all_refs` is
  # the node's resolved Ref legs, from which the GATED pairs are recorded (graph
  # assembly reads them back to interpose attested cells — no id-string parsing).
  defp extra_meta(resource, all_refs) do
    combinator_meta =
      case combinator(resource) do
        %Reduce{} = r -> %{reduce: r}
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

    attestation_meta =
      case attestation_reqs(resource) do
        [] -> %{}
        reqs -> %{attestations: Map.new(reqs, &{&1.name, &1})}
      end

    attested_meta =
      case attested(resource) do
        nil ->
          %{}

        %Attested{over: over, requirement: req, mode: mode} ->
          %{attested: %{over: to_string(over), requirement: req, mode: mode || :require}}
      end

    gated_meta =
      case gated_pairs(all_refs) do
        [] -> %{}
        pairs -> %{gated_inputs: Enum.uniq(pairs)}
      end

    Ext.get_opt(resource, [:reactive], :meta, [])
    |> Map.new()
    |> Map.merge(
      %{
        source: Ext.get_opt(resource, [:reactive], :source, nil),
        driver: Ext.get_opt(resource, [:reactive], :driver, nil),
        over: Ext.get_opt(resource, [:reactive], :over, nil),
        payload_key: Ext.get_opt(resource, [:reactive], :payload_key, nil) || derived_payload_key(resource),
        payload_action: Ext.get_opt(resource, [:reactive], :payload_action, nil),
        coordination_opts: Ext.get_opt(resource, [:reactive], :coordination_opts, nil),
        identity_fields: identity_fields(resource),
        verdict: verdict_flag(resource),
        context_inputs: context_inputs(resource)
      }
      |> Enum.reject(fn {_k, v} -> is_nil(v) end)
      |> Map.new()
    )
    |> Map.merge(combinator_meta)
    |> Map.merge(aggregate_meta)
    |> Map.merge(run_meta)
    |> Map.merge(attestation_meta)
    |> Map.merge(attested_meta)
    |> Map.merge(gated_meta)
  end

  # every (raw, requirement, mode) triple gated anywhere in this node's leg
  # tree — including refs nested inside `compose` legs, whose gated ids the
  # walk emits and which therefore need their interposed cells manufactured too.
  defp gated_pairs(legs) do
    Enum.flat_map(legs, fn
      %Ref{to: to, gate: g, mode: mode} when not is_nil(g) ->
        [{to_string(to), g, mode || :require}]

      %Compose{legs: nested} ->
        gated_pairs(nested)

      _ ->
        []
    end)
  end

  # the ids of this node's `context` edges (read-as-context, non-propagating) —
  # as strings matching the cell input ids, so `Graph.build_parents` can exclude
  # them from the propagation graph. nil when there are none.
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
        # a GATED ref resolves to the interposed attested cell's id — the cell
        # itself is manufactured at graph assembly (resolve_attestations/1),
        # which reads the consumer's `gated_inputs` meta rather than parsing ids.
        %Ref{to: to, gate: gate, mode: mode} when not is_nil(gate) ->
          gated_id(to_string(to), gate, mode || :require)

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

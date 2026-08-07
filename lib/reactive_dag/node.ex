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

        attributes do                             # the payload columns
          attribute :key, :string, primary_key?: true
          attribute :plant, :string
          attribute :avg_flow, :float
        end

        actions do
          create :upsert do upsert?(true); upsert_identity(:key); accept([:key, :plant, :avg_flow]) end
        end

        reactive do
          op :fold
          key_rule :all
          # `into` returns the row; the LIBRARY writes it into THIS resource
          # (keyed by :key) and does the coordination Op.put. No `upsert:` needed.
          reduce over: :dmr_rows,
                 read: &MyApp.FlowMonth.read/1,
                 group_by: &MyApp.FlowMonth.group/1,
                 key: &MyApp.FlowMonth.key/1,
                 into: &MyApp.FlowMonth.into/2
        end
      end

  The library closes the payload loop: a `reduce`/`join` whose `into` returns a
  row, with **no `upsert:`**, has that row written into the node's own resource
  (`ReactiveDag.Node.Payload`) with change-detection. Writing into a *different*
  resource is the explicit deviation — supply a custom `upsert:` for that.

  The cell key maps to the resource's `payload_key` attribute (default `:key`) via
  the `payload_action` upsert (default `:upsert`); set those in the `reactive`
  block if they're named otherwise.

  ## Which computation? (reduce / join / aggregate / compute)

  | you want to… | use | rows into BEAM? | needs |
  |---|---|---|---|
  | fold one input's rows into per-group summaries | `reduce` | all of `over` | a `read`/`group_by`/`into` |
  | same, but one group → many output rows | `reduce` (`into` returns a list) | all of `over` | list rows carry own `:key` |
  | left-join two inputs by key | `join` | all of `over` | `left`/`right`/`into` |
  | group + `avg`/`sum`/`count` a relationship | `aggregate` | **none** (datastore GROUP BY) | a `has_many` on this resource |
  | anything else (LLM, fetch, bespoke) | `compute Mod` | up to the module | a `ReactiveDag.Op` |

  Rule of thumb: `aggregate` when the fold is a datastore aggregate over a
  relationship (pushdown, no rows in memory); `reduce` for any other in-BEAM fold;
  `compute` when no combinator fits.

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
    schema: [to: [type: :atom, required: true, doc: "the referenced node's id"]]
  }

  defmodule Reference do
    @moduledoc """
    A by-name REFERENCE input edge: the node READS the target as context but is NOT
    recomputed when the target changes. Still a real input (validated, ordered by
    depth so the target settles first, read at recompute) — it just doesn't
    propagate. For a node whose recompute is expensive/non-deterministic and
    consults mutable reference data it shouldn't be re-triggered by (an LLM step
    that looks up a human-curated people/positions table). Contrast `ref`, which
    dirties this node on change.
    """
    defstruct [:to, :__identifier__, :__spark_metadata__]
  end

  @reference %Spark.Dsl.Entity{
    name: :reference,
    target: Reference,
    args: [:to],
    describe: "A by-name REFERENCE edge: read the target as context; its changes do NOT recompute this node.",
    schema: [to: [type: :atom, required: true, doc: "the referenced node's id (read-only context)"]]
  }

  @compose_base %Spark.Dsl.Entity{
    name: :compose,
    target: Compose,
    args: [:op],
    describe: "An anonymous nested op-expression leg; composes inline as an intermediate cell.",
    schema: [
      op: [type: :atom, required: true, doc: "free-atom label for this intermediate cell (positional; see `ReactiveDag.Cell` for what `op` means)"],
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
    defstruct [:over, :group_by, :key, :into, :read, :upsert, :__identifier__, :__spark_metadata__]
  end

  @reduce %Spark.Dsl.Entity{
    name: :reduce,
    target: Reduce,
    describe: "Declarative fold: read `over`, group by `group_by`, reduce each group with `into`.",
    schema: [
      over: [type: :atom, required: true, doc: "the input node id whose payload is read + grouped"],
      read: [
        type: {:or, [{:fun, 1}, {:fun, 2}]},
        required: true,
        doc:
          "`(over_id -> [item])`, or `(over_id, dirty_keys -> [item])` to SCOPE the read to the claimed dirty keys (`nil` = whole-cell). Host domain: `Ash.read` etc."
      ],
      group_by: [type: {:fun, 1}, required: true, doc: "`(item -> group_term)` — the grouping key"],
      key: [
        type: {:fun, 1},
        required: true,
        doc: "`(group_term -> cell_key_string)` — the output tuple key for a group"
      ],
      into: [
        type: {:fun, 2},
        required: true,
        doc: "`(group_term, [item] -> row)` — reduce a group to its output row/payload"
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
    A declarative LEFT JOIN: read an input's payload, index it into a LEFT and a
    RIGHT side (each a `%{join_key => item}` built from a per-side key fn), then
    emit one row per left key joined to its right item (right may be absent). The
    common two-input reconcile/variance shape — the author writes the two side
    keys + the join row, not the read/write/changed plumbing.
    """
    defstruct [:over, :read, :left, :right, :key, :into, :upsert, :__identifier__, :__spark_metadata__]
  end

  @join %Spark.Dsl.Entity{
    name: :join,
    target: Join,
    describe: "Declarative left join: index `over` into left/right sides, emit a row per left key.",
    schema: [
      over: [type: :atom, required: true, doc: "the input node id whose payload is read + indexed"],
      read: [
        type: {:or, [{:fun, 1}, {:fun, 2}]},
        required: true,
        doc: "`(over_id -> [item])`, or `(over_id, dirty_keys -> [item])` to scope the read (see `reduce`)."
      ],
      left: [
        type: {:fun, 1},
        required: true,
        doc: "`(item -> join_key | nil)` — the LEFT side's key (nil = not on the left)"
      ],
      right: [
        type: {:fun, 1},
        required: true,
        doc: "`(item -> join_key | nil)` — the RIGHT side's key (nil = not on the right)"
      ],
      key: [
        type: {:fun, 1},
        required: true,
        doc: "`(join_key -> cell_key_string)` — the output tuple key"
      ],
      into: [
        type: {:fun, 3},
        required: true,
        doc: "`(join_key, left_item, right_item_or_nil -> row)` — the joined output row"
      ],
      upsert: [
        type: {:fun, 2},
        required: false,
        doc: "OPTIONAL override `(key, row -> boolean)`. OMIT it → the library writes the row into the node's own resource (see `ReactiveDag.Node.Payload`)."
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
    # Computation is declared with an ENTITY: `reduce`/`join`/`aggregate`
    # (declarative) or `compute Module` (the escape hatch). legs (ref/compose) are
    # the nested dependency form; dep is the flat form.
    entities: [@ref, @reference, @compose, @reduce, @join, @aggregate, @compute],
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
        doc: "how a child key maps to this cell's key on propagation"
      ],
      leaf?: [type: :boolean, default: false, doc: "true for a source-fed leaf (no compute)"],
      payload_key: [
        type: :atom,
        default: :key,
        doc:
          "the resource attribute the cell key writes to when the library closes the payload loop (a `reduce`/`join` with no `upsert:`). Defaults to `:key`."
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
          "VERDICT-ONLY node (named `verdict?` to match `leaf?`): its computed result lives entirely in the coordination tuple (`status`/`strength`), with NO payload table of its own. A `reduce`/`join` on a verdict node writes each row's `:status`/`:strength` straight into the tuple via `Op.put` — no resource, no `upsert:`, no attributes needed. Use for nodes whose output fits the tuple's fixed schema (e.g. a compliance verdict), as opposed to nodes that materialize typed rows. (Distinct from `ReactiveDag.Verdict`, the read-side status rollup.)"
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
      depends_on: [
        type: {:list, :atom},
        default: [],
        doc: "input node ids (flat form; equivalent to a `dep` per id)"
      ]
    ]
  }

  use Spark.Dsl.Extension, sections: [@reactive]

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
    |> ReactiveDag.Graph.build()
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
        {_id, cells} = lower(resource, cell_id(resource) |> to_string(), %{})
        cells

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
      |> Enum.filter(&(match?(%Ref{}, &1) or match?(%Reference{}, &1) or match?(%Compose{}, &1)))

    flat_refs = Ext.get_opt(resource, [:reactive], :depends_on, []) |> Enum.map(&%Ref{to: &1})

    # a `reduce`/`join over: :x` implies an input edge to :x (the node it reads).
    combinator_refs =
      case combinator(resource) do
        nil -> []
        c -> [%Ref{to: c.over}]
      end

    {:op, root_id, Ext.get_opt(resource, [:reactive], :op, nil),
     compute_module(resource),
     Ext.get_opt(resource, [:reactive], :key_rule, :identity),
     Ext.get_opt(resource, [:reactive], :leaf?, false), resource,
     legs ++ flat_refs ++ combinator_refs, extra_meta(resource)}
  end

  # the escape-hatch `compute Module` entity's module, or nil.
  defp compute_module(resource) do
    case Ext.get_entities(resource, [:reactive]) |> Enum.find(&match?(%Compute{}, &1)) do
      %Compute{module: m} -> m
      nil -> nil
    end
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

  # public, non-primary-key attributes — the payload columns a verdict node must not have.
  defp payload_attributes(resource) do
    resource
    |> Ash.Resource.Info.public_attributes()
    |> Enum.reject(& &1.primary_key?)
    |> Enum.map(& &1.name)
  end

  # the OPEN host binding folded into a cell's meta: the `meta:` keyword list, the
  # source/driver/over conveniences, and the combinator spec under its kind key
  # (`:reduce` | `:join`) — which ReactiveDag.Node.Recompute runs.
  defp extra_meta(resource) do
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

    Ext.get_opt(resource, [:reactive], :meta, [])
    |> Map.new()
    |> Map.merge(
      %{
        source: Ext.get_opt(resource, [:reactive], :source, nil),
        driver: Ext.get_opt(resource, [:reactive], :driver, nil),
        over: Ext.get_opt(resource, [:reactive], :over, nil),
        payload_key: Ext.get_opt(resource, [:reactive], :payload_key, nil),
        payload_action: Ext.get_opt(resource, [:reactive], :payload_action, nil),
        coordination_opts: Ext.get_opt(resource, [:reactive], :coordination_opts, nil),
        verdict: verdict_flag(resource),
        reference_inputs: reference_inputs(resource)
      }
      |> Enum.reject(fn {_k, v} -> is_nil(v) end)
      |> Map.new()
    )
    |> Map.merge(combinator_meta)
    |> Map.merge(aggregate_meta)
  end

  # the ids of this node's `reference` edges (read-as-context, non-propagating) —
  # as strings matching the cell input ids, so `Graph.build_parents` can exclude
  # them from the propagation graph. nil when there are none.
  defp reference_inputs(resource) do
    case Ext.get_entities(resource, [:reactive]) |> Enum.filter(&match?(%Reference{}, &1)) do
      [] -> nil
      refs -> Enum.map(refs, &to_string(&1.to))
    end
  end

  defp walk_cbs do
    %{
      classify: fn
        %Ref{} -> :ref
        # a reference edge is an input edge too — it just won't propagate (the
        # non-propagation is enforced in Graph.build_parents via reference_inputs).
        %Reference{} -> :ref
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
        %Ref{to: to} -> to_string(to)
        %Reference{to: to} -> to_string(to)
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
      meta:
        Map.merge(%{resource: resource, compute: compute, key_rule: key_rule}, extra)
    }
  end

  defp build_cell(id, %Compose{op: op, compute: compute, key_rule: key_rule, leaf?: leaf?, meta: meta}, input_ids) do
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

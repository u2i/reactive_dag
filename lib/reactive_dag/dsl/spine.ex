defmodule ReactiveDag.Dsl.Spine do
  @moduledoc """
  The **shared authoring grammar** for a reactive DAG: one `graph do … end`
  section carrying the spine entities every host needs — `source`, `observed`,
  `node`, `ref`, `compose` — plus the `ReactiveDag.Source` seam and the leaf↔
  scanner validation. Both a data pipeline and a compliance model author against
  this same grammar; each keeps its own DOMAIN vocabulary as *op-kinds + meta* on
  the open `node` entity (or, if it needs typed fields, as its own entities added
  alongside — see "Domain vocabulary" below).

  ## Authoring

      defmodule MyApp.Pipeline do
        use ReactiveDag.Dsl.Spine

        graph do
          # a SCANNER — reads external state into a leaf, out-of-band (see
          # `ReactiveDag.Source`). `driver` must implement that behaviour, checked
          # at compile time.
          source :fleet_scan, driver: MyApp.Sources.FleetScan

          # an OBSERVED leaf — the substrate a scanner feeds. `fed_by` names a
          # declared `source` (validated at compile time: an unknown id fails).
          observed :machines, grain: :machine, strength: :measured, fed_by: :fleet_scan

          # a derived NODE — an op over input cells named by `ref`. `op` is an OPEN
          # atom: the library schedules the graph; the host's recompute interprets
          # the op-kind. `meta` carries anything the host needs and the library
          # never reads. Options are set INSIDE the block (`op`/`key_rule`/`meta`
          # as setters), like Ash's `attributes do attribute … end`.
          node :fleet_health do
            op :reduce
            meta grain: :machine
            ref :machines
          end

          # nested COMPOSE — an anonymous intermediate op-expression, so the algebra
          # reads as a tree rather than a pile of named nodes.
          node :variance do
            op :join
            ref :machines

            compose :fold do
              as :rolling
              ref :fleet_health
            end
          end
        end
      end

  ## What the library owns vs the host

  * **Library:** the `graph` section, the five spine entities, the structural
    checks (every `ref`/input resolves, ids unique, acyclic — via
    `ReactiveDag.Graph.build/1`), the `fed_by → source` compile-time check, and
    the `ReactiveDag.Source` behaviour + `verify!/2`.
  * **Host:** the meaning of each `op` (its recompute, via the
    `ReactiveDag.RecomputeStrategy` seam), the `poll` fetch inside each driver,
    and any DOMAIN-typed fields it wants beyond the open `meta:`.

  ## Domain vocabulary (op-kinds vs. typed entities)

  A host expresses its domain two ways, and can mix them:

  1. **op-kind + meta (default).** `node :g, op: :guarantee, meta: [claim: "…"]`.
     Zero host-side DSL code; the op-kind is a free atom and the fields ride in
     `meta`. Best when the domain fields don't need their own compile-time typing.
  2. **A typed host entity.** If a domain concept has fields that deserve Spark
     validation (`{:one_of, …}`, cross-referenced id lists), the host defines its
     own entity and adds it to the `graph` section. The spine exposes the section
     for this composition; the host lowers its entity through the same
     `ReactiveDag.Lowering` path the spine uses. (This is the "compose, don't
     flatten" path from ADR-002 — the spine is shared, the typed domain entity
     stays the host's.)

  ## Lowering + assembly

  `ReactiveDag.Dsl.Spine.Info.plan/1` lowers a module's `graph` block to a
  `ReactiveDag.Plan` (structural validation runs in the transformer at compile
  time; `plan/1` returns the built plan for the drain). `sources/1` returns the
  declared scanner driver modules — feed them to `ReactiveDag.Source.verify!/2`
  once the plan exists.
  """

  # ── spine entity structs ──────────────────────────────────────────────────

  defmodule Source do
    @moduledoc "A declared scanner: `source :id, driver: Mod`. `driver` implements `ReactiveDag.Source`."
    @type t :: %__MODULE__{id: atom(), driver: module()}
    defstruct [:id, :driver, __identifier__: nil, __spark_metadata__: nil]
  end

  defmodule Observed do
    @moduledoc """
    A source-fed leaf: `observed :id, grain:, strength:, fed_by:`. `fed_by` is the
    first-class edge to a declared `source` (validated at compile time).
    """
    @type t :: %__MODULE__{
            id: atom(),
            grain: atom() | nil,
            strength: atom(),
            fed_by: atom() | nil,
            meta: keyword()
          }
    defstruct [
      :id,
      :grain,
      :fed_by,
      strength: :measured,
      meta: [],
      __identifier__: nil,
      __spark_metadata__: nil
    ]
  end

  defmodule Ref do
    @moduledoc "A by-name edge to another node (`ref :id`) — resolves to an existing cell, emits none."
    @type t :: %__MODULE__{to: atom()}
    defstruct [:to, :__identifier__, :__spark_metadata__]
  end

  defmodule Compose do
    @moduledoc """
    An anonymous nested op-expression leg — composes inline as an intermediate
    cell. Options are set inside its block: `compose :op do as :id; meta … end`.
    """
    @type t :: %__MODULE__{
            op: atom(),
            as: atom() | nil,
            leaf?: boolean() | nil,
            key_rule: :identity | :all,
            meta: keyword(),
            legs: list()
          }
    defstruct [
      :op,
      :as,
      :leaf?,
      key_rule: :identity,
      meta: [],
      legs: [],
      __identifier__: nil,
      __spark_metadata__: nil
    ]
  end

  defmodule Node do
    @moduledoc """
    A derived node. Options are set inside its block (like Ash's `attributes`):

        node :health do
          op :reduce            # the op-kind (open atom)
          key_rule :all         # optional (default :identity)
          meta grain: :machine  # open host binding
          ref :machines         # input edges
        end
    """
    @type t :: %__MODULE__{
            id: atom(),
            op: atom(),
            leaf?: boolean() | nil,
            key_rule: :identity | :all,
            meta: keyword(),
            legs: list()
          }
    defstruct [
      :id,
      :op,
      :leaf?,
      key_rule: :identity,
      meta: [],
      legs: [],
      __identifier__: nil,
      __spark_metadata__: nil
    ]
  end

  # ── Spark entities ─────────────────────────────────────────────────────────

  @source %Spark.Dsl.Entity{
    name: :source,
    target: Source,
    args: [:id],
    describe: "A scanner (phase-1 source): reads external state into a leaf, out-of-band.",
    schema: [
      id: [type: :atom, required: true, doc: "stable source id (referenced by `observed.fed_by`)."],
      driver: [
        type: {:behaviour, ReactiveDag.Source},
        required: true,
        doc: "a module implementing `ReactiveDag.Source` (checked at compile time)."
      ]
    ]
  }

  @observed %Spark.Dsl.Entity{
    name: :observed,
    target: Observed,
    args: [:id],
    describe: "A source-fed leaf cell — the substrate a scanner writes.",
    schema: [
      id: [type: :atom, required: true, doc: "the leaf cell id."],
      grain: [type: :atom, doc: "what each member of the leaf is."],
      strength: [type: :atom, default: :measured, doc: "the observation's fidelity."],
      fed_by: [type: :atom, doc: "a declared `source :id` — validated at compile time."],
      meta: [type: :keyword_list, default: [], doc: "open host binding merged into the cell's meta."]
    ]
  }

  @ref %Spark.Dsl.Entity{
    name: :ref,
    target: Ref,
    args: [:to],
    describe: "A by-name edge to another node (resolves to an existing cell).",
    schema: [to: [type: :atom, required: true, doc: "the referenced node's id."]]
  }

  @compose_base %Spark.Dsl.Entity{
    name: :compose,
    target: Compose,
    args: [:op],
    describe: "An anonymous nested op-expression leg; composes inline as an intermediate cell.",
    schema: [
      op: [type: :atom, required: true, doc: "the op kind for this intermediate cell (open atom)."],
      as: [type: :atom, doc: "an explicit id for this intermediate cell (else positional)."],
      key_rule: [type: {:one_of, [:identity, :all]}, default: :identity],
      leaf?: [type: :boolean, default: false, doc: "true for a composed leaf (a source-fed set)."],
      meta: [type: :keyword_list, default: [], doc: "open host binding for this intermediate cell."]
    ]
  }
  # self-nest so a compose can hold compose legs (bounded depth, mirrors Node).
  @compose Enum.reduce(1..8, @compose_base, fn _i, child ->
             %{@compose_base | entities: [legs: [@ref, child]]}
           end)

  @node %Spark.Dsl.Entity{
    name: :node,
    target: Node,
    args: [:id],
    describe: "A derived node: an op over input cells (by `ref`/`compose`). `op` is an open atom.",
    entities: [legs: [@ref, @compose]],
    schema: [
      id: [type: :atom, required: true, doc: "the cell id."],
      op: [type: :atom, required: true, doc: "the op kind (open atom; the host's recompute interprets it)."],
      key_rule: [
        type: {:one_of, [:identity, :all]},
        default: :identity,
        doc: "how a child key maps to this cell's key on propagation."
      ],
      leaf?: [type: :boolean, default: false],
      meta: [
        type: :keyword_list,
        default: [],
        doc: "open host binding (grain/compute/claim/…) — merged into meta; the library never reads it."
      ]
    ]
  }

  @graph %Spark.Dsl.Section{
    name: :graph,
    describe: "The reactive DAG: scanners, observed leaves, and derived nodes over them.",
    entities: [@source, @observed, @node, @ref, @compose]
  }

  use Spark.Dsl.Extension,
    sections: [@graph],
    transformers: [ReactiveDag.Dsl.Spine.Transformer]
end

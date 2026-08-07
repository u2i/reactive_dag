defmodule ReactiveDag.DslCompositionTest do
  @moduledoc """
  A DOMAIN VOCABULARY composed onto the reactive_dag DSL — the mechanism that lets
  a host (e.g. the compliance portal) author its own nouns INSIDE `reactive do … end`
  rather than forking a separate DSL, using only:

    * `Spark.Dsl.Section` `patchable?: true` on `@reactive` (the one lib affordance),
    * `Spark.Dsl.Patch.AddEntity` — the host adds its entities into `[:reactive]`,
    * a host TRANSFORMER composed via `use Spark.Dsl.Extension, transformers: […]`
      that derives values over the composed entities at COMPILE time and persists
      the result for runtime read.

  The worked example is a miniature compliance model: a `guarantee` noun + an
  `addresses` (control-coverage) noun added to the reactive section, a compile-time
  `Derive` transformer that computes each node's evidence `strength` bottom-up (the
  portal's grain/strength analog) and persists it, and lowering to a `verdict?`
  cell — proving the node still rides `ReactiveDag.Node.cells/graph`.
  """
  use ExUnit.Case, async: true

  # ── the host "compliance" vocabulary as a composable Spark extension ──────────

  defmodule Guarantee do
    @moduledoc false
    defstruct [:claim, :__identifier__, :__spark_metadata__]
  end

  defmodule Addresses do
    @moduledoc false
    defstruct [:control, :__identifier__, :__spark_metadata__]
  end

  # a compile-time transformer: derive each node's evidence `strength` from its op
  # (declared=weak, measured=strong, reconcile=the weaker leg…), and PERSIST it so a
  # runtime read gets a compiled answer — no runtime derivation. Here we keep it
  # trivial: reconcile ⇒ :measured, leaf ⇒ :declared. The point is the SEAM, not the
  # lattice.
  defmodule Derive do
    @moduledoc false
    use Spark.Dsl.Transformer
    alias Spark.Dsl.Transformer

    @impl true
    def transform(dsl) do
      op = Transformer.get_option(dsl, [:reactive], :op)
      strength = if op == :reconcile, do: :measured, else: :declared
      {:ok, Transformer.persist(dsl, :compliance_strength, strength)}
    end
  end

  defmodule ComplianceExt do
    @moduledoc false
    @guarantee %Spark.Dsl.Entity{
      name: :guarantee,
      target: Guarantee,
      args: [:claim],
      describe: "a compliance claim this node certifies",
      schema: [claim: [type: :string, required: true]]
    }

    @addresses %Spark.Dsl.Entity{
      name: :addresses,
      target: Addresses,
      args: [:control],
      describe: "a control this guarantee helps satisfy",
      schema: [control: [type: :atom, required: true]]
    }

    use Spark.Dsl.Extension,
      dsl_patches: [
        %Spark.Dsl.Patch.AddEntity{section_path: [:reactive], entity: @guarantee},
        %Spark.Dsl.Patch.AddEntity{section_path: [:reactive], entity: @addresses}
      ],
      transformers: [Derive]

    # read-back helpers (the host's introspection over its own composed entities)
    def guarantee(module) do
      Spark.Dsl.Extension.get_entities(module, [:reactive])
      |> Enum.find(&match?(%Guarantee{}, &1))
    end

    def addresses(module) do
      for %Addresses{control: c} <- Spark.Dsl.Extension.get_entities(module, [:reactive]), do: c
    end

    def strength(module),
      do: Spark.Dsl.Extension.get_persisted(module, :compliance_strength)
  end

  # ── an in-memory Ash domain + two composed nodes (NO tables) ──────────────────

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false
    resources do
      allow_unregistered?(true)
    end
  end

  # a leaf the guarantee reconciles against (a source-fed set).
  defmodule Baseline do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Simple, extensions: [ReactiveDag.Node]

    reactive do
      id :baseline
      op :leaf
      leaf? true
      source :baseline_scan
    end
  end

  # the guarantee node: a verdict? node (no payload table) authored with the
  # composed compliance nouns AND the lib's own combinator (op :reconcile + a ref).
  defmodule MergeGated do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Simple,
      extensions: [ReactiveDag.Node, ComplianceExt]

    reactive do
      id :merge_gated
      op :reconcile
      verdict? true
      guarantee "every merge to a protected branch was reviewed & gated"
      addresses :CC8_1
      addresses :CC8_2
      ref :baseline
    end
  end

  # ── the checks ────────────────────────────────────────────────────────────────

  test "the composed compliance nouns are authored inside `reactive do…end`" do
    assert ComplianceExt.guarantee(MergeGated).claim =~ "reviewed & gated"
    assert ComplianceExt.addresses(MergeGated) == [:CC8_1, :CC8_2]
  end

  test "the host derivation transformer ran at COMPILE time and persisted its result" do
    # merge_gated composes ComplianceExt, so its Derive transformer ran over the
    # node's op at compile time (reconcile → :measured) and persisted the result —
    # a compiled answer, no runtime derivation. Baseline doesn't compose the ext,
    # so it has no compliance strength (the derivation is opt-in per host node).
    assert ComplianceExt.strength(MergeGated) == :measured
    assert ComplianceExt.strength(Baseline) == nil
  end

  test "the composed node still lowers to a cell via the lib (verdict? + ref edge)" do
    [cell] = ReactiveDag.Node.cells(MergeGated)
    assert cell.id == "merge_gated"
    assert cell.op == :reconcile
    # the lib's own `verdict?` rode through untouched by the composition
    # (lowering keys it as meta[:verdict])
    assert cell.meta[:verdict] == true
    # the ref became a propagating input edge to the baseline leaf's cell
    assert cell.inputs == ["baseline"]
  end

  test "the whole graph assembles + validates through the shared substrate" do
    plan = ReactiveDag.Node.graph([Baseline, MergeGated])

    assert Map.has_key?(plan.cells, "merge_gated")
    assert Map.has_key?(plan.cells, "baseline")
    # baseline is an input of merge_gated → it is at a shallower depth (a parent edge)
    assert plan.parents["baseline"] == ["merge_gated"]
    assert plan.depths["baseline"] < plan.depths["merge_gated"]
  end

  test "domain nouns are invisible to the lib's lowering (they ride the host's introspection)" do
    # the cell the lib built carries NO guarantee/addresses — those are the host's,
    # read via ComplianceExt, not the substrate. The lib stays domain-agnostic.
    [cell] = ReactiveDag.Node.cells(MergeGated)
    refute Map.has_key?(cell.meta, :guarantee)
    refute Map.has_key?(cell.meta, :claim)
  end
end

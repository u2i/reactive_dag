defmodule ReactiveDag.ComplianceAuthoringExampleTest do
  @moduledoc """
  A FAITHFUL portal-shaped example: the real `merge_gated` guarantee (its
  fold→reconcile→(declared⟗observed) tree + claim/addresses/shape/population/
  conformance nouns) authored on the reactive_dag DSL composition seam, organized
  under a parent module per R-section. Proves the authoring shape actually compiles
  + lowers, and that a compile-time derivation transformer reads the composed
  entities. (Mirrors the portal prototype at docs/prototype/compliance_on_node.exs.)
  """
  use ExUnit.Case, async: true

  # ── noun target structs ───────────────────────────────────────────────────────
  defmodule Claim, do: (defstruct [:text, :__identifier__, :__spark_metadata__])
  defmodule Addresses, do: (defstruct [:controls, :__identifier__, :__spark_metadata__])
  defmodule Shape, do: (defstruct [:kind, :__identifier__, :__spark_metadata__])
  defmodule Population, do: (defstruct [:kind, :__identifier__, :__spark_metadata__])
  defmodule Conformance, do: (defstruct [:tolerance_days, :working_days, :__identifier__, :__spark_metadata__])

  # ── a compile-time derivation transformer over the composed op-tree ───────────
  # derives the guarantee's evidence strength = the weakest leaf strength in meta
  # (the portal's op_strength analog, reading meta instead of struct fields).
  defmodule DeriveStrength do
    use Spark.Dsl.Transformer
    alias Spark.Dsl.Transformer

    @order [measured: 3, examined: 2, derived: 1, declared: 0]

    @impl true
    def transform(dsl) do
      strengths =
        dsl
        |> Transformer.get_entities([:reactive])
        |> collect_strengths()

      weakest =
        case strengths do
          [] -> nil
          ss -> Enum.min_by(ss, &Keyword.get(@order, &1, 99))
        end

      {:ok, Transformer.persist(dsl, :guarantee_strength, weakest)}
    end

    defp collect_strengths(entities) do
      Enum.flat_map(entities, fn e ->
        s = e |> meta_of() |> Keyword.get(:strength)
        legs = Map.get(e, :legs, [])
        List.wrap(s) ++ collect_strengths(legs)
      end)
    end

    defp meta_of(%{meta: m}) when is_list(m), do: m
    defp meta_of(_), do: []
  end

  # ── the compliance vocabulary composed onto ReactiveDag.Node ──────────────────
  defmodule Ext do
    @claim %Spark.Dsl.Entity{name: :claim, target: Claim, args: [:text], schema: [text: [type: :string, required: true]]}
    @addresses %Spark.Dsl.Entity{name: :addresses, target: Addresses, args: [:controls], schema: [controls: [type: {:list, :atom}]]}
    @shape %Spark.Dsl.Entity{name: :shape, target: Shape, args: [:kind], schema: [kind: [type: {:one_of, [:subject, :delta, :queue, :review, :readiness]}]]}
    @population %Spark.Dsl.Entity{name: :population, target: Population, args: [:kind], schema: [kind: [type: {:one_of, [:sites, :events, :dated]}]]}
    @conformance %Spark.Dsl.Entity{name: :conformance, target: Conformance, schema: [tolerance_days: [type: :integer], working_days: [type: :boolean]]}

    use Spark.Dsl.Extension,
      dsl_patches: [
        %Spark.Dsl.Patch.AddEntity{section_path: [:reactive], entity: @claim},
        %Spark.Dsl.Patch.AddEntity{section_path: [:reactive], entity: @addresses},
        %Spark.Dsl.Patch.AddEntity{section_path: [:reactive], entity: @shape},
        %Spark.Dsl.Patch.AddEntity{section_path: [:reactive], entity: @population},
        %Spark.Dsl.Patch.AddEntity{section_path: [:reactive], entity: @conformance}
      ],
      transformers: [DeriveStrength]

    def claim(m), do: Spark.Dsl.Extension.get_entities(m, [:reactive]) |> Enum.find(&match?(%Claim{}, &1))
    def addresses(m), do: (Spark.Dsl.Extension.get_entities(m, [:reactive]) |> Enum.find(&match?(%Addresses{}, &1)) || %Addresses{}).controls
    def strength(m), do: Spark.Dsl.Extension.get_persisted(m, :guarantee_strength)
  end

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false
    resources do: allow_unregistered?(true)
  end

  # a shared leaf the guarantee's observed leg is fed from (a value node, no compliance nouns)
  defmodule Values.RepoProtection do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Simple, extensions: [ReactiveDag.Node]
    reactive do
      id :repo_protection
      op :leaf
      leaf? true
      source :repo_protection
    end
  end

  # ── R5 · change & SDLC — merge_gated, faithfully ──────────────────────────────
  defmodule R5_ChangeSdlc.MergeGated do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Simple, extensions: [ReactiveDag.Node, Ext]

    reactive do
      id :merge_gated
      verdict? true

      claim "every merge to a protected branch was reviewed & gated"
      addresses [:CC8_1]
      shape :queue
      population :events
      conformance tolerance_days: 5, working_days: true

      op :fold
      meta grain: :change

      # NO `as:` — the lib auto-derives positional ids ("#{parent}/#{i}") exactly
      # like the portal's tree-position id grammar. The structure IS the id.
      compose :reconcile do
        meta grain: :"app×repo"

        compose :declared do
          leaf? true
          meta grain: :"app×repo", strength: :declared
        end

        compose :observed do
          leaf? true
          meta grain: :"app×repo", strength: :measured, source: :repo_protection
        end
      end
    end
  end

  alias R5_ChangeSdlc.MergeGated

  test "the compliance nouns author faithfully inside `reactive do…end`" do
    assert Ext.claim(MergeGated).text =~ "reviewed & gated"
    assert Ext.addresses(MergeGated) == [:CC8_1]
  end

  test "the derivation transformer computed strength over the composed op-tree at compile time" do
    # weakest of {declared, measured} = declared — a COMPILED answer, no runtime pass.
    assert Ext.strength(MergeGated) == :declared
  end

  test "merge_gated lowers to its real cell tree (fold → reconcile → 2 leaves), ids auto-derived" do
    cells = ReactiveDag.Node.cells(MergeGated) |> Map.new(&{&1.id, &1})

    # auto-derived positional ids: root=merge_gated, its 0th leg=merge_gated/0
    # (the reconcile), its legs merge_gated/0/0 + merge_gated/0/1 — the structure
    # IS the id, no `as:` authored.
    assert cells["merge_gated"].op == :fold
    assert cells["merge_gated"].meta[:verdict] == true
    assert cells["merge_gated/0"].op == :reconcile
    assert cells["merge_gated/0/0"].leaf?
    assert cells["merge_gated/0/1"].leaf?

    # the fold's input is the reconcile; the reconcile's inputs are its two leaves
    assert cells["merge_gated"].inputs == ["merge_gated/0"]
    assert Enum.sort(cells["merge_gated/0"].inputs) ==
             ["merge_gated/0/0", "merge_gated/0/1"]
  end

  test "the whole model assembles through the shared substrate" do
    plan = ReactiveDag.Node.graph([Values.RepoProtection, MergeGated])
    assert Map.has_key?(plan.cells, "merge_gated")
    assert Map.has_key?(plan.cells, "merge_gated/0/1")
    # depth-ordered: leaves shallower than the reconcile shallower than the fold
    assert plan.depths["merge_gated/0/0"] < plan.depths["merge_gated/0"]
    assert plan.depths["merge_gated/0"] < plan.depths["merge_gated"]
  end
end

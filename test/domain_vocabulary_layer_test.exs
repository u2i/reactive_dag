defmodule ReactiveDag.DomainVocabularyLayerTest do
  @moduledoc """
  SPIKE: the appropriate way to preserve a rich DOMAIN VOCABULARY (the portal's
  `guarantee`/`op`) while building ON the substrate — a domain Spark DSL whose
  compile-time lowering emits `ReactiveDag.Cell`s (via the shared Lowering.walk)
  and binds the op-kinds to `ReactiveDag.SetOp` templates. The vocabulary is SUGAR
  over the substrate: authors still write `guarantee`, but there's no bespoke
  cell-builder and no bespoke recompute — both reduce to library primitives.

  This models the stacked-extension architecture: `ReactiveDag.Node` is the
  substrate (vocabulary-neutral); a DOMAIN layer sits on top and lowers its
  entities to Node-shaped cells + SetOp bindings. Here the domain layer is a plain
  Spark.Dsl (a `compliance do … end` section) to keep the spike self-contained; in
  a host it would be its own extension composed with the substrate.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.{Cell, Lowering, SetOp}

  # ── the DOMAIN VOCABULARY DSL (a tiny portal-shaped `guarantee` language) ────
  defmodule Node do
    defmodule Guarantee, do: defstruct([:id, :set, :__identifier__, :__spark_metadata__])
    defmodule Op, do: defstruct([:type, :legs, :__identifier__, :__spark_metadata__])
    defmodule Leaf, do: defstruct([:id, :__identifier__, :__spark_metadata__])
  end

  defmodule Dsl do
    @leaf %Spark.Dsl.Entity{
      name: :leaf,
      target: ReactiveDag.DomainVocabularyLayerTest.Node.Leaf,
      args: [:id],
      schema: [id: [type: :atom, required: true]]
    }

    @op_base %Spark.Dsl.Entity{
      name: :op,
      target: ReactiveDag.DomainVocabularyLayerTest.Node.Op,
      args: [:type],
      schema: [type: [type: :atom, required: true]]
    }
    @op Enum.reduce(1..4, @op_base, fn _, child ->
          %{@op_base | entities: [legs: [@leaf, child]]}
        end)

    @guarantee %Spark.Dsl.Entity{
      name: :guarantee,
      target: ReactiveDag.DomainVocabularyLayerTest.Node.Guarantee,
      args: [:id],
      entities: [set: [@op, @leaf]],
      schema: [id: [type: :atom, required: true]]
    }

    @compliance %Spark.Dsl.Section{name: :compliance, top_level?: true, entities: [@guarantee]}

    use Spark.Dsl.Extension, sections: [@compliance]
  end

  defmodule Compliance do
    use Spark.Dsl, default_extensions: [extensions: [ReactiveDag.DomainVocabularyLayerTest.Dsl]]
  end

  # ── a catalog written in the DOMAIN VOCABULARY (preserved!) ──────────────────
  defmodule Catalog do
    use ReactiveDag.DomainVocabularyLayerTest.Compliance

    guarantee :edr_live do
      op :reconcile do
        leaf(:entitled)
        leaf(:observed)
      end
    end
  end

  # ── THE DOMAIN LAYER: lower the vocabulary → Node cells via the SHARED walk ──
  # This is the whole "cell builder", and it's just walk callbacks + a guarantee
  # wrapper — the portal's guarantee_cells/node_cells reduced to the primitive.
  defp walk_cbs do
    %{
      classify: fn
        %Node.Op{} -> :op
        %Node.Leaf{} -> :leaf
      end,
      legs: fn %Node.Op{legs: legs} -> legs end,
      leg_id: fn parent, i, _ -> "#{parent}/#{i}" end,
      ref_id: fn _ -> raise "no refs in this spike" end,
      to_cell: fn id, node, inputs ->
        case node do
          %Node.Op{type: t} -> %Cell{id: id, op: t, inputs: inputs, meta: %{}}
          %Node.Leaf{} -> %Cell{id: id, op: :leaf, leaf?: true, meta: %{}}
        end
      end
    }
  end

  defp lower(module) do
    guarantees = Spark.Dsl.Extension.get_entities(module, [:compliance])

    Enum.flat_map(guarantees, fn %Node.Guarantee{id: gid, set: [set]} ->
      {set_id, sub} = Lowering.walk("g:#{gid}/set", set, walk_cbs())
      gcell = %Cell{id: "g:#{gid}", op: :guarantee, inputs: [set_id], meta: %{watched?: true}}
      [gcell | sub]
    end)
  end

  # ── op-kinds bound to SetOp TEMPLATES (in-memory here; SQL in the real host) ──
  # the SAME recompute the portal's SetOp registry provides, over a toy store.
  defp templates(store) do
    %{
      reconcile: fn %Cell{inputs: [e, h]}, _dirty ->
        keys = MapSet.union(keyset(store, e), keyset(store, h))
        rows = for k <- keys, do: {k, both?(store, e, h, k)}
        {:ok, rows}
      end,
      guarantee: fn %Cell{inputs: [set]}, _dirty ->
        {:ok, for({k, :failing} <- verdicts(store, set), do: {k, :failing})}
      end
    }
  end

  defp keyset(store, id), do: store[id] |> Map.keys() |> MapSet.new()
  defp both?(store, e, h, k) do
    if Map.has_key?(store[e], k) and Map.has_key?(store[h], k), do: :present, else: :failing
  end
  defp verdicts(store, id), do: Map.get(store, id, %{})

  # drive bottom-up via Graph.build depths + SetOp, materializing into the store.
  # Each step (re)registers the SetOp templates closed over the CURRENT store, so
  # a cell's recompute reads its already-computed inputs — exactly what the real
  # SetOp does against model_tuple, here against the in-memory store.
  defp evaluate(cells, seed) do
    plan = ReactiveDag.Graph.build(cells)
    order = Enum.sort_by(Map.keys(plan.depths), &plan.depths[&1])

    Enum.reduce(order, seed, fn id, store ->
      cell = plan.cells[id]
      # leaves are seeded by their source (here, `seed`); the drain never
      # recomputes them — skip so SetOp's leaf path (a DB read) isn't hit.
      if cell.leaf? do
        store
      else
        Application.put_env(:reactive_dag, :set_op_templates, templates(store))
        {:ok, rows} = SetOp.recompute(cell, ["*"])
        Map.put(store, id, Map.new(rows))
      end
    end)
  end

  # ── tests ────────────────────────────────────────────────────────────────────
  test "the domain vocabulary is preserved AND lowers to Node cells via the shared walk" do
    cells = lower(Catalog)
    ids = cells |> Enum.map(& &1.id) |> Enum.sort()
    assert ids == ["g:edr_live", "g:edr_live/set", "g:edr_live/set/0", "g:edr_live/set/1"]

    # the guarantee entity survived to the DSL (authors wrote `guarantee :edr_live`)…
    [%Node.Guarantee{id: :edr_live}] = Spark.Dsl.Extension.get_entities(Catalog, [:compliance])
    # …and lowered to a :guarantee cell over a :reconcile cell (substrate shape).
    assert Enum.find(cells, &(&1.op == :guarantee)).inputs == ["g:edr_live/set"]
    assert Enum.find(cells, &(&1.op == :reconcile)).inputs == ["g:edr_live/set/0", "g:edr_live/set/1"]
  end

  test "the lowered graph runs on SetOp templates — drift surfaces as the failing-set" do
    cells = lower(Catalog)
    seed = %{
      "g:edr_live/set/0" => %{"alice" => :present, "bob" => :present},
      "g:edr_live/set/1" => %{"alice" => :present}
    }

    store = evaluate(cells, seed)
    assert store["g:edr_live"] == %{"bob" => :failing}
  end
end

defmodule ReactiveDag.PortalSkinSpikeTest do
  @moduledoc """
  SPIKE (no DB): can the portal's compositional DSL (a `guarantee` over an `op`
  expression tree, set-based recompute) be expressed as a THIN SKIN over library
  primitives — `Lowering.walk` for the tree→cells step + a set-op COMBINATOR for
  the op-kind recompute — so the portal needs no SEPARATE cell-builder and no
  separate `RecomputeStrategy` dispatch table?

  We model a tiny portal-shaped DSL, lower it with the SHARED walk, and run the
  op-kind recompute as a combinator over an in-memory tuple store. If the failing-
  set comes out right with ZERO bespoke lowering/recompute, the skin reduces to
  the primitives. (The drain itself is tested elsewhere; this isolates the
  authoring→cells→recompute layering.)
  """
  use ExUnit.Case, async: true

  alias ReactiveDag.{Cell, Lowering}

  # ── a tiny portal-shaped DSL (plain structs, as the portal's Node.* are) ─────
  defmodule G, do: defstruct([:id, :set])                 # guarantee: check∅ over a set
  defmodule Op, do: defstruct([:type, :legs])             # product/reconcile/… over legs
  defmodule Ref, do: defstruct([:to])                     # by-name leg
  defmodule Leaf, do: defstruct([:id])                    # a source-fed leaf

  # A guarantee: "every entitled key must be observed" — a reconcile of two leaves.
  defp catalog do
    %G{
      id: :edr_live,
      set: %Op{type: :reconcile, legs: [%Leaf{id: :entitled}, %Leaf{id: :observed}]}
    }
  end

  # ── THE SKIN: lower the DSL → cells using the SHARED Lowering.walk ───────────
  # This is the whole "portal cell-builder" — but it's just walk callbacks + a
  # guarantee wrapper. No bespoke recursion.
  defp walk_cbs do
    %{
      classify: fn
        %Op{} -> :op
        %Ref{} -> :ref
        %Leaf{} -> :leaf
      end,
      legs: fn %Op{legs: legs} -> legs end,
      leg_id: fn parent, i, _ -> "#{parent}/#{i}" end,
      ref_id: fn %Ref{to: to} -> "val:#{to}" end,
      to_cell: fn id, node, inputs ->
        case node do
          %Op{type: t} -> %Cell{id: id, op: t, inputs: inputs, meta: %{}}
          %Leaf{} -> %Cell{id: id, op: :leaf, leaf?: true, inputs: [], meta: %{}}
        end
      end
    }
  end

  defp lower(%G{id: gid, set: set}) do
    {set_id, sub} = Lowering.walk("g:#{gid}/set", set, walk_cbs())
    gcell = %Cell{id: "g:#{gid}", op: :guarantee, inputs: [set_id], meta: %{watched?: true}}
    [gcell | sub]
  end

  # ── THE SET-OP COMBINATOR: op-kind recompute over an in-memory tuple store ───
  # This is the layered analog of `reduce`/`join` for SET ops — the portal's
  # `reconcile`/`product`/`guarantee` verdicts as a combinator, NOT a per-op SQL
  # template. `store` is %{cell_id => %{key => status}}; returns the cell's rows.
  defp recompute(%Cell{op: :leaf, id: id}, store), do: Map.get(store, id, %{})

  defp recompute(%Cell{op: :reconcile, inputs: [e_in, o_in]}, store) do
    e = store[e_in] |> Map.keys() |> MapSet.new()
    o = store[o_in] |> Map.keys() |> MapSet.new()
    # present iff on both; failing iff on exactly one (drift) — the reconcile law.
    for k <- MapSet.union(e, o), into: %{} do
      status = if MapSet.member?(e, k) and MapSet.member?(o, k), do: "present", else: "failing"
      {k, status}
    end
  end

  defp recompute(%Cell{op: :guarantee, inputs: [set_in]}, store) do
    # the guarantee's set IS the set cell's FAILING keys (green = empty).
    for {k, "failing"} <- store[set_in], into: %{}, do: {k, "failing"}
  end

  # drive the whole graph bottom-up (depth order) over the store — the drain's job,
  # done inline for the spike so we can assert the end verdict.
  defp evaluate(cells, seed) do
    plan = ReactiveDag.Graph.build(cells)
    order = Enum.sort_by(Map.keys(plan.depths), &plan.depths[&1])

    Enum.reduce(order, seed, fn id, store ->
      cell = plan.cells[id]
      Map.put(store, id, recompute(cell, store))
    end)
  end

  # ── the tests ────────────────────────────────────────────────────────────────
  test "the skin lowers a guarantee-over-op-tree to cells via the shared walk" do
    cells = lower(catalog())
    ids = cells |> Enum.map(& &1.id) |> Enum.sort()

    # g:edr_live (guarantee) → g:edr_live/set (reconcile) → its two leaf legs.
    assert ids == ["g:edr_live", "g:edr_live/set", "g:edr_live/set/0", "g:edr_live/set/1"]
    g = Enum.find(cells, &(&1.op == :guarantee))
    assert g.inputs == ["g:edr_live/set"]
    recon = Enum.find(cells, &(&1.op == :reconcile))
    assert recon.inputs == ["g:edr_live/set/0", "g:edr_live/set/1"]
  end

  test "the layered set-op combinator computes the guarantee's failing-set — DRIFT case" do
    cells = lower(catalog())

    # entitled = {alice, bob}; observed = {alice} → bob is DRIFT (entitled, unobserved).
    seed = %{
      "g:edr_live/set/0" => %{"alice" => "present", "bob" => "present"},
      "g:edr_live/set/1" => %{"alice" => "present"}
    }

    store = evaluate(cells, seed)
    # the guarantee surfaces bob as failing; alice agreed → not in the failing-set.
    assert store["g:edr_live"] == %{"bob" => "failing"}
  end

  test "green when the two sides agree (empty failing-set)" do
    cells = lower(catalog())

    seed = %{
      "g:edr_live/set/0" => %{"alice" => "present"},
      "g:edr_live/set/1" => %{"alice" => "present"}
    }

    store = evaluate(cells, seed)
    assert store["g:edr_live"] == %{}
  end
end

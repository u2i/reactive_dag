defmodule ReactiveDag.ProductDecomposeSpikeTest do
  @moduledoc """
  SPIKE (no DB): does decomposing the portal's fused `product` op into two
  materialized nodes — `grid = a × b` and `antijoin = grid \\ coverage` — produce
  the SAME failing-set as the single fused statement?

  The portal's `product` is one SQL statement:
      WITH a, b, cov, grid = a×b, gaps = grid LEFT JOIN cov WHERE cov IS NULL
      INSERT failing = gaps ; DELETE covered
  The question from the design thread: is that "unsplittable" only because grid+
  antijoin+write are FUSED into one node? Split them into nodes and each is a
  clean per-node materialization the storage seam handles.

  Here we model tuples as plain MapSets (a stand-in for model_tuple rows) and run
  BOTH shapes over the same inputs + incremental changes, asserting they agree.
  This proves the STRUCTURAL claim; the cost (materializing the grid) is discussed
  in the plan, not measured here.
  """
  use ExUnit.Case, async: true

  # ── the FUSED product: gaps = (a×b) minus coverage, in one pass ─────────────
  # returns the failing-set (keys "a|b" with no coverage).
  defp fused_product(a, b, cov) do
    for ka <- a, kb <- b, "#{ka}|#{kb}" not in cov, into: MapSet.new(), do: "#{ka}|#{kb}"
  end

  # ── the DECOMPOSED shape: two nodes, each materialized ──────────────────────
  # node 1: grid = a × b (its tuple-set, materialized).
  defp grid_node(a, b) do
    for ka <- a, kb <- b, into: MapSet.new(), do: "#{ka}|#{kb}"
  end

  # node 2: antijoin = grid \\ coverage (reads the materialized grid + cov).
  defp antijoin_node(grid, cov), do: MapSet.difference(grid, MapSet.new(cov))

  defp decomposed_product(a, b, cov) do
    grid = grid_node(a, b)
    antijoin_node(grid, cov)
  end

  test "fused and decomposed agree on a basic grid with gaps" do
    a = ["alice", "bob"]
    b = ["gh", "aws"]
    cov = ["alice|gh", "bob|aws"]

    fused = fused_product(a, b, cov)
    dec = decomposed_product(a, b, cov)

    assert fused == dec
    # sanity: the gaps are the uncovered pairs.
    assert fused == MapSet.new(["alice|aws", "bob|gh"])
  end

  test "agree when coverage is complete (no failing)" do
    a = ["alice", "bob"]
    b = ["gh"]
    cov = ["alice|gh", "bob|gh"]
    assert fused_product(a, b, cov) == decomposed_product(a, b, cov)
    assert fused_product(a, b, cov) == MapSet.new()
  end

  test "agree under an INCREMENTAL change: a person leaves an axis" do
    a0 = ["alice", "bob", "carol"]
    b = ["gh", "aws"]
    cov = ["alice|gh", "alice|aws", "bob|gh"]
    # carol has no coverage → 2 gaps; bob missing aws → 1 gap.
    assert fused_product(a0, b, cov) == decomposed_product(a0, b, cov)

    # carol leaves: her grid rows + gaps must vanish in BOTH shapes.
    a1 = ["alice", "bob"]
    fused1 = fused_product(a1, b, cov)
    dec1 = decomposed_product(a1, b, cov)
    assert fused1 == dec1
    refute Enum.any?(fused1, &String.starts_with?(&1, "carol|"))
  end

  test "agree under an INCREMENTAL change: coverage is added (a gap closes)" do
    a = ["alice", "bob"]
    b = ["gh", "aws"]
    cov0 = ["alice|gh", "bob|aws"]
    cov1 = ["alice|gh", "bob|aws", "alice|aws"]

    assert fused_product(a, b, cov0) == decomposed_product(a, b, cov0)
    assert fused_product(a, b, cov1) == decomposed_product(a, b, cov1)
    # closing alice|aws removes it from the failing-set in both.
    assert "alice|aws" in fused_product(a, b, cov0)
    refute "alice|aws" in fused_product(a, b, cov1)
    assert fused_product(a, b, cov1) == decomposed_product(a, b, cov1)
  end

  test "the decomposed grid is MATERIALIZED (the cost the fused form avoids)" do
    # the whole point of the tradeoff: decomposed makes grid a real stored set of
    # size |a|*|b|, whereas fused never materializes it. Show the blow-up.
    a = Enum.map(1..50, &"p#{&1}")
    b = Enum.map(1..40, &"s#{&1}")
    grid = grid_node(a, b)
    assert MapSet.size(grid) == 50 * 40
    # the fused form would hold NONE of these as stored rows — only the gaps.
    cov = for ka <- a, kb <- b, into: [], do: "#{ka}|#{kb}"
    assert fused_product(a, b, cov) == MapSet.new()
    # fully covered → fused stores 0 rows; decomposed still stored 2000 grid rows.
  end
end

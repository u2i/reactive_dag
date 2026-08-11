defmodule ReactiveDag.SourceTest do
  @moduledoc """
  The `ReactiveDag.Source` seam's verify checks. `verify!/2` is module-based
  (calls each source's `leaf_cells/1`); `verify_cells!/2` takes already-resolved
  `{source, [cell]}` pairs so a host can keep an optional single-leaf fallback the
  module-based path can't express (the compliance portal's `leaf_cell/0` default).
  """
  use ExUnit.Case, async: true

  defmodule OneLeaf do
    @behaviour ReactiveDag.Source
    @impl true
    def id, do: :one
    @impl true
    def leaf_cells(_graph), do: ["a"]
    @impl true
    def poll(_), do: {:ok, %{changed: []}}
  end

  # a plan-shaped graph: `%{cells: %{id => cell}}` — verify only reads the keys.
  defp graph(ids), do: %{cells: Map.new(ids, &{&1, %{}})}

  describe "verify!/2 (module-based)" do
    test "passes when every source's leaf_cells resolve to real cells" do
      assert :ok = ReactiveDag.Source.verify!([OneLeaf], graph(["a", "b"]))
    end

    test "raises naming the source and its dangling leaf" do
      assert_raise ArgumentError, ~r/OneLeaf -> a/, fn ->
        ReactiveDag.Source.verify!([OneLeaf], graph(["b"]))
      end
    end
  end

  # single-leaf fallback: exports leaf_cell/0 only (no leaf_cells/1)
  defmodule SingleLeaf do
    def id, do: :single
    def leaf_cell, do: :a
    def poll(_), do: {:ok, %{changed: []}}
  end

  defmodule NoLeaves do
    def id, do: :nothing
    def poll(_), do: {:ok, %{changed: []}}
  end

  describe "cells_of/2 (the leaf resolver)" do
    test "leaf_cells/1 wins when exported" do
      assert ReactiveDag.Source.cells_of(OneLeaf, graph(["a"])) == ["a"]
    end

    test "falls back to leaf_cell/0 (stringified) for a single-leaf source" do
      assert ReactiveDag.Source.cells_of(SingleLeaf, graph(["a"])) == ["a"]
    end

    test "raises when the module exports neither" do
      assert_raise ArgumentError, ~r/neither leaf_cells\/1 nor leaf_cell\/0/, fn ->
        ReactiveDag.Source.cells_of(NoLeaves, graph(["a"]))
      end
    end

    test "verify!/2 accepts a leaf_cell/0-only source" do
      assert :ok = ReactiveDag.Source.verify!([SingleLeaf], graph(["a"]))

      assert_raise ArgumentError, ~r/SingleLeaf -> a/, fn ->
        ReactiveDag.Source.verify!([SingleLeaf], graph(["b"]))
      end
    end
  end

  describe "verify_cells!/2 (pre-resolved pairs — the fallback path)" do
    test "passes for resolved pairs whose cells all exist" do
      assert :ok =
               ReactiveDag.Source.verify_cells!(
                 [{OneLeaf, ["a"]}, {:fallback_driver, ["b"]}],
                 graph(["a", "b"])
               )
    end

    test "raises for any dangling cell across the pairs" do
      assert_raise ArgumentError, ~r/:fallback_driver -> ghost/, fn ->
        ReactiveDag.Source.verify_cells!(
          [{OneLeaf, ["a"]}, {:fallback_driver, ["ghost"]}],
          graph(["a"])
        )
      end
    end

    test "verify!/2 is verify_cells!/2 over module-resolved leaves (equivalent)" do
      # both raise identically for the same dangling leaf
      g = graph(["b"])
      msg = ~r/OneLeaf -> a/
      assert_raise ArgumentError, msg, fn -> ReactiveDag.Source.verify!([OneLeaf], g) end
      assert_raise ArgumentError, msg, fn -> ReactiveDag.Source.verify_cells!([{OneLeaf, ["a"]}], g) end
    end
  end
end

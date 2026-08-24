defmodule ReactiveDag.CompanionCellTest do
  @moduledoc """
  The TWO-CELL node (`companion:`): a node emits a companion cell at `<id>` as a
  derived VIEW over its op-tree (rooted at `<id>/set`). The general shape a
  THREE-VALUED verdict needs — the tree holds every evaluated member; the companion
  holds only the projected subset (e.g. the violations); a reader consults both to
  distinguish "green" (covered, no violations) from "unknown" (never evaluated).

  The library provides the two-cell STRUCTURE + id rooting; the host provides the
  companion's recompute (via its `op:`) and the
  read-side disambiguation. This mirrors the compliance portal's `g:<id>` (guarantee)
  over `g:<id>/set` (reconcile) split.
  """
  # NOT async: these tests define modules at RUNTIME, and module definition is not
  # safely concurrent. Elixir serialises compilation behind a lock, and a Spark
  # verifier building its error reads the CALLING process's stacktrace
  # (`Spark.Error.DslError.exception/1` → `Process.info/2`) — which returns nil for a
  # process that has already moved on, failing a test whose assertion never ran.
  #
  # Observed once in a full async run and not reproducible in ~10 attempts since,
  # including at `--max-cases 64`. Left non-async rather than chased: these are a
  # handful of fast tests, and the concurrency bought nothing.
  use ExUnit.Case, async: false

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false
    resources do
      allow_unregistered?(true)
    end
  end

  # a leaf the guarantee reconciles against
  defmodule Baseline do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Simple, extensions: [ReactiveDag.Node]
    reactive do
      id :baseline
      op :leaf
      leaf? true
      source :baseline_scan
    end
  end

  # a two-cell guarantee: op-tree is a reconcile; a companion `:guarantee` view sits
  # over it, watched.
  defmodule MergeGated do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Simple, extensions: [ReactiveDag.Node]

    reactive do
      id :merge_gated
      companion op: :guarantee, meta: [watched?: true]

      op :reconcile
      ref :baseline

      compose :observed do
        leaf? true
        meta source: :repo_protection
      end
    end
  end

  test "the node emits TWO cells: a companion at <id> over the tree at <id>/set" do
    cells = ReactiveDag.Node.cells(MergeGated) |> Map.new(&{&1.id, &1})

    # companion cell at the node's own id, carrying the host op + its meta.
    assert cells["merge_gated"].op == :guarantee
    assert cells["merge_gated"].meta[:watched?] == true
    # its SOLE input is the op-tree root, now rooted at <id>/set.
    assert cells["merge_gated"].inputs == ["merge_gated/set"]

    # the op-tree root lives at <id>/set (the reconcile), NOT at <id>.
    assert cells["merge_gated/set"].op == :reconcile
    # nested legs hang off /set positionally: leg 0 is `ref :baseline` (an edge, no
    # cell); leg 1 is the composed observed leaf → merge_gated/set/1.
    assert cells["merge_gated/set/1"].leaf?
  end

  test "the tree's ref resolves to the shared leaf (an edge, not a re-emitted cell)" do
    cells = ReactiveDag.Node.cells(MergeGated) |> Map.new(&{&1.id, &1})
    # the ref :baseline is an input of the reconcile; baseline itself is NOT re-emitted here
    assert "baseline" in cells["merge_gated/set"].inputs
    refute Map.has_key?(cells, "baseline")
  end

  test "the two-cell node assembles depth-ordered through Node.graph" do
    plan = ReactiveDag.Node.graph([Baseline, MergeGated])

    assert Map.has_key?(plan.cells, "merge_gated")
    assert Map.has_key?(plan.cells, "merge_gated/set")

    # the tree root is an INPUT of the companion → shallower depth (a parent edge).
    assert plan.parents["merge_gated/set"] == ["merge_gated"]
    assert plan.depths["merge_gated/set"] < plan.depths["merge_gated"]
    # and the composed leaf (leg 1) is below the tree root
    assert plan.depths["merge_gated/set/1"] < plan.depths["merge_gated/set"]
  end

  test "a default suffix of `set` and a custom id_suffix both work" do
    defmodule CustomSuffix do
      use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Simple, extensions: [ReactiveDag.Node]
      reactive do
        id :cov
        companion op: :rollup, id_suffix: "members"
        op :union
        ref :baseline
      end
    end

    cells = ReactiveDag.Node.cells(CustomSuffix) |> Map.new(&{&1.id, &1})
    assert cells["cov"].op == :rollup
    assert cells["cov"].inputs == ["cov/members"]
    assert cells["cov/members"].op == :union
  end
end

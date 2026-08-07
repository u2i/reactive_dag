defmodule ReactiveDag.ReferenceEdgeTest do
  @moduledoc """
  A `reference` edge: an input a node READS as context but is NOT recomputed on.
  It's a real input (validated, depth-ordered, read at recompute) — it just isn't
  a propagation parent. For an expensive/non-deterministic node (an LLM step) that
  consults mutable reference data (a human-curated people/positions table) it
  shouldn't be re-triggered by.
  """
  use ExUnit.Case, async: true

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false
    resources do
      allow_unregistered? true
    end
  end

  # two source leaves + a node that RECOMPUTES on :transcripts but only REFERENCES
  # :people (mirrors the enhanced-minutes shape).
  defmodule Transcripts do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Simple, extensions: [ReactiveDag.Node]
    reactive do id(:transcripts); op(:leaf); leaf?(true) end
  end

  defmodule People do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Simple, extensions: [ReactiveDag.Node]
    reactive do id(:people); op(:leaf); leaf?(true) end
  end

  defmodule EnhancedMinutes do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Simple, extensions: [ReactiveDag.Node]

    reactive do
      id :enhanced_minutes
      op :map
      compute ReactiveDag.ReferenceEdgeTest.FakeOp
      ref :transcripts        # RECOMPUTE edge — a transcript change re-runs the LLM
      reference :people       # REFERENCE edge — a people edit does NOT re-run it
    end
  end

  defmodule FakeOp do
    @behaviour ReactiveDag.Op
    @impl true
    def recompute(_cell, keys), do: {:ok, keys}
  end

  defp plan, do: ReactiveDag.Node.graph([Transcripts, People, EnhancedMinutes])

  test "a reference edge is still a real INPUT (in inputs, validated, depth-ordered)" do
    cell = ReactiveDag.Node.to_cell(EnhancedMinutes)
    # both edges are inputs — the node reads both when it recomputes
    assert Enum.sort(cell.inputs) == ["people", "transcripts"]
    # the reference edge is recorded so the graph can exclude it from propagation
    assert cell.meta.reference_inputs == ["people"]

    p = plan()
    # depth: the node sits BELOW both leaves (so people settles before it reads)
    assert p.depths["people"] < p.depths["enhanced_minutes"]
    assert p.depths["transcripts"] < p.depths["enhanced_minutes"]
  end

  test "a RECOMPUTE edge (:transcripts) dirties the node; a REFERENCE edge (:people) does NOT" do
    p = plan()

    # a transcript change → enhanced_minutes is dirtied (the LLM should re-run)
    assert [{"enhanced_minutes", _}] =
             ReactiveDag.Graph.dirty_parents(p, "transcripts", ["m1"], ReactiveDag.Node.KeyRule)

    # a PEOPLE change → NO parent dirtied (editing the people list must not re-fire
    # the expensive LLM node; it'll read current people when it next runs anyway)
    assert [] = ReactiveDag.Graph.dirty_parents(p, "people", ["smythe"], ReactiveDag.Node.KeyRule)
  end

  test "a node with only a reference edge to a leaf is never triggered by that leaf" do
    # sanity: the propagation graph has no edge people → enhanced_minutes
    p = plan()
    assert Map.get(p.parents, "people", []) == []
    assert Map.get(p.parents, "transcripts") == ["enhanced_minutes"]
  end
end

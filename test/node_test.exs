defmodule ReactiveDag.NodeTest do
  @moduledoc "The ReactiveDag.Node resource extension: reactive block → cells → plan."
  use ExUnit.Case, async: true

  # Toy node resources. Ash.DataLayer.Simple = no persistence; we only exercise
  # the `reactive` extension, not any table. A tableless node (Meetings) simply
  # declares no attributes beyond its implicit fields.
  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false
    resources do
      allow_unregistered? true
    end
  end

  defmodule AgendaDocs do
    # an OBSERVED-style leaf: cascade's source+observed pair collapses onto the
    # leaf resource — the driver binding (source/driver) rides on the leaf itself.
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Simple, extensions: [ReactiveDag.Node]
    reactive do
      op :leaf
      leaf? true
      source :agenda_scan
      driver FakeAgendaDriver
    end
  end

  defmodule MeetingShell do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Simple, extensions: [ReactiveDag.Node]
    reactive do
      op :union
      compute FakeShell
      depends_on [:agenda_docs]
    end
  end

  defmodule Meeting do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Simple, extensions: [ReactiveDag.Node]
    reactive do
      op :join
      compute FakeJoin
      key_rule :identity
      depends_on [:meeting_shell, :agenda_docs]
    end
  end

  defmodule Meetings do
    # a TABLELESS publish-root: an explicit id + a single dep entity form.
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Simple, extensions: [ReactiveDag.Node]
    reactive do
      id :meetings
      op :passthrough
      compute FakePassthrough
      dep :meeting
    end
  end

  defmodule Resolutions do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Simple, extensions: [ReactiveDag.Node]
    reactive do
      op :source
      leaf? true
    end
  end

  defmodule MeetingEvents do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Simple, extensions: [ReactiveDag.Node]
    reactive do
      op :source
      leaf? true
    end
  end

  # mirrors cascade's meeting_shell: a union of a by-name ref (agenda_docs) and an
  # inline composed fold (projected_meetings), the fold nesting two more refs.
  defmodule ShellNested do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Simple, extensions: [ReactiveDag.Node]
    reactive do
      id :meeting_shell_nested
      op :union
      compute FakeShell
      ref :agenda_docs

      compose :fold do
        as :projected_meetings
        compute FakeProjected
        key_rule :all
        ref :resolutions
        ref :meeting_events
      end
    end
  end

  @resources [AgendaDocs, MeetingShell, Meeting, Meetings]

  test "cell_id defaults to the resource's snake short-name, or the explicit id" do
    assert ReactiveDag.Node.cell_id(AgendaDocs) == :agenda_docs
    assert ReactiveDag.Node.cell_id(Meetings) == :meetings
  end

  test "to_cell reads op / inputs / leaf? / compute+key_rule (meta) off the reactive block" do
    cell = ReactiveDag.Node.to_cell(Meeting)
    assert cell.id == "meeting"
    assert cell.op == :join
    assert cell.leaf? == false
    assert Enum.sort(cell.inputs) == ["agenda_docs", "meeting_shell"]
    assert cell.meta.compute == FakeJoin
    assert cell.meta.key_rule == :identity
    assert cell.meta.resource == Meeting
  end

  test "a `dep` entity and the flat `depends_on` both become inputs (deduped)" do
    assert ReactiveDag.Node.to_cell(Meetings).inputs == ["meeting"]
    leaf = ReactiveDag.Node.to_cell(AgendaDocs)
    assert leaf.leaf? == true
    assert leaf.inputs == []
  end

  test "an observed-style leaf carries its source/driver binding in meta" do
    leaf = ReactiveDag.Node.to_cell(AgendaDocs)
    assert leaf.meta.source == :agenda_scan
    assert leaf.meta.driver == FakeAgendaDriver
    assert leaf.meta.compute == nil
  end

  test "a nested compose lowers to an intermediate cell via Lowering.walk" do
    cells = ReactiveDag.Node.cells(ShellNested)
    ids = cells |> Enum.map(& &1.id) |> Enum.sort()

    # root + one intermediate (the composed fold, named by its `as`).
    assert "meeting_shell_nested" in ids
    assert "projected_meetings" in ids

    root = Enum.find(cells, &(&1.id == "meeting_shell_nested"))
    # root's inputs: the by-name ref + the composed cell's id.
    assert Enum.sort(root.inputs) == ["agenda_docs", "projected_meetings"]

    proj = Enum.find(cells, &(&1.id == "projected_meetings"))
    assert proj.op == :fold
    assert proj.meta.compute == FakeProjected
    assert proj.meta.key_rule == :all
    assert proj.meta.resource == nil
    assert Enum.sort(proj.inputs) == ["meeting_events", "resolutions"]
  end

  test "the nested node assembles into a valid plan (intermediate deeper than its refs)" do
    plan = ReactiveDag.Node.graph([AgendaDocs, Resolutions, MeetingEvents, ShellNested])
    assert plan.depths["projected_meetings"] > plan.depths["resolutions"]
    assert plan.depths["meeting_shell_nested"] > plan.depths["projected_meetings"]
    assert "meeting_shell_nested" in plan.parents["projected_meetings"]
  end

  # ── portal-flavored: rich leaf bindings, generator (for_each), second-order ──

  defmodule EdrAgents do
    # portal `observed` leaf: rich live-data binding via the open meta:.
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Simple, extensions: [ReactiveDag.Node]
    reactive do
      op :leaf
      leaf? true
      meta source: :probe, check: "edr", strength: :measured, source_table: "probe_results"
    end
  end

  defmodule RuleConcerns do
    # portal generator: one guarantee template → N instances over a population.
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Simple, extensions: [ReactiveDag.Node]
    reactive do
      id :rule_concern
      op :guarantee
      compute FakeGuarantee
      for_each :walk_risks
      ref :edr_agents
    end
  end

  defmodule Readiness do
    # portal second-order: population computed from the graph (over: :findings).
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Simple, extensions: [ReactiveDag.Node]
    reactive do
      id :readiness
      op :analysis
      compute FakeReadiness
      over :findings
    end
  end

  test "a rich leaf carries an arbitrary host binding via the open meta:" do
    leaf = ReactiveDag.Node.to_cell(EdrAgents)
    assert leaf.leaf?
    assert leaf.meta.source == :probe
    assert leaf.meta.check == "edr"
    assert leaf.meta.strength == :measured
    assert leaf.meta.source_table == "probe_results"
  end

  test "a for_each generator expands to one instance sub-tree per member, stamped" do
    members = [
      %{id: "r1", meta: %{probe_filter: "r1"}},
      %{id: "r2", meta: %{probe_filter: "r2"}}
    ]

    cells = ReactiveDag.Node.cells(RuleConcerns, fn :walk_risks -> members end)
    ids = cells |> Enum.map(& &1.id) |> Enum.sort()

    # instances `rule_concern.r1` / `.r2` — NO bare template cell.
    assert ids == ["rule_concern.r1", "rule_concern.r2"]
    refute "rule_concern" in ids

    # each instance carries its member stamp merged into meta.
    r1 = Enum.find(cells, &(&1.id == "rule_concern.r1"))
    assert r1.meta.probe_filter == "r1"
    assert r1.inputs == ["edr_agents"]
  end

  test "a generator with no fetcher builds nothing" do
    assert ReactiveDag.Node.cells(RuleConcerns, nil) == []
  end

  test "second-order `over` rides in meta for a host post-build hook" do
    assert ReactiveDag.Node.to_cell(Readiness).meta.over == :findings
  end

  test "graph/2 expands generators via the member-fetcher" do
    fetch = fn :walk_risks -> [%{id: "r1", meta: %{}}, %{id: "r2", meta: %{}}] end
    plan = ReactiveDag.Node.graph([EdrAgents, RuleConcerns], for_each: fetch)
    assert plan.depths["rule_concern.r1"] > plan.depths["edr_agents"]
    assert "rule_concern.r1" in plan.parents["edr_agents"]
    assert "rule_concern.r2" in plan.parents["edr_agents"]
  end

  test "graph/1 assembles a valid depth-ordered plan from the node resources" do
    plan = ReactiveDag.Node.graph(@resources)

    # leaf at depth 0; each dependent strictly deeper than its inputs.
    assert plan.depths["agenda_docs"] == 0
    assert plan.depths["meeting_shell"] > plan.depths["agenda_docs"]
    assert plan.depths["meeting"] > plan.depths["meeting_shell"]
    assert plan.depths["meetings"] > plan.depths["meeting"]

    # parent edges are the reverse of inputs.
    assert "meeting" in plan.parents["meeting_shell"]
    assert "meetings" in plan.parents["meeting"]
  end
end

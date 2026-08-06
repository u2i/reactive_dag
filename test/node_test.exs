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
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Simple, extensions: [ReactiveDag.Node]
    reactive do
      op :source
      leaf? true
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

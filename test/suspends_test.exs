defmodule ReactiveDag.SuspendsTest do
  @moduledoc """
  `suspends` — this node is too slow to run inside a transaction.

  Two things are under test, and the second is the reason the first exists.

  **The declaration reaches the plan.** `suspends` and `gated` are one
  structural fact with two causes — a cascade reaching this node cannot finish
  it inline — so they normalise into one `meta[:suspends]` map keyed by reason.
  Nothing downstream should have to ask two questions to learn one thing.

  **Assembly refuses a graph that would resume badly.** A suspension records
  which row of what moved; resuming means reading that version and recomputing
  only the units it touched. That needs `version_diff` on the suspending node
  and `version_id` on each of its inputs. Miss either and nothing errors at
  runtime — the resumption simply cannot be narrowed and recomputes the whole
  cell, which is correct, silent, and an LLM call over a whole table instead of
  the rows that moved.

  So the check raises at assembly, where the whole graph is known. A Spark
  verifier cannot do it: it sees one resource and cannot know what feeds it.
  """
  use ExUnit.Case, async: false

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      allow_unregistered?(true)
    end
  end

  def changes_for(_version_id), do: %{}
  def version_for(record, _changeset), do: "v-" <> record.key

  # A leaf that RECORDS versions — the shape a suspendable node's input needs.
  defmodule VersionedLeaf do
    use Ash.Resource,
      domain: ReactiveDag.SuspendsTest.Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
      private?(true)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :body, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]
      create :upsert, upsert?: true, accept: [:key, :body]
    end

    reactive do
      id :versioned_leaf
      leaf? true
      version_id {ReactiveDag.SuspendsTest, :version_for, []}
    end
  end

  # The same leaf without `version_id` — nothing records WHICH row moved.
  defmodule BareLeaf do
    use Ash.Resource,
      domain: ReactiveDag.SuspendsTest.Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
      private?(true)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
    end

    actions do
      defaults [:read, :destroy]
      create :upsert, upsert?: true, accept: [:key]
    end

    reactive do
      id :bare_leaf
      leaf? true
    end
  end

  # Expensive, well-formed: reads versions, and its input records them.
  defmodule SlowOk do
    use Ash.Resource,
      domain: ReactiveDag.SuspendsTest.Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
      private?(true)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :out, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]
      create :upsert, upsert?: true, accept: [:key, :out]

      action :extract, :map do
        run fn _input, _ctx -> {:ok, %{}} end
      end
    end

    reactive do
      id :slow_ok
      depends_on [:versioned_leaf]
      run :extract
      suspends true
      version_diff {ReactiveDag.SuspendsTest, :changes_for, []}
    end
  end

  # Expensive, but cannot READ a version.
  defmodule SlowNoReader do
    use Ash.Resource,
      domain: ReactiveDag.SuspendsTest.Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
      private?(true)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
    end

    actions do
      defaults [:read, :destroy]
      create :upsert, upsert?: true, accept: [:key]

      action :extract, :map do
        run fn _input, _ctx -> {:ok, %{}} end
      end
    end

    reactive do
      id :slow_no_reader
      depends_on [:versioned_leaf]
      run :extract
      suspends true
    end
  end

  # Expensive, reads versions — but its input never RECORDS one.
  defmodule SlowBareInput do
    use Ash.Resource,
      domain: ReactiveDag.SuspendsTest.Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
      private?(true)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
    end

    actions do
      defaults [:read, :destroy]
      create :upsert, upsert?: true, accept: [:key]

      action :extract, :map do
        run fn _input, _ctx -> {:ok, %{}} end
      end
    end

    reactive do
      id :slow_bare_input
      depends_on [:bare_leaf]
      run :extract
      suspends true
      version_diff {ReactiveDag.SuspendsTest, :changes_for, []}
    end
  end

  # Gated rather than expensive — the other reason a cascade stops.
  defmodule Reviewed do
    use Ash.Resource,
      domain: ReactiveDag.SuspendsTest.Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
      private?(true)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
    end

    actions do
      defaults [:read, :destroy]
      create :upsert, upsert?: true, accept: [:key]

      action :summarise, :map do
        run fn _input, _ctx -> {:ok, %{}} end
      end
    end

    reactive do
      id :reviewed
      depends_on [:versioned_leaf]
      run :summarise
      gated true
      version_diff {ReactiveDag.SuspendsTest, :changes_for, []}
    end
  end

  describe "the declaration reaches the plan" do
    test "suspends true lowers to an :expensive reason" do
      plan = ReactiveDag.Node.graph([VersionedLeaf, SlowOk])

      assert %{expensive: []} = plan.cells["slow_ok"].meta[:suspends]
    end

    test "gated lowers to an :approval reason, in the same map" do
      plan = ReactiveDag.Node.graph([VersionedLeaf, Reviewed])

      assert %{approval: []} = plan.cells["reviewed"].meta[:suspends],
             "both reasons normalise into one place — a cascade asks once " <>
               "whether this node stops it, not twice"
    end

    test "a node declaring neither carries no :suspends key at all" do
      plan = ReactiveDag.Node.graph([VersionedLeaf])

      refute Map.has_key?(plan.cells["versioned_leaf"].meta, :suspends),
             "the nil-reject in extra_meta/2 is what keeps meta honest about " <>
               "what was actually declared"
    end
  end

  describe "assembly refuses a graph that could not resume" do
    test "a suspendable node with no version_diff" do
      assert_raise ArgumentError, ~r/declares no `version_diff`/, fn ->
        ReactiveDag.Node.graph([VersionedLeaf, SlowNoReader])
      end
    end

    test "a suspendable node whose INPUT records no version" do
      assert_raise ArgumentError, ~r/must declare `version_id`/, fn ->
        ReactiveDag.Node.graph([BareLeaf, SlowBareInput])
      end
    end

    test "the error names the COST, not just the missing option" do
      err =
        assert_raise ArgumentError, fn ->
          ReactiveDag.Node.graph([BareLeaf, SlowBareInput])
        end

      assert err.message =~ "whole table instead of the rows that moved",
             "a host reading this needs to know what it will pay. The failure " <>
               "is silent — the bill is the only symptom — so the message has " <>
               "to carry what the runtime never will"
    end

    test "a LEAF input is not exempt" do
      # The case that matters in practice: expensive nodes sit one hop from a
      # scanner. Exempting leaves would let the check pass while delivering
      # none of the precision it exists to guarantee.
      assert_raise ArgumentError, ~r/its input "bare_leaf" must declare `version_id`/, fn ->
        ReactiveDag.Node.graph([BareLeaf, SlowBareInput])
      end
    end

    test "a graph with versions everywhere assembles" do
      plan = ReactiveDag.Node.graph([VersionedLeaf, SlowOk])

      assert plan.cells["slow_ok"]
      assert plan.cells["versioned_leaf"]
    end

    test "a GATED node is held to the same rule" do
      # `gated` and `suspends` both stop a cascade, so both need a resumption
      # that can be narrowed. Nothing about approval makes a whole-cell
      # recompute cheaper.
      plan = ReactiveDag.Node.graph([VersionedLeaf, Reviewed])
      assert plan.cells["reviewed"]
    end
  end
end

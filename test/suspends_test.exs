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

  # A leaf that suspends: gated at the source, before anything downstream sees
  # its rows. It has NO inputs, so there is no upstream change to read back.
  defmodule GatedLeaf do
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
      id :gated_leaf
      leaf? true
      gated true
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

    test "a suspendable LEAF needs no version_diff" do
      # The reader half of the rule says how to read back the change that
      # stopped a cascade. A leaf has no inputs, so there is no upstream change
      # to read: the thing that stopped IS the thing that changed, and its own
      # key names it exactly. Demanding a reader here asks for something with
      # nothing to read.
      #
      # This is a real shape, not a loophole — a gated source-fed leaf holds a
      # crawler's rows for review before anything downstream sees them.
      plan = ReactiveDag.Node.graph([GatedLeaf])

      assert %{approval: []} = plan.cells["gated_leaf"].meta[:suspends]
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

    defmodule WellFormed do
      use Ash.Resource,
        domain: ReactiveDag.SuspendsTest.Domain,
        data_layer: Ash.DataLayer.Ets,
        extensions: [ReactiveDag.Node]

      ets do
        private?(true)
      end

      attributes do
        attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
        attribute :outcome, :string, public?: true
        attribute :approval_id, :string, public?: true
      end

      actions do
        defaults [:read, :destroy]
        create :upsert, upsert?: true, accept: [:key, :outcome, :approval_id]

        action :extract, :map do
          run fn _i, _c -> {:ok, %{}} end
        end
      end

      reactive do
        id :well_formed
        depends_on [:versioned_leaf]
        run :extract
        compare [:outcome]
        approved_by via: :approval_id, resource: ReactiveDag.SuspendsTest.VersionedLeaf
      end
    end

  # Declares the loop on purpose. It compiles — Spark warns rather than raising
  # — which is why the test below calls the verifier instead of expecting a
  # raise. The `@moduledoc false` keeps it out of the docs.
  defmodule SelfCancelling do
    @moduledoc false
    use Ash.Resource,
      domain: ReactiveDag.SuspendsTest.Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
      private?(true)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :outcome, :string, public?: true
      attribute :approval_id, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]
      create :upsert, upsert?: true, accept: [:key, :outcome, :approval_id]

      action :extract, :map do
        run fn _i, _c -> {:ok, %{}} end
      end
    end

    reactive do
      id :self_cancelling
      run :extract
      compare [:outcome, :approval_id]
      approved_by via: :approval_id, resource: ReactiveDag.SuspendsTest.VersionedLeaf
    end
  end

  describe "approved_by, reported by the verifier" do
    test "the reference column must not be in compare:" do
      # Recording an approval WRITES the reference column. If that column is
      # part of what `compare:` calls the result, the write moves the row's
      # version — and the approval names the version it covered, so it stops
      # matching the instant it is made. Forever. The symptom is a review queue
      # nobody can clear, which is a long way from the cause.
      #
      # The verifier is CALLED, which is how this suite tests every DSL error.
      # Spark surfaces a verifier's `{:error, _}` as a compile-time WARNING and
      # still defines the module — so `assert_raise` around a `defmodule` never
      # fires, and neither does `Code.compile_string/1`. I spent a while
      # asserting on the wrong mechanism before reading what Spark actually
      # does with the return value.
      assert {:error, %Spark.Error.DslError{message: msg}} =
               ReactiveDag.Node.Verifiers.VerifyReactive.verify(
                 SelfCancelling.spark_dsl_config()
               )

      assert msg =~ "invalidate itself"
      assert msg =~ "Remove :approval_id from `compare:`"
    end

    test "the same declaration is fine when the column is not compared" do
      plan = ReactiveDag.Node.graph([VersionedLeaf, WellFormed])
      assert plan.cells["well_formed"].meta[:approved_by][:via] == :approval_id
    end
  end
end

defmodule ReactiveDag.FeedbackDslTest do
  @moduledoc """
  The `feedback` entity, end to end through the DSL: authored on a resource,
  lowered into the cell's inputs + `meta.feedback_inputs`, assembled by
  `Node.graph/2` — and REFUSED at compile time on the node shapes where a
  second input edge is silently wrong.

  The behavioural story (depth, propagation, convergence, the two bounds) lives
  in `feedback_edge_test.exs` on hand-built plans; this file covers the
  declaration surface. Refusals matter as much as the feature: each one below
  is a specific silent failure — a per_key read spec vanishing, a combinator
  minting a duplicate edge, a poll order quietly changing, a leaf echoing every
  claim as a change — not caution for its own sake.
  """
  use ExUnit.Case, async: true

  # Spark also runs the verifier at `defmodule` and LOGS the refusal (a module
  # compiled at test runtime is not failed outright); each refusal test then
  # asserts the same error from `VerifyReactive.verify/1` directly. Captured so
  # nine deliberate refusals don't read as nine stack traces in the output.
  @moduletag capture_log: true

  alias ReactiveDag.Node.Verifiers.VerifyReactive

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      allow_unregistered?(true)
    end
  end

  defmodule Echo do
    @behaviour ReactiveDag.Op
    @impl true
    def recompute(_cell, keys), do: {:ok, keys}
  end

  # ── the worked loop, authored ─────────────────────────────────────────────
  # meeting_docs → meetings → minutes → schedule ──(feedback)──→ meetings

  defmodule MeetingDocs do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Simple, extensions: [ReactiveDag.Node]
    reactive do id(:meeting_docs); op(:leaf); leaf?(true) end
  end

  defmodule Meetings do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Simple, extensions: [ReactiveDag.Node]

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
    end

    actions do
      defaults [:read]
    end

    reactive do
      id :meetings
      op :map
      compute ReactiveDag.FeedbackDslTest.Echo
      depends_on [:meeting_docs]
      # A meeting's minutes announce future meetings, and those meetings are
      # rows of THIS resource — the loop the whole feature exists for.
      feedback :schedule
    end
  end

  defmodule Minutes do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Simple, extensions: [ReactiveDag.Node]

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
    end

    actions do
      defaults [:read]
    end

    reactive do
      id :minutes
      op :map
      compute ReactiveDag.FeedbackDslTest.Echo
      ref :meetings
    end
  end

  defmodule Schedule do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Simple, extensions: [ReactiveDag.Node]

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
    end

    actions do
      defaults [:read]
    end

    reactive do
      id :schedule
      op :map
      compute ReactiveDag.FeedbackDslTest.Echo
      ref :minutes
    end
  end

  defp plan, do: ReactiveDag.Node.graph([MeetingDocs, Meetings, Minutes, Schedule])

  test "the authored loop lowers, assembles, and is ordered by the forward edges alone" do
    p = plan()

    # the edge is a real INPUT (validated, readable at recompute)…
    assert Enum.sort(p.cells["meetings"].inputs) == ["meeting_docs", "schedule"]
    # …recorded as strings, which is what Graph and Cascade compare against
    assert p.cells["meetings"].meta.feedback_inputs == ["schedule"]

    # depth comes from meeting_docs alone — the canonical record sits at 1,
    # not below the schedule derived from it
    assert p.depths == %{"meeting_docs" => 0, "meetings" => 1, "minutes" => 2, "schedule" => 3}
  end

  test "a feedback edge PROPAGATES — the difference from `context`" do
    assert [{"meetings", ["m9"]}] = ReactiveDag.Graph.claims_for(plan(), "schedule", ["m9"])
  end

  # ── the refusals ──────────────────────────────────────────────────────────

  test "feedback on a `per_key` node is refused: it would cost the read spec" do
    defmodule PerKeyLoop do
      use Ash.Resource,
        domain: ReactiveDag.FeedbackDslTest.Domain,
        data_layer: Ash.DataLayer.Simple,
        extensions: [ReactiveDag.Node]

      attributes do
        attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
        attribute :summary, :string, public?: true
      end

      actions do
        defaults [:read]
        action :summarise, :map, do: run(fn _i, _c -> {:ok, %{}} end)
      end

      reactive do
        id(:per_key_loop)
        recompute_by :key, to: :transcripts, from: :key
        per_key :summarise, into: [summary: :summary]
        feedback :downstream
      end
    end

    assert {:error, %Spark.Error.DslError{message: msg}} =
             VerifyReactive.verify(PerKeyLoop.spark_dsl_config())

    assert msg =~ "per_key"
    assert msg =~ "ONE input"
  end

  test "feedback on a declarative combinator is refused: the edge would be minted twice" do
    defmodule ReduceLoop do
      use Ash.Resource,
        domain: ReactiveDag.FeedbackDslTest.Domain,
        data_layer: Ash.DataLayer.Simple,
        extensions: [ReactiveDag.Node]

      attributes do
        attribute :cat, :string, primary_key?: true, allow_nil?: false, public?: true
        attribute :n, :integer, public?: true
      end

      actions do
        defaults [:read]
      end

      reactive do
        id(:reduce_loop)
        recompute_by :cat, to: :lines, from: :cat
        reduce into: [count: :n]
        feedback :verdicts
      end
    end

    assert {:error, %Spark.Error.DslError{message: msg}} =
             VerifyReactive.verify(ReduceLoop.spark_dsl_config())

    assert msg =~ "declarative combinator"
  end

  test "feedback on a `poll` node is refused: it would silently reorder polls" do
    defmodule PollLoop do
      use Ash.Resource,
        domain: ReactiveDag.FeedbackDslTest.Domain,
        data_layer: Ash.DataLayer.Simple,
        extensions: [ReactiveDag.Node]

      reactive do
        id(:poll_loop)
        leaf?(true)
        poll(ReactiveDag.FeedbackDslTest.Echo)
        feedback :derived
      end
    end

    assert {:error, %Spark.Error.DslError{message: msg}} =
             VerifyReactive.verify(PollLoop.spark_dsl_config())

    assert msg =~ "poll"
    assert msg =~ "reorder"
  end

  test "feedback on a leaf is refused: a leaf echoes every claim as a change" do
    defmodule LeafLoop do
      use Ash.Resource,
        domain: ReactiveDag.FeedbackDslTest.Domain,
        data_layer: Ash.DataLayer.Simple,
        extensions: [ReactiveDag.Node]

      reactive do
        id(:leaf_loop)
        leaf?(true)
        feedback :derived
      end
    end

    assert {:error, %Spark.Error.DslError{message: msg}} =
             VerifyReactive.verify(LeafLoop.spark_dsl_config())

    assert msg =~ "leaf"
    assert msg =~ "never settles"
  end

  test "one target declared both `feedback` and `ref` is refused" do
    defmodule BothWays do
      use Ash.Resource,
        domain: ReactiveDag.FeedbackDslTest.Domain,
        data_layer: Ash.DataLayer.Simple,
        extensions: [ReactiveDag.Node]

      attributes do
        attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      end

      actions do
        defaults [:read]
      end

      reactive do
        id(:both_ways)
        op(:map)
        compute(ReactiveDag.FeedbackDslTest.Echo)
        ref :schedule
        feedback :schedule
      end
    end

    assert {:error, %Spark.Error.DslError{message: msg}} =
             VerifyReactive.verify(BothWays.spark_dsl_config())

    assert msg =~ "both ordered and"
  end

  test "the same feedback target twice is refused" do
    defmodule Twice do
      use Ash.Resource,
        domain: ReactiveDag.FeedbackDslTest.Domain,
        data_layer: Ash.DataLayer.Simple,
        extensions: [ReactiveDag.Node]

      attributes do
        attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      end

      actions do
        defaults [:read]
      end

      reactive do
        id(:twice)
        op(:map)
        compute(ReactiveDag.FeedbackDslTest.Echo)
        depends_on [:upstream]
        feedback :schedule
        feedback :schedule
      end
    end

    assert {:error, %Spark.Error.DslError{message: msg}} =
             VerifyReactive.verify(Twice.spark_dsl_config())

    assert msg =~ "declared twice"
  end

  # ── behind the verifier: assembly must be loud too ────────────────────────

  test "a per_key cell that somehow gains a second input fails ASSEMBLY, not silently" do
    # The verifier above refuses the declarations that produce this — but
    # `per_key_read_spec/1` used to fall through to a silent nil on ANY input
    # shape it didn't expect, and a per_key node without a read spec reads
    # nothing and reports nothing. An extra `ref` is the one way to author the
    # shape today, and assembly must name it rather than assemble it.
    defmodule PkDocs do
      use Ash.Resource,
        domain: ReactiveDag.FeedbackDslTest.Domain,
        data_layer: Ash.DataLayer.Simple,
        extensions: [ReactiveDag.Node]

      reactive do id(:pk_docs); op(:leaf); leaf?(true) end
    end

    defmodule PkTranscripts do
      use Ash.Resource,
        domain: ReactiveDag.FeedbackDslTest.Domain,
        data_layer: Ash.DataLayer.Simple,
        extensions: [ReactiveDag.Node]

      attributes do
        attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      end

      actions do
        defaults [:read]
      end

      reactive do id(:pk_transcripts); op(:leaf); leaf?(true) end
    end

    defmodule TwoInputPerKey do
      use Ash.Resource,
        domain: ReactiveDag.FeedbackDslTest.Domain,
        data_layer: Ash.DataLayer.Simple,
        extensions: [ReactiveDag.Node]

      attributes do
        attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
        attribute :summary, :string, public?: true
      end

      actions do
        defaults [:read]
        action :summarise, :map, do: run(fn _i, _c -> {:ok, %{}} end)
      end

      reactive do
        id(:two_input_per_key)
        recompute_by :key, to: :pk_transcripts, from: :key
        per_key :summarise, into: [summary: :summary]
        ref :pk_docs
      end
    end

    assert_raise ArgumentError, ~r/exactly ONE input/, fn ->
      ReactiveDag.Node.graph([PkDocs, PkTranscripts, TwoInputPerKey])
    end
  end
end

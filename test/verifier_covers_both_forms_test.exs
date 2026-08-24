defmodule ReactiveDag.VerifierCoversBothFormsTest do
  @moduledoc """
  Two compile-time checks that each covered one DSL form and silently passed on
  another (#168, #169).

  A verifier that no-ops on a form it does not understand is worse than one that
  is absent: it reads as coverage. Both bugs were found not by a failing test but
  by reading dispatch code to find out why a declaration had no effect.
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

  alias ReactiveDag.Node.Verifiers.VerifyReactive

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      allow_unregistered?(true)
    end
  end

  describe "#169 — the composite `recompute_by` form" do
    # `block_group_by/1` re-derived the unit's grouping and handled only the
    # `unit: u, from: f` shape, so a COMPOSITE unit read as "groups by nothing".
    # `verify_dests` then compared the `into:` row against nothing and reported
    # it "would carry [nil]" — on a correct declaration, on every build.
    #
    # It now shares ONE derivation with assembly: `RecomputeBy.group_by/1`.
    test "a composite unit's group columns are seen, so no bogus warning fires" do
      defmodule CompositeRollups do
        use Ash.Resource,
          domain: Domain,
          data_layer: Ash.DataLayer.Ets,
          extensions: [ReactiveDag.Node]

        ets do
          private?(true)
        end

        attributes do
          attribute :fund, :string, primary_key?: true, allow_nil?: false, public?: true
          attribute :fy, :string, primary_key?: true, allow_nil?: false, public?: true
          attribute :total, :float, public?: true
        end

        actions do
          defaults [:read]
        end

        reactive do
          id :composite_rollups
          recompute_by [fund: :fund_code, fy: :fy], to: :lines
          reduce into: [sum: [amount: :total]]
        end
      end

      assert :ok = VerifyReactive.verify(CompositeRollups.spark_dsl_config())
    end

    # The three shapes, from the one function both callers now use. A regression
    # here is exactly what #169 was: a clause quietly missing.
    test "every unit shape lowers to its grouping" do
      alias ReactiveDag.Node.RecomputeBy

      assert RecomputeBy.group_by(%RecomputeBy{unit: :cell}) == nil
      assert RecomputeBy.group_by(%RecomputeBy{unit: :month, from: nil}) == nil
      assert RecomputeBy.group_by(%RecomputeBy{unit: :month, from: :read_on}) == [month: :read_on]

      assert RecomputeBy.group_by(%RecomputeBy{unit: [fund: :fund_code, fy: :fy]}) ==
               [fund: :fund_code, fy: :fy]
    end
  end

  describe "#168 — `fingerprint` that nothing reads" do
    # The top-level `fingerprint` is consulted on two paths only: a leaf's
    # reconcile through `Rows`, and `per_key`. Declared anywhere else it is inert
    # — and inert is invisible, which is how cascade came to carry two comments
    # in two files describing a skip that never happened.
    test "a `compute` node declaring one is a compile-time error" do
      defmodule ComputeWithFingerprint do
        use Ash.Resource,
          domain: Domain,
          data_layer: Ash.DataLayer.Ets,
          extensions: [ReactiveDag.Node]

        ets do
          private?(true)
        end

        attributes do
          attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
          attribute :fingerprint, :string, public?: true
        end

        actions do
          defaults [:read]
        end

        reactive do
          id :compute_with_fingerprint
          depends_on [:upstream]
          compute ReactiveDag.VerifierCoversBothFormsTest.NoopOp
          fingerprint [:key]
        end
      end

      assert {:error, %Spark.Error.DslError{message: msg}} =
               VerifyReactive.verify(ComputeWithFingerprint.spark_dsl_config())

      assert msg =~ "nothing will read it"
      assert msg =~ "per_key"
      # names what the node actually declared, so the fix is obvious
      assert msg =~ "compute"
    end

    test "a leaf may declare one — its reconcile is one of the two readers" do
      defmodule LeafWithFingerprint do
        use Ash.Resource,
          domain: Domain,
          data_layer: Ash.DataLayer.Ets,
          extensions: [ReactiveDag.Node]

        ets do
          private?(true)
        end

        attributes do
          attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
          attribute :etag, :string, public?: true
          attribute :fingerprint, :string, public?: true
        end

        actions do
          defaults [:read]
        end

        reactive do
          id :leaf_with_fingerprint
          leaf? true
          fingerprint [:etag]
        end
      end

      assert :ok = VerifyReactive.verify(LeafWithFingerprint.spark_dsl_config())
    end

    test "a node declaring no fingerprint is untouched by the check" do
      defmodule NoFingerprint do
        use Ash.Resource,
          domain: Domain,
          data_layer: Ash.DataLayer.Ets,
          extensions: [ReactiveDag.Node]

        ets do
          private?(true)
        end

        attributes do
          attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
        end

        actions do
          defaults [:read]
        end

        reactive do
          id :no_fingerprint
          depends_on [:upstream]
          compute ReactiveDag.VerifierCoversBothFormsTest.NoopOp
        end
      end

      assert :ok = VerifyReactive.verify(NoFingerprint.spark_dsl_config())
    end
  end

  defmodule NoopOp do
    @behaviour ReactiveDag.Op
    @impl true
    def recompute(_cell, keys), do: {:ok, keys}
  end
end

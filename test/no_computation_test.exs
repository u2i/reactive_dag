defmodule ReactiveDag.NoComputationTest do
  @moduledoc """
  Declaring no computation is a compile error (#91).

  A cell with neither a combinator nor `compute` passed its claimed keys through
  untouched — so everything downstream recomputed against inputs that never
  moved. That was a `Logger.warning` at drain time, which is both the wrong
  moment and the wrong channel: you found out from stale data, long after the
  deploy that caused it.

  It is now rejected at compile time, where an authoring mistake belongs.

  The interesting half is what stays legal. Three shapes have no computation and
  are correct: a `leaf?` fed from outside the graph, a `compose` node whose legs
  do the work, and a `union`, which computes without being one of the
  combinators the verifier previously counted.
  """
  use ExUnit.Case, async: true

  alias ReactiveDag.Node.Verifiers.VerifyReactive

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      allow_unregistered?(true)
    end
  end

  describe "rejected" do
    test "a node with an op label but no computation" do
      # the exact shape #91 opened with: `op` reads like the selector and is not
      defmodule LabelOnly do
        use Ash.Resource,
          domain: ReactiveDag.NoComputationTest.Domain,
          data_layer: Ash.DataLayer.Simple,
          extensions: [ReactiveDag.Node]

        reactive do
          id(:label_only)
          op(:map)
          depends_on([:something])
        end
      end

      assert {:error, %Spark.Error.DslError{message: msg}} =
               VerifyReactive.verify(LabelOnly.spark_dsl_config())

      assert msg =~ "declares no computation"
      assert msg =~ "`op` is a label"
    end

    test "the message names every way to fix it" do
      defmodule Bare do
        use Ash.Resource,
          domain: ReactiveDag.NoComputationTest.Domain,
          data_layer: Ash.DataLayer.Simple,
          extensions: [ReactiveDag.Node]

        reactive do
          id(:bare)
        end
      end

      assert {:error, %Spark.Error.DslError{message: msg}} =
               VerifyReactive.verify(Bare.spark_dsl_config())

      for way <- ~w(aggregate reduce join union per_key run compute leaf? compose) do
        assert msg =~ way, "the fix list omits #{way}"
      end
    end

    test "it says WHY, not just that it is invalid" do
      defmodule Silent do
        use Ash.Resource,
          domain: ReactiveDag.NoComputationTest.Domain,
          data_layer: Ash.DataLayer.Simple,
          extensions: [ReactiveDag.Node]

        reactive do
          id(:silent)
          ref(:upstream)
        end
      end

      assert {:error, %Spark.Error.DslError{message: msg}} =
               VerifyReactive.verify(Silent.spark_dsl_config())

      assert msg =~ "pass its dirty keys through"
      assert msg =~ "recompute against inputs that never moved"
    end
  end

  describe "still legal — a node can be correct with no computation" do
    test "a leaf is fed from outside the graph" do
      defmodule Leaf do
        use Ash.Resource,
          domain: ReactiveDag.NoComputationTest.Domain,
          data_layer: Ash.DataLayer.Ets,
          extensions: [ReactiveDag.Node]

        ets do
        end

        attributes do
          attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
        end

        actions do
          defaults [:read, :destroy]
          create :upsert, upsert?: true, accept: [:key]
        end

        reactive do
          id(:leaf)
          leaf?(true)
        end
      end

      assert VerifyReactive.verify(Leaf.spark_dsl_config()) == :ok
    end

    test "a compose node's legs do the work" do
      defmodule Composed do
        use Ash.Resource,
          domain: ReactiveDag.NoComputationTest.Domain,
          data_layer: Ash.DataLayer.Simple,
          extensions: [ReactiveDag.Node]

        reactive do
          id(:composed)

          compose :fold do
            compose :a do
              leaf?(true)
            end

            compose :b do
              leaf?(true)
            end
          end
        end
      end

      assert VerifyReactive.verify(Composed.spark_dsl_config()) == :ok
    end

    test "a union computes, even though it is not one of the folds" do
      # the near-miss: `union` was absent from the verifier's computation list,
      # so a naive zero-check would have rejected every union node
      defmodule Unioned do
        use Ash.Resource,
          domain: ReactiveDag.NoComputationTest.Domain,
          data_layer: Ash.DataLayer.Ets,
          extensions: [ReactiveDag.Node]

        ets do
        end

        attributes do
          attribute :check, :string, primary_key?: true, allow_nil?: false, public?: true
          attribute :subject, :string, primary_key?: true, allow_nil?: false, public?: true
        end

        actions do
          defaults [:read, :destroy]
          create :upsert, upsert?: true, accept: [:check, :subject]
        end

        reactive do
          id(:unioned)
          union from: [:a, :b], into: [check: :cell, subject: :key]
        end
      end

      assert VerifyReactive.verify(Unioned.spark_dsl_config()) == :ok
    end
  end

  describe "`op` on a compose leg" do
    test "is optional — it dispatches nothing there either" do
      # it was `required: true` on the leg and optional on the main entity: same
      # field, same non-role, opposite obligation
      defmodule UnlabelledLeg do
        use Ash.Resource,
          domain: ReactiveDag.NoComputationTest.Domain,
          data_layer: Ash.DataLayer.Simple,
          extensions: [ReactiveDag.Node]

        reactive do
          id(:unlabelled_leg)

          compose :root do
            # no `op` on this leg — it was `required: true` here and optional on
            # the main entity: same field, same non-role, opposite obligation
            compose do
              leaf?(true)
            end
          end
        end
      end

      assert VerifyReactive.verify(UnlabelledLeg.spark_dsl_config()) == :ok
    end
  end
end

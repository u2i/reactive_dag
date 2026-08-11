defmodule ReactiveDag.RunActionTest do
  @moduledoc """
  `run :action` — the Ash-NATIVE escape hatch: the node's recompute is a
  GENERIC action on its own resource. The action gets only the arguments it
  declares (`keys` — nil for whole-cell — and `cell_id`), does its own domain
  writes, and returns the changed keys; the library `Op.put`s each.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.Node.Recompute

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      allow_unregistered?(true)
    end
  end

  defmodule Extractor do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Simple,
      extensions: [ReactiveDag.Node]

    actions do
      action :recompute_keys, {:array, :string} do
        argument :keys, {:array, :string}, allow_nil?: true
        argument :cell_id, :string

        run fn input, _ctx ->
          send(self(), {:ran, input.arguments[:keys], input.arguments[:cell_id]})
          {:ok, ["e1", "e2"]}
        end
      end
    end

    reactive do
      id(:extractor)
      op(:map)
      run(:recompute_keys)
    end
  end

  # declares NO arguments — a whole-cell recompute that needs neither.
  defmodule NoArgs do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Simple,
      extensions: [ReactiveDag.Node]

    actions do
      action :refresh, {:array, :string} do
        run fn _input, _ctx ->
          send(self(), :refreshed)
          {:ok, []}
        end
      end
    end

    reactive do
      id(:no_args)
      op(:map)
      run(:refresh)
    end
  end

  defmodule Failing do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Simple,
      extensions: [ReactiveDag.Node]

    actions do
      action :boom, {:array, :string} do
        run fn _input, _ctx -> {:error, "the upstream API said no"} end
      end
    end

    reactive do
      id(:failing)
      op(:map)
      run(:boom)
    end
  end

  defmodule CapturingWriter do
    @behaviour ReactiveDag.CoordinationWriter
    @impl true
    def put(cell_id, key, opts), do: send(self(), {:put, cell_id, key, opts}) && :ok
    @impl true
    def delete(_cell_id, _keys), do: :ok
  end

  setup do
    prev = Application.get_env(:reactive_dag, :coordination_writer)
    Application.put_env(:reactive_dag, :coordination_writer, CapturingWriter)
    on_exit(fn -> Application.put_env(:reactive_dag, :coordination_writer, prev) end)
    :ok
  end

  test "the action receives the scoped keys + cell id, and its returned keys are Op.put" do
    cell = ReactiveDag.Node.to_cell(Extractor)

    {:ok, changed} = Recompute.recompute(cell, ["k1", "k2"])
    assert changed == ["e1", "e2"]
    assert_received {:ran, ["k1", "k2"], "extractor"}
    assert_received {:put, "extractor", "e1", _}
    assert_received {:put, "extractor", "e2", _}
  end

  test "a whole-cell claim arrives as nil keys (the scope contract)" do
    cell = ReactiveDag.Node.to_cell(Extractor)
    {:ok, _} = Recompute.recompute(cell, ["*"])
    assert_received {:ran, nil, "extractor"}
  end

  test "an action declaring no arguments still runs (only declared args are passed)" do
    cell = ReactiveDag.Node.to_cell(NoArgs)
    {:ok, []} = Recompute.recompute(cell, ["*"])
    assert_received :refreshed
    refute_received {:put, _, _, _}
  end

  test "an action error surfaces as an informative raise, naming action and resource" do
    cell = ReactiveDag.Node.to_cell(Failing)

    assert_raise RuntimeError, ~r/:boom.*Failing.*failed/s, fn ->
      Recompute.recompute(cell, ["*"])
    end
  end

  test "the verifier rejects a run naming a missing or non-generic action" do
    alias ReactiveDag.Node.Verifiers.VerifyReactive

    defmodule MissingAction do
      use Ash.Resource,
        domain: ReactiveDag.RunActionTest.Domain,
        data_layer: Ash.DataLayer.Simple,
        extensions: [ReactiveDag.Node]

      actions do
        action :real_one, {:array, :string} do
          run fn _i, _c -> {:ok, []} end
        end
      end

      reactive do
        id(:missing_action)
        run(:no_such_action)
      end
    end

    assert {:error, %Spark.Error.DslError{message: msg}} =
             VerifyReactive.verify(MissingAction.spark_dsl_config())

    assert msg =~ "no_such_action"
    assert msg =~ ":real_one"

    defmodule WrongType do
      use Ash.Resource,
        domain: ReactiveDag.RunActionTest.Domain,
        data_layer: Ash.DataLayer.Simple,
        extensions: [ReactiveDag.Node]

      attributes do
        attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      end

      actions do
        defaults [:read]
      end

      reactive do
        id(:wrong_type)
        run(:read)
      end
    end

    assert {:error, %Spark.Error.DslError{message: msg}} =
             VerifyReactive.verify(WrongType.spark_dsl_config())

    assert msg =~ "GENERIC"
  end

  test "run + a combinator in one block is rejected (one computation per node)" do
    alias ReactiveDag.Node.Verifiers.VerifyReactive

    defmodule TwoComputations do
      use Ash.Resource,
        domain: ReactiveDag.RunActionTest.Domain,
        data_layer: Ash.DataLayer.Simple,
        extensions: [ReactiveDag.Node]

      actions do
        action :refresh, {:array, :string} do
          run fn _i, _c -> {:ok, []} end
        end
      end

      reactive do
        id(:two_computations)
        verdict?(true)
        run(:refresh)

        reduce over: :somewhere,
               group_by: :key,
               into: fn k, _ -> %{key: k, status: "present"} end
      end
    end

    assert {:error, %Spark.Error.DslError{message: msg}} =
             VerifyReactive.verify(TwoComputations.spark_dsl_config())

    assert msg =~ "ONE computation"
  end
end

defmodule ReactiveDag.NodeRunnableTest do
  @moduledoc """
  The generic glue that makes a `ReactiveDag.Node` graph RUNNABLE by `Drain`:
  `Node.Recompute` (dispatch to meta.compute) + `Node.KeyRule` (read meta.key_rule).
  Tested at the dispatch level (pure over a Cell) — the DB-backed drain integration
  is proven by the host suites, as with Frontier/Tuple.
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

  alias ReactiveDag.Cell
  alias ReactiveDag.Node.{KeyRule, Recompute}

  # a toy op: records that it ran, echoes back the keys as "changed".
  defmodule EchoOp do
    @behaviour ReactiveDag.Op
    @impl true
    def recompute(%Cell{id: id}, keys) do
      send(self(), {:ran, id, keys})
      {:ok, keys}
    end
  end

  # an op that changes only a subset (proves "only changed keys" is the op's call).
  defmodule HalfOp do
    @behaviour ReactiveDag.Op
    @impl true
    def recompute(_cell, keys), do: {:ok, Enum.take(keys, div(length(keys), 2))}
  end

  describe "Node.Recompute dispatch" do
    test "dispatches to the cell's meta.compute op and returns its changed keys" do
      cell = %Cell{id: "budget", op: :fold, meta: %{compute: EchoOp}}
      assert {:ok, ["a", "b"]} = Recompute.recompute(cell, ["a", "b"])
      assert_received {:ran, "budget", ["a", "b"]}
    end

    test "an op may report a strict subset as changed (drives O(real changes))" do
      cell = %Cell{id: "j", op: :join, meta: %{compute: HalfOp}}
      assert {:ok, changed} = Recompute.recompute(cell, ["a", "b", "c", "d"])
      assert length(changed) == 2
    end

    test "a leaf passes its keys through without calling any op" do
      cell = %Cell{id: "src", op: :leaf, leaf?: true, meta: %{compute: nil}}
      assert {:ok, ["k1", "k2"]} = Recompute.recompute(cell, ["k1", "k2"])
    end

    test "a non-leaf with no compute module passes through (with a warning)" do
      cell = %Cell{id: "x", op: :map, meta: %{compute: nil}}
      assert {:ok, ["k"]} = Recompute.recompute(cell, ["k"])
    end

    test "a cell whose meta has no :compute key at all passes through" do
      cell = %Cell{id: "y", op: :map, meta: %{}}
      assert {:ok, ["k"]} = Recompute.recompute(cell, ["k"])
    end
  end

  describe "Node.KeyRule" do
    test ":identity (default) maps a changed input key to the same output key" do
      parent = %Cell{id: "p", op: :map, meta: %{key_rule: :identity}}
      assert KeyRule.rule(parent, "child", ["a", "b"]) == {:keys, ["a", "b"]}
    end

    test ":all escalates any input change to a whole-cell recompute" do
      parent = %Cell{id: "rollup", op: :fold, meta: %{key_rule: :all}}
      assert KeyRule.rule(parent, "child", ["a"]) == :all
    end

    test "a cell with no key_rule in meta defaults to identity" do
      assert KeyRule.rule(%Cell{id: "p", op: :map, meta: %{}}, "c", ["a"]) == {:keys, ["a"]}
    end
  end

  test "a Node graph's cells carry compute+key_rule where the generic modules read them" do
    # the contract between Node (writes meta.compute/key_rule) and the generic
    # strategy/key_rule (read them) — the glue that makes graph -> Drain work.
    defmodule Domain2 do
      use Ash.Domain, validate_config_inclusion?: false
      resources do
        allow_unregistered?(true)
      end
    end

    defmodule Fold do
      use Ash.Resource,
        domain: Domain2,
        data_layer: Ash.DataLayer.Ets,
        extensions: [ReactiveDag.Node]

      ets do
        private?(true)
      end

      # EchoOp returns the changed keys itself — a `compute` op owns its writes,
      # so the recompute below writes nothing through this table. It exists
      # because a node that computes must have somewhere for its rows to go.
      attributes do
        attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      end

      actions do
        defaults [:read, :destroy]

        create :upsert do
          upsert?(true)
          upsert_identity(:by_key)
          accept([:key])
        end
      end

      identities do
        identity :by_key, [:key]
      end

      reactive do
        op :fold
        compute EchoOp
        key_rule :all
      end
    end

    cell = ReactiveDag.Node.to_cell(Fold)
    assert cell.meta.compute == EchoOp
    assert KeyRule.rule(cell, "c", ["a"]) == :all
    assert {:ok, ["a"]} = Recompute.recompute(cell, ["a"])
    assert_received {:ran, "fold", ["a"]}
  end
end

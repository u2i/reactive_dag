defmodule ReactiveDag.JoinExpandCombinatorTest do
  @moduledoc """
  The `join` (two-input left join) and `expand` (group → many rows, via reduce's
  list-returning `into`) combinators — the shapes the plain `reduce` fold couldn't
  express (budget_vs_actual = join; cost_allocation = expand).
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.Node.Recompute

  defmodule Writer do
    @behaviour ReactiveDag.CoordinationWriter
    @impl true
    def put(cell_id, key, _opts), do: send(self(), {:put, cell_id, key})
    @impl true
    def delete(cell_id, keys), do: send(self(), {:delete, cell_id, keys})
  end

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false
    resources do
      allow_unregistered?(true)
    end
  end

  # JOIN: budget×actual → variance, keyed by account (left join; actual may be absent).
  defmodule Variance do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Simple, extensions: [ReactiveDag.Node]

    reactive do
      id :variance
      op :join
      key_rule :all

      join over: :fiscal,
           read: fn :fiscal -> Process.get(:fiscal, []) end,
           left: fn %{kind: k, acct: a} -> k == :budget && a end,
           right: fn %{kind: k, acct: a} -> k == :actual && a end,
           key: fn acct -> "va|#{acct}" end,
           into: fn acct, b, a ->
             %{acct: acct, budget: b.amount, actual: a && a.amount,
               variance: a && a.amount - b.amount}
           end,
           upsert: fn key, row ->
             send(self(), {:upsert, key, row})
             true
           end
    end
  end

  # EXPAND: group budget lines by fy, then FAN OUT one row per bucket in the fy.
  defmodule Allocations do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Simple, extensions: [ReactiveDag.Node]

    reactive do
      id :allocations
      op :fold
      key_rule :all

      reduce over: :lines,
             read: fn :lines -> Process.get(:lines, []) end,
             group_by: fn l -> l.fy end,
             key: fn _fy -> :unused end,
             # into returns a LIST → expand: one row per bucket in the fy.
             into: fn fy, lines ->
               lines
               |> Enum.group_by(& &1.bucket)
               |> Enum.map(fn {bucket, ls} ->
                 %{key: "#{fy}|#{bucket}", fy: fy, bucket: bucket,
                   total: ls |> Enum.map(& &1.value) |> Enum.sum()}
               end)
             end,
             upsert: fn key, row ->
               send(self(), {:upsert, key, row})
               true
             end
    end
  end

  setup do
    prev = Application.get_env(:reactive_dag, :coordination_writer)
    Application.put_env(:reactive_dag, :coordination_writer, Writer)
    on_exit(fn -> Application.put_env(:reactive_dag, :coordination_writer, prev) end)
    :ok
  end

  describe "join" do
    test "left-joins budget to actual, emitting a row per left (budget) key" do
      Process.put(:fiscal, [
        %{kind: :budget, acct: "A", amount: 100},
        %{kind: :budget, acct: "B", amount: 50},
        %{kind: :actual, acct: "A", amount: 90}
        # B has no actual → variance nil (left join keeps it)
      ])

      cell = ReactiveDag.Node.to_cell(Variance)
      assert cell.op == :join
      assert cell.inputs == ["fiscal"]

      {:ok, changed} = Recompute.recompute(cell, ["*"])
      assert Enum.sort(changed) == ["va|A", "va|B"]

      upserts = drain_upserts()
      # variance = actual - budget = 90 - 100 = -10
      assert upserts["va|A"].variance == -10
      assert upserts["va|A"].actual == 90
      assert upserts["va|B"].variance == nil
      assert upserts["va|B"].actual == nil
    end
  end

  # collect all {:upsert, key, row} messages → %{key => row} (order-independent).
  defp drain_upserts(acc \\ %{}) do
    receive do
      {:upsert, key, row} -> drain_upserts(Map.put(acc, key, row))
      {:put, _, _} -> drain_upserts(acc)
    after
      0 -> acc
    end
  end

  describe "expand" do
    test "a group fans out to MANY rows (list-returning into), each self-keyed" do
      Process.put(:lines, [
        %{fy: "25", bucket: "police", value: 10},
        %{fy: "25", bucket: "police", value: 5},
        %{fy: "25", bucket: "fire", value: 20},
        %{fy: "26", bucket: "police", value: 7}
      ])

      cell = ReactiveDag.Node.to_cell(Allocations)
      {:ok, changed} = Recompute.recompute(cell, ["*"])

      # fy 25 → {police:15, fire:20}; fy 26 → {police:7}. 3 rows from 2 groups.
      assert Enum.sort(changed) == ["25|fire", "25|police", "26|police"]
      upserts = drain_upserts()
      assert upserts["25|police"].total == 15
      assert upserts["25|fire"].total == 20
      assert upserts["26|police"].total == 7
    end
  end
end

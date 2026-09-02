defmodule ReactiveDag.SavepointRollbackTest do
  @moduledoc """
  A rolled-back savepoint must not be mistaken for a recompute's return value.

  `Suspension.savepoint/1` returns `{:error, reason}` when the transaction
  rolls back. `recompute_and_queue/10` then matched it with

      {changed, meta} ->

  which accepts ANY two-tuple — binding `changed = :error`, `meta = reason`.
  The cascade carried that into `length(changed)` and died with

      ** (ArgumentError) :erlang.length(:error)  * 1st argument: not a list

  Observed in production (job 240, `MeetingEvents`, rc.62) after cascades ran
  there for the first time. The existing `FakeRepo` could not reach it: its
  `transaction/2` always returns `{:ok, _}`, so the rollback shape never
  appeared and the clause looked total.

  A failure inside the op is a DIFFERENT path — `recompute/3` maps `{:error,
  reason}` to `{:failed, reason}`, which is handled. This is the repo itself
  aborting.
  """
  use ExUnit.Case, async: false

  defmodule RollbackRepo do
    @moduledoc "A repo whose transaction always rolls back."
    def query!("SELECT" <> _, _), do: %{rows: [], num_rows: 0}
    def query!("INSERT INTO " <> _, _), do: %{rows: [], num_rows: 1}
    def query!("DELETE" <> _, _), do: %{rows: [], num_rows: 0}
    def query!(_, _), do: %{rows: [], num_rows: 0}

    # Only the INNER transaction (the per-cell savepoint) aborts. The outer
    # cascade transaction must succeed, or the walk never reaches the clause
    # under test — which is what made the first version of this test fail for
    # the wrong reason.
    def transaction(fun, _opts \\ []) do
      depth = Process.get(:rb_depth, 0)
      Process.put(:rb_depth, depth + 1)

      try do
        if depth == 0 do
          {:ok, fun.()}
        else
          _ = fun.()
          {:error, :deadlock_detected}
        end
      after
        Process.put(:rb_depth, depth)
      end
    end

    def rollback(reason), do: throw({:rollback, reason})
  end

  setup do
    prev = Application.get_env(:reactive_dag, :repo)
    Application.put_env(:reactive_dag, :repo, RollbackRepo)

    on_exit(fn ->
      if prev,
        do: Application.put_env(:reactive_dag, :repo, prev),
        else: Application.delete_env(:reactive_dag, :repo)
    end)

    :ok
  end

  test "savepoint/1 surfaces a rollback as {:error, reason}" do
    # The shape the caller has to cope with. Pinned here so a change to
    # `savepoint/1`'s contract shows up as a failure in this file too.
    # At depth 0 the fake commits; the savepoint under a cascade runs at
    # depth 1, which is where the rollback shape appears.
    Process.put(:rb_depth, 1)

    assert ReactiveDag.Suspension.savepoint(fn -> {:ok, [:a]} end) ==
             {:error, :deadlock_detected}

    Process.put(:rb_depth, 0)
  end

  defmodule PassThrough do
    @behaviour ReactiveDag.Op

    @impl true
    def recompute(_cell, keys), do: {:ok, keys}
  end

  defp cell(id, inputs) do
    %ReactiveDag.Cell{
      id: id,
      op: {:compute, PassThrough},
      inputs: inputs,
      leaf?: inputs == [],
      meta: %{}
    }
  end

  test "a cascade whose savepoint rolls back does not crash on length/1" do
    # The production failure, end to end: the op itself is fine, but the repo
    # aborts the transaction around it. Before the fix this raised
    # ArgumentError from `:erlang.length(:error)` rather than reporting a
    # failed cell.
    plan =
      ReactiveDag.Graph.build([
        cell("leaf", []),
        cell("child", ["leaf"])
      ])

    result = ReactiveDag.Cascade.run(plan, [%{cell: "leaf", keys: ["k1"]}])

    assert match?({:ok, _}, result) or match?({:error, _}, result),
           "expected a report or a contained error, got: " <> inspect(result)
  end
end

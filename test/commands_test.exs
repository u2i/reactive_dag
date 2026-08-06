defmodule ReactiveDag.CommandsTest do
  @moduledoc """
  The command frontier (`ReactiveDag.Commands`): claim in seq order → dispatch by
  kind to a `CommandExecutor` → settle. Tested over an in-memory `Store` (the
  Postgres store's SQL is a separate concern) so the LOOP behavior is exercised:
  serialized order, done/blocked/error outcomes, followups, scope-freeze, and the
  blocked→answer resume + the on_settled drain-kick.
  """
  use ExUnit.Case, async: false

  # ── an in-memory Store: seq-ordered claim, scope-freeze, dedup ───────────────
  defmodule MemStore do
    @behaviour ReactiveDag.Commands.Store
    use Agent

    def start, do: Agent.start(fn -> %{seq: 0, cmds: []} end, name: __MODULE__)
    def stop, do: if(Process.whereis(__MODULE__), do: Agent.stop(__MODULE__), else: :ok)
    def all, do: Agent.get(__MODULE__, & &1.cmds)

    @impl true
    def enqueue(attrs) do
      Agent.update(__MODULE__, fn s ->
        # dedup: skip an open command with the same dedup_key
        open? =
          attrs[:dedup_key] &&
            Enum.any?(s.cmds, &(&1["dedup_key"] == attrs[:dedup_key] and &1["status"] in ~w(queued running)))

        if open? do
          s
        else
          seq = s.seq + 1

          cmd = %{
            "id" => "c#{seq}",
            "seq" => seq,
            "kind" => attrs[:kind],
            "scope" => attrs[:scope],
            "payload" => attrs[:payload] || %{},
            "dedup_key" => attrs[:dedup_key],
            "actor" => attrs[:actor],
            "answers_id" => attrs[:answers_id],
            "status" => "queued"
          }

          %{s | seq: seq, cmds: s.cmds ++ [cmd]}
        end
      end)

      :ok
    end

    @impl true
    def claim(run_id, freeze_exempt) do
      Agent.get_and_update(__MODULE__, fn s ->
        frozen = for c <- s.cmds, c["status"] in ~w(blocked failed), into: MapSet.new(), do: c["scope"]

        claimable =
          s.cmds
          |> Enum.filter(fn c ->
            c["status"] == "queued" and
              (c["kind"] in freeze_exempt or not MapSet.member?(frozen, c["scope"]))
          end)
          |> Enum.sort_by(& &1["seq"])
          |> List.first()

        case claimable do
          nil ->
            {nil, s}

          c ->
            claimed = c |> Map.put("status", "running") |> Map.put("run_id", run_id)
            {claimed, %{s | cmds: replace(s.cmds, claimed)}}
        end
      end)
    end

    @impl true
    def settle(id, status, fields) do
      Agent.update(__MODULE__, fn s ->
        cmds =
          Enum.map(s.cmds, fn c ->
            if c["id"] == id do
              c
              |> Map.put("status", status)
              |> Map.put("result", Keyword.get(fields, :result))
              |> Map.put("needs", Keyword.get(fields, :needs))
            else
              c
            end
          end)

        %{s | cmds: cmds}
      end)

      :ok
    end

    defp replace(cmds, updated), do: Enum.map(cmds, &if(&1["id"] == updated["id"], do: updated, else: &1))
  end

  # ── executors ───────────────────────────────────────────────────────────────
  defmodule ApproveExec do
    @behaviour ReactiveDag.CommandExecutor
    @impl true
    # simulate writing an approval leaf: record the approval, done.
    def execute(cmd, _ctx) do
      send(ReactiveDag.CommandsTest, {:approved, cmd["payload"]["thing"]})
      {:done, %{"approved" => cmd["payload"]["thing"]}}
    end
  end

  defmodule NeedsHumanExec do
    @behaviour ReactiveDag.CommandExecutor
    @impl true
    # first time: block asking a human. (The answer would settle it out-of-band.)
    def execute(_cmd, _ctx), do: {:blocked, %{"question" => "approve?"}}
  end

  defmodule FanoutExec do
    @behaviour ReactiveDag.CommandExecutor
    @impl true
    # done + enqueue a followup command.
    def execute(_cmd, _ctx), do: {:done, %{}, [%{kind: "approve", payload: %{"thing" => "child"}}]}
  end

  defmodule BoomExec do
    @behaviour ReactiveDag.CommandExecutor
    @impl true
    def execute(_cmd, _ctx), do: raise("boom")
  end

  setup do
    Process.register(self(), ReactiveDag.CommandsTest)
    {:ok, _} = MemStore.start()

    prev = Application.get_env(:reactive_dag, :commands_store)
    prev_x = Application.get_env(:reactive_dag, :command_executors)
    prev_f = Application.get_env(:reactive_dag, :command_freeze_exempt)

    Application.put_env(:reactive_dag, :commands_store, MemStore)

    Application.put_env(:reactive_dag, :command_executors, %{
      "approve" => ApproveExec,
      "needs_human" => NeedsHumanExec,
      "fanout" => FanoutExec,
      "boom" => BoomExec,
      "answer" => ApproveExec
    })

    Application.put_env(:reactive_dag, :command_freeze_exempt, ["answer"])

    on_exit(fn ->
      MemStore.stop()
      Application.put_env(:reactive_dag, :commands_store, prev)
      Application.put_env(:reactive_dag, :command_executors, prev_x)
      Application.put_env(:reactive_dag, :command_freeze_exempt, prev_f)
    end)

    :ok
  end

  test "claims in seq order, dispatches by kind, on_settled fires per done command" do
    ReactiveDag.Commands.enqueue!(%{kind: "approve", scope: "a", payload: %{"thing" => "x"}})
    ReactiveDag.Commands.enqueue!(%{kind: "approve", scope: "b", payload: %{"thing" => "y"}})

    kicked = self()
    {:ok, tally} = ReactiveDag.Commands.run(on_settled: fn cmd, _r -> send(kicked, {:kick, cmd["id"]}) end)

    assert tally.done == 2
    # both approvals ran, in order
    assert_received {:approved, "x"}
    assert_received {:approved, "y"}
    # the drain-kick fired once per done command
    assert_received {:kick, "c1"}
    assert_received {:kick, "c2"}
  end

  test "a blocked command parks and FREEZES its scope (later same-scope cmds wait)" do
    # two commands in scope "s": the first blocks, the second must NOT run.
    ReactiveDag.Commands.enqueue!(%{kind: "needs_human", scope: "s"})
    ReactiveDag.Commands.enqueue!(%{kind: "approve", scope: "s", payload: %{"thing" => "z"}})

    {:ok, tally} = ReactiveDag.Commands.run([])

    assert tally.blocked == 1
    assert tally.done == 0
    # the second command never executed (scope frozen)
    refute_received {:approved, "z"}

    cmds = MemStore.all() |> Map.new(&{&1["id"], &1["status"]})
    assert cmds["c1"] == "blocked"
    assert cmds["c2"] == "queued"     # still waiting behind the frozen scope
  end

  test "a freeze-exempt 'answer' claims through a frozen scope" do
    ReactiveDag.Commands.enqueue!(%{kind: "needs_human", scope: "s"})       # will block scope s
    ReactiveDag.Commands.enqueue!(%{kind: "answer", scope: "s", payload: %{"thing" => "ans"}})

    {:ok, tally} = ReactiveDag.Commands.run([])

    assert tally.blocked == 1
    assert tally.done == 1            # the answer ran despite the frozen scope
    assert_received {:approved, "ans"}
  end

  test "a done+followups command enqueues the followup, which then runs" do
    ReactiveDag.Commands.enqueue!(%{kind: "fanout", scope: "f"})
    {:ok, tally} = ReactiveDag.Commands.run([])

    assert tally.done == 2           # the fanout + its enqueued child
    assert tally.enqueued == 1
    assert_received {:approved, "child"}
  end

  test "an executor that raises settles the command as failed (contained)" do
    ReactiveDag.Commands.enqueue!(%{kind: "boom", scope: "e"})
    {:ok, tally} = ReactiveDag.Commands.run([])

    assert tally.failed == 1
    assert MemStore.all() |> hd() |> Map.get("status") == "failed"
  end

  test "dedup: a second identical open intent is coalesced" do
    ReactiveDag.Commands.enqueue!(%{kind: "approve", scope: "d", dedup_key: "k1", payload: %{"thing" => "once"}})
    ReactiveDag.Commands.enqueue!(%{kind: "approve", scope: "d", dedup_key: "k1", payload: %{"thing" => "once"}})

    assert length(MemStore.all()) == 1
  end
end

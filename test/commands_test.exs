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
            # store payload as ENCODED JSON, mirroring the Postgres store's jsonb
            # write — so claim/outstanding must decode it (a regression guard for
            # the encode-on-write-but-not-read bug).
            "payload" => Jason.encode!(attrs[:payload] || %{}),
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
            # state keeps the encoded payload; the RETURNED command is decoded
            # (mirrors Store.Postgres decoding jsonb on read).
            {decode(claimed), %{s | cmds: replace(s.cmds, claimed)}}
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

    @impl true
    def outstanding do
      all()
      |> Enum.filter(&(&1["status"] in ~w(queued running blocked)))
      |> Enum.sort_by(& &1["seq"])
      |> Enum.map(&decode/1)
    end

    defp replace(cmds, updated), do: Enum.map(cmds, &if(&1["id"] == updated["id"], do: updated, else: &1))
    defp decode(cmd), do: Map.update!(cmd, "payload", &Jason.decode!/1)
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

    @impl true
    # anticipated effect: an approve for `thing` will set (approvals, thing) present.
    def project(cmd), do: [%{cell_id: "approvals", key: cmd["payload"]["thing"], status: "present"}]
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
    prev_r = Application.get_env(:reactive_dag, :command_executor_resolver)

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
      Application.put_env(:reactive_dag, :command_executor_resolver, prev_r)
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

  test "on_event fires :claimed then a terminal event per command, across ALL outcomes" do
    ReactiveDag.Commands.enqueue!(%{kind: "approve", scope: "a", payload: %{"thing" => "x"}})
    ReactiveDag.Commands.enqueue!(%{kind: "needs_human", scope: "b"})
    ReactiveDag.Commands.enqueue!(%{kind: "boom", scope: "e"})

    me = self()
    {:ok, _} = ReactiveDag.Commands.run(on_event: fn ev, cmd, info -> send(me, {:ev, ev, cmd["kind"], info}) end)

    # each command is claimed before dispatch
    assert_received {:ev, :claimed, "approve", %{}}
    assert_received {:ev, :claimed, "needs_human", %{}}
    assert_received {:ev, :claimed, "boom", %{}}

    # a terminal event per outcome — done/blocked/failed all reach on_event
    # (on_settled would see only the :done). Each carries a duration.
    assert_received {:ev, :done, "approve", %{result: %{"approved" => "x"}, enqueued: 0, duration_ms: d1}}
    assert is_integer(d1) and d1 >= 0
    assert_received {:ev, :blocked, "needs_human", %{needs: %{"question" => "approve?"}, duration_ms: _}}
    assert_received {:ev, :failed, "boom", %{error: _, duration_ms: _}}
  end

  test "on_event :done reports the followup count in :enqueued" do
    ReactiveDag.Commands.enqueue!(%{kind: "fanout", scope: "f"})

    me = self()
    {:ok, _} = ReactiveDag.Commands.run(on_event: fn ev, cmd, info -> send(me, {:ev, ev, cmd["kind"], info}) end)

    assert_received {:ev, :done, "fanout", %{enqueued: 1}}
    # the enqueued child then runs too
    assert_received {:ev, :done, "approve", %{enqueued: 0}}
  end

  # ── pending-aware reads (optimistic overlay) ────────────────────────────────

  test "pending_effects projects outstanding commands via the executor's project/1" do
    ReactiveDag.Commands.enqueue!(%{kind: "approve", scope: "a", payload: %{"thing" => "store-1"}})
    # a kind with an executor that has NO project/1 → invisible to the overlay
    ReactiveDag.Commands.enqueue!(%{kind: "needs_human", scope: "n"})

    effects = ReactiveDag.Commands.pending_effects()

    # only the projectable approve shows up
    assert [%{cell_id: "approvals", key: "store-1", status: "present", command_id: _}] = effects
  end

  test "overlay: a read reflects committed state + anticipated pending mods" do
    # committed base: store-1 failing, store-2 failing (from the tuple)
    base = %{"store-1" => "failing", "store-2" => "failing"}

    # an approve for store-1 is queued → the read should anticipate store-1 present
    ReactiveDag.Commands.enqueue!(%{kind: "approve", scope: "a", payload: %{"thing" => "store-1"}})

    overlaid = ReactiveDag.Commands.overlay("approvals", base)

    # store-1: anticipated present, with the pending command attached
    assert overlaid["store-1"].status == "present"
    assert [%{status: "present"}] = overlaid["store-1"].pending

    # store-2: no outstanding command → committed value, no pending
    assert overlaid["store-2"] == %{status: "failing", pending: []}
  end

  test "overlay: later command wins on a (cell,key) collision" do
    ReactiveDag.Commands.enqueue!(%{kind: "approve", scope: "a", payload: %{"thing" => "store-1"}})
    # a second, later approve for the SAME key (different scope so both stay open)
    ReactiveDag.Commands.enqueue!(%{kind: "answer", scope: "b", payload: %{"thing" => "store-1"}})

    overlaid = ReactiveDag.Commands.overlay("approvals", %{"store-1" => "failing"})

    # both are present-projecting; both listed, anticipated status is present
    assert overlaid["store-1"].status == "present"
    assert length(overlaid["store-1"].pending) == 2
  end

  # ── host-shape adaptation (map-or-struct commands, family dispatch, tuple followups)

  # an executor that reads a STRUCT command via atom keys — the shape a host with
  # an Ash-resource store returns. `field/2` must let the lib bookkeep over it.
  defmodule StructCmd do
    defstruct [:id, :kind, :scope, :payload, :actor, claimed: false]
  end

  defmodule StructStore do
    @behaviour ReactiveDag.Commands.Store
    use Agent

    def start, do: Agent.start(fn -> [] end, name: __MODULE__)
    def stop, do: if(Process.whereis(__MODULE__), do: Agent.stop(__MODULE__), else: :ok)

    @impl true
    def enqueue(attrs) do
      Agent.update(__MODULE__, fn cmds ->
        cmds ++
          [
            %StructCmd{
              id: "s#{length(cmds) + 1}",
              kind: attrs[:kind],
              scope: attrs[:scope],
              payload: attrs[:payload] || %{},
              actor: attrs[:actor]
            }
          ]
      end)

      :ok
    end

    @impl true
    def claim(_run_id, _exempt) do
      Agent.get_and_update(__MODULE__, fn cmds ->
        idx = Enum.find_index(cmds, &(not Map.get(&1, :claimed, false)))
        if idx, do: {Enum.at(cmds, idx), List.update_at(cmds, idx, &Map.put(&1, :claimed, true))}, else: {nil, cmds}
      end)
    end

    @impl true
    def settle(_id, _status, _fields), do: :ok
    @impl true
    def outstanding, do: Agent.get(__MODULE__, & &1)
  end

  test "field/2 reads kind/id/scope/actor from a string-map, atom-map, or struct" do
    smap = %{"kind" => "k", "id" => "i", "scope" => "s", "actor" => "a"}
    amap = %{kind: "k", id: "i", scope: "s", actor: "a"}
    strc = %StructCmd{kind: "k", id: "i", scope: "s", actor: "a"}

    for cmd <- [smap, amap, strc] do
      assert ReactiveDag.Commands.field(cmd, :kind) == "k"
      assert ReactiveDag.Commands.field(cmd, :id) == "i"
      assert ReactiveDag.Commands.field(cmd, :scope) == "s"
      assert ReactiveDag.Commands.field(cmd, :actor) == "a"
    end

    assert ReactiveDag.Commands.field(%{}, :kind) == nil
  end

  test "a struct-returning store drains end to end (executor gets the struct)" do
    {:ok, _} = StructStore.start()
    on_exit(&StructStore.stop/0)
    Application.put_env(:reactive_dag, :commands_store, StructStore)

    me = self()

    Application.put_env(:reactive_dag, :command_executors, %{
      "struct_approve" => struct_exec(me)
    })

    ReactiveDag.Commands.enqueue!(%{kind: "struct_approve", scope: "a", payload: %{"thing" => "st"}})

    {:ok, tally} = ReactiveDag.Commands.run([])

    assert tally.done == 1
    # the executor received the %StructCmd{} unchanged — proving the lib never
    # coerced it to a string-keyed map.
    assert_received {:struct_cmd, %StructCmd{payload: %{"thing" => "st"}}}
  end

  test "command_executor_resolver dispatches by family (kind prefix), overriding the map" do
    me = self()
    exec = struct_exec(me)

    # a resolver: everything before the first dot is the family; only "rule" maps.
    Application.put_env(:reactive_dag, :command_executor_resolver, fn kind ->
      case kind |> String.split(".", parts: 2) |> hd() do
        "rule" -> exec
        _ -> nil
      end
    end)

    # the exact-kind map does NOT contain "rule.configure" — only the resolver resolves it
    ReactiveDag.Commands.enqueue!(%{kind: "rule.configure", scope: "r", payload: %{"thing" => "cfg"}})
    ReactiveDag.Commands.enqueue!(%{kind: "unknown.thing", scope: "u", payload: %{}})

    {:ok, tally} = ReactiveDag.Commands.run([])

    assert tally.done == 1
    assert tally.failed == 1
    assert_received {:struct_cmd, _}
  end

  test "tuple followups {kind, attrs} enqueue (a host whose enqueue takes kind + attrs)" do
    me = self()

    Application.put_env(:reactive_dag, :command_executors, %{
      "tuple_fanout" => tuple_fanout_exec(),
      "approve" => struct_or_map_exec(me)
    })

    ReactiveDag.Commands.enqueue!(%{kind: "tuple_fanout", scope: "t"})
    {:ok, tally} = ReactiveDag.Commands.run([])

    assert tally.done == 2
    assert tally.enqueued == 1
    assert_received {:approved2, "kid"}
  end

  # runtime-built executors (module attrs can't close over `self()`)
  defp struct_exec(pid) do
    Module.create(
      :"Elixir.StructExec#{System.unique_integer([:positive])}",
      quote do
        @behaviour ReactiveDag.CommandExecutor
        @impl true
        def execute(cmd, _ctx) do
          send(unquote(pid), {:struct_cmd, cmd})
          {:done, %{}}
        end
      end,
      Macro.Env.location(__ENV__)
    )
    |> elem(1)
  end

  defp tuple_fanout_exec do
    Module.create(
      :"Elixir.TupleFanout#{System.unique_integer([:positive])}",
      quote do
        @behaviour ReactiveDag.CommandExecutor
        @impl true
        # a {kind, attrs} tuple followup — the shape a host with enqueue(kind, attrs) emits
        def execute(_cmd, _ctx), do: {:done, %{}, [{"approve", %{payload: %{"thing" => "kid"}}}]}
      end,
      Macro.Env.location(__ENV__)
    )
    |> elem(1)
  end

  defp struct_or_map_exec(pid) do
    Module.create(
      :"Elixir.SOMExec#{System.unique_integer([:positive])}",
      quote do
        @behaviour ReactiveDag.CommandExecutor
        @impl true
        def execute(cmd, _ctx) do
          p = ReactiveDag.Commands.field(cmd, :payload) || cmd["payload"]
          send(unquote(pid), {:approved2, p["thing"]})
          {:done, %{}}
        end
      end,
      Macro.Env.location(__ENV__)
    )
    |> elem(1)
  end
end

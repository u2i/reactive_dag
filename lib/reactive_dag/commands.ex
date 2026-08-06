defmodule ReactiveDag.Commands do
  @moduledoc """
  The **command frontier** — a second dirty frontier, for INTENTS instead of dirty
  keys. It's the drain pattern one layer up: claim queued commands in `seq` order
  (serialized, single-concurrency), execute each via its host `CommandExecutor`,
  settle the outcome, repeat — then let the host kick the model drain so a
  command's downstream consequences propagate in the same pass.

  This is how HUMAN INPUT enters the graph atomically and in order:

    * a human-managed list / an approval is a **leaf** an executor writes;
    * a command is the ordered, transactional INTENT to write it;
    * a `blocked` command is a pending human question that freezes its scope
      without stranding the queue (the answer arrives as another command).

  A command is a **frontier row**, not an Ash resource — the same category as a
  dirty-key tuple: a claimed row in a table, not something authored. The claim is
  `FOR UPDATE SKIP LOCKED` SQL (not expressible as an Ash action), so the lib owns
  the enqueue/claim/settle mechanics; how commands are LISTED or shown is host
  territory (a host may put its own read-only Ash resource over the table for a
  LiveView — the lib ships none, just as it ships none for the dirty-key frontier).

  ## Seams

    * `config :reactive_dag, command_executors: %{kind => module}` — the
      `ReactiveDag.CommandExecutor` per kind (like the recompute/set_op registries).
    * `config :reactive_dag, commands_store: Module` — the `ReactiveDag.Commands.
      Store` (enqueue/claim/settle). Default is the `seq`-ordered Postgres table
      (`ReactiveDag.Commands.Store.Postgres`); its required columns are documented
      there. Swappable for an in-memory store in tests.
    * `run/1`'s `:on_settled` — called after each `:done` command; the host uses it
      to kick the model drain (the lib doesn't hard-wire to a specific drain).
    * `config :reactive_dag, command_freeze_exempt: [kind]` — kinds that claim
      through a frozen scope (answers: the cure, not the disease).

  ## Serialization + transaction

  `claim/1` locks one queued row (`FOR UPDATE SKIP LOCKED`, `ORDER BY seq`), so runs
  never interleave commands. Each command's executor runs inside a per-command
  transaction with its settle, so the leaf write + the command's terminal status
  commit together. A blocked/failed command **freezes its scope**: later commands
  with the same scope are skipped until it clears — except commands whose kind is in
  `:command_freeze_exempt` (answers: the cure, not the disease).
  """
  require Logger

  @default_cap 500

  @doc """
  Enqueue an intent. `attrs` = `%{kind, scope, payload, dedup_key \\ nil, actor \\
  nil, answers_id \\ nil}`. Coalesced by `dedup_key` while open (a partial unique
  index on `(dedup_key) WHERE status IN ('queued','running')` makes the insert a
  no-op for an already-open identical intent). Returns `:ok`.
  """
  @spec enqueue!(map()) :: :ok
  def enqueue!(attrs), do: store().enqueue(attrs)

  @doc """
  Drain the command frontier now. Claims queued commands in `seq` order, executes
  each, settles the outcome, and (on `:done`) invokes `:on_settled`. Options:

    * `:cap` — max commands this run (default #{@default_cap}); the rest stay queued.
    * `:on_settled` — `(command, result -> any)` run after each `:done` (kick the drain here).
    * `:ctx` — extra fields merged into the executor `ctx` (default `%{}`).

  Returns `{:ok, tally}` where tally = `%{done, blocked, failed, enqueued, capped?}`.
  """
  @spec run(keyword()) :: {:ok, map()}
  def run(opts \\ []) do
    cap = Keyword.get(opts, :cap, @default_cap)
    on_settled = Keyword.get(opts, :on_settled, fn _cmd, _result -> :ok end)
    base_ctx = Keyword.get(opts, :ctx, %{})
    run_id = Ecto.UUID.generate()

    tally = %{done: 0, blocked: 0, failed: 0, enqueued: 0, capped?: false}
    loop(run_id, base_ctx, on_settled, tally, cap)
  end

  # ── the claim → dispatch → settle loop ──────────────────────────────────────
  defp loop(_run_id, _ctx, _on_settled, tally, 0), do: {:ok, %{tally | capped?: true}}

  defp loop(run_id, base_ctx, on_settled, tally, budget) do
    case claim(run_id) do
      nil ->
        {:ok, tally}

      cmd ->
        outcome = dispatch(cmd, Map.merge(base_ctx, %{run_id: run_id, actor: cmd["actor"]}))
        tally = settle(run_id, cmd, outcome, on_settled, tally)
        loop(run_id, base_ctx, on_settled, tally, budget - 1)
    end
  end

  defp dispatch(cmd, ctx) do
    case Map.get(executors(), cmd["kind"]) do
      nil ->
        {:error, "no executor for kind #{inspect(cmd["kind"])}"}

      mod ->
        try do
          mod.execute(cmd, ctx)
        rescue
          e -> {:error, e}
        catch
          kind, reason -> {:error, {kind, reason}}
        end
    end
  end

  defp settle(_run_id, cmd, outcome, on_settled, tally) do
    case outcome do
      {:done, result} ->
        set_status!(cmd["id"], "done", result: result)
        on_settled.(cmd, result)
        %{tally | done: tally.done + 1}

      {:done, result, followups} ->
        set_status!(cmd["id"], "done", result: result)
        n = Enum.count(followups, &enqueue_followup(&1, cmd))
        on_settled.(cmd, result)
        %{tally | done: tally.done + 1, enqueued: tally.enqueued + n}

      {:blocked, needs} when is_map(needs) ->
        set_status!(cmd["id"], "blocked", needs: needs)
        %{tally | blocked: tally.blocked + 1}

      {:error, e} ->
        set_status!(cmd["id"], "failed", error: String.slice(inspect(e), 0, 500))
        Logger.warning("reactive_dag commands: #{cmd["kind"]} #{cmd["id"]} failed: #{inspect(e)}")
        %{tally | failed: tally.failed + 1}
    end
  end

  defp enqueue_followup(fu, parent) do
    enqueue!(%{
      kind: fu.kind,
      scope: Map.get(fu, :scope, parent["scope"]),
      payload: Map.get(fu, :payload, %{}),
      actor: parent["actor"]
    })

    true
  end

  defp claim(run_id), do: store().claim(run_id, freeze_exempt())
  defp set_status!(id, status, fields), do: store().settle(id, status, fields)

  # ── config ──────────────────────────────────────────────────────────────────
  defp store, do: ReactiveDag.Commands.Store.impl()
  defp executors, do: Application.get_env(:reactive_dag, :command_executors, %{})
  defp freeze_exempt, do: Application.get_env(:reactive_dag, :command_freeze_exempt, [])
end

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

  ## Pending-aware reads (optimistic overlay)

  A read can reflect **outstanding** commands — "the world as it will be once the
  queue drains" — via `overlay/2`. Because a command's meaning is opaque to the
  lib, each executor optionally implements `c:ReactiveDag.CommandExecutor.project/1`,
  declaring the coordination effect its still-queued intent anticipates
  (`{cell_id, key, status}`). `overlay/2` folds those onto a committed
  `%{key => status}` base (e.g. from `ReactiveDag.Tuple`):

      base     = ReactiveDag.Tuple.keys_by_status("approvals", ~w(present failing))  # committed
      overlaid = ReactiveDag.Commands.overlay("approvals", base)
      # overlaid[key] => %{status: anticipated, pending: [outstanding commands]}

  This composes with a verdict rollup (feed the overlaid statuses to
  `ReactiveDag.Verdict.rollup/2`) without the read layer depending on the command
  layer — the overlay is a view, never a write.

  ## Seams

    * `config :reactive_dag, command_executors: %{kind => module}` — the
      `ReactiveDag.CommandExecutor` per kind (like the recompute/set_op registries).
      For open-ended kinds (families like `rule.configure`/`rule.delete` that can't
      be enumerated), set `config :reactive_dag, command_executor_resolver: fun`
      instead — a `(kind -> module | nil)` that maps a kind to its executor (e.g. by
      family). When set it takes precedence over the exact-kind map.
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
    * `:on_event` — `(event, command, info -> any)`, called at each step INSIDE the
      loop so a host can record an audit/processing log the `:done`-only
      `:on_settled` can't reach. `event` is `:claimed` (just before dispatch;
      `info = %{}`) then one terminal event per command — `:done` / `:blocked` /
      `:failed` — with `info` carrying `:result` / `:needs` / `:error`,
      `:duration_ms` (executor wall time), and `:enqueued` (followup count, `:done`
      only). Fires for EVERY outcome, unlike `:on_settled`. Default: no-op.
    * `:ctx` — extra fields merged into the executor `ctx` (default `%{}`).

  Returns `{:ok, tally}` where tally = `%{done, blocked, failed, enqueued, capped?}`.
  """
  @spec run(keyword()) :: {:ok, map()}
  def run(opts \\ []) do
    cap = Keyword.get(opts, :cap, @default_cap)
    on_settled = Keyword.get(opts, :on_settled, fn _cmd, _result -> :ok end)
    on_event = Keyword.get(opts, :on_event, fn _event, _cmd, _info -> :ok end)
    base_ctx = Keyword.get(opts, :ctx, %{})
    run_id = Ecto.UUID.generate()

    tally = %{done: 0, blocked: 0, failed: 0, enqueued: 0, capped?: false}
    loop(run_id, base_ctx, on_settled, on_event, tally, cap)
  end

  # ── the claim → dispatch → settle loop ──────────────────────────────────────
  defp loop(_run_id, _ctx, _on_settled, _on_event, tally, 0), do: {:ok, %{tally | capped?: true}}

  defp loop(run_id, base_ctx, on_settled, on_event, tally, budget) do
    case claim(run_id) do
      nil ->
        {:ok, tally}

      cmd ->
        on_event.(:claimed, cmd, %{})
        {micros, outcome} = timed(fn -> dispatch(cmd, Map.merge(base_ctx, %{run_id: run_id, actor: field(cmd, :actor)})) end)
        tally = settle(run_id, cmd, outcome, on_settled, on_event, div(micros, 1000), tally)
        loop(run_id, base_ctx, on_settled, on_event, tally, budget - 1)
    end
  end

  # executor wall time in microseconds — :timer.tc is resume-safe (monotonic,
  # not wall-clock), unlike Date.now/System.system_time.
  defp timed(fun), do: :timer.tc(fun)

  defp dispatch(cmd, ctx) do
    case resolve_executor(field(cmd, :kind)) do
      nil ->
        {:error, "no executor for kind #{inspect(field(cmd, :kind))}"}

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

  # A claimed command is whatever the Store returns — the default Postgres store
  # yields a string-keyed map, but a host store may return an atom-keyed map or a
  # struct (e.g. an Ash resource). The lib touches only `kind`/`id`/`actor` for its
  # own bookkeeping; `field/2` reads them from any of those shapes so an executor
  # can receive its native command unchanged.
  @doc false
  def field(cmd, key) when is_atom(key) do
    cond do
      is_map(cmd) and Map.has_key?(cmd, key) -> Map.get(cmd, key)
      is_map(cmd) and Map.has_key?(cmd, Atom.to_string(key)) -> Map.get(cmd, Atom.to_string(key))
      true -> nil
    end
  end

  # Kind → executor. Default: exact-kind lookup in the `:command_executors` map. A
  # host with open-ended kinds (families like `rule.configure`/`rule.delete`) sets
  # `:command_executor_resolver` to a `(kind -> module | nil)` fun instead.
  defp resolve_executor(kind) do
    case Application.get_env(:reactive_dag, :command_executor_resolver) do
      fun when is_function(fun, 1) -> fun.(kind)
      nil -> Map.get(executors(), kind)
    end
  end

  defp settle(_run_id, cmd, outcome, on_settled, on_event, ms, tally) do
    case outcome do
      {:done, result} ->
        set_status!(field(cmd, :id), "done", result: result)
        on_event.(:done, cmd, %{result: result, enqueued: 0, duration_ms: ms})
        on_settled.(cmd, result)
        %{tally | done: tally.done + 1}

      {:done, result, followups} ->
        set_status!(field(cmd, :id), "done", result: result)
        n = Enum.count(followups, &enqueue_followup(&1, cmd))
        on_event.(:done, cmd, %{result: result, enqueued: n, duration_ms: ms})
        on_settled.(cmd, result)
        %{tally | done: tally.done + 1, enqueued: tally.enqueued + n}

      {:blocked, needs} when is_map(needs) ->
        set_status!(field(cmd, :id), "blocked", needs: needs)
        on_event.(:blocked, cmd, %{needs: needs, duration_ms: ms})
        %{tally | blocked: tally.blocked + 1}

      {:error, e} ->
        set_status!(field(cmd, :id), "failed", error: String.slice(inspect(e), 0, 500))
        on_event.(:failed, cmd, %{error: e, duration_ms: ms})
        Logger.warning("reactive_dag commands: #{field(cmd, :kind)} #{field(cmd, :id)} failed: #{inspect(e)}")
        %{tally | failed: tally.failed + 1}
    end
  end

  # A followup may be a `%{kind, scope?, payload?}` map or a `{kind, attrs}` tuple
  # (attrs a map or keyword list) — the latter is what a host whose enqueue takes
  # `(kind, attrs)` naturally emits. Both normalize to the enqueue attrs map,
  # defaulting scope/actor to the parent's.
  defp enqueue_followup({kind, attrs}, parent) do
    a = Map.new(attrs)

    enqueue!(%{
      kind: kind,
      scope: Map.get(a, :scope, field(parent, :scope)),
      payload: Map.get(a, :payload, %{}),
      dedup_key: Map.get(a, :dedup_key),
      actor: field(parent, :actor)
    })

    true
  end

  defp enqueue_followup(fu, parent) when is_map(fu) do
    enqueue!(%{
      kind: fu.kind,
      scope: Map.get(fu, :scope, field(parent, :scope)),
      payload: Map.get(fu, :payload, %{}),
      dedup_key: Map.get(fu, :dedup_key),
      actor: field(parent, :actor)
    })

    true
  end

  defp claim(run_id), do: store().claim(run_id, freeze_exempt())
  defp set_status!(id, status, fields), do: store().settle(id, status, fields)

  # ── pending-aware reads (optimistic overlay) ────────────────────────────────

  @doc """
  The anticipated coordination effects of every OUTSTANDING command — each command
  projected via its executor's optional `project/1` — as
  `[%{cell_id, key, status, command_id}]`, in `seq` order. Commands whose kind has
  no executor, or whose executor omits `project/1`, contribute nothing (their intent
  is opaque to a pending-aware read). Later commands in `seq` order win on a
  `(cell_id, key)` collision (the queue would apply them last).
  """
  @spec pending_effects() :: [%{cell_id: String.t(), key: String.t(), status: String.t(), command_id: String.t()}]
  def pending_effects do
    for cmd <- store().outstanding(),
        mod = resolve_executor(field(cmd, :kind)),
        is_atom(mod) and function_exported?(mod, :project, 1),
        eff <- mod.project(cmd) do
      %{cell_id: eff.cell_id, key: eff.key, status: eff.status, command_id: field(cmd, :id)}
    end
  end

  @doc """
  Overlay outstanding commands onto a base per-key status map for one cell — the
  "world as it will be once the queue drains" view. `base` is `%{key => status}`
  (e.g. from `ReactiveDag.Tuple`); returns `%{key => %{status:, pending: [...]}}`
  where `status` is the ANTICIPATED status (base, or the last pending effect's) and
  `pending` lists the outstanding commands that will touch this key. Keys with no
  outstanding command keep their base status and `pending: []`.
  """
  @spec overlay(String.t(), %{String.t() => String.t()}) ::
          %{String.t() => %{status: String.t(), pending: [map()]}}
  def overlay(cell_id, base) do
    effects =
      pending_effects()
      |> Enum.filter(&(&1.cell_id == cell_id))
      |> Enum.group_by(& &1.key)

    keys = Map.keys(base) ++ Map.keys(effects)

    Map.new(Enum.uniq(keys), fn key ->
      pend = Map.get(effects, key, [])
      # the anticipated status: the last pending effect wins, else the committed base.
      status = if pend == [], do: Map.get(base, key), else: List.last(pend).status
      {key, %{status: status, pending: Enum.map(pend, &Map.take(&1, [:command_id, :status]))}}
    end)
  end

  # ── config ──────────────────────────────────────────────────────────────────
  defp store, do: ReactiveDag.Commands.Store.impl()
  defp executors, do: Application.get_env(:reactive_dag, :command_executors, %{})
  defp freeze_exempt, do: Application.get_env(:reactive_dag, :command_freeze_exempt, [])
end

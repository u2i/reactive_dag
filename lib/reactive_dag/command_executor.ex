defmodule ReactiveDag.CommandExecutor do
  @moduledoc """
  The seam for executing a **command** — a human/external INTENT that enters the
  graph. Where `RecomputeStrategy` says how a cell recomputes, `CommandExecutor`
  says how one command applies: it writes leaf tuples (`ReactiveDag.Op.put`) and/or
  enqueues follow-ups, then returns an outcome.

  A command is dispatched by its `kind` to the host executor registered for that
  kind (see `ReactiveDag.Commands` + the `:command_executors` config). This is the
  intent-layer analogue of `RecomputeStrategy` dispatching a cell by op.

  ## The outcome

  `execute/2` returns one of:

    * `{:done, result}` — applied; `result` is an opaque map recorded on the command.
    * `{:done, result, followups}` — applied, and enqueue these follow-up commands
      (each `%{kind, scope, payload}`) in the same run.
    * `{:blocked, needs}` — the intent needs a HUMAN decision. `needs` is the
      renderable question (`%{"question" => …, …}`). The command parks as `blocked`,
      **freezing its scope** so later same-scope commands wait; it resumes when an
      answering command (pointing back via `answers_id`) settles it. This is the
      human-in-the-loop pause — a first-class state, not an error.
    * `{:error, reason}` — failed; the command parks as `failed` (also freezing its
      scope until a human discards or an answer clears it).

  `ctx` carries `%{run_id, actor}` (+ anything the host adds). The executor runs
  INSIDE the processor's per-command transaction, so its leaf writes + the command
  settle commit atomically — the serialization guarantee.
  """

  @type command :: map()
  @type ctx :: map()
  @type followup :: %{required(:kind) => String.t(), optional(:scope) => String.t(), optional(:payload) => map()}

  @type outcome ::
          {:done, map()}
          | {:done, map(), [followup()]}
          | {:blocked, map()}
          | {:error, term()}

  @doc "Apply one command. Runs inside the processor's per-command transaction."
  @callback execute(command(), ctx()) :: outcome()

  @typedoc """
  A command's ANTICIPATED effect on the coordination tuple — which `(cell_id, key)`
  it will set and to what `status` — used to overlay outstanding commands onto a
  read (see `ReactiveDag.Commands.pending_effects/0`). This is the inverse of
  `execute`, at the coordination grain: what the intent *will* do, before it runs.
  """
  @type effect :: %{
          required(:cell_id) => String.t(),
          required(:key) => String.t(),
          required(:status) => String.t()
        }

  @doc """
  OPTIONAL: the coordination effects a still-QUEUED/BLOCKED command anticipates —
  what a "world as it will be once the queue drains" read should reflect. Only the
  executor knows what its kind of intent means, so only it can project. An executor
  that omits `project/1` contributes no overlay (its outstanding commands are
  invisible to pending-aware reads).
  """
  @callback project(command()) :: [effect()]

  @optional_callbacks project: 1
end

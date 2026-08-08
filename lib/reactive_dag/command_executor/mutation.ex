defmodule ReactiveDag.CommandExecutor.Mutation do
  @moduledoc """
  A **narrowed** `ReactiveDag.CommandExecutor` for the common case: a command is an
  ordered DATA MODIFICATION, not a human-in-the-loop intent. `execute/2` returns
  only `{:done, result}` or `{:error, reason}` — there is no `{:blocked, _}` arm.

  This is a blessed variant, not a new seam: the drain dispatches a command by
  calling the module's `execute/2` regardless of which behaviour it declares, so a
  mutation executor plugs into the same `:command_executors` config. What this
  buys is INTENT — the module (and its readers) state that its commands never pause
  for a human; one that can't apply *fails* (freezing its scope) rather than parking
  a question. Hosts with no approval/answer flow (e.g. a straight CRUD editor over a
  source-of-truth) author against this instead of the full `CommandExecutor`.

  The frontier's other guarantees still apply unchanged — total order (`seq`,
  serialized claim), the per-command transaction (the write + the settle commit
  atomically), and the audit log (`ReactiveDag.Commands.history/1`).

  ## Usage

      defmodule MyApp.Commands.AddThing do
        use ReactiveDag.CommandExecutor.Mutation

        @impl true
        def execute(cmd, _ctx) do
          case MyApp.Things.create(cmd["payload"]) do
            {:ok, thing} -> {:done, %{"id" => thing.id}}
            {:error, e}  -> {:error, inspect(e)}
          end
        end
      end
  """

  @type command :: ReactiveDag.CommandExecutor.command()
  @type ctx :: ReactiveDag.CommandExecutor.ctx()
  @type result :: map()

  @doc """
  Apply one command's data modification, inside the frontier's per-command
  transaction. `{:done, result}` records `result` on the command row; `{:error,
  reason}` parks it `failed` and freezes its scope. No `{:blocked, _}`.
  """
  @callback execute(command(), ctx()) :: {:done, result()} | {:error, term()}

  @doc """
  `use ReactiveDag.CommandExecutor.Mutation` — declare a mutation executor.

  Adopts THIS behaviour only (not the broader `ReactiveDag.CommandExecutor`), so its
  narrower `execute/2` typespec is the one enforced and there's no
  conflicting-behaviours warning. Dispatch is unaffected: the drain calls
  `execute/2` directly.
  """
  defmacro __using__(_opts) do
    quote do
      @behaviour ReactiveDag.CommandExecutor.Mutation
    end
  end
end

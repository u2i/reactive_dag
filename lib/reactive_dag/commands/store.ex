defmodule ReactiveDag.Commands.Store do
  @moduledoc """
  The storage seam for the command frontier — the physical `enqueue`/`claim`/
  `settle` operations `ReactiveDag.Commands` drains over. The default
  (`ReactiveDag.Commands.Store.Postgres`) is the `seq`-ordered, `FOR UPDATE SKIP
  LOCKED`, scope-freezing Postgres table; a host can swap it (an in-memory store
  for tests, a different backend) via `config :reactive_dag, commands_store:`.

  Commands are plain maps with string keys: `"id" / "kind" / "scope" / "payload" /
  "dedup_key" / "actor" / "answers_id"`.
  """

  @type command :: %{optional(String.t()) => term()}

  @doc "Insert a queued command; coalesce identical open intents by `dedup_key`."
  @callback enqueue(attrs :: map()) :: :ok

  @doc """
  Claim ONE queued command in `seq` order, marking it `running` under `run_id`,
  skipping scopes frozen by a blocked/failed command (except kinds in
  `freeze_exempt`). Returns the claimed command map, or `nil` when none is claimable.
  """
  @callback claim(run_id :: String.t(), freeze_exempt :: [String.t()]) :: command() | nil

  @doc "Set a command's terminal status + fields (`result`/`needs`/`error`)."
  @callback settle(id :: String.t(), status :: String.t(), fields :: keyword()) :: :ok

  @doc "The configured store module (default: the Postgres store)."
  @spec impl() :: module()
  def impl, do: Application.get_env(:reactive_dag, :commands_store, ReactiveDag.Commands.Store.Postgres)
end

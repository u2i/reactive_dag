defmodule ReactiveDag.Commands.Store do
  @moduledoc """
  The storage seam for the command frontier — the physical `enqueue`/`claim`/
  `settle` operations `ReactiveDag.Commands` drains over. The default
  (`ReactiveDag.Commands.Store.Postgres`) is the `seq`-ordered, `FOR UPDATE SKIP
  LOCKED`, scope-freezing Postgres table; a host can swap it (an in-memory store
  for tests, a different backend) via `config :reactive_dag, commands_store:`.

  The default store yields plain **string-keyed maps**: `"id" / "kind" / "scope" /
  "payload" / "dedup_key" / "actor" / "answers_id"`. A host store MAY instead
  return atom-keyed maps or a **struct** (e.g. an Ash resource row) so its executors
  receive their native command shape — `ReactiveDag.Commands` reads only
  `kind`/`id`/`scope`/`actor` for its own bookkeeping and does so via a
  shape-agnostic accessor (`ReactiveDag.Commands.field/2`); everything else in the
  command is opaque to the lib and passed to the executor untouched.
  """

  @type command :: %{optional(String.t()) => term()} | map() | struct()

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

  @doc """
  All OUTSTANDING commands — those not yet applied (`queued` / `running` /
  `blocked`), in `seq` order. The overlay source for pending-aware reads
  (`ReactiveDag.Commands.pending_effects/0`).
  """
  @callback outstanding() :: [command()]

  @doc "The configured store module (default: the Postgres store)."
  @spec impl() :: module()
  def impl, do: Application.get_env(:reactive_dag, :commands_store, ReactiveDag.Commands.Store.Postgres)
end

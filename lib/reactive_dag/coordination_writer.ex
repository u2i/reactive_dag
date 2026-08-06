defmodule ReactiveDag.CoordinationWriter do
  @moduledoc """
  The seam for WRITING a cell's coordination tuples — the third seam, alongside
  `RecomputeStrategy` (how a cell recomputes) and `KeyRule` (how a change
  propagates). An op, mid-recompute, records which of its keys are present /
  gone; this behaviour is where those writes land.

  Why a seam and not just `ReactiveDag.Tuple`: the spine (`cell_id, key, status,
  freshness`) is shared, but each host's coordination write also touches its
  EXTENSION columns in the SAME atomic upsert — cascade stamps `source_ref` /
  `last_seen_at` and CLEARS `tombstoned_at` (retain-if-vanish revival); the
  portal stamps `strength`. That extension write is host policy, so the write
  can't be a pure spine call. The library provides the INTERFACE ops use
  (`ReactiveDag.Op.put/tombstone/delete`) and routes it here; the host supplies
  the writer via `config :reactive_dag, coordination_writer: MyApp.Writer`.

  The default (`ReactiveDag.Tuple.Writer`) is spine-only — fine for a host with
  no extension columns; hosts that have them (cascade) configure their own.
  """

  @type key :: String.t()

  # The callbacks take a `cell_id` STRING, not a Cell struct — a writer only ever
  # needs the id, and `ReactiveDag.Op` extracts it up front so an op may pass
  # EITHER cell type (a `ReactiveDag.Cell` from the drain, or a host's own cell
  # in a unit test). This keeps the writer independent of the caller's cell type.

  @doc "Mark `key` of `cell_id` present. `opts` may carry host fields (source_ref, strength, stale_after)."
  @callback put(cell_id :: key(), key :: key(), opts :: keyword()) :: :ok

  @doc "Tombstone `keys` of `cell_id` — vanished-but-retained (a host with a retain policy)."
  @callback tombstone(cell_id :: key(), keys :: [key()]) :: :ok

  @doc "Hard-delete `keys` of `cell_id`."
  @callback delete(cell_id :: key(), keys :: [key()]) :: :ok

  @optional_callbacks tombstone: 2

  @doc "The configured writer module (default: the spine-only `ReactiveDag.Tuple.Writer`)."
  @spec writer() :: module()
  def writer, do: Application.get_env(:reactive_dag, :coordination_writer, ReactiveDag.Tuple.Writer)
end

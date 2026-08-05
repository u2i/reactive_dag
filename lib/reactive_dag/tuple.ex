defmodule ReactiveDag.Tuple do
  @moduledoc """
  The shared COORDINATION tuple — the reactive layer's projection of a cell into
  a thin `(cell_id, key, status, freshness)` row. A cell IS its set of these
  rows; a parent reads a child's set by `(cell_id, key)`.

  This is NOT payload. The authoritative value for a key lives in the host's own
  typed resource (cascade's `BudgetVsActual`, the portal's `Grant`/`Attestation`),
  joined back by `key`. The coordination row carries only the verdict (`status`)
  and freshness — enough for the substrate to schedule and for a downstream cell
  to know *which* keys exist, without copying their payload.

  ## Spine vs. extension

  The library owns the SPINE — the columns both hosts share:

      cell_id, key            (composite PK)
      status                  (string; the host defines the vocabulary)
      observed_at, updated_at, stale_after   (freshness)

  Each host's physical table ALSO carries its own extension columns, which the
  library neither reads nor writes:

    * the portal adds `strength` (the evidence modality — its `derive` output);
    * cascade adds `source_ref` / `last_seen_at` / `tombstoned_at`
      (its retain-if-vanished + fingerprint policy).

  So the library provides a spine CONTRACT + shared operators over the configured
  table — not the table itself (the host owns that, extension columns and all),
  exactly as `ReactiveDag.Frontier` owns the dirty ops but the host owns the
  `*_dirty` table. `put/3` writes only spine columns (leaving any extension
  columns to their DB defaults / a host wrapper that sets them in the same
  upsert). Reads project spine columns.

      config :reactive_dag, repo: MyApp.Repo, tuple_table: "my_tuple"

  ## The join contract (what makes stratification work)

  `key` is the universal join handle. A BEAM producing-node writes its spine row
  + its typed payload under a `key`; a SQL proving-node reads other cells' spine
  rows by `(cell_id, key)`. Both wrote rows into ONE `tuple_table`, so a SQL cell
  can consume a BEAM cell's output by joining on key. That is the two-layer
  (produce → prove) graph, made concrete. It commits hosts to one constraint:
  a node's `key` strings must be stable and join-compatible across the
  producer/consumer seam.
  """

  @default_table "reactive_dag_tuple"

  @type key :: String.t()

  @doc """
  Upsert the SPINE of a tuple for `(cell_id, key)`: presence/verdict + freshness.
  Only spine columns are touched — extension columns (strength, source_ref, …)
  keep their DB defaults on insert and are left untouched on update, so a host
  that needs them writes its own upsert (calling this for the spine, or setting
  them in one combined statement host-side).

  Opts:
    * `:status`      — verdict string (default `"present"`)
    * `:stale_after` — freshness horizon (default nil)
    * `:observed_at` — when the source produced this (default now)
  """
  @spec put(String.t(), key(), keyword()) :: :ok
  def put(cell_id, key, opts \\ []) do
    now = DateTime.utc_now()
    status = Keyword.get(opts, :status, "present")
    stale_after = Keyword.get(opts, :stale_after)
    observed_at = Keyword.get(opts, :observed_at, now)

    query!(
      """
      INSERT INTO #{table()}
        (cell_id, key, status, observed_at, stale_after, updated_at)
      VALUES ($1, $2, $3, $4, $5, $6)
      ON CONFLICT (cell_id, key) DO UPDATE
        SET status = EXCLUDED.status,
            observed_at = EXCLUDED.observed_at,
            stale_after = EXCLUDED.stale_after,
            updated_at = EXCLUDED.updated_at
      """,
      [cell_id, key, status, observed_at, stale_after, now]
    )

    :ok
  end

  @doc "Delete the tuples for `(cell_id, keys)`. No-op on an empty key list."
  @spec delete(String.t(), [key()]) :: :ok
  def delete(_cell_id, []), do: :ok

  def delete(cell_id, keys) do
    query!("DELETE FROM #{table()} WHERE cell_id = $1 AND key = ANY($2)", [cell_id, keys])
    :ok
  end

  @doc "All keys of a cell (any status)."
  @spec all_keys(String.t()) :: [key()]
  def all_keys(cell_id) do
    %{rows: rows} = query!("SELECT key FROM #{table()} WHERE cell_id = $1", [cell_id])
    Enum.map(rows, fn [k] -> k end)
  end

  @doc ~S{Keys of a cell whose status is `"present"`.}
  @spec present_keys(String.t()) :: [key()]
  def present_keys(cell_id) do
    %{rows: rows} =
      query!("SELECT key FROM #{table()} WHERE cell_id = $1 AND status = 'present'", [cell_id])

    Enum.map(rows, fn [k] -> k end)
  end

  @doc "Count of tuples per cell, as `%{cell_id => count}`."
  @spec counts() :: %{String.t() => non_neg_integer()}
  def counts do
    %{rows: rows} = query!("SELECT cell_id, COUNT(*) FROM #{table()} GROUP BY cell_id", [])
    Map.new(rows, fn [cid, n] -> {cid, n} end)
  end

  defp query!(sql, params), do: repo().query!(sql, params)

  defp repo do
    Application.get_env(:reactive_dag, :repo) ||
      raise "reactive_dag: set `config :reactive_dag, repo: MyApp.Repo`"
  end

  defp table, do: Application.get_env(:reactive_dag, :tuple_table, @default_table)
end

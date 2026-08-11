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

  @doc """
  All keys of a cell (any status), optionally narrowed by `:key_scope` (a
  `t:key_scope/0`) — e.g. only the keys whose i-th segment names one service.
  """
  @spec all_keys(String.t(), keyword()) :: [key()]
  def all_keys(cell_id, opts \\ []) do
    {scope_sql, scope_params} = key_scope_clause(Keyword.get(opts, :key_scope), 2)

    %{rows: rows} =
      query!(
        "SELECT key FROM #{table()} WHERE cell_id = $1" <> scope_sql,
        [cell_id] ++ scope_params
      )

    Enum.map(rows, fn [k] -> k end)
  end

  @doc """
  Keys of a cell whose status is `"present"`, optionally narrowed by
  `:key_scope` (a `t:key_scope/0`). Equivalent to
  `keys_by_status(cell, ["present"], key_scope: …)` but unordered.
  """
  @spec present_keys(String.t(), keyword()) :: [key()]
  def present_keys(cell_id, opts \\ []) do
    {scope_sql, scope_params} = key_scope_clause(Keyword.get(opts, :key_scope), 2)

    %{rows: rows} =
      query!(
        "SELECT key FROM #{table()} WHERE cell_id = $1 AND status = 'present'" <> scope_sql,
        [cell_id] ++ scope_params
      )

    Enum.map(rows, fn [k] -> k end)
  end

  @doc """
  The SPINE ROWS of a cell — `%{key, status, observed_at}` maps, ordered by key,
  optionally narrowed by `:key_scope`. The full-row companion to the key reads
  above: what an evaluation that needs status alongside key consumes (the
  attestation machinery's raw-rows and basis-digest input).
  """
  @spec rows(String.t(), keyword()) :: [%{key: key(), status: String.t(), observed_at: term()}]
  def rows(cell_id, opts \\ []) do
    {scope_sql, scope_params} = key_scope_clause(Keyword.get(opts, :key_scope), 2)

    %{rows: rows} =
      query!(
        "SELECT key, status, observed_at FROM #{table()} WHERE cell_id = $1" <>
          scope_sql <> " ORDER BY key",
        [cell_id] ++ scope_params
      )

    Enum.map(rows, fn [k, s, o] -> %{key: k, status: s, observed_at: o} end)
  end

  @doc "Count of tuples per cell, as `%{cell_id => count}`."
  @spec counts() :: %{String.t() => non_neg_integer()}
  def counts do
    %{rows: rows} = query!("SELECT cell_id, COUNT(*) FROM #{table()} GROUP BY cell_id", [])
    Map.new(rows, fn [cid, n] -> {cid, n} end)
  end

  @typedoc """
  A KEY-SCOPE selector — a host-declared narrowing of a spine read to a subset of
  a cell's keys, WITHOUT the host hand-writing SQL. The library turns each shape
  into a safe PARAMETERIZED predicate (no string interpolation of host values):

    * `{:prefix, p}`            → `key LIKE p`   (p is a full LIKE pattern, e.g. "app|%")
    * `{:exact_or_prefix, k, p}`→ `(key = k OR key LIKE p)`  (a bare id OR its children)
    * `{:segment, i, sep, v}`   → `split_part(key, sep, i) = v`  (the i-th key segment)

  Key GRAMMAR stays the host's — it names which segment / prefix means what; the
  library only assembles the predicate. `nil` (the default) means no scoping.
  """
  @type key_scope ::
          nil
          | {:prefix, String.t()}
          | {:exact_or_prefix, String.t(), String.t()}
          | {:segment, pos_integer(), String.t(), String.t()}

  @doc """
  Keys of a cell whose status is in `statuses`, ordered by key. Options:

    * `:limit`     — cap the result (e.g. a failing-sample)
    * `:key_scope` — a `t:key_scope/0` narrowing to a subset of the cell's keys

  This is the general spine status-read; `present_keys/1` is the `["present"]`
  special case.
  """
  @spec keys_by_status(String.t(), [String.t()], keyword()) :: [key()]
  def keys_by_status(cell_id, statuses, opts \\ []) when is_list(statuses) do
    {scope_sql, scope_params} = key_scope_clause(Keyword.get(opts, :key_scope), 3)
    next = 3 + length(scope_params)

    {limit_sql, limit_params} =
      case Keyword.get(opts, :limit) do
        nil -> {"", []}
        n -> {" LIMIT $#{next}", [n]}
      end

    params = [cell_id, statuses] ++ scope_params ++ limit_params

    %{rows: rows} =
      query!(
        "SELECT key FROM #{table()} WHERE cell_id = $1 AND status = ANY($2)" <>
          scope_sql <> " ORDER BY key" <> limit_sql,
        params
      )

    Enum.map(rows, fn [k] -> k end)
  end

  @doc """
  Status histogram for a cell: `%{status => count}` over its tuples (optionally
  narrowed by `:key_scope`). The spine read behind a cell's verdict (failing /
  pending / green rollups are the host's to compute from this).
  """
  @spec status_histogram(String.t(), keyword()) :: %{String.t() => non_neg_integer()}
  def status_histogram(cell_id, opts \\ []) do
    {scope_sql, scope_params} = key_scope_clause(Keyword.get(opts, :key_scope), 2)

    %{rows: rows} =
      query!(
        "SELECT status, COUNT(*) FROM #{table()} WHERE cell_id = $1" <>
          scope_sql <> " GROUP BY status",
        [cell_id] ++ scope_params
      )

    Map.new(rows, fn [s, n] -> {s, n} end)
  end

  @doc "Most-recent `observed_at` across the given cells, or nil if none have rows."
  @spec max_observed_at([String.t()]) :: DateTime.t() | nil
  def max_observed_at(cell_ids) do
    %{rows: rows} =
      query!("SELECT max(observed_at) FROM #{table()} WHERE cell_id = ANY($1)", [cell_ids])

    case rows do
      [[ts]] -> ts
      _ -> nil
    end
  end

  @doc """
  Reconcile a cell's tuple set against a host-computed DESIRED key set — the one
  algorithm the portal's leaf drivers AND cascade's leaf refresh both hand-rolled:

      current  = the cell's current keys
      want     = `want_keys` (the host computed the desired set + its payload)
      upsert   each want key   → host writes the spine + its own extension columns
      vanished = current − want
      retire   the vanished    → host policy: DELETE (portal) or TOMBSTONE (cascade)
      ⇒ changed_upserts ++ vanished     (the keys to propagate to parents)

  The library owns the SKELETON and the `current`/`vanished` set math; the two
  variation points are seams the host supplies:

    * `:upsert` — `(key -> boolean)` called per want-key; returns true iff this
      key's verdict ACTUALLY changed (so only real changes propagate). The host
      writes the row here (spine + strength/source_ref/…), because WHAT a present
      row contains and WHAT counts as "changed" are host domain logic.
    * `:retire` — how vanished keys leave. `:delete` (the default) uses the spine
      `delete/2`; a host with a retain-if-vanished policy passes a
      `(keys -> any)` fun (cascade tombstones). Vanished keys always propagate.
    * `:current` — the baseline set `vanished` is computed against, as
      `current − want`. Defaults to `all_keys(cell)`. A host whose "live" set is
      narrower than all rows passes it explicitly — cascade's retain-if-vanished
      leaf passes its NON-tombstoned keys, so already-tombstoned keys are neither
      re-retired nor spuriously reported as newly vanished.

  Returns `{:ok, changed_keys}` where `changed_keys = changed_upserts ++ vanished`.
  """
  @spec reconcile(String.t(), [key()] | MapSet.t(), keyword()) :: {:ok, [key()]}
  def reconcile(cell_id, want_keys, opts) do
    upsert = Keyword.fetch!(opts, :upsert)
    retire = Keyword.get(opts, :retire, :delete)

    want_set = MapSet.new(want_keys)
    want = MapSet.to_list(want_set)

    changed_up = Enum.filter(want, fn key -> upsert.(key) == true end)

    current = Keyword.get_lazy(opts, :current, fn -> all_keys(cell_id) end)
    vanished = Enum.reject(current, &MapSet.member?(want_set, &1))

    do_retire(cell_id, vanished, retire)

    {:ok, changed_up ++ vanished}
  end

  @doc """
  The BULK variant of `reconcile/3`, for set-based recomputes. Where `reconcile`
  calls `upsert.(key)` once per want-key (N statements — the per-key leaf
  shape), `reconcile_set` hands the WHOLE want set to one `:upsert_all`
  callback, so an interior set-op that produced its result row-set in one pass
  writes it in ONE bulk statement (spine + the host's extension columns
  together). Same skeleton, same set math, one write.

  Options:

    * `:upsert_all` (required) — `([key] -> [changed_key])`: write every want
      row in one statement (the host's bulk `VALUES` upsert; WHAT a row contains
      is host domain), returning the subset whose verdict ACTUALLY changed (an
      `IS DISTINCT FROM`-guarded upsert's `RETURNING`). Not called for an empty
      want set.
    * `:retire` — how vanished keys leave, as `reconcile/3`: `:delete` (the
      default) or a `(keys -> any)` fun (tombstone).
    * `:current` — the baseline `vanished` is computed against, as
      `reconcile/3`. Defaults to `all_keys(cell_id, key_scope: opts[:key_scope])`.
    * `:key_scope` — a `t:key_scope/0` narrowing the DEFAULT baseline to the
      slice this recompute repriced: a dirty-key-scoped set-op must not see keys
      outside its slice as vanished. Ignored when `:current` is given.

  Returns `{:ok, changed_keys}` where `changed_keys = changed_upserts ++ vanished`.
  """
  @spec reconcile_set(String.t(), [key()] | MapSet.t(), keyword()) :: {:ok, [key()]}
  def reconcile_set(cell_id, want_keys, opts) do
    upsert_all = Keyword.fetch!(opts, :upsert_all)
    retire = Keyword.get(opts, :retire, :delete)

    want_set = MapSet.new(want_keys)
    want = MapSet.to_list(want_set)

    changed_up = if want == [], do: [], else: upsert_all.(want)

    current =
      Keyword.get_lazy(opts, :current, fn ->
        all_keys(cell_id, key_scope: Keyword.get(opts, :key_scope))
      end)

    vanished = Enum.reject(current, &MapSet.member?(want_set, &1))
    do_retire(cell_id, vanished, retire)

    {:ok, changed_up ++ vanished}
  end

  defp do_retire(_cell_id, [], _), do: :ok
  defp do_retire(cell_id, keys, :delete), do: delete(cell_id, keys)
  defp do_retire(_cell_id, keys, fun) when is_function(fun, 1), do: fun.(keys)

  # Build a `t:key_scope/0` into `{" AND <predicate>", params}`, numbering
  # placeholders from `next` (the first unused $N in the caller's query). Host
  # values ride in the params list — never interpolated into the SQL string.
  defp key_scope_clause(nil, _next), do: {"", []}

  defp key_scope_clause({:prefix, pat}, next) do
    {" AND key LIKE $#{next}", [pat]}
  end

  defp key_scope_clause({:exact_or_prefix, k, pat}, next) do
    {" AND (key = $#{next} OR key LIKE $#{next + 1})", [k, pat]}
  end

  defp key_scope_clause({:segment, i, sep, v}, next) when is_integer(i) and i > 0 do
    {" AND split_part(key, $#{next}, $#{next + 1}) = $#{next + 2}", [sep, i, v]}
  end

  defp query!(sql, params), do: repo().query!(sql, params)

  defp repo do
    Application.get_env(:reactive_dag, :repo) ||
      raise "reactive_dag: set `config :reactive_dag, repo: MyApp.Repo`"
  end

  defp table, do: Application.get_env(:reactive_dag, :tuple_table, @default_table)
end

defmodule ReactiveDag.Tuple do
  @moduledoc """
  The coordination spine: which `(cell_id, key)` pairs a cell currently holds.

  A **presence set**, and nothing more. The row for a key says that key exists;
  it carries no verdict, no value, no freshness. What a key *means* — its status,
  its total, its computed columns — lives in the node's own resource, read back
  through `ReactiveDag.Node.Rows`.

  ## Why it shrank

  This table used to carry `status` and freshness columns, because a node could
  be *tableless* — a `verdict? true` node had nowhere else to put its answer, so
  the spine had to hold results as well as track them. That shape is gone: every
  node emits rows into its own resource, so results have a home with real
  columns, real types and real policies. The spine kept only the job no resource
  can do for it.

  Two jobs remain, and both are about keys the library cannot otherwise
  enumerate:

    * **Leaf reconcile** (`reconcile/3`, `reconcile_set/3`) — a source-fed leaf
      has no derived rows of its own, so subtracting "what the scan returned"
      from "what we had" needs a record of what we had.
    * **Write-elsewhere nodes** — a `compute` node with a custom `upsert:` keeps
      its rows somewhere this library never sees, so the spine is the only
      record of which units it holds (see `ReactiveDag.Node.Recompute`'s
      `current_keys`).

  A graph of ordinary payload nodes fed by `dirties_on` touches this table for
  bookkeeping alone.

  ## Spine vs. extension

  The library owns two columns and is their only reader:

      cell_id, key            (composite PK)
      updated_at              (bookkeeping)

  A host's physical table may carry anything else beside them — cascade's
  `source_ref`/`tombstoned_at`, a `strength` modality — and the library neither
  reads nor writes those. It provides a CONTRACT plus shared operators over the
  configured table, not the table itself, exactly as `ReactiveDag.Frontier` owns
  the dirty ops while the host owns the `*_dirty` table.

      config :reactive_dag, repo: MyApp.Repo, tuple_table: "my_tuple"

  Extension columns keep their DB defaults on insert and are untouched on
  update, so a host that needs them writes its own `ReactiveDag.CoordinationWriter`
  setting them in the same upsert.

  ## The join contract

  `key` is the universal join handle, and it is the one constraint this table
  still imposes: a node's `key` strings must be stable and join-compatible
  across the producer/consumer seam, because a parent addresses a child's units
  by exactly the strings the child wrote.
  """

  @default_table "reactive_dag_tuple"

  @type key :: String.t()

  @doc """
  Record that `(cell_id, key)` is present.

  Idempotent: re-putting an existing key refreshes `updated_at` and nothing
  else. `opts` is accepted and ignored — the `ReactiveDag.CoordinationWriter`
  contract passes host extension fields through it, and this spine-only
  implementation has no columns to put them in.
  """
  @spec put(String.t(), key(), keyword()) :: :ok
  def put(cell_id, key, _opts \\ []) do
    query!(
      """
      INSERT INTO #{table()} (cell_id, key, updated_at)
      VALUES ($1, $2, $3)
      ON CONFLICT (cell_id, key) DO UPDATE
        SET updated_at = EXCLUDED.updated_at
      """,
      [cell_id, key, DateTime.utc_now()]
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
  All keys a cell currently holds, optionally narrowed by `:key_scope` (a
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
    # ::int — the one key_scope param that isn't text; cast explicitly rather
    # than lean on the driver/PG inferring split_part's third argument type.
    {" AND split_part(key, $#{next}, $#{next + 1}::int) = $#{next + 2}", [sep, i, v]}
  end

  defp query!(sql, params), do: repo().query!(sql, params)

  defp repo do
    Application.get_env(:reactive_dag, :repo) ||
      raise "reactive_dag: set `config :reactive_dag, repo: MyApp.Repo`"
  end

  defp table do
    name = Application.get_env(:reactive_dag, :tuple_table, @default_table)

    if name =~ ~r/\A[a-zA-Z_][a-zA-Z0-9_]*\z/ do
      name
    else
      raise ArgumentError,
            "reactive_dag: tuple_table #{inspect(name)} is not a valid table identifier"
    end
  end
end

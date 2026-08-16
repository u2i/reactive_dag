defmodule ReactiveDag.Frontier do
  @moduledoc """
  The dirty frontier, owned by the library and backed by the `reactive_dag_dirty`
  table (created by `ReactiveDag.Migration`). The host is an Ash/AshPostgres app,
  so we go through its repo with raw SQL — values always parameterized; the
  table name (the one identifier SQL cannot parameterize) comes from config and
  is validated against an identifier grammar at read time, so a typo fails
  loudly instead of as a syntax error deep in a query. Claim-as-delete is a raw
  `DELETE … RETURNING` that Ash actions don't express cleanly.

  The host supplies its `repo` (its AshPostgres repo module) via config, and may
  override the table name (default `reactive_dag_dirty`) so a host adopting the
  library keeps its existing table without a rename:

      config :reactive_dag, repo: MyApp.Repo, dirty_table: "my_dirty"

  Coalesced by `(cell, key)`; depth-ordered `next_cell`; `claim` atomic per
  cell (`DELETE … RETURNING` — a key is consumed exactly once). The
  `next_cell`-then-`claim` PAIR is not serialized: concurrent drains can pick
  the same cell — see the concurrency note on `ReactiveDag.Drain`. This is
  the shared substrate both hosts previously hand-rolled (cascade's
  `Cascade.Engine.Frontier`, the portal's `model_dirty` access) — now provided.
  """

  @default_dirty "reactive_dag_dirty"

  @type key :: String.t()

  @doc """
  Mark `keys` of `cell` dirty, coalesced (idempotent per `(cell, key)`).

  `keys` is a list of key strings, or of `{key, prior}` pairs where `prior` is
  the row AS IT WAS when marked — a map the parent can derive its claim from
  without reading the live row.

  That snapshot is what makes a claim survive its subject. A deleted row cannot
  say which unit it belonged to, and a row that MOVED between units cannot say
  where it came from; the snapshot answers both, so a claim stays precise where
  it would otherwise degrade to a whole-cell recompute.

  Coalescing keeps the FIRST snapshot (`ON CONFLICT DO NOTHING`), which is
  deliberate: if a row is written twice before a drain, the oldest prior state
  is the one that names the unit it started in.
  """
  @spec mark_dirty(String.t(), [key() | {key(), map() | nil}], String.t() | nil) :: :ok
  def mark_dirty(_cell, [], _reason), do: :ok

  def mark_dirty(cell, keys, reason) do
    entries = keys |> Enum.map(&normalize_entry/1) |> Enum.uniq_by(&elem(&1, 0))
    now = DateTime.utc_now()

    placeholders =
      entries
      |> Enum.with_index()
      |> Enum.map_join(", ", fn {_e, i} ->
        b = i * 5
        "($#{b + 1}, $#{b + 2}, $#{b + 3}, $#{b + 4}, $#{b + 5})"
      end)

    params = Enum.flat_map(entries, fn {k, prior} -> [cell, k, reason, now, prior] end)

    query!(
      "INSERT INTO #{dirty()} (cell_id, key, reason, enqueued_at, prior) " <>
        "VALUES #{placeholders} ON CONFLICT (cell_id, key) DO NOTHING",
      params
    )

    :ok
  end

  defp normalize_entry({key, prior}) when is_map(prior) or is_nil(prior), do: {key, prior}
  defp normalize_entry(key), do: {key, nil}

  @doc """
  Every cell with dirty keys waiting — what the next drain would work on.

  A READ: unlike `claim/1` it consumes nothing, so it is safe to call for
  reporting (`ReactiveDag.Insights.pending/1`) while a drain is running.
  """
  @spec dirty_cells() :: [String.t()]
  def dirty_cells do
    %{rows: rows} = query!("SELECT DISTINCT cell_id FROM #{dirty()}", [])
    Enum.map(rows, fn [id] -> id end)
  end

  @doc "The dirty cell with the smallest depth, or nil if the frontier is empty."
  @spec next_cell(%{String.t() => non_neg_integer()}) :: String.t() | nil
  def next_cell(depths) do
    case dirty_cells() do
      [] -> nil
      ids -> Enum.min_by(ids, &Map.get(depths, &1, 1_000_000))
    end
  end

  @doc "Atomically claim (delete-returning) all dirty keys for `cell`."
  @spec claim(String.t()) :: [key()]
  def claim(cell), do: claim_with_priors(cell) |> Enum.map(&elem(&1, 0))

  @doc """
  `claim/1`, but returning `{key, prior}` pairs — the snapshot each key was
  marked with (`nil` for a source-fed key, which has no row behind it).

  The drain uses this so a parent can derive its claim from what the row WAS,
  which is the only thing that survives a delete.
  """
  @spec claim_with_priors(String.t()) :: [{key(), map() | nil}]
  def claim_with_priors(cell) do
    %{rows: rows} =
      query!("DELETE FROM #{dirty()} WHERE cell_id = $1 RETURNING key, prior", [cell])

    Enum.map(rows, fn [k, prior] -> {k, prior} end)
  end

  @doc "True when nothing is dirty."
  @spec empty?() :: boolean()
  def empty? do
    %{rows: [[n]]} = query!("SELECT COUNT(*) FROM #{dirty()}", [])
    n == 0
  end

  @doc """
  Run `fun` holding a cluster-wide lock on this graph's frontier, or skip.

  The per-cell claim is atomic, so two concurrent drains never process a key
  twice — but they CAN pick the same cell and split its keys, recomputing it
  twice for a disjoint slice each. `ReactiveDag.Drain` has always said "run one
  drain at a time per graph"; this is how a host actually gets that when it runs
  more than one node.

  A Postgres advisory lock, because the requirement is exactly what they are
  for: cluster-wide, held on a connection, and released automatically if that
  connection dies — a node crashing mid-drain does not leave the graph locked,
  which a lock table would.

  Returns `{:ok, result}`, or `:busy` when another node holds it. **Busy is not
  an error**: the other drain is doing this drain's work, and the frontier is a
  set rather than a queue — anything this one would have claimed is still there
  for whoever holds the lock. A caller that treats `:busy` as a failure will
  retry work that is already happening.

      case Frontier.with_lock(fn -> Drain.run(plan, opts) end) do
        {:ok, {:ok, report}} -> report
        :busy -> :already_draining
      end
  """
  @spec with_lock((-> result), keyword()) :: {:ok, result} | :busy when result: term()
  def with_lock(fun, opts \\ []) when is_function(fun, 0) do
    key = :erlang.phash2(Keyword.get(opts, :scope, dirty()))

    case query!("SELECT pg_try_advisory_lock($1)", [key]) do
      %{rows: [[true]]} ->
        try do
          {:ok, fun.()}
        after
          query!("SELECT pg_advisory_unlock($1)", [key])
        end

      _ ->
        :busy
    end
  end

  defp query!(sql, params), do: repo().query!(sql, params)

  defp repo do
    Application.get_env(:reactive_dag, :repo) ||
      raise "reactive_dag: set `config :reactive_dag, repo: MyApp.Repo`"
  end

  defp dirty do
    name = Application.get_env(:reactive_dag, :dirty_table, @default_dirty)

    if name =~ ~r/\A[a-zA-Z_][a-zA-Z0-9_]*\z/ do
      name
    else
      raise ArgumentError,
            "reactive_dag: dirty_table #{inspect(name)} is not a valid table identifier"
    end
  end
end

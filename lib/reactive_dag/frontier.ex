defmodule ReactiveDag.Frontier do
  @moduledoc """
  The dirty frontier, owned by the library and backed by the `reactive_dag_dirty`
  table (created by `ReactiveDag.Migration`). The host is an Ash/AshPostgres app,
  so we go through its repo with parameterized SQL — claim-as-delete is a raw
  `DELETE … RETURNING` that Ash actions don't express cleanly.

  The host supplies its `repo` (its AshPostgres repo module) via config, and may
  override the table name (default `reactive_dag_dirty`) so a host adopting the
  library keeps its existing table without a rename:

      config :reactive_dag, repo: MyApp.Repo, dirty_table: "my_dirty"

  Coalesced by `(cell, key)`; depth-ordered `next_cell`; atomic `claim`. This is
  the shared substrate both hosts previously hand-rolled (cascade's
  `Cascade.Engine.Frontier`, the portal's `model_dirty` access) — now provided.
  """

  @default_dirty "reactive_dag_dirty"

  @type key :: String.t()

  @doc "Mark `keys` of `cell` dirty, coalesced (idempotent per (cell,key))."
  @spec mark_dirty(String.t(), [key()], String.t() | nil) :: :ok
  def mark_dirty(_cell, [], _reason), do: :ok

  def mark_dirty(cell, keys, reason) do
    keys = Enum.uniq(keys)
    now = DateTime.utc_now()

    placeholders =
      keys
      |> Enum.with_index()
      |> Enum.map_join(", ", fn {_k, i} ->
        b = i * 4
        "($#{b + 1}, $#{b + 2}, $#{b + 3}, $#{b + 4})"
      end)

    params = Enum.flat_map(keys, fn k -> [cell, k, reason, now] end)

    query!(
      "INSERT INTO #{dirty()} (cell_id, key, reason, enqueued_at) VALUES #{placeholders} " <>
        "ON CONFLICT (cell_id, key) DO NOTHING",
      params
    )

    :ok
  end

  @doc "The dirty cell with the smallest depth, or nil if the frontier is empty."
  @spec next_cell(%{String.t() => non_neg_integer()}) :: String.t() | nil
  def next_cell(depths) do
    %{rows: rows} = query!("SELECT DISTINCT cell_id FROM #{dirty()}", [])
    ids = Enum.map(rows, fn [id] -> id end)

    case ids do
      [] -> nil
      _ -> Enum.min_by(ids, &Map.get(depths, &1, 1_000_000))
    end
  end

  @doc "Atomically claim (delete-returning) all dirty keys for `cell`."
  @spec claim(String.t()) :: [key()]
  def claim(cell) do
    %{rows: rows} = query!("DELETE FROM #{dirty()} WHERE cell_id = $1 RETURNING key", [cell])
    Enum.map(rows, fn [k] -> k end)
  end

  @doc "True when nothing is dirty."
  @spec empty?() :: boolean()
  def empty? do
    %{rows: [[n]]} = query!("SELECT COUNT(*) FROM #{dirty()}", [])
    n == 0
  end

  defp query!(sql, params), do: repo().query!(sql, params)

  defp repo do
    Application.get_env(:reactive_dag, :repo) ||
      raise "reactive_dag: set `config :reactive_dag, repo: MyApp.Repo`"
  end

  defp dirty, do: Application.get_env(:reactive_dag, :dirty_table, @default_dirty)
end

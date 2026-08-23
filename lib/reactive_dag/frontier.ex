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

  `keys` is a list of key strings, or of `{key, diff}` pairs where `diff` is the
  change's two sides — `%{attr => %{"from" => old, "to" => new}}` — which a
  consumer derives its claim from without reading the live row.

  The diff is what makes a claim survive its subject. A deleted row cannot say
  which unit it belonged to; a row that MOVED between units says only where it
  went. The diff answers both, so a claim stays precise where it would otherwise
  degrade to a whole-cell recompute.

  Coalescing MERGES the diffs: the earliest `from` and the latest `to`, per
  attribute. If a row moves meals → travel → lodging before a drain, the claim
  must name `meals` (where the last settled state had it) and `lodging` (where it
  is now) — never `travel`, an intermediate no settled state ever saw.

  `DO NOTHING` would keep `meals → travel` and strand lodging; overwriting would
  keep `travel → lodging` and strand meals. Both lose a unit that needs
  repricing, which is why this merges rather than picks.

  The merge is done in SQL, in the `ON CONFLICT` clause, because marks arrive
  from arbitrary concurrent writes. Reading the stored diff into Elixir to merge
  it there would be a read-modify-write with no lock around it — and the marking
  path deliberately holds none, since it runs inside a host's own write
  transaction.
  """
  @spec mark_dirty(String.t(), [key() | {key(), map() | nil}], String.t() | nil, keyword()) :: :ok
  def mark_dirty(cell, keys, reason, opts \\ [])

  def mark_dirty(_cell, [], _reason, _opts), do: :ok

  def mark_dirty(cell, keys, reason, opts) do
    entries = keys |> Enum.map(&normalize_entry/1) |> Enum.uniq_by(&elem(&1, 0))
    now = DateTime.utc_now()
    tenant = tenant(opts)

    placeholders =
      entries
      |> Enum.with_index()
      |> Enum.map_join(", ", fn {_e, i} ->
        b = i * 7
        "($#{b + 1}, $#{b + 2}, $#{b + 3}, $#{b + 4}, $#{b + 5}, $#{b + 6}, $#{b + 7})"
      end)

    # `awaiting_approval` is per CALL, not per key: a gated cell holds every
    # change of a given write, and a caller marking a mixed batch would be
    # marking two different things.
    held = if Keyword.get(opts, :awaiting_approval, false), do: true, else: nil

    params =
      Enum.flat_map(entries, fn {k, prior} ->
        [cell, tenant, k, reason, now, prior, held]
      end)

    query!(
      "INSERT INTO #{dirty()} " <>
        "(cell_id, tenant, key, reason, enqueued_at, prior, awaiting_approval) " <>
        "VALUES #{placeholders} " <> on_conflict_merge(),
      params
    )

    :ok
  end

  @doc """
  The tenant a call is about: `opts[:tenant]`, else `"*"` (untenanted).

  `"*"` rather than `nil` because the coalescing unique index backs
  `mark_dirty`'s ON CONFLICT and Postgres treats NULLs as DISTINCT in a unique
  index — a nullable column would stop untenanted marks coalescing and grow a
  queue row per mark. `"*"` is also already this library's spelling for "the
  whole thing" in claim sets, so the vocabulary is not new.

  Normalised in ONE place so a caller passing `nil`, omitting the option, or
  passing an atom all mean the same row.
  """
  @spec tenant(keyword()) :: String.t()
  def tenant(opts) do
    case Keyword.get(opts, :tenant) do
      nil -> "*"
      t when is_binary(t) -> t
      t -> to_string(t)
    end
  end

  defp normalize_entry({key, prior}) when is_map(prior) or is_nil(prior), do: {key, prior}
  defp normalize_entry(key), do: {key, nil}

  @doc """
  Merge two diffs for the same key: the earliest prior side, the latest `to`.

  The rule the `ON CONFLICT` clause implements, in Elixir — so a caller modelling
  the frontier (a fake repo in a test, a host inspecting what a burst of writes
  would collapse to) has ONE definition to agree with rather than reimplementing
  it and drifting.

      iex> merge_diffs(%{"c" => %{"from" => "meals", "to" => "travel"}},
      ...>             %{"c" => %{"from" => "travel", "to" => "lodging"}})
      %{"c" => %{"from" => "meals", "to" => "lodging"}}
  """
  @spec merge_diffs(map() | nil, map() | nil) :: map() | nil
  def merge_diffs(nil, incoming), do: incoming
  def merge_diffs(stored, nil), do: stored

  def merge_diffs(stored, incoming) do
    for k <- Enum.uniq(Map.keys(stored) ++ Map.keys(incoming)), into: %{} do
      {k, merge_entry(Map.get(stored, k), Map.get(incoming, k))}
    end
  end

  defp merge_entry(%{"from" => from}, %{} = new), do: Map.put(new, "from", from)

  defp merge_entry(%{"unchanged" => was}, %{"to" => to}),
    do: %{"from" => was, "to" => to}

  # A stored CREATE (`to` only, no prior side) stays a create however many times
  # the row then moves: settled state never held it in any of the intermediate
  # units, so there is nothing there to reprice.
  defp merge_entry(%{"to" => _}, %{"to" => to}), do: %{"to" => to}

  defp merge_entry(stored, nil), do: stored
  defp merge_entry(_stored, new), do: new

  # Per attribute: the EARLIEST prior side (`from`, or `unchanged` — which is a
  # `from` that did not move) and the LATEST `to`. See `mark_dirty/4`'s docs for
  # why this merges rather than picking a side.
  #
  # `jsonb_object_keys` over the union of both diffs, so an attribute present in
  # only one of them survives.
  defp on_conflict_merge do
    d = dirty()

    """
    ON CONFLICT (tenant, cell_id, key) DO UPDATE SET
      -- A second change to a HELD key stays held: a reviewer approves the merged
      -- net effect, not a moving target. And a change arriving on an
      -- already-approved key does not re-hold it — the approval stands for the
      -- key, and re-holding would make a gate un-approve on its own.
      awaiting_approval = CASE
        WHEN #{dirty()}.awaiting_approval IS TRUE THEN TRUE
        ELSE #{dirty()}.awaiting_approval
      END,
      prior =
      CASE
        WHEN #{d}.prior IS NULL THEN EXCLUDED.prior
        WHEN EXCLUDED.prior IS NULL THEN #{d}.prior
        ELSE (
          SELECT COALESCE(jsonb_object_agg(k, merged), '{}'::jsonb)
            FROM (
              SELECT k,
                     CASE
                       WHEN jsonb_exists(#{d}.prior -> k, 'from')
                         THEN COALESCE(EXCLUDED.prior -> k, '{}'::jsonb)
                              || jsonb_build_object('from', #{d}.prior -> k -> 'from')
                       WHEN jsonb_exists(#{d}.prior -> k, 'unchanged')
                            AND jsonb_exists(EXCLUDED.prior -> k, 'to')
                         THEN jsonb_build_object(
                                'from', #{d}.prior -> k -> 'unchanged',
                                'to', EXCLUDED.prior -> k -> 'to')
                       -- a stored CREATE stays a create however many times the
                       -- row then moves: settled state never held it in any
                       -- intermediate unit, so there is nothing there to reprice
                       WHEN jsonb_exists(#{d}.prior -> k, 'to')
                            AND NOT jsonb_exists(#{d}.prior -> k, 'from')
                            AND jsonb_exists(EXCLUDED.prior -> k, 'to')
                         THEN jsonb_build_object('to', EXCLUDED.prior -> k -> 'to')
                       ELSE COALESCE(EXCLUDED.prior -> k, #{d}.prior -> k)
                     END AS merged
                FROM jsonb_object_keys(
                       COALESCE(#{d}.prior, '{}'::jsonb) ||
                       COALESCE(EXCLUDED.prior, '{}'::jsonb)) AS k
            ) m
        )
      END
    """
  end

  @doc """
  Approve a gated cell's held changes, so the next drain claims them.

  `keys` names which — or `:all` for every held change of that cell. Returns the
  keys it released, so a caller can report what a click actually did rather than
  assuming.

  Approving something already claimable is a no-op rather than an error: a double
  click, or two reviewers, must not fail.
  """
  @spec approve(String.t(), [key()] | :all, keyword()) :: [key()]
  def approve(cell, keys \\ :all, opts \\ [])

  def approve(cell, :all, opts) do
    %{rows: rows} =
      query!(
        "UPDATE #{dirty()} SET awaiting_approval = NULL " <>
          "WHERE cell_id = $1 AND tenant = $2 AND awaiting_approval IS TRUE RETURNING key",
        [cell, tenant(opts)]
      )

    Enum.map(rows, fn [k] -> k end)
  end

  def approve(_cell, [], _opts), do: []

  def approve(cell, keys, opts) when is_list(keys) do
    %{rows: rows} =
      query!(
        "UPDATE #{dirty()} SET awaiting_approval = NULL " <>
          "WHERE cell_id = $1 AND tenant = $2 AND awaiting_approval IS TRUE " <>
          "AND key = ANY($3) RETURNING key",
        [cell, tenant(opts), keys]
      )

    Enum.map(rows, fn [k] -> k end)
  end

  @doc """
  Reject a gated cell's held changes — DISCARD the marks without recomputing.

  The rows are already written; this says the graph should not propagate from
  them. So a rejected change leaves the derived table as it stands and the
  consumers as they were, which is the honest meaning of "no" given the gate
  holds propagation rather than the write.

  Returns the keys it discarded.
  """
  @spec reject(String.t(), [key()] | :all, keyword()) :: [key()]
  def reject(cell, keys \\ :all, opts \\ [])

  def reject(cell, :all, opts) do
    %{rows: rows} =
      query!(
        "DELETE FROM #{dirty()} WHERE cell_id = $1 AND tenant = $2 " <>
          "AND awaiting_approval IS TRUE RETURNING key",
        [cell, tenant(opts)]
      )

    Enum.map(rows, fn [k] -> k end)
  end

  def reject(_cell, [], _opts), do: []

  def reject(cell, keys, opts) when is_list(keys) do
    %{rows: rows} =
      query!(
        "DELETE FROM #{dirty()} WHERE cell_id = $1 AND tenant = $2 " <>
          "AND awaiting_approval IS TRUE AND key = ANY($3) RETURNING key",
        [cell, tenant(opts), keys]
      )

    Enum.map(rows, fn [k] -> k end)
  end

  @doc """
  The changes a gated cell is holding — `{key, diff}` pairs, for review.

  A READ: it consumes nothing, so a UI can poll it while a drain runs.
  """
  @spec awaiting(String.t(), keyword()) :: [{key(), map() | nil}]
  def awaiting(cell, opts \\ []) do
    %{rows: rows} =
      query!(
        "SELECT key, prior FROM #{dirty()} WHERE cell_id = $1 AND tenant = $2 " <>
          "AND awaiting_approval IS TRUE ORDER BY enqueued_at",
        [cell, tenant(opts)]
      )

    Enum.map(rows, fn [k, d] -> {k, d} end)
  end

  @doc false
  @deprecated "Renamed to claim_with_diffs/2 — a mark carries both sides now, not just the prior"
  def claim_with_priors(cell, opts \\ []), do: claim_with_diffs(cell, opts)

  @doc """
  Every cell with dirty keys waiting — what the next drain would work on.

  A READ: unlike `claim/1` it consumes nothing, so it is safe to call for
  reporting (`ReactiveDag.Insights.pending/1`) while a drain is running.
  """
  @spec dirty_cells(keyword()) :: [String.t()]
  def dirty_cells(opts \\ []) do
    %{rows: rows} =
      query!(
        "SELECT DISTINCT cell_id FROM #{dirty()} " <>
          "WHERE tenant = $1 AND awaiting_approval IS NOT TRUE",
        [tenant(opts)]
      )

    Enum.map(rows, fn [id] -> id end)
  end

  @doc """
  The dirty cell with the smallest depth **of the cells in `depths`**, or nil.

  `except` skips cells a caller has already tried this run. The drain uses it
  for a cell whose recompute FAILED: the failure rolled back, so its keys are
  still dirty and `next_cell` would hand back the same cell forever. Excluding
  it lets the rest of the cascade drain and leaves the retry to the next run.

  ## Tenant, and the cell this plan does not know

  Reads only this tenant's rows (`tenant:`, default `"*"`). One frontier serves
  every plan in the application, and that is what makes the tenant load-bearing
  rather than bookkeeping: without it, "a cell this plan does not know" and "a
  cell nobody owns" are the same observation, and they need OPPOSITE handling.

    * **Foreign** — a row belonging to another tenant. Never returned, never
      touched. It has an owner; the frontier is a set, so it is still there for
      the drain that can recompute it.
    * **Orphaned** — a row in THIS tenant whose `cell_id` the plan does not
      declare: a renamed or removed cell, a source writing an old leaf id. Still
      returned, so the drain can claim it, log it and drop it (`ReactiveDag.Drain`
      does exactly that). Claiming rather than skipping is what stops the row
      being re-selected on every pass forever.

  So the tenant filter is in SQL and the plan filter is not: an unknown cell of
  ours is work to clear, and another tenant's cell is not ours to look at.
  """
  @spec next_cell(%{String.t() => non_neg_integer()}, [String.t()], keyword()) ::
          String.t() | nil
  def next_cell(depths, except \\ [], opts \\ []) do
    case Enum.reject(dirty_cells(opts), &(&1 in except)) do
      [] -> nil
      ids -> Enum.min_by(ids, &Map.get(depths, &1, 1_000_000))
    end
  end

  @doc "Atomically claim (delete-returning) all dirty keys for `cell`."
  @spec claim(String.t(), keyword()) :: [key()]
  def claim(cell, opts \\ []), do: claim_with_diffs(cell, opts) |> Enum.map(&elem(&1, 0))

  @doc """
  `claim/1`, but returning `{key, diff}` pairs — the DIFF each key was marked
  with (`nil` for a source-fed key, which has no Ash row behind it).

  A diff is `%{attr => %{"from" => old, "to" => new}}`: both sides, so a
  consumer can name the unit a row LEFT as well as the one it landed in. Neither
  survives a live read — the row is gone, or already says only where it went.
  """
  @spec claim_with_diffs(String.t(), keyword()) :: [{key(), map() | nil}]
  def claim_with_diffs(cell, opts \\ []) do
    # Scoped to ONE tenant: an unscoped claim would consume every tenant's keys
    # for this cell and recompute them under this tenant's plan. That is the
    # failure the tenant column exists to prevent, and it is silent — the other
    # tenants' work is simply gone, and their next drain finds nothing to do.
    %{rows: rows} =
      query!(
        # `awaiting_approval` is NULL for an ordinary mark and TRUE for a gated
        # change nobody has approved. `IS NOT TRUE` covers both, and covers a row
        # written before this column existed.
        #
        # The unapproved rows stay in the table: they are not lost work, they are
        # work waiting on a person. A gated cell with nothing approved simply is
        # not selected — see `dirty_cells/1`, which filters the same way, or the
        # drain would pick a cell it cannot claim from and loop.
        "DELETE FROM #{dirty()} WHERE cell_id = $1 AND tenant = $2 " <>
          "AND awaiting_approval IS NOT TRUE RETURNING key, prior",
        [cell, tenant(opts)]
      )

    Enum.map(rows, fn [k, prior] -> {k, prior} end)
  end

  @doc "True when nothing is dirty."
  @spec empty?(keyword()) :: boolean()
  def empty?(opts \\ []) do
    %{rows: [[n]]} =
      query!(
        # CLAIMABLE work, not all work. A gated change awaiting approval leaves
        # this true: the drain has nothing to do, which is exactly the state.
        # Counting it would make `empty?/1` false forever and any caller looping
        # on it never finish.
        "SELECT COUNT(*) FROM #{dirty()} " <>
          "WHERE tenant = $1 AND awaiting_approval IS NOT TRUE",
        [tenant(opts)]
      )

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
    # `Keyword.get/3`'s default only applies when the key is ABSENT, so an
    # explicit `scope: nil` would hash nil and silently move the lock for every
    # caller that passes the option through. nil means "no scope" here.
    key = :erlang.phash2(Keyword.get(opts, :scope) || dirty())

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

  @doc """
  Run `fun` in a transaction, so a claim it makes is undone if it raises.

  This is what makes a claim survive a failed recompute. `claim/1` is a
  `DELETE … RETURNING`: the keys are consumed before the work happens, so
  without this a recompute that raises — a deadlock, a timeout, an upstream 503
  — leaves those keys gone from the frontier and silently stale. Rolling back
  puts nothing back; it means they were never taken.

  `timeout: :infinity`, because the work inside is a recompute and a recompute
  is the host's: an op that reads a PDF with a model legitimately runs for
  minutes, and Ecto's 15s default would abort it. The connection is held for
  the duration, which is affordable only because drains are SERIALIZED — one
  at a time per graph (`with_lock/2`) — so this is one connection, not one per
  concurrent drain.

  Readers are unaffected: the `DELETE` takes row locks on one cell's keys, and
  Postgres readers never block on row locks. A `mark_dirty` on a DIFFERENT cell
  proceeds; only marking the same key of the same cell waits for the commit.

  ## A repo without `transaction/2`

  Runs `fun` directly. A host may configure a minimal repo — the library only
  ever needed `query!/2` — and requiring a new capability would break it on
  upgrade for a guarantee it may not want. Such a host keeps the old behaviour:
  a failed recompute loses its claim.
  """
  @spec transaction((-> result)) :: result when result: term()
  def transaction(fun) when is_function(fun, 0) do
    repo = repo()

    if function_exported?(repo, :transaction, 2) do
      case repo.transaction(fun, timeout: :infinity) do
        {:ok, result} -> result
        {:error, reason} -> raise "reactive_dag: transaction rolled back: #{inspect(reason)}"
      end
    else
      fun.()
    end
  end

  @doc """
  Run `fun` in a SAVEPOINT: if it returns `{:error, reason}`, everything it did
  is undone and `{:error, reason}` comes back — without disturbing the
  transaction around it.

  This is what lets one fallible unit fail inside a drain that is otherwise
  committing. A poll that could not reach its upstream rolls back its own claim
  and any rows it managed to write, and the drain carries on with every other
  cell — which is the containment `Source.poll_all/2` gives a sweep, expressed
  where the work actually happens.

  ## It must RETURN its failure, not raise

  An exception inside a nested transaction aborts the OUTER one: Postgres marks
  the connection failed and every later statement errors until the whole thing
  rolls back. A savepoint only isolates a failure that arrives as a value.

  That is why `ReactiveDag.Source`'s `poll/1` returns `{:error, reason}`
  "contained, not raised" — the contract was already the right shape for this.
  A scanner that raises anyway is a scanner that takes the drain down with it,
  and the `rescue` in `safe_poll/2` is what stops that.

  Returns `fun`'s value unchanged when it does not error, and `{:error, reason}`
  when it does. A repo without `transaction/2` runs `fun` directly: no
  savepoint, so a failure is not isolated — the same degradation
  `transaction/1` makes.
  """
  @spec savepoint((-> result)) :: result | {:error, term()} when result: term()
  def savepoint(fun) when is_function(fun, 0) do
    repo = repo()

    if function_exported?(repo, :transaction, 2) do
      case repo.transaction(
             fn ->
               case fun.() do
                 {:error, reason} -> repo.rollback(reason)
                 other -> other
               end
             end,
             timeout: :infinity
           ) do
        {:ok, result} -> result
        {:error, reason} -> {:error, reason}
      end
    else
      fun.()
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

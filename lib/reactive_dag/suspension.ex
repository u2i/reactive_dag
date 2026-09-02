defmodule ReactiveDag.Suspension do
  @moduledoc """
  WHERE A CASCADE STOPPED — the library's only table, and the successor to the
  dirty queue.

  A cascade runs to completion in one transaction, following the graph from
  whatever changed until it reaches something that cannot be done
  synchronously. Two things stop it: work too expensive to hold a transaction
  open, and work that needs a person. This table records those stopping points
  and nothing else.

  The distinction from the queue it replaces is the whole design. A queue row
  said *this cell needs recomputing* — a conclusion, derived by walking the
  graph at mark time, which is stale by the time it is read. A suspension says
  *this change reached here and could not continue* — a fact, which stays true.
  Everything a resumption needs to know it derives when it runs.

  ## The shape

      suspension
        id          019a41…9f4          this row, never modified
        tenant      "red_hook_village"  whose graph
        waiting     "meeting_events"    the RESOURCE that stopped
        resource    "minutes_docs"      what changed
        row_uuid    019a3f…c21            which row
        version_id  v-019a40…7b           and what moved
        reason      :expensive | :approval

  `waiting` and `resource` are both resource names, and neither implies the
  other: one changed row may stop several resources, and they complete
  independently.

  ## Immutable, and why that is the point

  Rows are inserted and deleted, never updated. There is no unique constraint
  and no `ON CONFLICT`: a second change to the same stopping point writes a
  SECOND row.

  What coalesces is the WORK, not the rows. A job reads every suspension at its
  point, does the expensive thing once, and discharges exactly the ids it read
  — so a suspension written during a nine-minute recompute is not in that list
  and survives for the next pass. An append-only table cannot lose a change
  that arrives mid-flight, and needs no revision counter that every future
  write path must remember to carry.

  The cost is rows: a document changing fifty times before its resumption runs
  leaves fifty suspensions, all discharged together. That is a retention
  question, not a correctness one, and it is the trade taken deliberately —
  duplicate rows are cheap, a lost cascade is not.

  A single mutable row per point, deleted only if unchanged since the job read
  it, would keep the count flat. It also makes correctness depend on every
  write path carrying a revision, and one that forgets loses a change with no
  error. Worth revisiting if volume becomes a real cost; not worth the
  invariant before then.

  ## Configuration

      config :reactive_dag, repo: MyApp.Repo, suspension_table: "my_suspensions"

  Values are always parameterized. The table name — the one identifier SQL
  cannot parameterize — comes from config and is validated against an
  identifier grammar at read time, so a typo fails loudly rather than as a
  syntax error deep in a query.
  """

  @default_table "reactive_dag_suspension"

  @typedoc """
  A stopping point: which tenant's graph, what stopped, and which row of what
  moved. This is what a resumption job carries — never a suspension id, never a
  version — so that a job queued at 12:00 and run at 12:05 acts on what is true
  at 12:05.
  """
  @type point :: %{
          tenant: String.t(),
          waiting: String.t(),
          resource: String.t(),
          row_uuid: String.t()
        }

  @type reason :: :expensive | :approval

  @type t :: %{
          id: String.t(),
          version_id: String.t(),
          reason: reason(),
          lap: non_neg_integer()
        }

  @doc """
  Record that a cascade stopped, and return the new row's id.

  Called from INSIDE the cascade's transaction, so a rolled-back cascade leaves
  no suspension: the change it describes never happened.

  `version_id` may be `"*"` when the change could not be attributed — a source
  that cannot say which of its items moved, or a write path that records no
  diff. Resumption then recomputes the whole cell: expensive, correct, and
  honest. This gives the whole-cell marker a principled meaning it has never
  had — not "something happened somewhere", but "this suspension could not be
  narrowed".

  `lap` is how many consecutive resumption-driven trips around a declared
  `feedback` loop led to this suspension — 0 (the default, and the value for
  every suspension outside a loop) means the work arrived here from an external
  change. The column exists because a loop through a suspending cell ends every
  CASCADE cleanly: no in-memory counter survives the suspend → commit → resume
  chain, so the count must ride on the one thing that does — this row.
  `ReactiveDag.Cascade` refuses to record a lap past its budget, which is what
  bounds the chain.
  """
  @spec record(point(), String.t(), reason(), non_neg_integer()) :: String.t()
  def record(%{} = point, version_id, reason, lap \\ 0)
      when is_binary(version_id) and reason in [:expensive, :approval] and is_integer(lap) and
             lap >= 0 do
    id = uuid_v7()

    query!(
      """
      INSERT INTO #{table()}
        (id, tenant, waiting, resource, row_uuid, version_id, reason, lap, inserted_at)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now())
      """,
      [
        id,
        point.tenant,
        point.waiting,
        point.resource,
        point.row_uuid,
        version_id,
        Atom.to_string(reason),
        lap
      ]
    )

    id
  end

  @doc """
  Every suspension at a stopping point, oldest first.

  A resumption job's first act. An empty list is the ORDINARY outcome of a
  duplicate job — the work was already done and discharged — not an error.

  Ordered by `id`, which is a UUIDv7 and therefore sorts by creation. That is
  not decorative: when several versions merge into one recompute, they must be
  applied in the order the changes happened.
  """
  @spec at(point()) :: [t()]
  def at(%{} = point) do
    %{rows: rows} =
      query!(
        """
        SELECT id, version_id, reason, lap FROM #{table()}
        WHERE tenant = $1 AND waiting = $2 AND resource = $3 AND row_uuid = $4
        ORDER BY id
        """,
        [point.tenant, point.waiting, point.resource, point.row_uuid]
      )

    Enum.map(rows, fn [id, version_id, reason, lap] ->
      %{id: id, version_id: version_id, reason: String.to_existing_atom(reason), lap: lap || 0}
    end)
  end

  @doc """
  Discharge suspensions BY ID, returning how many were removed.

  By id, never by point — this is the line the append-only design rests on. A
  `DELETE … WHERE tenant = … AND waiting = …` would also remove suspensions
  written while the job was running, silently discarding changes nobody
  observed. Naming the ids the job actually read cannot do that.

  Called in the same transaction as the resumption's writes, so a failed
  resumption leaves its suspensions in place and the work is retried.
  """
  @spec discharge([String.t()]) :: non_neg_integer()
  def discharge([]), do: 0

  def discharge(ids) when is_list(ids) do
    %{num_rows: n} = query!("DELETE FROM #{table()} WHERE id = ANY($1)", [ids])
    n
  end

  @doc """
  The distinct stopping points with work outstanding, for one tenant.

  Non-consuming, and aggregated: one entry per point per reason, with how many
  suspensions have accumulated there and how long the oldest has waited. This
  is what an operator's view reads — "twelve things waiting on a person" — and
  what surfaces a point whose resumption keeps failing, since its count climbs
  while its oldest recedes.
  """
  @spec points(keyword()) :: [
          %{point: point(), reason: reason(), count: pos_integer(), oldest: DateTime.t()}
        ]
  def points(opts \\ []) do
    %{rows: rows} =
      query!(
        """
        SELECT tenant, waiting, resource, row_uuid, reason, COUNT(*), MIN(inserted_at)
        FROM #{table()}
        WHERE tenant = $1
        GROUP BY tenant, waiting, resource, row_uuid, reason
        ORDER BY MIN(inserted_at)
        """,
        [tenant(opts)]
      )

    Enum.map(rows, fn [tenant, waiting, resource, row_uuid, reason, count, oldest] ->
      %{
        point: %{tenant: tenant, waiting: waiting, resource: resource, row_uuid: row_uuid},
        reason: String.to_existing_atom(reason),
        count: count,
        oldest: as_utc(oldest)
      }
    end)
  end

  # `inserted_at` is `:utc_datetime_usec`, which Ecto maps to a NAIVE column
  # holding UTC — so Postgres hands back a `NaiveDateTime` and a caller doing
  # date arithmetic against `DateTime.utc_now/0` would get a
  # `FunctionClauseError` for its trouble. Converted once, here, rather than
  # leaving every caller to discover the shape.
  defp as_utc(%NaiveDateTime{} = naive), do: DateTime.from_naive!(naive, "Etc/UTC")
  defp as_utc(%DateTime{} = dt), do: dt
  defp as_utc(other), do: other

  @doc """
  Whether anything is suspended for this tenant.
  """
  @spec pending?(keyword()) :: boolean()
  def pending?(opts \\ []) do
    %{rows: [[n]]} =
      query!("SELECT COUNT(*) FROM #{table()} WHERE tenant = $1", [tenant(opts)])

    n > 0
  end

  @doc """
  The tenant a call is about: `opts[:tenant]`, else `"*"` (untenanted).

  `"*"` rather than `nil` because a null never equals itself. Every read here
  matches on the tenant, and `tenant = NULL` matches nothing — an untenanted
  suspension would be written and then never found, which is the quiet failure
  of tenancy in its worst form: a resumption that finds no work reports
  SUCCESS. `"*"` is also already this library's spelling for "the whole thing",
  so the vocabulary is not new.

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

  @doc """
  Run `fun` in a transaction — the cascade's transaction.

  Bounded, unlike the drain's it replaces. The drain wrapped its recompute in
  `timeout: :infinity` and justified it with "a recompute legitimately runs for
  minutes" — which is exactly the condition that made a nine-minute extraction
  hold a connection until the database killed it.

  A cascade transaction contains only fast work by construction: anything slow
  declared itself so and became a suspension. An infinite timeout here would
  hide the one bug this design exists to remove — a cell declared cheap that
  is not. Configure with:

      config :reactive_dag, cascade_timeout: 30_000

  A repo without `transaction/2` runs `fun` directly. The library only ever
  needed `query!/2`, and requiring a new capability would break such a host on
  upgrade for a guarantee it may not want — at the cost that its cascades are
  not atomic.
  """
  @spec transaction((-> result)) :: result when result: term()
  def transaction(fun) when is_function(fun, 0) do
    repo = repo()

    if function_exported?(repo, :transaction, 2) do
      case repo.transaction(fun, timeout: timeout()) do
        {:ok, result} -> result
        {:error, reason} -> raise "reactive_dag: cascade rolled back: #{inspect(reason)}"
      end
    else
      fun.()
    end
  end

  @doc """
  Run `fun` in a SAVEPOINT: if it returns `{:error, reason}`, everything it did
  is undone and `{:error, reason}` comes back — without disturbing the
  transaction around it.

  This is what lets one fallible cell fail inside a cascade that is otherwise
  committing. The branch that failed stops; every other branch carries on and
  commits.

  ## It must RETURN its failure, not raise

  An exception inside a nested transaction aborts the OUTER one: Postgres marks
  the connection failed and every later statement errors until the whole thing
  rolls back. A savepoint only isolates a failure that arrives as a value.

  That is why `ReactiveDag.Source`'s `poll/1` returns `{:error, reason}`
  "contained, not raised" — the contract was already the right shape for this.

  A repo without `transaction/2` runs `fun` directly: no savepoint, so a
  failure is not isolated.
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
             timeout: timeout()
           ) do
        {:ok, result} -> result
        {:error, reason} -> {:error, reason}
      end
    else
      fun.()
    end
  end

  @doc """
  Stopping points whose resumption job can never run.

  A job that exhausts its attempts is normally `discarded`, which is visible.
  But Oban returns a failed job to `available` with a backoff BEFORE checking
  attempts, so a final attempt that fails leaves `attempt == max_attempts` in
  state `available` — and the fetch query requires `attempt < max_attempts`
  (`Oban.Engines.Basic`). The job is then unfetchable and undiscarded: it will
  never run and nothing reports it as failed.

  That would merely be a stalled point, except the resumption worker's
  uniqueness is `states: :incomplete`, which INCLUDES `available`. So the
  stranded job also dedups every future enqueue for its point. The suspension
  stays outstanding, no job will ever discharge it, and the queue looks healthy.

  Observed in production: a resumption whose third attempt hit
  `DBConnection.ConnectionError` while the machine was being replaced.

  Returns a point plus its `job_id` for each. Repair with `revive/1`.
  Note that `Oban.retry_job/1` does NOT fix these — it skips jobs already in
  `available` (`retry_all_jobs/2`), so it reports success and changes nothing.
  """
  @spec stranded(keyword()) :: [map()]
  def stranded(opts \\ []) do
    %{rows: rows} =
      query!(
        """
        SELECT j.id, j.args->>'tenant', j.args->>'waiting',
               j.args->>'resource', j.args->>'row_uuid'
        FROM #{oban_table()} j
        WHERE j.worker = $1
          AND j.state = 'available'
          AND j.attempt >= j.max_attempts
        ORDER BY j.id
        """,
        [Keyword.get(opts, :worker, "ReactiveDag.ResumptionWorker")]
      )

    Enum.map(rows, fn [id, tenant, waiting, resource, row_uuid] ->
      %{
        job_id: id,
        tenant: tenant,
        waiting: waiting,
        resource: resource,
        row_uuid: row_uuid
      }
    end)
  end

  @doc """
  Make stranded resumption jobs fetchable again, returning the job ids revived.

  Raises `max_attempts` above `attempt` — the same expression Oban's own
  `retry_all_jobs/2` uses — and clears the backoff so the queue picks the job
  up on its next poll. Idempotent: a job already fetchable does not match.

  Safe to run blind. A resumption whose suspensions were discharged in the
  meantime finds nothing at step 1 and exits, which is its ordinary duplicate
  path.
  """
  @spec revive(keyword()) :: [integer()]
  def revive(opts \\ []) do
    %{rows: rows} =
      query!(
        """
        UPDATE #{oban_table()}
        SET max_attempts = GREATEST(max_attempts, attempt + 1),
            scheduled_at = now()
        WHERE worker = $1
          AND state = 'available'
          AND attempt >= max_attempts
        RETURNING id
        """,
        [Keyword.get(opts, :worker, "ReactiveDag.ResumptionWorker")]
      )

    List.flatten(rows)
  end

  @doc """
  Oban's job table, validated. Configurable only so the SQL above can be
  exercised against a real database without colliding with a host's own table.
  """
  @spec oban_table() :: String.t()
  def oban_table do
    name = Application.get_env(:reactive_dag, :oban_table, "public.oban_jobs")

    if is_binary(name) and name =~ ~r/\A[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)?\z/ do
      name
    else
      raise ArgumentError,
            "reactive_dag: oban_table #{inspect(name)} is not a valid table identifier"
    end
  end

  @doc """
  The suspension table's name, validated.
  """
  @spec table() :: String.t()
  def table do
    name = Application.get_env(:reactive_dag, :suspension_table, @default_table)

    if is_binary(name) and name =~ ~r/\A[a-zA-Z_][a-zA-Z0-9_]*\z/ do
      name
    else
      raise ArgumentError,
            "reactive_dag: suspension_table #{inspect(name)} is not a valid table identifier"
    end
  end

  # UUIDv7: time-ordered, so `ORDER BY id` is `ORDER BY when it happened` and
  # the version column needs no companion timestamp to sort by.
  defp uuid_v7 do
    ms = System.system_time(:millisecond)
    <<rand_a::12, rand_b::62, _::bitstring>> = :crypto.strong_rand_bytes(10)

    <<ms::48, 7::4, rand_a::12, 2::2, rand_b::62>>
    |> then(&Base.encode16(&1, case: :lower))
    |> then(fn <<a::binary-8, b::binary-4, c::binary-4, d::binary-4, e::binary-12>> ->
      "#{a}-#{b}-#{c}-#{d}-#{e}"
    end)
  end

  defp timeout, do: Application.get_env(:reactive_dag, :cascade_timeout, 30_000)

  defp query!(sql, params), do: repo().query!(sql, params)

  defp repo do
    Application.get_env(:reactive_dag, :repo) ||
      raise "reactive_dag: set `config :reactive_dag, repo: MyApp.Repo`"
  end
end

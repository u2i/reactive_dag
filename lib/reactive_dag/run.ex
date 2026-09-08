defmodule ReactiveDag.Run do
  @moduledoc """
  What the engine is doing, and what it did — one row per JOB.

  `ReactiveDag.Insights` answers the same question from an ETS buffer, and
  answers it two ways this cannot: it holds nothing across a restart, and it
  records only on COMPLETION. A buffer written at the end can say what happened;
  it can never say what is queued, what is running now, or what stopped waiting
  for a person. Those are the same question at three moments, and this table is
  written at all three.

  ## A row is a job, and it appears when the job is created

      queued -> running -> done | failed | suspended | blocked

  `enqueued_at` is the only timestamp that is always set, because a row exists
  BECAUSE a job was created. Everything else fills in as the job progresses, so
  the row is a live status line early and a history entry later, without ever
  being rewritten from scratch.

  ## One job, one row — even when the work continues elsewhere

  A scan enqueues a cascade and returns. That is TWO jobs: two durations, two
  ways to fail, two retry counts. They get two rows, linked by `parent_run_id`:

      scan agenda_docs           2 changed      1.2s
      └─ cascade agenda_docs     7 cells        41s
         └─ resumption transcript_extract       6094s   <- rescued twice

  Collapsing them into one row would have to lie about at least one duration,
  and would hide exactly the case worth seeing — the child that ran 101 minutes
  under a parent that finished in one second.

  What happens WITHIN a job is a tree, not more rows: which cells a cascade
  reached, and by which routes. That belongs in `detail`, and renders as the
  hierarchy the expressions page already draws.

  ## Not the source of truth for anything

  This table is an observation. Nothing in the engine reads it to decide what to
  do, and a write here failing must never fail the work it describes — see
  `safely/1`. The suspension table is the opposite: it IS the record that work is
  outstanding, and losing a row there loses the work.
  """

  require Logger

  @type status :: :queued | :running | :done | :failed | :suspended | :blocked
  @type kind :: :scan | :cascade | :resumption | :reprocess

  @statuses ~w(queued running done failed suspended blocked)

  @context_key {__MODULE__, :current}
  @available_key {__MODULE__, :available}
  @buffer_key {__MODULE__, :buffer}

  @doc """
  The run this process is currently executing, or nil.

  PROCESS-LOCAL because that is the only place it can live. A child job is
  enqueued from deep inside the parent's work — `Source` enqueuing a cascade,
  `Cascade` scheduling a resumption — and threading a run id down through every
  call between would mean changing signatures the whole way. The parent is a
  fact about the PROCESS, not about any one call in it.

  Set by `executing/3` and read by `queued/2`, so a job enqueued while another
  runs names it as parent without either knowing about the other.
  """
  @spec current() :: String.t() | nil
  def current, do: Process.get(@context_key)

  @doc """
  Mark `id` as running, and make it the parent of anything this process enqueues.

  The bracket around a job's work: everything `queued/2` sees while this is set
  becomes a child of `id`. Restores the previous value afterwards rather than
  clearing it, so a nested call cannot orphan its caller's context.
  """
  @spec executing(String.t() | nil, keyword(), (-> result)) :: result when result: term()
  def executing(id, opts \\ [], fun) do
    previous = Process.get(@context_key)
    Process.put(@context_key, id)
    started(id, opts)

    try do
      fun.()
    after
      if previous, do: Process.put(@context_key, previous), else: Process.delete(@context_key)
    end
  end

  @doc """
  Record that a job was CREATED, and return the new row's id.

  Called at the enqueue, not at the start of work: a job sitting in a queue
  behind a long one is a fact worth showing, and it is invisible if the row
  waits for the job to run.

  `parent:` is the run id of the job that created this one, which is what makes
  the stack. Nil for work nothing else caused — a cron scan, an operator's
  reprocess.
  """
  @spec queued(kind(), keyword()) :: String.t() | nil
  def queued(kind, opts \\ []) do
    id = uuid_v7()

    t = table()

    safely(fn ->
      query!(
        """
        INSERT INTO #{t}
          (id, tenant, kind, cell_id, status, parent_run_id, oban_job_id,
           enqueued_at, detail)
        VALUES ($1, $2, $3, $4, 'queued', $5, $6, now(), $7)
        """,
        [
          id,
          tenant(opts),
          to_string(kind),
          opts[:cell] && to_string(opts[:cell]),
          Keyword.get(opts, :parent, current()),
          opts[:oban_job_id],
          encode(opts[:detail] || %{})
        ]
      )

      id
    end)
  end

  @doc """
  Mark a job as started.

  Separate from `queued/2` because the gap between them is the queue wait, and
  a page that cannot show that cannot explain why nothing appears to be
  happening on a single-concurrency queue.
  """
  @spec started(String.t() | nil, keyword()) :: :ok
  def started(nil, _opts), do: :ok

  def started(id, opts) do
    t = table()

    safely(fn ->
      query!(
        "UPDATE #{t} SET status = 'running', started_at = now(), " <>
          "detail = detail || $2 WHERE id = $1",
        [id, encode(opts[:detail] || %{})]
      )
    end)

    :ok
  end

  @doc """
  Note a fact about the running job WITHOUT writing it yet.

  Accumulated in the process and flushed by `flush/1`, or by the next
  `finished/3`. A cascade names every cell it reaches — on this graph 49 of them
  — and a write per cell would put a round trip inside the hot loop, on the very
  connection that must never starve the work.

  So the default is to buffer. `progress/2` remains for a caller that genuinely
  wants the row updated now, and is what `flush/1` uses.
  """
  @spec note(String.t() | nil, map()) :: :ok
  def note(nil, _detail), do: :ok

  def note(id, detail) when is_map(detail) do
    Process.put({@buffer_key, id}, Map.merge(Process.get({@buffer_key, id}, %{}), detail))
    :ok
  end

  @doc """
  Write everything `note/2` accumulated for this run.

  Called by `finished/3`, so an ordinary job needs no explicit flush. Call it
  directly to make a long job's progress visible before it ends — which is the
  whole point of a status page, and worth one write at a natural boundary
  rather than one per cell.
  """
  @spec flush(String.t() | nil) :: :ok
  def flush(nil), do: :ok

  def flush(id) do
    case Process.delete({@buffer_key, id}) do
      nil -> :ok
      buffered when buffered == %{} -> :ok
      buffered -> progress(id, buffered)
    end
  end

  @doc """
  Add to what a job has recorded NOW, without changing its status.

  One write. Prefer `note/2` in a loop — a cascade reaches 49 cells on this
  graph, and a write per cell would put a round trip inside the hot loop.
  Merged into `detail` rather than replacing it, so an arriving fact never
  erases an earlier one.
  """
  @spec progress(String.t() | nil, map()) :: :ok
  def progress(nil, _detail), do: :ok

  def progress(id, detail) when is_map(detail) do
    t = table()

    safely(fn ->
      query!("UPDATE #{t} SET detail = detail || $2 WHERE id = $1", [id, encode(detail)])
    end)

    :ok
  end

  @doc """
  Close a job out.

  `status` is one of `:done`, `:failed`, `:suspended`, `:blocked`. The last two
  are not failures: a suspension is work that stopped ON PURPOSE and will be
  resumed by another job, and `blocked` is work waiting on a person. A page that
  renders either as an error teaches its reader to ignore errors.
  """
  @spec finished(String.t() | nil, status(), keyword()) :: :ok
  def finished(nil, _status, _opts), do: :ok

  def finished(id, status, opts) when status in [:done, :failed, :suspended, :blocked] do
    # BUFFERED NOTES FIRST. A job that noted its progress and then finished must
    # not lose what it noted — and merging it into this UPDATE would drop it on
    # any path that finishes without going through here.
    flush(id)

    t = table()

    safely(fn ->
      query!(
        """
        UPDATE #{t}
           SET status = $2,
               finished_at = now(),
               duration_us = $3,
               detail = detail || $4
         WHERE id = $1
        """,
        [id, to_string(status), opts[:duration_us], encode(opts[:detail] || %{})]
      )
    end)

    :ok
  end

  @doc """
  The most recent runs for a tenant, newest first.

  `status:` narrows to the outstanding ones — which is the STATUS half of the
  page, and the reason the partial index exists.
  """
  @spec recent(keyword()) :: [map()]
  def recent(opts \\ []) do
    limit = Keyword.get(opts, :limit, 50)

    {clause, params} =
      case Keyword.get(opts, :status) do
        nil -> {"", [tenant(opts), limit]}
        list when is_list(list) -> {"AND status = ANY($3)", [tenant(opts), limit, list]}
        one -> {"AND status = $3", [tenant(opts), limit, to_string(one)]}
      end

    t = table()

    safely(
      fn ->
        query!(
          """
          SELECT id, tenant, kind, cell_id, status, parent_run_id, oban_job_id,
                 enqueued_at, started_at, finished_at, duration_us, detail
            FROM #{t}
           WHERE tenant = $1 #{clause}
           ORDER BY enqueued_at DESC
           LIMIT $2
          """,
          params
        ).rows
        |> Enum.map(&row/1)
      end,
      []
    )
  end

  @doc """
  Every run whose parent is one of `ids` — one level of the stack.

  A level at a time rather than a recursive walk: the page renders a bounded
  list of parents and then their children, and a `WITH RECURSIVE` over the whole
  table would read history the page will not show.
  """
  @spec children([String.t()], keyword()) :: %{String.t() => [map()]}
  def children([], _opts), do: %{}

  def children(ids, opts) do
    t = table()

    safely(
      fn ->
        query!(
          """
          SELECT id, tenant, kind, cell_id, status, parent_run_id, oban_job_id,
                 enqueued_at, started_at, finished_at, duration_us, detail
            FROM #{t}
           WHERE parent_run_id = ANY($1) AND tenant = $2
           ORDER BY enqueued_at ASC
          """,
          [ids, tenant(opts)]
        ).rows
        |> Enum.map(&row/1)
        |> Enum.group_by(& &1.parent_run_id)
      end,
      %{}
    )
  end

  @doc """
  Work that will not proceed without a person.

  Four kinds, and the page must not render them alike — they need different
  actions from whoever is reading:

    * `:approval` — a suspension whose reason names a person, as opposed to
      `:expensive`, which resumes itself. The library knows these.
    * `:orphaned` — a suspension with NO resumption job at all, never enqueued
      or long pruned. The only kind where nothing is coming: every check that
      looks for a job in a bad state misses it, which is how one sat unreported
      for two days behind a page saying "it resumes when a job runs".
    * `:stranded` — a resumption job Oban can never fetch again. See
      `Suspension.stranded/1`: a final attempt that fails leaves the job
      `available` with `attempt == max_attempts`, unfetchable AND undiscarded,
      and because the worker's uniqueness includes `available` it also dedups
      every future enqueue for that point. The queue looks healthy and the work
      is dead. Repair with `Suspension.revive/1`.
    * `:discarded` — a job that exhausted its attempts and stopped. Nothing
      retries it.
    * whatever a host adds — see `:blocked_resolver` below.

  ## Why a host hook

  The first three are visible to the library because it owns the suspension
  table and can read Oban's. The rest are not knowable here at all: whether an
  LLM gate is off, whether a budget is spent, whether a row is parked awaiting
  a judgement — those are facts about a host's own domain, and a library that
  guessed at them would be wrong in a way nobody could correct.

      config :reactive_dag, blocked_resolver: {MyApp.Blocked, :list, []}

  Called with the opts, and must return a list of maps carrying at least
  `:kind` and `:detail`. Its failures are contained: a resolver that raises
  costs its own entries and not the three kinds above, because a blocked panel
  that goes blank when one contributor breaks is worse than one that is
  incomplete and says so.
  """
  @spec blocked(keyword()) :: [map()]
  def blocked(opts \\ []) do
    approvals(opts) ++
      orphaned(opts) ++ stranded(opts) ++ discarded(opts) ++ host_blocked(opts)
  end

  defp approvals(opts) do
    safely(
      fn ->
        # `points/1` returns the point NESTED under `:point`, with `:reason`,
        # `:count` and `:oldest` beside it — not flat keys. Verified against its
        # own SELECT rather than assumed; my first version read `&1[:waiting]`
        # and got nil for every row.
        for %{point: point, reason: :approval} = entry <-
              ReactiveDag.Suspension.points(opts) do
          %{
            kind: :approval,
            cell: point.waiting,
            detail: %{
              "row_uuid" => point.row_uuid,
              "count" => entry[:count],
              "since" => entry[:oldest] && to_string(entry[:oldest])
            }
          }
        end
      end,
      []
    )
  end

  # A point with NO job at all — never enqueued, or pruned. Distinct from
  # `:stranded`, which is a job in a bad state: every check that looks for a
  # job misses this one, which is why it sat unreported in production for two
  # days behind a page saying "it resumes when a job runs".
  #
  # Listed FIRST among the library's kinds, because it is the only one where
  # nothing at all is coming.
  defp orphaned(opts) do
    safely(
      fn ->
        for entry <- ReactiveDag.Suspension.orphaned(opts) do
          %{
            kind: :orphaned,
            cell: entry.point.waiting,
            detail: %{
              "row_uuid" => entry.point.row_uuid,
              "count" => entry.count,
              "since" => entry.oldest && to_string(entry.oldest),
              "repair" => "ReactiveDag.ResumptionWorker.enqueue/2"
            }
          }
        end
      end,
      []
    )
  end

  defp stranded(opts) do
    safely(
      fn ->
        for point <- ReactiveDag.Suspension.stranded(opts) do
          %{
            kind: :stranded,
            cell: point[:waiting],
            detail: %{
              "job_id" => point[:job_id],
              "row_uuid" => point[:row_uuid],
              # The repair, named on the row: `Oban.retry_job/1` does NOT fix
              # these — it skips jobs already `available` and reports success.
              "repair" => "ReactiveDag.Suspension.revive/1"
            }
          }
        end
      end,
      []
    )
  end

  defp discarded(opts) do
    safely(
      fn ->
        %{rows: rows} =
          query!(
            """
            SELECT id, worker, args->>'cell', errors
              FROM #{ReactiveDag.Suspension.oban_table()}
             WHERE state = 'discarded'
             ORDER BY id DESC
             LIMIT $1
            """,
            [Keyword.get(opts, :limit, 50)]
          )

        for [id, worker, cell, errors] <- rows do
          %{
            kind: :discarded,
            # THE WORKER when there is no cell. A discarded job is often not a
            # cascade job at all — a host's own backfill worker carries
            # `%{"batch" => 80}` and no cell — and three rows reading
            # `discarded | —` tell a reader nothing about which three. The
            # worker's last segment is the identifying fact there.
            cell: cell || short_worker(worker),
            detail: %{
              "job_id" => id,
              "worker" => worker,
              # LAST error only, and bounded. An exhausted job carries one entry
              # per attempt, each with a stacktrace, and this is read on a page.
              "error" =>
                errors |> List.wrap() |> List.last() |> then(&(&1 && &1["error"])) |>
                  then(&(&1 && String.slice(to_string(&1), 0, 300)))
            }
          }
        end
      end,
      []
    )
  end

  defp utc(nil), do: nil
  defp utc(%DateTime{} = dt), do: dt
  defp utc(%NaiveDateTime{} = n), do: DateTime.from_naive!(n, "Etc/UTC")

  # `MuniWatch.RedHook.AttendanceBackfillWorker` -> `AttendanceBackfillWorker`.
  # The prefix is the same on every row and costs the width that distinguishes
  # them.
  defp short_worker(nil), do: nil
  defp short_worker(worker), do: worker |> to_string() |> String.split(".") |> List.last()

  # CONTAINED. A resolver that raises costs its own entries, not the three kinds
  # the library can see for itself — a blocked panel that goes blank because one
  # contributor broke is worse than one that is incomplete.
  defp host_blocked(opts) do
    case Application.get_env(:reactive_dag, :blocked_resolver) do
      {m, f, a} -> apply(m, f, [opts | a])
      fun when is_function(fun, 1) -> fun.(opts)
      _ -> []
    end
  rescue
    e ->
      Logger.warning("reactive_dag: blocked_resolver failed (#{Exception.message(e)})")
      []
  end

  @doc """
  Delete runs finished before `cutoff`.

  Runs accumulate; suspensions do not, because they discharge. Without a prune
  this table is a slow leak, so the policy is stated here rather than left to
  each host to discover.

  Only FINISHED rows: an outstanding job is outstanding however old it is, and
  deleting it would hide exactly the stuck work the page exists to show.
  """
  @spec prune(DateTime.t(), keyword()) :: non_neg_integer()
  def prune(%DateTime{} = cutoff, opts \\ []) do
    t = table()

    # EVERY TENANT when none is named, rather than defaulting to `"*"` the way
    # a read does. A read scoped to the wrong tenant returns nothing, which is
    # merely unhelpful; a DELETE scoped to `"*"` on a multi-tenant host would
    # prune only the untenanted rows and silently leave every real tenant's
    # history growing forever.
    #
    # The table is its own authority on which tenants exist — it holds rows for
    # exactly the ones that have run something, which is exactly the set worth
    # pruning. That is what lets this live in the library at all: no plan, no
    # host callback, no list to keep in step.
    {clause, params} =
      case Keyword.fetch(opts, :tenant) do
        {:ok, tenant} -> {"AND tenant = $2", [cutoff, to_string(tenant)]}
        :error -> {"", [cutoff]}
      end

    safely(
      fn ->
        %{num_rows: n} =
          query!(
            "DELETE FROM #{t} WHERE finished_at IS NOT NULL AND finished_at < $1 #{clause}",
            params
          )

        n
      end,
      0
    )
  end

  @doc false
  def statuses, do: @statuses

  defp row([id, tenant, kind, cell, status, parent, job, enq, start, fin, us, detail]) do
    %{
      id: id,
      tenant: tenant,
      kind: kind,
      cell_id: cell,
      status: status,
      parent_run_id: parent,
      oban_job_id: job,
      # UTC `DateTime`, not the naive struct Postgrex hands back. The column is
      # `:utc_datetime_usec`, but the library speaks to the host's repo through
      # raw SQL — there is no Ecto type to do the loading — so a `timestamp`
      # arrives as `~N[...]`.
      #
      # That is not a cosmetic difference. A consumer sorting these against any
      # other timestamp gets `FunctionClauseError` in `DateTime.compare/2` the
      # moment both shapes meet, which is exactly what took `/admin/dag` down
      # with a 500 the first time a persisted row reached the page. Returning
      # the type the column declares is the fix; asking every caller to
      # normalise is not.
      enqueued_at: utc(enq),
      started_at: utc(start),
      finished_at: utc(fin),
      duration_us: us,
      detail: detail || %{}
    }
  end

  # AN OBSERVATION MUST NOT BREAK THE THING IT OBSERVES.
  #
  # These calls run inside the CALLER's transaction — a cascade's, or an Ash
  # action's — and that makes a rescue insufficient on its own. A statement that
  # fails marks the whole Postgres transaction aborted; rescuing in Elixir does
  # not un-abort it, so every later statement errors and the caller rolls back.
  # A missing run table then takes down the work it was only supposed to
  # describe.
  #
  # Measured: cascade's `:correct` / `:uncorrect` actions failed with
  # `** (DBConnection.ConnectionError) transaction rolling back` on a database
  # that simply had not run `runs_up/1` yet.
  #
  # A savepoint does NOT fix this, which was the first thing tried. Ecto's
  # nested `transaction/2` does not issue a real Postgres `SAVEPOINT`, so a
  # failed statement inside one still poisons the outer transaction — verified
  # directly rather than assumed. `Suspension.savepoint/1` works in the cascade
  # for a different reason, stated in its own docs: it isolates "a failure that
  # arrives as a VALUE", and an op returning `{:error, _}` never executed a
  # failing statement in the first place.
  #
  # So the only reliable answer is not to ATTEMPT a write that cannot succeed.
  # `available?/0` asks once per process and caches the answer, and every call
  # short-circuits when the table is absent. The rescue below stays for what it
  # can actually catch — no repo configured, a pool checkout timeout, a
  # connection already lost — none of which involve issuing bad SQL.
  defp safely(fun, default \\ nil) do
    if available?() do
      fun.()
    else
      default
    end
  rescue
    e ->
      Logger.debug(fn -> "reactive_dag: run log unavailable (#{Exception.message(e)})" end)
      default
  end

  @doc """
  Is the run table present?

  Asked ONCE per process and cached, because the answer cannot change under a
  running node — a table is created by a migration, and a release restarts.
  Caching matters: this is checked before every write, and a query per write
  would make the log more expensive than the work it records.

  `to_regclass` returns NULL rather than raising for an absent table, which is
  the whole reason it is used here: asking any other way would be the very
  failed statement this exists to avoid.
  """
  @spec available?() :: boolean()
  def available? do
    # KEYED BY (repo, table), not a bare flag. The cache is node-wide — a
    # cascade runs in a fresh process per job, so a process-local one would
    # re-probe on every job, a query per write, which is what caching exists to
    # avoid. But node-wide means a test suite that swaps in a fake repo, or a
    # host that reconfigures the table name, would otherwise inherit an answer
    # about a DIFFERENT database and silently log nothing.
    #
    # Including both in the key makes a changed configuration re-probe by
    # construction rather than by remembering to call `forget_availability/0`.
    key = {@available_key, repo(), table()}

    case :persistent_term.get(key, :unknown) do
      :unknown ->
        answer = probe()
        :persistent_term.put(key, answer)
        answer

      cached ->
        cached
    end
  end

  @doc """
  Forget whether the table exists, so the next call re-probes.

  For a host that migrates a running node, and for tests that create or drop the
  table between cases.
  """
  @spec forget_availability() :: :ok
  def forget_availability do
    # Every key for this module, since the caller changing the table name is
    # exactly when this is called and the old key would otherwise linger.
    for {{tag, _repo, _table} = k, _v} <- :persistent_term.get(), tag == @available_key do
      :persistent_term.erase(k)
    end

    :ok
  rescue
    # `:persistent_term.get/0` returns every term on the node, and a malformed
    # one elsewhere must not make this raise.
    _ -> :ok
  end

  # `query!/2`, NOT `query/2`. The library's contract with a host repo is
  # `query!/2` — it is what `Suspension` uses, and the only function this
  # library required before this one. A host shim exporting just that is
  # legitimate, and asking for `query/2` made every probe raise into the rescue
  # below and answer "no table" against a database that had one.
  #
  # `to_regclass` returns NULL rather than raising for an absent table, which is
  # why it is safe to call at all: any other way of asking would be the very
  # failed statement this exists to avoid.
  defp probe do
    case repo().query!("SELECT to_regclass($1)", [table()]) do
      %{rows: [[nil]]} -> false
      _ -> true
    end
  rescue
    _ -> false
  end

  defp encode(map) when is_map(map), do: map

  @doc """
  The table these reads and writes use.

  VALIDATED, exactly as `Suspension.table/0` is: the name is interpolated into
  SQL rather than passed as a parameter — a table name cannot be one — so a
  config value that is not a plain identifier must be refused here rather than
  concatenated. `safely/1` would otherwise swallow the resulting syntax error
  and the log would simply stay empty.
  """
  @spec table() :: String.t()
  def table do
    name = ReactiveDag.Migration.runs_table_name()

    if is_binary(name) and name =~ ~r/\A[a-zA-Z_][a-zA-Z0-9_]*\z/ do
      name
    else
      raise ArgumentError,
            "reactive_dag: runs_table #{inspect(name)} is not a valid table identifier"
    end
  end

  defp tenant(opts), do: ReactiveDag.Suspension.tenant(opts)

  defp query!(sql, params), do: repo().query!(sql, params)

  @doc """
  The repo the log writes through.

  `:run_repo` when a host configures one, otherwise the main `:repo`.

  ## Why this wants to be its own connection

  Every write here happens INSIDE somebody else's transaction, and sharing it
  breaks the log in both directions:

    * `queued/2` runs where the job is enqueued, which for `MarkDirty` is inside
      an Ash action's transaction. If that action rolls back, the row rolls back
      with it — so an attempt that FAILED leaves no trace, which is exactly the
      case a history page exists for.

    * `progress/2` runs mid-work, and `Cascade.run/3` wraps its whole walk in
      one transaction (`cascade.ex:161`). A progress row is therefore invisible
      until the cascade commits — so "what is running now" cannot show a running
      cascade at all — and lost entirely if it rolls back.

  Neither is fixable on the caller's connection. `Repo.checkout/1` does NOT
  escape an open transaction (verified: same `txid_current()`), and Ecto's
  nested `transaction/2` issues no real SAVEPOINT, so a failed statement still
  poisons the outer one. Oban does not attempt it either — `Oban.Repo`'s
  `with_dynamic_repo/2` explicitly refuses to switch repos when the caller is
  already in a transaction, preferring to join it. Oban can afford that because
  a job row SHOULD vanish with the transaction that enqueued it. A log row
  should not, and that is the whole difference.

  ## Falling back to the main repo

  Supported and degraded, rather than refused. With no `:run_repo` the log still
  records history — which is most of the value — but rows participate in the
  caller's transaction, so a rolled-back attempt leaves no trace and in-flight
  progress is invisible until commit. A host that wants attempt-tracking
  configures a second repo:

      config :reactive_dag, run_repo: MyApp.RunLogRepo

  A small pool is right for it (2-3). The log must never starve the work of
  connections, and it is the reason to keep the pools separate rather than raise
  the main one.
  """
  @spec repo() :: module()
  def repo do
    Application.get_env(:reactive_dag, :run_repo) ||
      Application.get_env(:reactive_dag, :repo) ||
      raise "reactive_dag: set `config :reactive_dag, repo: MyApp.Repo`"
  end

  @doc """
  Does the log have a connection of its own?

  False means writes share the caller's transaction — see `repo/0`. The page can
  say so rather than implying a completeness the storage cannot deliver.
  """
  @spec isolated?() :: boolean()
  def isolated?, do: not is_nil(Application.get_env(:reactive_dag, :run_repo))

  # Same generator as `Suspension` — UUIDv7, so `ORDER BY id` is chronological.
  defp uuid_v7 do
    ms = System.system_time(:millisecond)
    <<rand_a::12, rand_b::62, _::bitstring>> = :crypto.strong_rand_bytes(10)

    <<ms::48, 7::4, rand_a::12, 2::2, rand_b::62>>
    |> then(&Base.encode16(&1, case: :lower))
    |> then(fn <<a::binary-8, b::binary-4, c::binary-4, d::binary-4, e::binary-12>> ->
      "#{a}-#{b}-#{c}-#{d}-#{e}"
    end)
  end
end

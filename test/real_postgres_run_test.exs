defmodule ReactiveDag.RealPostgresRunTest do
  @moduledoc """
  The run table's SQL, EXECUTED.

  Same reasoning as `real_postgres_suspension_test.exs`: a fake repo that
  matches on `"INSERT INTO " <> _` covers the logic and cannot see inside the
  SQL, which is where that file's three real bugs lived.

  One of those three is live here. `detail = detail || $2` is the SAME
  right-biased jsonb merge that kept the wrong side of a frontier merge — and
  this module depends on its bias being right-biased, because a later fact must
  win over an earlier one while leaving untouched keys alone. That is asserted
  against a real database rather than reasoned about.

  ## Opt-in

  Skipped unless `REACTIVE_DAG_TEST_DATABASE_URL` is set:

      REACTIVE_DAG_TEST_DATABASE_URL=postgres://cascade:cascade@localhost:5544/reactive_dag_test \\
        mix test test/real_postgres_run_test.exs
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.Run

  @url System.get_env("REACTIVE_DAG_TEST_DATABASE_URL")
  @table "rd_test_run"

  defmodule Repo do
    def query!(sql, params \\ []), do: Postgrex.query!(conn(), sql, params)
    def put_conn(pid), do: :persistent_term.put({__MODULE__, :conn}, pid)
    def conn, do: :persistent_term.get({__MODULE__, :conn})
  end

  setup_all do
    if @url do
      pid = start_supervised!({Postgrex, url_opts(@url)})
      Repo.put_conn(pid)

      Postgrex.query!(pid, "DROP TABLE IF EXISTS #{@table}", [])

      # The DDL `Migration.runs_up/1` produces, kept in step BY HAND — a
      # divergence here would hide the class of bug this file exists to catch.
      Postgrex.query!(
        pid,
        """
        CREATE TABLE #{@table} (
          id            text PRIMARY KEY,
          tenant        text NOT NULL,
          kind          text NOT NULL,
          cell_id       text,
          status        text NOT NULL,
          parent_run_id text,
          oban_job_id   bigint,
          enqueued_at   timestamp NOT NULL,
          started_at    timestamp,
          finished_at   timestamp,
          duration_us   bigint,
          detail        jsonb NOT NULL DEFAULT '{}'
        )
        """,
        []
      )

      prev_repo = Application.get_env(:reactive_dag, :repo)
      prev_table = Application.get_env(:reactive_dag, :runs_table)
      Application.put_env(:reactive_dag, :repo, Repo)
      Application.put_env(:reactive_dag, :runs_table, @table)

      on_exit(fn ->
        {:ok, cleanup} = Postgrex.start_link(url_opts(@url))
        Postgrex.query!(cleanup, "DROP TABLE IF EXISTS #{@table}", [])
        GenServer.stop(cleanup)

        restore(:repo, prev_repo)
        restore(:runs_table, prev_table)
      end)

      :ok
    else
      :ok
    end
  end

  setup do
    if @url, do: Repo.query!("DELETE FROM #{@table}")

    # `available?/0` caches per PROCESS, and ExUnit runs a test module's tests
    # in one process — so the missing-table test below caches `false` and every
    # test after it in that process would silently write nothing. Cleared here
    # rather than in that one test, so the order cannot matter.
    ReactiveDag.Run.forget_availability()
    :ok
  end

  describe "the lifecycle" do
    @describetag :postgres
    test "a row exists from the moment the job is CREATED" do
      if @url do
        id = Run.queued(:cascade, tenant: "t", cell: "agenda_items")

        assert [row] = Run.recent(tenant: "t")
        assert row.id == id
        assert row.status == "queued"
        assert row.enqueued_at, "a created job must be visible before it runs"
        refute row.started_at, "it has not started"

        # THE POINT OF THE STATUS HALF: queued work is findable as outstanding.
        assert [^row] = Run.recent(tenant: "t", status: ~w(queued running blocked))
      end
    end

    test "status advances without losing what was recorded earlier" do
      if @url do
        id = Run.queued(:scan, tenant: "t", cell: "agenda_docs", detail: %{"trigger" => "cron"})
        Run.started(id, detail: %{"claimed" => 3})
        Run.progress(id, %{"sources_done" => 1})
        Run.finished(id, :done, duration_us: 1234, detail: %{"changed" => 2})

        assert [row] = Run.recent(tenant: "t")
        assert row.status == "done"
        assert row.duration_us == 1234

        # EVERY fact survives, from all four calls. This is the incremental
        # build-up: `||` merges rather than replaces.
        assert row.detail["trigger"] == "cron"
        assert row.detail["claimed"] == 3
        assert row.detail["sources_done"] == 1
        assert row.detail["changed"] == 2
      end
    end

    test "a later fact wins over an earlier one for the same key" do
      if @url do
        # The bias `detail || $2` depends on, asserted rather than assumed —
        # the frontier bug this file's sibling records was exactly this
        # operator keeping the wrong side.
        id = Run.queued(:cascade, tenant: "t")
        Run.progress(id, %{"cells" => 1})
        Run.progress(id, %{"cells" => 7})

        assert [%{detail: %{"cells" => 7}}] = Run.recent(tenant: "t")
      end
    end
  end

  describe "incremental notes" do
    test "notes accumulate in the process and land in one write" do
      if @url do
        id = Run.queued(:cascade, tenant: "t", cell: "c")

        Run.note(id, %{"reached" => "a"})
        Run.note(id, %{"cells" => 1})

        # NOTHING written yet — that is the point. A cascade reaches 49 cells on
        # this graph, and a write per cell would put a round trip inside the hot
        # loop, on the connection that must never starve the work.
        assert [%{detail: before}] = Run.recent(tenant: "t", limit: 1)
        refute Map.has_key?(before, "reached")

        Run.flush(id)

        assert [%{detail: d}] = Run.recent(tenant: "t", limit: 1)
        assert d["reached"] == "a"
        assert d["cells"] == 1
      end
    end

    test "finishing flushes what was noted, so nothing is lost" do
      if @url do
        id = Run.queued(:cascade, tenant: "t", cell: "c")
        Run.note(id, %{"reached" => "z"})
        Run.finished(id, :done, duration_us: 5)

        assert [%{detail: d, status: "done"}] = Run.recent(tenant: "t", limit: 1)

        assert d["reached"] == "z",
               "a job that noted its progress and then finished must not lose it"
      end
    end

    test "a flush with nothing buffered writes nothing" do
      if @url do
        id = Run.queued(:cascade, tenant: "t", cell: "c")
        assert Run.flush(id) == :ok
        assert Run.flush(id) == :ok
      end
    end
  end

  describe "the stack" do
    test "a child names the job that created it" do
      if @url do
        scan = Run.queued(:scan, tenant: "t", cell: "agenda_docs")
        cascade = Run.queued(:cascade, tenant: "t", cell: "agenda_docs", parent: scan)
        resumption = Run.queued(:resumption, tenant: "t", cell: "extract", parent: cascade)

        by_parent = Run.children([scan, cascade], tenant: "t")

        assert [%{id: ^cascade}] = by_parent[scan]
        assert [%{id: ^resumption}] = by_parent[cascade]
      end
    end

    test "children are scoped to the tenant, not just the parent id" do
      if @url do
        # A parent id is unique, so this cannot collide today. It is asserted
        # because the day it can — a restored backup, a copied id — the failure
        # is one tenant's work appearing under another's, which is the same
        # class of bug the suspension table's tenant filter exists to prevent.
        scan = Run.queued(:scan, tenant: "t")
        Run.queued(:cascade, tenant: "other", parent: scan)

        assert Run.children([scan], tenant: "t") == %{}
      end
    end
  end

  describe "the ambient parent" do
    test "a job enqueued while another runs becomes its child" do
      if @url do
        # THE MECHANISM THE STACK RESTS ON. A cascade is enqueued from deep
        # inside a scan's work — `Source.enqueue_cascade/3` — and threading a
        # run id down through every call between would mean changing signatures
        # the whole way. The parent is a fact about the PROCESS.
        parent = Run.queued(:scan, tenant: "t", cell: "agenda_docs")

        child =
          Run.executing(parent, [], fn ->
            assert Run.current() == parent
            Run.queued(:cascade, tenant: "t", cell: "agenda_items")
          end)

        assert %{^parent => [%{id: ^child, kind: "cascade"}]} = Run.children([parent], tenant: "t")
      end
    end

    test "the context is restored, so a nested job cannot orphan its caller" do
      if @url do
        outer = Run.queued(:scan, tenant: "t")
        inner = Run.queued(:cascade, tenant: "t")

        Run.executing(outer, [], fn ->
          Run.executing(inner, [], fn -> :ok end)

          # Still the outer one: a nested bracket that cleared rather than
          # restored would send every later sibling to the top level.
          assert Run.current() == outer
        end)

        assert Run.current() == nil, "the bracket must not leak out of the job"
      end
    end

    test "an explicit parent beats the ambient one" do
      if @url do
        ambient = Run.queued(:scan, tenant: "t")
        stated = Run.queued(:scan, tenant: "t")

        child =
          Run.executing(ambient, [], fn ->
            Run.queued(:cascade, tenant: "t", parent: stated)
          end)

        assert %{^stated => [%{id: ^child}]} = Run.children([stated], tenant: "t")
        assert Run.children([ambient], tenant: "t") == %{}
      end
    end

    test "a nil run still brackets, and its children are top-level" do
      if @url do
        # `queued/2` returns nil when the log is unavailable. The bracket must
        # still run the work — the engine does not depend on this table.
        assert Run.executing(nil, [], fn -> Run.current() end) == nil
      end
    end
  end

  describe "what the workers record" do
    # These assert the STATUS VOCABULARY the workers use, because the choice of
    # status is the whole editorial content of this feature: a page that renders
    # "stopped on purpose" and "crashed" alike teaches its reader to ignore both.
    test "stopped-on-purpose is not failure" do
      if @url do
        for {status, note} <- [
              {:suspended, "a cascade with suspensions; each point has its own row"},
              {:blocked, "waiting on a person — an unreachable source, a missing scanner"},
              {:done, "finished, whether or not anything changed"},
              {:failed, "raised"}
            ] do
          id = Run.queued(:cascade, tenant: "t", detail: %{"note" => note})
          Run.finished(id, status, [])

          assert [%{status: recorded}] = Run.recent(tenant: "t", limit: 1)
          assert recorded == to_string(status)

          Repo.query!("DELETE FROM #{@table}")
        end
      end
    end

    test "outstanding work is exactly what is not finished" do
      if @url do
        # THE STATUS HALF of the page, and the reason for the partial index.
        # `blocked` counts as outstanding: it is work that has not happened and
        # will not happen without someone. `failed` does not — Oban either
        # retries it or it is over.
        queued = Run.queued(:cascade, tenant: "t")

        running = Run.queued(:cascade, tenant: "t")
        Run.started(running, [])

        blocked = Run.queued(:scan, tenant: "t")
        Run.finished(blocked, :blocked, [])

        done = Run.queued(:cascade, tenant: "t")
        Run.finished(done, :done, [])

        outstanding =
          Run.recent(tenant: "t", status: ~w(queued running blocked))
          |> Enum.map(& &1.id)
          |> Enum.sort()

        assert outstanding == Enum.sort([queued, running, blocked])
      end
    end
  end

  describe "blocked work" do
    # Each kind needs a DIFFERENT action from whoever reads the page, so the
    # page must not render them alike. These assert they arrive distinguished.
    setup do
      if @url do
        Repo.query!("DROP TABLE IF EXISTS rd_test_oban", [])

        Repo.query!(
          """
          CREATE TABLE rd_test_oban (
            id bigserial PRIMARY KEY, worker text NOT NULL, state text NOT NULL,
            args jsonb NOT NULL DEFAULT '{}', errors jsonb NOT NULL DEFAULT '[]',
            attempt integer NOT NULL DEFAULT 0, max_attempts integer NOT NULL DEFAULT 20
          )
          """,
          []
        )

        prev = Application.get_env(:reactive_dag, :oban_table)
        Application.put_env(:reactive_dag, :oban_table, "rd_test_oban")

        on_exit(fn ->
          {:ok, c} = Postgrex.start_link(url_opts(@url))
          Postgrex.query!(c, "DROP TABLE IF EXISTS rd_test_oban", [])
          GenServer.stop(c)
          if prev do
            Application.put_env(:reactive_dag, :oban_table, prev)
          else
            Application.delete_env(:reactive_dag, :oban_table)
          end
        end)
      end

      :ok
    end

    test "a discarded job is blocked, with its LAST error" do
      if @url do
        Repo.query!(
          "INSERT INTO rd_test_oban (worker, state, args, errors, attempt, max_attempts) " <>
            "VALUES ($1, 'discarded', $2, $3, 3, 3)",
          [
            "MyApp.BackfillWorker",
            %{"cell" => "agenda_docs"},
            [%{"error" => "first try"}, %{"error" => "gave up here"}]
          ]
        )

        assert [%{kind: :discarded} = b] =
                 Enum.filter(ReactiveDag.Run.blocked(), &(&1.kind == :discarded))

        assert b.cell == "agenda_docs"
        assert b.detail["worker"] == "MyApp.BackfillWorker"

        assert b.detail["error"] == "gave up here",
               "the LAST error — an exhausted job carries one per attempt, each " <>
                 "with a stacktrace, and this is read on a page"
      end
    end

    test "a discarded job with no cell is named by its WORKER" do
      if @url do
        # FROM REAL DATA. Production's three discarded jobs are a host's own
        # backfill worker, carrying `%{"batch" => 80}` and no cell at all — so
        # three rows rendered `discarded | —` and told a reader nothing about
        # which three. The worker's last segment is the identifying fact.
        Repo.query!(
          "INSERT INTO rd_test_oban (worker, state, args) VALUES ($1, 'discarded', $2)",
          ["MyApp.RedHook.AttendanceBackfillWorker", %{"batch" => 80}]
        )

        assert [%{cell: "AttendanceBackfillWorker"}] =
                 Enum.filter(ReactiveDag.Run.blocked(), &(&1.kind == :discarded))
      end
    end

    test "a healthy job is not blocked" do
      if @url do
        Repo.query!(
          "INSERT INTO rd_test_oban (worker, state) VALUES ('W', 'available')",
          []
        )

        assert Enum.filter(ReactiveDag.Run.blocked(), &(&1.kind == :discarded)) == []
      end
    end

    test "a host resolver contributes, and its failure is contained" do
      if @url do
        Application.put_env(:reactive_dag, :blocked_resolver, fn _opts ->
          [%{kind: :spend_gated, cell: "transcript_extract", detail: %{"pending" => 10}}]
        end)

        assert Enum.any?(ReactiveDag.Run.blocked(), &(&1.kind == :spend_gated))

        # A resolver that raises must cost ITS entries and nothing else. A
        # blocked panel that goes blank because one contributor broke is worse
        # than one that is incomplete.
        Application.put_env(:reactive_dag, :blocked_resolver, fn _opts ->
          raise "resolver is broken"
        end)

        Repo.query!(
          "INSERT INTO rd_test_oban (worker, state, args) VALUES ('W', 'discarded', $1)",
          [%{"cell" => "c"}]
        )

        assert Enum.any?(ReactiveDag.Run.blocked(), &(&1.kind == :discarded)),
               "the library's own kinds must survive a broken host resolver"

        Application.delete_env(:reactive_dag, :blocked_resolver)
      end
    end
  end

  describe "retention" do
    test "with no tenant, every tenant's history is pruned" do
      if @url do
        # NOT `"*"`. A read scoped to the wrong tenant returns nothing, which is
        # merely unhelpful; a DELETE scoped to `"*"` on a multi-tenant host would
        # prune only the untenanted rows and leave every real tenant's history
        # growing forever.
        for t <- ~w(village town tivoli) do
          id = Run.queued(:cascade, tenant: t)
          Run.finished(id, :done, duration_us: 1)

          Repo.query!(
            "UPDATE #{@table} SET finished_at = now() - interval '200 days' WHERE id = $1",
            [id]
          )
        end

        assert Run.prune(DateTime.add(DateTime.utc_now(), -7, :day)) == 3

        for t <- ~w(village town tivoli) do
          assert Run.recent(tenant: t) == []
        end
      end
    end

    test "a named tenant prunes only its own" do
      if @url do
        keep = Run.queued(:cascade, tenant: "town")
        Run.finished(keep, :done, duration_us: 1)

        drop = Run.queued(:cascade, tenant: "village")
        Run.finished(drop, :done, duration_us: 1)

        Repo.query!("UPDATE #{@table} SET finished_at = now() - interval '200 days'", [])

        assert Run.prune(DateTime.add(DateTime.utc_now(), -7, :day), tenant: "village") == 1

        assert Run.recent(tenant: "town") |> Enum.map(& &1.id) == [keep]
      end
    end

    test "prune removes finished rows and keeps outstanding ones" do
      if @url do
        old = Run.queued(:cascade, tenant: "t")
        Run.finished(old, :done, duration_us: 1)

        stuck = Run.queued(:cascade, tenant: "t")

        # Backdate the finished one past the cutoff.
        Repo.query!(
          "UPDATE #{@table} SET finished_at = now() - interval '30 days' WHERE id = $1",
          [old]
        )

        assert Run.prune(DateTime.utc_now() |> DateTime.add(-7, :day), tenant: "t") == 1

        ids = Run.recent(tenant: "t") |> Enum.map(& &1.id)

        assert ids == [stuck],
               "an outstanding job is outstanding however old — pruning it hides stuck work"
      end
    end
  end

  describe "its own connection" do
    # A SECOND repo is what makes the log record ATTEMPTS. On the caller's
    # connection a row rolls back with the caller, so an attempt that failed
    # leaves no trace — and mid-work progress is invisible until commit, so
    # "what is running now" cannot show a running cascade at all.
    #
    # Verified rather than reasoned about: `Repo.checkout/1` does NOT escape an
    # open transaction (same `txid_current()`), and Ecto's nested
    # `transaction/2` issues no real SAVEPOINT.
    defmodule SideRepo do
      def query!(sql, params \\ []), do: Postgrex.query!(conn(), sql, params)
      def put_conn(pid), do: :persistent_term.put({__MODULE__, :conn}, pid)
      def conn, do: :persistent_term.get({__MODULE__, :conn})
    end

    setup do
      if @url do
        pid = start_supervised!({Postgrex, url_opts(@url) ++ [name: :side_conn]})
        SideRepo.put_conn(pid)
        Application.put_env(:reactive_dag, :run_repo, SideRepo)
        ReactiveDag.Run.forget_availability()

        on_exit(fn ->
          Application.delete_env(:reactive_dag, :run_repo)
          ReactiveDag.Run.forget_availability()
        end)
      end

      :ok
    end

    test "a row written inside a rolled-back transaction SURVIVES" do
      if @url do
        # The attempt-tracking guarantee, stated as a test. On the shared
        # connection this row is gone.
        Repo.query!("BEGIN", [])
        id = Run.queued(:cascade, tenant: "t", cell: "attempted")
        Repo.query!("ROLLBACK", [])

        assert id, "the write must have happened"

        assert Enum.any?(Run.recent(tenant: "t"), &(&1.id == id)),
               "an attempt whose transaction rolled back must still be recorded — " <>
                 "that is the case a history page exists for"
      end
    end

    test "a row is visible WHILE the caller's transaction is still open" do
      if @url do
        # The status guarantee. `Cascade.run/3` wraps its whole walk in one
        # transaction, so without this a running cascade cannot be shown as
        # running — the row would appear only once it committed.
        Repo.query!("BEGIN", [])
        id = Run.queued(:cascade, tenant: "t", cell: "in_flight")

        assert Enum.any?(Run.recent(tenant: "t"), &(&1.id == id)),
               "in-flight work must be visible before its transaction commits"

        Repo.query!("ROLLBACK", [])
      end
    end

    test "isolated?/0 says which mode the log is in" do
      assert ReactiveDag.Run.isolated?(), "a run_repo is configured in this describe block"
    end
  end

  describe "a missing table must not poison the caller's transaction" do
    test "a write inside a transaction leaves it usable" do
      if @url do
        # THE BUG THIS EXISTS FOR. These calls run inside the caller's
        # transaction — a cascade's, or an Ash action's. A statement that FAILS
        # marks the whole Postgres transaction aborted, and rescuing in Elixir
        # does not un-abort it: every later statement then errors and the caller
        # rolls back. A missing run table took down the work it was only meant
        # to describe.
        #
        # Measured in cascade: `:correct` / `:uncorrect` failed with
        # `** (DBConnection.ConnectionError) transaction rolling back` on a
        # database that had simply not run `runs_up/1` yet.
        #
        # A savepoint does NOT fix it — Ecto's nested `transaction/2` issues no
        # real Postgres SAVEPOINT, so the failed statement still poisons the
        # outer transaction. The answer is not to ATTEMPT a write that cannot
        # succeed, which is what `available?/0` decides.
        prev = Application.get_env(:reactive_dag, :runs_table)
        Application.put_env(:reactive_dag, :runs_table, "rd_test_absent_table")
        ReactiveDag.Run.forget_availability()

        # INSIDE A TRANSACTION, which is the whole point: the poisoning only
        # happens when there is one to poison. An earlier version of this test
        # called `queued/2` on a bare connection and passed with the guard
        # REMOVED — it proved nothing.
        Repo.query!("BEGIN", [])

        assert Run.queued(:cascade, tenant: "t") == nil

        # The transaction must still be usable. Without the guard, the failed
        # INSERT marks it aborted and this raises `current transaction is
        # aborted, commands ignored until end of transaction block`.
        assert %{rows: [[1]]} = Repo.query!("SELECT 1", [])

        Repo.query!("COMMIT", [])

        Application.put_env(:reactive_dag, :runs_table, prev)
        ReactiveDag.Run.forget_availability()
      end
    end

    test "the probe uses query!/2 — the only repo function the library requires" do
      if @url do
        # `Suspension` uses `query!/2`, and it was the ONLY repo function this
        # library needed. A probe asking for `query/2` raised on a host shim
        # that legitimately exports just the bang version, and answered "no
        # table" against a database that had one — silently logging nothing.
        refute function_exported?(Repo, :query, 2),
               "this shim deliberately exports only query!/2, as a host may"

        ReactiveDag.Run.forget_availability()
        assert Run.available?(), "the probe must work against a query!-only repo"
      end
    end
  end

  describe "an observation must not break what it observes" do
    test "a missing table costs a gap in the log, not an exception" do
      if @url do
        prev = Application.get_env(:reactive_dag, :runs_table)
        Application.put_env(:reactive_dag, :runs_table, "rd_test_does_not_exist")

        # Every call has to survive it: a host that has not run `runs_up/1` must
        # still be able to run cascades.
        assert Run.queued(:cascade, tenant: "t") == nil
        assert Run.started("x", []) == :ok
        assert Run.progress("x", %{}) == :ok
        assert Run.finished("x", :done, []) == :ok
        assert Run.recent(tenant: "t") == []
        assert Run.children(["x"], tenant: "t") == %{}
        assert Run.prune(DateTime.utc_now(), tenant: "t") == 0

        Application.put_env(:reactive_dag, :runs_table, prev)
        ReactiveDag.Run.forget_availability()
      end
    end

    test "a nil id is a no-op rather than a crash" do
      # `queued/2` returns nil when the log is unavailable, and every later call
      # receives that nil. They must all accept it.
      assert Run.started(nil, []) == :ok
      assert Run.progress(nil, %{}) == :ok
      assert Run.finished(nil, :done, []) == :ok
    end
  end
  defp restore(key, nil), do: Application.delete_env(:reactive_dag, key)
  defp restore(key, value), do: Application.put_env(:reactive_dag, key, value)

  defp url_opts(url) do
    %URI{host: host, port: port, path: path, userinfo: userinfo} = URI.parse(url)
    [user, pass] = String.split(userinfo || "postgres:postgres", ":", parts: 2)

    [
      hostname: host || "localhost",
      port: port || 5432,
      username: user,
      password: pass,
      database: String.trim_leading(path || "/postgres", "/")
    ]
  end
end

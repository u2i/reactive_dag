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

  describe "retention" do
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

defmodule ReactiveDag.RealPostgresSuspensionTest do
  @moduledoc """
  The suspension table's SQL, EXECUTED — not pattern-matched.

  Every other test in this suite hands the library a fake repo that matches on
  `"INSERT INTO " <> _` and interprets the parameters. That covers the logic and
  cannot cover the SQL itself, which is where three real bugs lived in the
  frontier this table replaces:

    * `?` is BOTH Postgres's jsonb-exists operator and Postgrex's parameter
      marker, so two `CASE` branches of an `ON CONFLICT` clause were silently
      dead;
    * `||` is right-biased, so a merge kept the incoming side where it needed
      the stored one — exactly the wrong end;
    * a column dropped from the schema but still named in the INSERT.

  Each was found by hand against a real database, after the whole suite passed.
  Two of the three were in the `ON CONFLICT` merge, which no longer exists —
  suspensions are append-only, so there is nothing to merge. The record stays
  because the lesson does: a fake that interprets SQL cannot see inside it.

  ## What is riskiest here

  `discharge/1` issues `DELETE … WHERE id = ANY($1)` with a list of ids. If the
  parameter typing is wrong the statement deletes ZERO rows and reports
  success — and the stopping point re-suspends forever while its work is
  silently never discharged. That failure is invisible to a fake and invisible
  to a test that only checks the return value, so it is asserted here.

  This is not hypothetical: the first run of this file failed every insert with
  `Postgrex expected a binary of 16 bytes`, because `id` was declared `uuid`
  and the library passes the textual form. That is why `id` is `text` — see
  `ReactiveDag.Migration`.

  ## Opt-in

  Skipped unless `REACTIVE_DAG_TEST_DATABASE_URL` is set, so a checkout with no
  database still gets a green suite:

      REACTIVE_DAG_TEST_DATABASE_URL=postgres://cascade:cascade@localhost:5544/reactive_dag_test \\
        mix test test/real_postgres_suspension_test.exs

  The table is created and dropped per run, so it cannot touch a host's data.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.Suspension

  @url System.get_env("REACTIVE_DAG_TEST_DATABASE_URL")
  @table "rd_test_suspension"
  @oban "rd_test_oban_jobs"

  # A repo shim: the library calls `query!/2`, Postgrex offers `query!/3`.
  defmodule Repo do
    def query!(sql, params \\ []), do: Postgrex.query!(conn(), sql, params)
    def put_conn(pid), do: :persistent_term.put({__MODULE__, :conn}, pid)
    def conn, do: :persistent_term.get({__MODULE__, :conn})
  end

  setup_all do
    if @url do
      # `start_supervised!`, not `Postgrex.start_link`: a connection started
      # here is LINKED to the setup process, which exits as soon as setup
      # returns — taking the connection with it, and every test then fails on
      # checkout with `(EXIT) normal`. The test supervisor outlives the module.
      pid = start_supervised!({Postgrex, url_opts(@url)})
      Repo.put_conn(pid)

      Postgrex.query!(pid, "DROP TABLE IF EXISTS #{@table}", [])

      # The DDL `ReactiveDag.Migration.up/1` produces. Written out rather than
      # run through Ecto.Migration because that needs a migration runner — so
      # the column types and nullability have to be kept in step with
      # migration.ex BY HAND, and a divergence here would hide exactly the
      # class of bug this file exists to catch.
      #
      # `timestamp`, not `timestamptz`: `:utc_datetime_usec` maps to a naive
      # column holding UTC. Verified against a real migrated database rather
      # than assumed.
      Postgrex.query!(
        pid,
        """
        CREATE TABLE #{@table} (
          id          text PRIMARY KEY,
          tenant      text NOT NULL DEFAULT '*',
          waiting     text NOT NULL,
          resource    text NOT NULL,
          row_uuid    text NOT NULL DEFAULT '*',
          version_id  text NOT NULL DEFAULT '*',
          reason      text NOT NULL,
          inserted_at timestamp NOT NULL DEFAULT now(),
          lap         integer NOT NULL DEFAULT 0
        )
        """,
        []
      )

      Postgrex.query!(
        pid,
        "CREATE INDEX #{@table}_point ON #{@table} (tenant, waiting, resource, row_uuid)",
        []
      )

      prev_repo = Application.get_env(:reactive_dag, :repo)
      prev_table = Application.get_env(:reactive_dag, :suspension_table)
      Application.put_env(:reactive_dag, :repo, Repo)
      Application.put_env(:reactive_dag, :suspension_table, @table)

      on_exit(fn ->
        # A FRESH connection: the supervised one is already gone by the time
        # `on_exit` runs (the test supervisor tears down first), so reusing it
        # fails the whole module on cleanup even though every test passed.
        {:ok, cleanup} = Postgrex.start_link(url_opts(@url))
        Postgrex.query!(cleanup, "DROP TABLE IF EXISTS #{@table}", [])
        GenServer.stop(cleanup)

        restore(:repo, prev_repo)
        restore(:suspension_table, prev_table)
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

  defp point(opts \\ []) do
    %{
      tenant: Keyword.get(opts, :tenant, "red_hook"),
      waiting: Keyword.get(opts, :waiting, "meeting_events"),
      resource: Keyword.get(opts, :resource, "minutes_docs"),
      row_uuid: Keyword.get(opts, :row_uuid, "019a3f00-0000-7000-8000-00000000c21a")
    }
  end

  defp count do
    %{rows: [[n]]} = Repo.query!("SELECT COUNT(*) FROM #{@table}")
    n
  end

  describe "record/3, against real Postgres" do
    @describetag :real_postgres

    test "a suspension lands with every column the statement names" do
      if @url do
        id = Suspension.record(point(), "v-019a40", :expensive)

        %{rows: [row]} =
          Repo.query!(
            "SELECT id, tenant, waiting, resource, row_uuid, version_id, reason " <>
              "FROM #{@table}"
          )

        assert [^id, "red_hook", "meeting_events", "minutes_docs", _uuid, "v-019a40", "expensive"] =
                 row
      end
    end

    test "the SAME point twice writes TWO rows" do
      if @url do
        Suspension.record(point(), "v-1", :expensive)
        Suspension.record(point(), "v-2", :expensive)

        assert count() == 2,
               "suspensions are append-only — a unique constraint or ON CONFLICT " <>
                 "here would silently merge two real changes into one"
      end
    end

    test "an unattributable change stores the sentinel, not NULL" do
      if @url do
        Suspension.record(point(row_uuid: "*"), "*", :expensive)

        %{rows: [[row_uuid, version_id]]} =
          Repo.query!("SELECT row_uuid, version_id FROM #{@table}")

        assert {"*", "*"} == {row_uuid, version_id},
               "`*` must be storable — a uuid column would reject it, and NULL " <>
                 "would never match the equality `at/1` reads with"
      end
    end

    test "both reasons round-trip" do
      if @url do
        Suspension.record(point(), "v-1", :expensive)
        Suspension.record(point(waiting: "published"), "v-2", :approval)

        reasons = Enum.map(Suspension.at(point()), & &1.reason)
        assert reasons == [:expensive]

        assert [%{reason: :approval}] = Suspension.at(point(waiting: "published"))
      end
    end
  end

  describe "at/1, against real Postgres" do
    @describetag :real_postgres

    test "reads every suspension at the point" do
      if @url do
        Suspension.record(point(), "v-1", :expensive)
        Suspension.record(point(), "v-2", :expensive)

        assert ["v-1", "v-2"] == Enum.map(Suspension.at(point()), & &1.version_id)
      end
    end

    test "the feedback lap survives the round trip" do
      if @url do
        # The lap column is what bounds a loop through a suspending cell: the
        # count survives ONLY here, so a write that dropped it (or a read that
        # didn't return it) would silently unbound every declared loop. The
        # default matters too — a record with no lap is lap 0, not NULL.
        Suspension.record(point(), "v-1", :expensive)
        Suspension.record(point(), "v-2", :expensive, 7)

        assert [0, 7] == Enum.map(Suspension.at(point()), & &1.lap)
      end
    end

    test "orders by id, which is insertion order" do
      if @url do
        for v <- ["v-1", "v-2", "v-3", "v-4"] do
          Suspension.record(point(), v, :expensive)
          Process.sleep(2)
        end

        assert ["v-1", "v-2", "v-3", "v-4"] ==
                 Enum.map(Suspension.at(point()), & &1.version_id),
               "a resumption merging several versions must apply them in the " <>
                 "order the changes happened; UUIDv7 is what makes ORDER BY id mean that"
      end
    end

    test "another TENANT's suspension at the same point is invisible" do
      if @url do
        Suspension.record(point(tenant: "red_hook"), "mine", :expensive)
        Suspension.record(point(tenant: "other_village"), "theirs", :expensive)

        assert ["mine"] == Enum.map(Suspension.at(point(tenant: "red_hook")), & &1.version_id)

        assert ["theirs"] ==
                 Enum.map(Suspension.at(point(tenant: "other_village")), & &1.version_id)
      end
    end

    test "a different waiting resource is a different point" do
      if @url do
        Suspension.record(point(waiting: "meeting_events"), "a", :expensive)
        Suspension.record(point(waiting: "transcript_record"), "b", :expensive)

        assert ["a"] == Enum.map(Suspension.at(point(waiting: "meeting_events")), & &1.version_id)
      end
    end

    test "no suspensions is an empty list, not an error" do
      if @url do
        assert [] == Suspension.at(point()),
               "the ordinary outcome of a duplicate job — the work was already " <>
                 "done and discharged"
      end
    end
  end

  describe "discharge/1, against real Postgres" do
    @describetag :real_postgres

    test "removes exactly the named ids" do
      if @url do
        a = Suspension.record(point(), "v-1", :expensive)
        b = Suspension.record(point(), "v-2", :expensive)

        assert 2 == Suspension.discharge([a, b]),
               "string ids against a uuid column must actually match — a " <>
                 "mistyped ANY($1) deletes nothing and reports success"

        assert count() == 0
      end
    end

    test "a suspension written DURING the work survives its discharge" do
      if @url do
        # The job reads what is there...
        a = Suspension.record(point(), "v-1", :expensive)
        read = Suspension.at(point())
        assert length(read) == 1

        # ...a change lands while the nine-minute recompute runs...
        _b = Suspension.record(point(), "v-2", :expensive)

        # ...and the job discharges only what it read.
        Suspension.discharge(Enum.map(read, & &1.id))

        assert ["v-2"] == Enum.map(Suspension.at(point()), & &1.version_id),
               "THE property that justifies an append-only table: discharging " <>
                 "by point instead of by id would silently drop v-2, and nothing " <>
                 "would ever observe the loss"

        refute a in Enum.map(Suspension.at(point()), & &1.id)
      end
    end

    test "discharging twice removes nothing the second time" do
      if @url do
        a = Suspension.record(point(), "v-1", :expensive)

        assert 1 == Suspension.discharge([a])
        assert 0 == Suspension.discharge([a]), "a duplicate job must be a no-op"
      end
    end

    test "an empty list issues no statement and removes nothing" do
      if @url do
        Suspension.record(point(), "v-1", :expensive)
        assert 0 == Suspension.discharge([])
        assert count() == 1
      end
    end

    test "does not touch another tenant's rows" do
      if @url do
        mine = Suspension.record(point(tenant: "red_hook"), "mine", :expensive)
        Suspension.record(point(tenant: "other_village"), "theirs", :expensive)

        Suspension.discharge([mine])

        assert count() == 1
        assert ["theirs"] == Enum.map(Suspension.at(point(tenant: "other_village")), & &1.version_id)
      end
    end
  end

  describe "points/1 and pending?/1, against real Postgres" do
    @describetag :real_postgres

    test "aggregates a point's suspensions into one entry with a count" do
      if @url do
        Suspension.record(point(), "v-1", :expensive)
        Suspension.record(point(), "v-2", :expensive)
        Suspension.record(point(), "v-3", :expensive)

        assert [%{count: 3, reason: :expensive, point: p, oldest: %DateTime{}}] =
                 Suspension.points(tenant: "red_hook")

        assert p.waiting == "meeting_events"
      end
    end

    test "separates points, and separates reasons at one point" do
      if @url do
        Suspension.record(point(), "v-1", :expensive)
        Suspension.record(point(), "v-2", :approval)
        Suspension.record(point(waiting: "other"), "v-3", :expensive)

        entries = Suspension.points(tenant: "red_hook")
        assert length(entries) == 3
      end
    end

    test "is tenant-scoped" do
      if @url do
        Suspension.record(point(tenant: "red_hook"), "a", :expensive)
        Suspension.record(point(tenant: "other_village"), "b", :expensive)

        assert [_] = Suspension.points(tenant: "red_hook")
      end
    end

    test "pending? reflects outstanding work" do
      if @url do
        refute Suspension.pending?(tenant: "red_hook")

        id = Suspension.record(point(), "v-1", :expensive)
        assert Suspension.pending?(tenant: "red_hook")
        refute Suspension.pending?(tenant: "nobody")

        Suspension.discharge([id])
        refute Suspension.pending?(tenant: "red_hook")
      end
    end
  end

  describe "concurrency, without locks" do
    @describetag :real_postgres

    test "two connections record at the same point simultaneously" do
      if @url do
        # There is no unique constraint, so neither insert can violate one.
        # This is the behaviour that lets the design drop locking entirely.
        {:ok, other} = Postgrex.start_link(url_opts(@url))

        Suspension.record(point(), "v-1", :expensive)

        Postgrex.query!(
          other,
          "INSERT INTO #{@table} (id, tenant, waiting, resource, row_uuid, version_id, reason) " <>
            "VALUES (gen_random_uuid()::text, $1, $2, $3, $4, $5, $6)",
          ["red_hook", "meeting_events", "minutes_docs", point().row_uuid, "v-2", "expensive"]
        )

        GenServer.stop(other)

        assert count() == 2
      end
    end
  end

  describe "stranded/1 and revive/1, against real Postgres" do
    @describetag :real_postgres

    setup do
      if @url do
        Postgrex.query!(Repo.conn(), "DROP TABLE IF EXISTS #{@oban}", [])

        # Only the columns the two statements touch. `state` is text here
        # rather than Oban's enum: the queries compare it to a literal, so the
        # type is irrelevant to what is being tested, and the enum would need
        # its own CREATE TYPE.
        Postgrex.query!(
          Repo.conn(),
          """
          CREATE TABLE #{@oban} (
            id            bigserial PRIMARY KEY,
            worker        text NOT NULL,
            state         text NOT NULL,
            attempt       integer NOT NULL DEFAULT 0,
            max_attempts  integer NOT NULL DEFAULT 3,
            scheduled_at  timestamp NOT NULL DEFAULT now(),
            args          jsonb NOT NULL DEFAULT '{}'
          )
          """,
          []
        )

        prev = Application.get_env(:reactive_dag, :oban_table)
        Application.put_env(:reactive_dag, :oban_table, @oban)

        on_exit(fn ->
          if prev,
            do: Application.put_env(:reactive_dag, :oban_table, prev),
            else: Application.delete_env(:reactive_dag, :oban_table)
        end)
      end

      :ok
    end

    defp job!(state, attempt, max_attempts, args) do
      %{rows: [[id]]} =
        Postgrex.query!(
          Repo.conn(),
          "INSERT INTO #{@oban} (worker, state, attempt, max_attempts, args) " <>
            "VALUES ($1, $2, $3, $4, $5) RETURNING id",
          ["ReactiveDag.ResumptionWorker", state, attempt, max_attempts, args]
        )

      id
    end

    @tag :real_postgres
    test "finds the job that is `available` but can never be fetched" do
      if @url do
        point = %{
          "tenant" => "red_hook_village",
          "waiting" => "Derived.TranscriptRecord",
          "resource" => "Derived.TranscriptDocsLeaf",
          "row_uuid" => "_07132026-743"
        }

        stranded_id = job!("available", 3, 3, point)

        # Each of these is FETCHABLE or already visible as failed, so none is
        # stranded. Without them the query could be `state = 'available'` alone
        # and still pass.
        _fresh = job!("available", 0, 3, point)
        _mid_retry = job!("available", 1, 3, point)
        _discarded = job!("discarded", 3, 3, point)
        _executing = job!("executing", 3, 3, point)
        _retryable = job!("retryable", 2, 3, point)

        assert [found] = Suspension.stranded()
        assert found.job_id == stranded_id
        assert found.tenant == "red_hook_village"
        assert found.waiting == "Derived.TranscriptRecord"
        assert found.row_uuid == "_07132026-743"
      end
    end

    @tag :real_postgres
    test "a max_attempts: 1 worker strands on its FIRST failure" do
      # `ReprocessWorker` and `ScanWorker` both run `max_attempts: 1`, so
      if @url do
        # `attempt >= max_attempts` the moment one attempt is recorded — there is
        # no retry budget to absorb it.
        id = job!("available", 1, 1, %{})

        assert [%{job_id: ^id}] = Suspension.stranded()
      end
    end

    @tag :real_postgres
    test "revive/1 makes it fetchable, and is idempotent" do
      if @url do
        id = job!("available", 3, 3, %{})

        assert Suspension.revive() == [id]

        %{rows: [[attempt, max_attempts]]} =
          Postgrex.query!(
            Repo.conn(),
            "SELECT attempt, max_attempts FROM #{@oban} WHERE id = $1",
            [id]
          )

        # The fetch predicate is `attempt < max_attempts` (Oban.Engines.Basic),
        # so this is the assertion that matters: not that a column changed, but
        # that the job now satisfies the query that had been excluding it.
        assert attempt < max_attempts

        # Nothing is stranded any more, so a second pass is a no-op rather than
        # an ever-climbing max_attempts.
        assert Suspension.stranded() == []
        assert Suspension.revive() == []
      end
    end

    @tag :real_postgres
    test "revive/1 leaves a healthy job's budget alone" do
      if @url do
        id = job!("available", 0, 3, %{})

        assert Suspension.revive() == []

        %{rows: [[max_attempts]]} =
          Postgrex.query!(Repo.conn(), "SELECT max_attempts FROM #{@oban} WHERE id = $1", [id])

        assert max_attempts == 3
      end
    end

    @tag :real_postgres
    test "another worker's stranded job is not touched" do
      if @url do
        Postgrex.query!(
          Repo.conn(),
          "INSERT INTO #{@oban} (worker, state, attempt, max_attempts) VALUES ($1, $2, $3, $4)",
          ["MyApp.SomeOtherWorker", "available", 3, 3]
        )

        assert Suspension.stranded() == []
        assert Suspension.revive() == []
      end
    end
  end

  test "the suite says so when this file is inert" do
    unless @url do
      IO.puts(
        "\n  [real_postgres] skipped — set REACTIVE_DAG_TEST_DATABASE_URL to run " <>
          "the suspension table's SQL against a real database.\n"
      )
    end

    assert true
  end
end

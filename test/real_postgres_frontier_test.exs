defmodule ReactiveDag.RealPostgresFrontierTest do
  @moduledoc """
  The frontier's SQL, EXECUTED — not pattern-matched.

  Every other test in this suite hands the frontier a fake repo that matches on
  `"INSERT INTO " <> _` and interprets the parameters. That covers the library's
  logic and cannot cover the SQL itself, which is where three real bugs lived:

    * `?` is BOTH Postgres's jsonb-exists operator and Postgrex's parameter
      marker, so two `CASE` branches of an `ON CONFLICT` clause were silently
      dead;
    * `||` is right-biased, so a merge kept the incoming side where it needed the
      stored one — exactly the wrong end;
    * a column dropped from the schema but still named in the INSERT.

    Each was found by hand against a real database, after the whole suite passed.
  This closes that: the statements below are the ones `Frontier` issues, run
  against Postgres, asserting the behaviour the fakes only model.

  ## Opt-in

  Skipped unless `REACTIVE_DAG_TEST_DATABASE_URL` is set, so a checkout with no
  database still gets a green suite:

      REACTIVE_DAG_TEST_DATABASE_URL=postgres://cascade:cascade@localhost:5544/reactive_dag_test \\
        mix test test/real_postgres_frontier_test.exs

  The table is created and dropped per run, in its own schema, so it cannot touch
  a host's data.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.Frontier

  @url System.get_env("REACTIVE_DAG_TEST_DATABASE_URL")
  @table "rd_test_dirty"

  # A repo shim: the library calls `query!/2`, Postgrex offers `query!/3`.
  defmodule Repo do
    def query!(sql, params \\ []), do: Postgrex.query!(conn(), sql, params)
    def put_conn(pid), do: :persistent_term.put({__MODULE__, :conn}, pid)
    def conn, do: :persistent_term.get({__MODULE__, :conn})
  end

  setup_all do
    if @url do
      # `start_supervised!`, not `Postgrex.start_link`: a connection started here
      # is LINKED to the setup process, which exits as soon as setup returns —
      # taking the connection with it, and every test then fails on checkout with
      # `(EXIT) normal`. The test supervisor outlives the whole module.
      pid = start_supervised!({Postgrex, url_opts(@url)})
      Repo.put_conn(pid)

      Postgrex.query!(pid, "DROP TABLE IF EXISTS #{@table}", [])

      Postgrex.query!(
        pid,
        """
        CREATE TABLE #{@table} (
          cell_id text NOT NULL,
          tenant text NOT NULL DEFAULT '*',
          key text NOT NULL,
          reason text,
          enqueued_at timestamptz NOT NULL DEFAULT now(),
          awaiting_approval boolean,
          version_id text
        )
        """,
        []
      )

      # The uniqueness `ON CONFLICT` targets. Tenant first: every read is
      # tenant-scoped.
      Postgrex.query!(
        pid,
        "CREATE UNIQUE INDEX #{@table}_pkey ON #{@table} (tenant, cell_id, key)",
        []
      )

      prev_repo = Application.get_env(:reactive_dag, :repo)
      prev_table = Application.get_env(:reactive_dag, :dirty_table)
      Application.put_env(:reactive_dag, :repo, Repo)
      Application.put_env(:reactive_dag, :dirty_table, @table)

      on_exit(fn ->
        # A FRESH connection: the supervised one is already gone by the time
        # `on_exit` runs (the test supervisor tears down first), so reusing it
        # fails the whole module on cleanup even though every test passed.
        {:ok, cleanup} = Postgrex.start_link(url_opts(@url))
        Postgrex.query!(cleanup, "DROP TABLE IF EXISTS #{@table}", [])
        GenServer.stop(cleanup)

        restore(:repo, prev_repo)
        restore(:dirty_table, prev_table)
      end)

      :ok
    else
      :ok
    end
  end

  setup do
    if @url do
      Repo.query!("DELETE FROM #{@table}")
      :ok
    else
      :ok
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

  defp rows do
    %{rows: rows} =
      Repo.query!(
        "SELECT tenant, cell_id, key, version_id, awaiting_approval FROM #{@table} " <>
          "ORDER BY tenant, cell_id, key"
      )

    rows
  end

  describe "the INSERT and its ON CONFLICT, against real Postgres" do
    @describetag :real_postgres

    test "a mark lands with every column the statement names" do
      if @url do
        Frontier.mark_dirty("c1", [{"k1", "v-1"}], "because", tenant: "t1")

        assert [["t1", "c1", "k1", "v-1", nil]] = rows()
      end
    end

    test "re-marking the SAME unit coalesces to one row" do
      if @url do
        Frontier.mark_dirty("c1", [{"k1", "v-1"}], "first", tenant: "t1")
        Frontier.mark_dirty("c1", [{"k1", "v-2"}], "again", tenant: "t1")

        assert length(rows()) == 1, "ON CONFLICT (tenant, cell_id, key) must merge"
      end
    end

    test "coalescing keeps the EARLIEST version reference" do
      if @url do
        Frontier.mark_dirty("c1", [{"k1", "v-1"}], "first", tenant: "t1")
        Frontier.mark_dirty("c1", [{"k1", "v-2"}], "again", tenant: "t1")

        assert [[_t, _c, _k, "v-1", _held]] = rows(),
               "the earliest version records the change that succeeded the last " <>
                 "settled state — COALESCE(stored, incoming), not the reverse"
      end
    end

    test "a NULL stored version is filled by a later one" do
      if @url do
        Frontier.mark_dirty("c1", ["k1"], "no version", tenant: "t1")
        Frontier.mark_dirty("c1", [{"k1", "v-2"}], "now with one", tenant: "t1")

        assert [[_t, _c, _k, "v-2", _held]] = rows(),
               "COALESCE takes the incoming side only when the stored one is NULL"
      end
    end

    test "two TENANTS marking the same unit are two rows" do
      if @url do
        Frontier.mark_dirty("c1", ["k1"], "a", tenant: "t1")
        Frontier.mark_dirty("c1", ["k1"], "b", tenant: "t2")

        assert length(rows()) == 2,
               "a unique constraint blind to tenant makes the second mark raise"
      end
    end

    test "a batch of keys inserts one row each, parameters in the right order" do
      if @url do
        Frontier.mark_dirty("c1", [{"a", "v-a"}, {"b", "v-b"}, "c"], "batch", tenant: "t1")

        assert [
                 ["t1", "c1", "a", "v-a", nil],
                 ["t1", "c1", "b", "v-b", nil],
                 ["t1", "c1", "c", nil, nil]
               ] = rows(),
               "a multi-row VALUES list must not shear its parameters"
      end
    end
  end

  describe "the CLAIM, against real Postgres" do
    @describetag :real_postgres

    test "a claim consumes its own tenant's rows and returns their versions" do
      if @url do
        Frontier.mark_dirty("c1", [{"k1", "v-1"}], "a", tenant: "t1")
        Frontier.mark_dirty("c1", ["k2"], "b", tenant: "t2")

        assert [{"k1", "v-1"}] == Frontier.claim_with_diffs("c1", tenant: "t1")

        assert [["t2", "c1", "k2", nil, nil]] = rows(),
               "the other tenant's work must survive — a claim is a DELETE"
      end
    end

    test "a claim skips what awaits approval, and approving releases it" do
      if @url do
        Repo.query!(
          "INSERT INTO #{@table} (cell_id, tenant, key, awaiting_approval) VALUES ($1,$2,$3,true)",
          ["c1", "t1", "held"]
        )

        assert [] == Frontier.claim_with_diffs("c1", tenant: "t1")
        assert length(rows()) == 1, "a held mark is not lost, it is waiting"

        assert ["held"] == Frontier.approve("c1", :all, tenant: "t1")
        assert [{"held", nil}] = Frontier.claim_with_diffs("c1", tenant: "t1")
      end
    end

    test "rejecting discards the held mark" do
      if @url do
        Repo.query!(
          "INSERT INTO #{@table} (cell_id, tenant, key, awaiting_approval) VALUES ($1,$2,$3,true)",
          ["c1", "t1", "held"]
        )

        assert ["held"] == Frontier.reject("c1", :all, tenant: "t1")
        assert rows() == []
      end
    end

    test "dirty_cells sees only this tenant's claimable cells" do
      if @url do
        Frontier.mark_dirty("c1", ["k"], "a", tenant: "t1")
        Frontier.mark_dirty("c2", ["k"], "b", tenant: "t2")

        Repo.query!(
          "INSERT INTO #{@table} (cell_id, tenant, key, awaiting_approval) VALUES ($1,$2,$3,true)",
          ["c3", "t1", "held"]
        )

        assert Frontier.dirty_cells(tenant: "t1") == ["c1"],
               "a gated cell with nothing approved is not selectable, or the drain " <>
                 "would pick a cell it cannot claim from and loop"
      end
    end
  end

  test "the suite says so when this file is inert" do
    unless @url do
      IO.puts(
        "\n  [real_postgres] skipped — set REACTIVE_DAG_TEST_DATABASE_URL to run " <>
          "the frontier's SQL against a real database.\n"
      )
    end

    assert true
  end
end

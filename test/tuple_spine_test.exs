defmodule ReactiveDag.TupleSpineTest do
  @moduledoc """
  `ReactiveDag.Tuple` — the coordination spine, reduced to a PRESENCE SET.

  The spine used to carry `status` and freshness, because a tableless verdict
  node had nowhere else to put its answer. Results now live in each node's own
  resource, so what remains is the one question no resource can answer for
  itself: which keys does this cell hold, when the library cannot enumerate
  them? That is a source-fed leaf (no derived rows of its own) and a node whose
  `upsert:` writes somewhere the library never sees.

  These tests pin the SQL through a capturing repo. Two properties matter:

    * `put/3` writes ONLY spine columns. A host's extension columns
      (`source_ref`, `tombstoned_at`) must keep their DB defaults on insert and
      go untouched on update, or the library would be silently clobbering
      columns it does not own.
    * `put/3` accepts and IGNORES opts. The `CoordinationWriter` contract passes
      host fields through them, so a host that configures the default writer
      while passing `strength:` must not crash — it just has nowhere to put it.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.Tuple

  defmodule CapturingRepo do
    def query!(sql, params) do
      send(Process.get(:spine_test_pid), {:query, sql, params})
      %{rows: []}
    end
  end

  setup do
    Process.put(:spine_test_pid, self())
    prev = Application.get_env(:reactive_dag, :repo)
    Application.put_env(:reactive_dag, :repo, CapturingRepo)

    on_exit(fn ->
      if prev,
        do: Application.put_env(:reactive_dag, :repo, prev),
        else: Application.delete_env(:reactive_dag, :repo)
    end)

    :ok
  end

  defp captured do
    assert_received {:query, sql, params}
    {sql, params}
  end

  describe "put/3" do
    test "writes only the spine columns" do
      Tuple.put("cell", "k1")
      {sql, params} = captured()

      assert sql =~ "INSERT INTO reactive_dag_tuple (cell_id, key, updated_at)"
      assert ["cell", "k1", %DateTime{}] = params

      # the columns that used to be here are gone, not defaulted
      refute sql =~ "status"
      refute sql =~ "observed_at"
      refute sql =~ "stale_after"
    end

    test "is an upsert on (cell_id, key) — re-putting a key is idempotent" do
      Tuple.put("cell", "k1")
      {sql, _} = captured()

      assert sql =~ "ON CONFLICT (cell_id, key) DO UPDATE"
      # ONLY updated_at is touched, so a host's extension columns survive a re-put
      assert sql =~ "SET updated_at = EXCLUDED.updated_at"
      refute sql =~ ~r/SET.*,/s
    end

    test "accepts and ignores host opts — the writer contract passes them through" do
      # a host on the default writer may still pass extension fields; the spine
      # has nowhere to put them, and must not fail
      Tuple.put("cell", "k1", status: "failing", strength: "attested", source_ref: "abc")
      {sql, params} = captured()

      assert params == ["cell", "k1", List.last(params)]
      refute sql =~ "strength"
      refute sql =~ "source_ref"
    end
  end

  describe "the presence set" do
    test "all_keys/2 selects keys, with no status predicate" do
      Tuple.all_keys("cell")
      {sql, params} = captured()

      assert sql == "SELECT key FROM reactive_dag_tuple WHERE cell_id = $1"
      assert params == ["cell"]
    end

    test "delete/2 removes the named keys" do
      Tuple.delete("cell", ["k1", "k2"])
      {sql, params} = captured()

      assert sql =~ "DELETE FROM reactive_dag_tuple WHERE cell_id = $1 AND key = ANY($2)"
      assert params == ["cell", ["k1", "k2"]]
    end

    test "delete/2 on an empty list issues no query at all" do
      assert Tuple.delete("cell", []) == :ok
      refute_received {:query, _, _}
    end
  end

  describe "the status API is gone" do
    test "the functions that read a verdict no longer exist" do
      # each of these read a column the spine no longer has; a caller wanting a
      # node's statuses goes to ReactiveDag.Node.Rows, which reads the resource
      for {fun, arity} <- [
            put_changed: 3,
            present_keys: 2,
            rows: 2,
            keys_by_status: 3,
            status_histogram: 2,
            max_observed_at: 1
          ] do
        refute function_exported?(Tuple, fun, arity),
               "ReactiveDag.Tuple.#{fun}/#{arity} still exists"
      end
    end
  end

  describe "the default writer" do
    test "put/3 returns :ok, not a changed-signal" do
      # it used to compare the stored status; nothing consumed the answer, and
      # every caller now decides `changed?` against the node's own rows
      assert ReactiveDag.Tuple.Writer.put("cell", "k1", status: "failing") == :ok
      assert_received {:query, sql, _}
      refute sql =~ "SELECT status"
    end
  end

  describe "reconcile/3 — the job the spine kept" do
    test "retires what vanished and propagates both sides" do
      # a source-fed leaf: it scanned k1+k2, the spine says it held k1+k3, so k3
      # vanished. `current:` is passed explicitly here since the fake repo
      # returns no rows.
      {:ok, changed} =
        Tuple.reconcile("machines", ["k1", "k2"],
          upsert: fn key -> key == "k2" end,
          current: ["k1", "k3"]
        )

      # k2 changed on write; k3 vanished. k1 was re-seen unchanged.
      assert Enum.sort(changed) == ["k2", "k3"]

      assert_received {:query, sql, params}
      assert sql =~ "DELETE FROM"
      assert params == ["machines", ["k3"]]
    end

    test "a retire fun replaces deletion — the retain-if-vanish policy" do
      test_pid = self()

      {:ok, changed} =
        Tuple.reconcile("machines", ["k1"],
          upsert: fn _ -> false end,
          current: ["k1", "gone"],
          retire: fn keys -> send(test_pid, {:tombstoned, keys}) end
        )

      assert changed == ["gone"]
      assert_received {:tombstoned, ["gone"]}
      # ...and nothing was deleted
      refute_received {:query, "DELETE" <> _, _}
    end
  end
end

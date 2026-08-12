defmodule ReactiveDag.TupleKeyScopeTest do
  @moduledoc """
  Pins the SQL predicate each `key_scope` shape generates — text and param
  order — via a capturing repo.

  `key_scope` survived the spine's reduction to a presence set because it
  narrows the BASELINE a reconcile subtracts from: a host reconciling one
  tenant's slice must not retire every other tenant's keys. That makes these
  predicates load-bearing for correctness, not just for reads. (Executing them
  against live Postgres belongs to a host suite — this repo's CI has no
  database.)
  """
  use ExUnit.Case, async: false

  defmodule CapturingRepo do
    def query!(sql, params) do
      send(Process.get(:key_scope_test_pid), {:query, sql, params})
      %{rows: []}
    end
  end

  setup do
    Process.put(:key_scope_test_pid, self())
    prev = Application.get_env(:reactive_dag, :repo)
    Application.put_env(:reactive_dag, :repo, CapturingRepo)

    on_exit(fn ->
      if prev,
        do: Application.put_env(:reactive_dag, :repo, prev),
        else: Application.delete_env(:reactive_dag, :repo)
    end)

    :ok
  end

  defp issued_sql(key_scope) do
    ReactiveDag.Tuple.all_keys("cell", key_scope: key_scope)
    assert_received {:query, sql, params}
    {sql, params}
  end

  test "nil — no predicate" do
    {sql, params} = issued_sql(nil)
    refute sql =~ " AND "
    assert params == ["cell"]
  end

  test "{:prefix, p} — LIKE, pattern parameterized" do
    {sql, params} = issued_sql({:prefix, "app|%"})
    assert sql =~ " AND key LIKE $2"
    assert params == ["cell", "app|%"]
  end

  test "{:exact_or_prefix, k, p} — equality OR LIKE, both parameterized" do
    {sql, params} = issued_sql({:exact_or_prefix, "app", "app|%"})
    assert sql =~ " AND (key = $2 OR key LIKE $3)"
    assert params == ["cell", "app", "app|%"]
  end

  test "{:segment, i, sep, v} — split_part with the index CAST to int" do
    # the one non-text param: without ::int the driver/PG must infer
    # split_part's third argument type from a bare placeholder.
    {sql, params} = issued_sql({:segment, 2, "|", "a@x"})
    assert sql =~ " AND split_part(key, $2, $3::int) = $4"
    assert params == ["cell", "|", 2, "a@x"]
  end
end

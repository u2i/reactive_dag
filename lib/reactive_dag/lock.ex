defmodule ReactiveDag.Lock do
  @moduledoc """
  A cluster-wide advisory lock, for the one thing that still needs one.

  Propagation does not. A cascade takes no lock and a resumption takes no lock:
  at-least-once delivery over idempotent work is a stronger position than
  exactly-once scheduling, because it degrades gracefully rather than depending
  on job-state bookkeeping staying correct across a node death.

  What still needs a lock is EXTERNAL I/O. Two nodes sweeping the same upstreams
  in the same minute is duplicated fetching — someone else's server, someone
  else's rate limit — and no amount of idempotence downstream makes those
  requests not happen. That concern is unrelated to propagation, which is why
  this survived the queue it used to live in.

  A Postgres advisory lock because the requirement is exactly what they are for:
  cluster-wide, held on a connection, and released automatically if that
  connection dies — a node crashing mid-sweep does not leave the graph locked,
  which a lock table would.

  Returns `{:ok, result}`, or `:busy` when another node holds it. **Busy is not
  an error**: the other node is doing this node's work.

      case Lock.with_lock(fn -> sweep(plan) end, scope: tenant) do
        {:ok, result} -> result
        :busy -> :already_sweeping
      end
  """

  @spec with_lock((-> result), keyword()) :: {:ok, result} | :busy when result: term()
  def with_lock(fun, opts \\ []) when is_function(fun, 0) do
    # `Keyword.get/3`'s default only applies when the key is ABSENT, so an
    # explicit `scope: nil` would hash nil and silently move the lock for every
    # caller that passes the option through. nil means "no scope" here.
    key = :erlang.phash2(Keyword.get(opts, :scope) || "reactive_dag")

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

  defp query!(sql, params), do: repo().query!(sql, params)

  defp repo do
    Application.get_env(:reactive_dag, :repo) ||
      raise "reactive_dag: set `config :reactive_dag, repo: MyApp.Repo`"
  end
end

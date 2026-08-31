defmodule ReactiveDag.Test.FakeSuspensionRepo do
  @moduledoc """
  ONE in-memory stand-in for the suspension table, and the transaction
  primitives around it.

  It replaces `FakeFrontierRepo`, and is much smaller than it was: seven
  `query!` clauses become four, and the `ON CONFLICT` merge — its trickiest and
  most bug-prone part, where two of the three real SQL bugs lived — is simply
  gone. Suspensions are append-only, so there is nothing to merge.

  ## What this does NOT fix

  It still pattern-matches SQL rather than executing it, so it cannot see a bug
  INSIDE the SQL. That is `test/real_postgres_suspension_test.exs`'s job, and
  the reason that file exists: the first run of it caught a `uuid`-vs-`text`
  parameter mismatch that failed every insert, which this fake would have
  accepted happily.

  ## Transactions are real here

  `transaction/2` and `rollback/1` are implemented rather than absent, which
  matters more than it sounds. `Suspension.transaction/1` only wraps when the
  repo exports `transaction/2` — so a fake without it silently takes the
  no-transaction path, and a test asserting anything about transaction
  boundaries passes for the wrong reason.

  ## Why this ships in `lib/` rather than `test/support/`

  Because it is for HOSTS, and Hex does not package `test/support`. A dependent
  that wanted it had to hand-copy the file — which is exactly what makes a copy
  drift, and one already did: the frontier's INSERT grew a column and the
  copies broke three releases later, in a repo whose own tests had passed the
  whole time.

  Shipping it costs a module in the compiled artifact and removes a recurring
  failure mode for every dependent. It has no runtime callers and pulls in
  nothing.

  ## Use

      setup do
        start_supervised!(ReactiveDag.Test.FakeSuspensionRepo)
        ReactiveDag.Test.FakeSuspensionRepo.install()
      end
  """

  use Agent

  @doc false
  def child_spec(_opts), do: %{id: __MODULE__, start: {__MODULE__, :start_link, [[]]}}

  def start_link(_opts \\ []), do: Agent.start_link(fn -> [] end, name: __MODULE__)

  @doc """
  Point the library's repo at this fake for the duration of the test.
  """
  def install do
    prev = Application.get_env(:reactive_dag, :repo)
    Application.put_env(:reactive_dag, :repo, __MODULE__)

    ExUnit.Callbacks.on_exit(fn ->
      if prev,
        do: Application.put_env(:reactive_dag, :repo, prev),
        else: Application.delete_env(:reactive_dag, :repo)
    end)

    :ok
  end

  @doc "Every suspension recorded, oldest first."
  def recorded, do: Agent.get(__MODULE__, &Enum.reverse/1)

  @doc "Suspensions as `{waiting, resource, row_uuid}`, sorted — the usual assertion."
  def points do
    recorded()
    |> Enum.map(&{&1.waiting, &1.resource, &1.row_uuid})
    |> Enum.sort()
  end

  @doc "The distinct resources with work suspended."
  def waiting, do: recorded() |> Enum.map(& &1.waiting) |> Enum.uniq() |> Enum.sort()

  @doc "Drop everything — for a test wanting a known-empty state mid-run."
  def reset, do: Agent.update(__MODULE__, fn _ -> [] end)

  @doc "Whether anything is suspended."
  def any?, do: recorded() != []

  # ---- the repo surface the library calls ----

  def query!(sql, params \\ [])

  def query!("INSERT INTO " <> _, [id, tenant, waiting, resource, row_uuid, version_id, reason]) do
    Agent.update(
      __MODULE__,
      &[
        %{
          id: id,
          tenant: tenant,
          waiting: waiting,
          resource: resource,
          row_uuid: row_uuid,
          version_id: version_id,
          reason: reason
        }
        | &1
      ]
    )

    %{rows: [], num_rows: 1}
  end

  # `at/1` — every suspension at one point, oldest first.
  def query!("SELECT id, version_id, reason" <> _, [tenant, waiting, resource, row_uuid]) do
    rows =
      recorded()
      |> Enum.filter(
        &(&1.tenant == tenant and &1.waiting == waiting and &1.resource == resource and
            &1.row_uuid == row_uuid)
      )
      |> Enum.map(&[&1.id, &1.version_id, &1.reason])

    %{rows: rows, num_rows: length(rows)}
  end

  # `points/1` — one entry per (point, reason), with a count and the oldest.
  def query!("SELECT tenant, waiting, resource" <> _, [tenant]) do
    rows =
      recorded()
      |> Enum.filter(&(&1.tenant == tenant))
      |> Enum.group_by(&{&1.tenant, &1.waiting, &1.resource, &1.row_uuid, &1.reason})
      |> Enum.map(fn {{t, w, r, u, reason}, group} ->
        [t, w, r, u, reason, length(group), DateTime.utc_now()]
      end)

    %{rows: rows, num_rows: length(rows)}
  end

  # `pending?/1`
  def query!("SELECT COUNT" <> _, [tenant]) do
    n = Enum.count(recorded(), &(&1.tenant == tenant))
    %{rows: [[n]], num_rows: 1}
  end

  # `discharge/1` — BY ID, never by point. A suspension written while a job ran
  # is not in the id list and must survive; this fake models that faithfully
  # because it is the property the whole append-only design rests on.
  def query!("DELETE FROM " <> _, [ids]) when is_list(ids) do
    removed =
      Agent.get_and_update(__MODULE__, fn all ->
        {gone, kept} = Enum.split_with(all, &(&1.id in ids))
        {length(gone), kept}
      end)

    %{rows: [], num_rows: removed}
  end

  # Advisory locks, for `ReactiveDag.Lock`: a single-process fake never contends.
  def query!("SELECT pg_try_advisory_lock" <> _, _), do: %{rows: [[true]], num_rows: 1}
  def query!("SELECT pg_advisory_unlock" <> _, _), do: %{rows: [[true]], num_rows: 1}

  # ---- transactions ----

  @doc """
  A real transaction, flagging the process while inside.

  The flag is what lets a test assert that expensive work runs with NOTHING
  open — the property the whole redesign exists for, and one that cannot be
  checked against a repo that has no transactions at all.
  """
  def transaction(fun, _opts \\ []) do
    outer = Process.get(:rd_in_txn, false)
    Process.put(:rd_in_txn, true)

    try do
      {:ok, fun.()}
    catch
      :throw, {:rd_rollback, reason} -> {:error, reason}
    after
      Process.put(:rd_in_txn, outer)
    end
  end

  def rollback(reason), do: throw({:rd_rollback, reason})

  @doc "Whether the calling process is inside a fake transaction."
  def in_transaction?, do: Process.get(:rd_in_txn, false)
end

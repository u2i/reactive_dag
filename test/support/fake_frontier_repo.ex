defmodule ReactiveDag.Test.FakeFrontierRepo do
  @moduledoc """
  ONE in-memory stand-in for the frontier's repo, replacing 26 hand-written copies.

  Why it exists: every test that drains needs a repo, and each grew its own fake by
  pattern-matching the SQL and re-deriving the wire format — `chunk_every(7)` and
  the column order, spelled out 26 times. So a change to the queue's schema was a
  26-file edit, and each copy could drift from the real INSERT independently.

  ## What this does NOT fix

  It still pattern-matches SQL rather than executing it, so it cannot see a bug
  INSIDE the SQL. Three real ones got through the suite this way: `?` being both
  Postgres's jsonb-exists operator and Postgrex's parameter marker, `||` being
  right-biased so a merge kept the wrong end, and a dropped column. Each was found
  against real Postgres.

  What it does fix is the drift and the 26× cost. The semantics below are the
  library's own, written once:

    * `ON CONFLICT (tenant, cell_id, key)` — one row per unit of work per tenant;
    * the version reference is the EARLIEST (`COALESCE(stored, incoming)`), because
      that is the change which succeeded the last settled state;
    * a claim is a DELETE — consuming, and scoped to one tenant;
    * `awaiting_approval IS NOT TRUE` gates both the claim and `dirty_cells`, so a
      gated change waits for a person without blocking the drain.

  A test needing to observe something else (a partial write, a claim that fails
  mid-flight) should still write its own fake and say why.

  ## Use

      setup do
        start_supervised!(ReactiveDag.Test.FakeFrontierRepo)
        ReactiveDag.Test.FakeFrontierRepo.install()
      end

  `install/0` points `:reactive_dag, :repo` here and restores it on exit.
  """

  use Agent

  @columns [:cell_id, :tenant, :key, :reason, :enqueued_at, :awaiting_approval, :version_id]

  @doc false
  def child_spec(_opts), do: %{id: __MODULE__, start: {__MODULE__, :start_link, [[]]}}

  def start_link(_opts \\ []), do: Agent.start_link(fn -> %{} end, name: __MODULE__)

  @doc """
  Point the library's repo at this fake for the duration of the test.

  Call from `setup`; the previous value is restored on exit, so a test that runs
  against real Postgres in the same suite is unaffected.
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

  @doc "Every mark currently held, as `{tenant, cell_id, key}` tuples."
  def marks, do: Agent.get(__MODULE__, &Map.keys(&1))

  @doc "The full row for one mark, or nil."
  def mark(tenant, cell, key), do: Agent.get(__MODULE__, &Map.get(&1, {tenant, cell, key}))

  @doc """
  Held marks as `{cell_id, key}`, sorted — the assertion most tests actually make.

  Tenant-free on purpose: a test asserting WHICH work is queued does not care which
  tenant queued it, and the ones that do care use `tenants/0` or `marks/0`.
  """
  def dirty do
    Agent.get(__MODULE__, &Map.keys(&1))
    |> Enum.map(fn {_tenant, cell, key} -> {cell, key} end)
    |> Enum.sort()
  end

  @doc "The distinct tenants holding marks."
  def tenants do
    Agent.get(__MODULE__, &Map.keys(&1)) |> Enum.map(&elem(&1, 0)) |> Enum.uniq()
  end

  @doc "Drop everything — for a test that wants a known-empty frontier mid-run."
  def reset, do: Agent.update(__MODULE__, fn _ -> %{} end)

  # ---- the repo surface the library calls ----

  def query!(sql, params \\ [])

  def query!("INSERT INTO " <> _, params) do
    params
    |> Enum.chunk_every(length(@columns))
    |> Enum.each(fn values ->
      row = @columns |> Enum.zip(values) |> Map.new()
      Agent.update(__MODULE__, &upsert(&1, row))
    end)

    %{rows: [], num_rows: 0}
  end

  def query!("SELECT DISTINCT cell_id" <> _, [tenant]) do
    ids =
      Agent.get(__MODULE__, & &1)
      |> Enum.filter(fn {{t, _c, _k}, row} -> t == tenant and claimable?(row) end)
      |> Enum.map(fn {{_t, c, _k}, _row} -> c end)
      |> Enum.uniq()

    %{rows: Enum.map(ids, &[&1]), num_rows: length(ids)}
  end

  def query!("SELECT DISTINCT cell_id" <> _, _params) do
    ids = __MODULE__ |> Agent.get(& &1) |> Map.keys() |> Enum.map(&elem(&1, 1)) |> Enum.uniq()
    %{rows: Enum.map(ids, &[&1]), num_rows: length(ids)}
  end

  # REJECT: discard a held mark. `IS TRUE` distinguishes it from a claim, which
  # takes `IS NOT TRUE` — the params are identical, so only the SQL says which.
  def query!("DELETE FROM " <> _ = sql, params) when is_list(params) do
    cond do
      String.contains?(sql, "awaiting_approval IS TRUE") -> delete_held(params)
      true -> claim(params)
    end
  end


  def query!("SELECT COUNT" <> _, params) do
    n =
      case params do
        [tenant] ->
          Agent.get(__MODULE__, & &1)
          |> Enum.count(fn {{t, _c, _k}, row} -> t == tenant and claimable?(row) end)

        _ ->
          Agent.get(__MODULE__, &map_size(&1))
      end

    %{rows: [[n]], num_rows: 1}
  end

  # Advisory locks: a single-process fake never contends.
  def query!("SELECT pg_try_advisory_lock" <> _, _params), do: %{rows: [[true]], num_rows: 1}
  def query!("SELECT pg_advisory_unlock" <> _, _params), do: %{rows: [[true]], num_rows: 1}

  # APPROVE: clear the hold on every held mark, or on the named keys.
  def query!("UPDATE " <> _ = sql, [cell, tenant | rest]) do
    if String.contains?(sql, "awaiting_approval = NULL") do
      only = List.first(rest)

      cleared =
        Agent.get_and_update(__MODULE__, fn all ->
          {mine, others} =
            Enum.split_with(all, fn {{t, c, k}, row} ->
              t == tenant and c == cell and row[:awaiting_approval] == true and
                (is_nil(only) or k in only)
            end)

          freed = Enum.map(mine, fn {id, row} -> {id, %{row | awaiting_approval: nil}} end)
          {Enum.map(mine, fn {{_t, _c, k}, _row} -> [k] end), Map.new(others ++ freed)}
        end)

      %{rows: cleared, num_rows: length(cleared)}
    else
      %{rows: [], num_rows: 0}
    end
  end

  def query!("SELECT key" <> _, [cell, tenant]) do
    rows =
      Agent.get(__MODULE__, & &1)
      |> Enum.filter(fn {{t, c, _k}, row} ->
        t == tenant and c == cell and row[:awaiting_approval] == true
      end)
      |> Enum.map(fn {{_t, _c, k}, row} -> [k, row[:version_id]] end)

    %{rows: rows, num_rows: length(rows)}
  end

  defp delete_held([cell, tenant | rest]) do
    only = List.first(rest)

    taken =
      Agent.get_and_update(__MODULE__, fn all ->
        {mine, keep} =
          Enum.split_with(all, fn {{t, c, k}, row} ->
            t == tenant and c == cell and row[:awaiting_approval] == true and
              (is_nil(only) or k in only)
          end)

        {Enum.map(mine, fn {{_t, _c, k}, _row} -> [k] end), Map.new(keep)}
      end)

    %{rows: taken, num_rows: length(taken)}
  end

  # A CLAIM: consuming, one tenant, skipping what awaits approval.
  defp claim([cell, tenant]) do
    rows =
      Agent.get_and_update(__MODULE__, fn all ->
        {mine, rest} =
          Enum.split_with(all, fn {{t, c, _k}, row} ->
            t == tenant and c == cell and claimable?(row)
          end)

        {Enum.map(mine, fn {{_t, _c, k}, row} -> [k, row[:version_id]] end), Map.new(rest)}
      end)

    %{rows: rows, num_rows: length(rows)}
  end

  # ---- semantics ----

  # ON CONFLICT (tenant, cell_id, key): keep the EARLIEST version reference. It
  # records the change that succeeded the last settled state, which is the one a
  # consumer must reprice from — `COALESCE(stored.version_id, incoming)`.
  defp upsert(all, row) do
    id = {row[:tenant], row[:cell_id], row[:key]}

    case Map.get(all, id) do
      nil ->
        Map.put(all, id, row)

      stored ->
        Map.put(all, id, %{stored | version_id: stored[:version_id] || row[:version_id]})
    end
  end

  # NULL and FALSE alike mean claimable; only TRUE holds a change back.
  defp claimable?(row), do: row[:awaiting_approval] != true

end

defmodule ReactiveDag.Test.Pending do
  @moduledoc """
  What a test wants to change, remembered until it cascades.

  This exists because of a real asymmetry between the two engines, and it is
  worth naming rather than hiding.

  The drain read a QUEUE. A test could mark three cells in three places, at
  three different times, and then call `Drain.run/1` once — the queue was the
  memory between them. A cascade has no queue: it is told what changed, all of
  it, at the moment it runs.

  So a test that marked-then-drained needs somewhere to hold its origins in the
  meantime. That somewhere is here, per-process, rather than a queue table —
  which keeps the tests' shape close to what they had while being honest that
  the library no longer provides it.

  ## What this is not

  Not a queue the library uses. Nothing in `lib/` knows this exists. A host
  writes through `dirties_on`/`augmented_by` or a `Source` poll, each of which
  enqueues its own cascade at the moment of the write — there is no accumulate
  step in production.

  ## Use

      Pending.add("lines", ["l1"])
      Pending.add("rates", ["usd"])
      {:ok, report} = Pending.cascade(plan)
  """

  @key {__MODULE__, :origins}

  @doc """
  Remember that `keys` of `cell` changed.

  `opts` may carry `versions: %{key => version_id}` — what a real write records
  and what lets a downstream fold narrow to the units a change actually
  touched. `tenant:` is accepted and ignored here: a cascade takes its tenant
  from the plan, not from an individual origin.
  """
  def add(cell, keys, opts \\ []) do
    origin = %{
      cell: to_string(cell),
      keys: Enum.map(List.wrap(keys), &plain_key/1),
      versions: normalise_versions(keys, Keyword.get(opts, :versions, %{}))
    }

    Process.put(@key, Process.get(@key, []) ++ [origin])
    :ok
  end

  @doc """
  Cascade from everything remembered, and forget it.

  Returns `Cascade.run/3`'s `{:ok, report}`. Forgetting is what makes a second
  call in one test mean "what changed since", rather than replaying the first
  cascade's origins on top of the second's.
  """
  def cascade(plan, opts \\ []) do
    origins = Process.get(@key, [])
    Process.delete(@key)

    ReactiveDag.Cascade.run(plan, origins, opts)
  end

  @doc "Forget everything without cascading."
  def reset, do: Process.delete(@key)

  @doc """
  Capture what the library ENQUEUES, instead of reaching for Oban.

  `dirties_on` and `augmented_by` used to write a mark — a row a test could
  read back. They now enqueue a cascade, so a test that writes a record and
  asserts the graph noticed needs somewhere for that enqueue to land. This is
  it: `config :reactive_dag, cascade_enqueuer:` is the seam the library already
  provides for a host with its own queue.

  Call from `setup`; restored on exit.

      setup do: Pending.capture_enqueues()

      test "a write originates a cascade" do
        Ash.create!(Thing, %{key: "k1"})
        assert [{"things", ["k1"], _opts}] = Pending.enqueued()
      end
  """
  def capture_enqueues do
    prev = Application.get_env(:reactive_dag, :cascade_enqueuer)

    # `start_supervised!`, NOT `Agent.start_link`, and the difference is a
    # flake this suite has hit.
    #
    # `start_link` links the agent to the TEST process, which exits at the end
    # of the test — asynchronously. The next test calling this can reach
    # `start_link` before the name is released and get
    # `{:error, {:already_started, dying_pid}}`; the result was discarded here,
    # so the failure surfaced later as an `Agent.update` against a dead pid,
    # in whichever test happened to run next.
    #
    # Measured rather than assumed: starting a globally-named agent from a
    # short-lived process and immediately restarting it collides 2999 times out
    # of 3000. ExUnit's supervisor WAITS for the exit, so it cannot.
    #
    # The same reasoning is written into `op_tenant_test` and
    # `tenant_plan_test`, which hit this before.
    ExUnit.Callbacks.start_supervised!(%{
      id: __MODULE__.Enqueued,
      start: {Agent, :start_link, [fn -> [] end, [name: __MODULE__.Enqueued]]}
    })

    Application.put_env(:reactive_dag, :cascade_enqueuer, fn cell, keys, opts ->
      Agent.update(__MODULE__.Enqueued, &[{cell, keys, opts} | &1])
      {:ok, :captured}
    end)

    ExUnit.Callbacks.on_exit(fn ->
      if prev,
        do: Application.put_env(:reactive_dag, :cascade_enqueuer, prev),
        else: Application.delete_env(:reactive_dag, :cascade_enqueuer)
    end)

    :ok
  end

  @doc "What has been enqueued since `capture_enqueues/0`, oldest first."
  def enqueued do
    case Process.whereis(__MODULE__.Enqueued) do
      nil -> []
      _ -> Agent.get(__MODULE__.Enqueued, &Enum.reverse/1)
    end
  end

  @doc "Enqueued cells and keys as `{cell, keys}`, sorted — the usual assertion."
  def enqueued_work do
    enqueued() |> Enum.map(fn {cell, keys, _} -> {cell, Enum.sort(keys)} end) |> Enum.sort()
  end

  @doc "Forget captured enqueues."
  def reset_enqueued do
    case Process.whereis(__MODULE__.Enqueued) do
      nil -> :ok
      _ -> Agent.update(__MODULE__.Enqueued, fn _ -> [] end)
    end
  end

  @doc "What is currently remembered."
  def origins, do: Process.get(@key, [])

  defp plain_key({key, _version_id}), do: key
  defp plain_key(key), do: key

  # `add("c", [{"k", "v-1"}])` — the entry form a versioned write produces — is
  # accepted and split, so a test can use whichever shape reads better.
  defp normalise_versions(keys, base) do
    Enum.reduce(List.wrap(keys), base, fn
      {key, version_id}, acc when is_binary(version_id) -> Map.put(acc, key, version_id)
      _, acc -> acc
    end)
  end
end

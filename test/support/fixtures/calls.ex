defmodule ReactiveDag.Fixtures.Calls do
  @moduledoc """
  A recorder for "what did the computation actually get called with?"

  Shared, and in `test/support`, because fixture resources NAME it from inside
  their actions — a per-test spy cannot be reached from a module that was
  compiled before the test existed.

  The question it answers is the one most of these tests are really about: a
  fingerprint, a `compare` list or an invalidation is only doing its job if an
  unchanged row produces NO call. Counting rows cannot show that; only the
  call list can.

  Not started by default. A test that asserts on calls starts it and resets
  between tests; a fixture that runs without it records nothing and carries on,
  so one test's spy never leaks into another's run.
  """

  @doc false
  def start_link, do: Agent.start_link(fn -> [] end, name: __MODULE__)

  @doc "Record one call. A no-op when nothing is recording."
  def record(key) do
    if Process.whereis(__MODULE__), do: Agent.update(__MODULE__, &[key | &1])
    :ok
  end

  @doc "Everything recorded, oldest first."
  def all do
    if Process.whereis(__MODULE__), do: Agent.get(__MODULE__, &Enum.reverse/1), else: []
  end

  @doc "Forget what was recorded."
  def reset do
    if Process.whereis(__MODULE__), do: Agent.update(__MODULE__, fn _ -> [] end)
    :ok
  end
end

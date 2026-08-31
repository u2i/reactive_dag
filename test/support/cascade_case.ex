defmodule ReactiveDag.Test.CascadeCase do
  @moduledoc """
  The shape a test uses to drive the engine, in one place.

  Most tests in this suite are not about propagation at all — they are about
  node semantics (a fold's grain, a join's sides, a payload write) and used the
  drain as scaffolding to make a recompute happen. That scaffolding was two
  steps, `mark_dirty` then `Drain.run`, and it appeared in about thirty files.

  A cascade is told what changed, so those two steps become one. Putting it here
  rather than in each file means the next change to the engine's entry point is
  one edit, not thirty — which is exactly the lesson `FakeFrontierRepo` recorded
  when it replaced 26 hand-written fakes.

  ## Use

      use ReactiveDag.Test.CascadeCase

      test "a change reaches the fold" do
        {:ok, report} = cascade(plan, "lines", ["l1"])
        assert "rollup" in cells(report)
      end
  """

  defmacro __using__(_opts) do
    quote do
      alias ReactiveDag.Test.FakeSuspensionRepo
      import ReactiveDag.Test.CascadeCase

      setup do
        start_supervised!(FakeSuspensionRepo)
        FakeSuspensionRepo.install()
        :ok
      end
    end
  end

  @doc """
  Run a cascade from one cell's changed keys.

  The replacement for `mark_dirty(cell, keys); Drain.run(plan)`. `versions:`
  attaches a version per key, which is what lets a downstream fold narrow to the
  units a change actually touched.
  """
  def cascade(plan, cell, keys, opts \\ []) do
    {versions, opts} = Keyword.pop(opts, :versions, %{})

    ReactiveDag.Cascade.run(
      plan,
      [%{cell: to_string(cell), keys: List.wrap(keys), versions: versions}],
      opts
    )
  end

  @doc """
  Run a cascade from several origins at once — a scan that touched two leaves,
  or a diamond fed from both sides.
  """
  def cascade_many(plan, origins, opts \\ []) do
    ReactiveDag.Cascade.run(
      plan,
      Enum.map(origins, fn {cell, keys} ->
        %{cell: to_string(cell), keys: List.wrap(keys), versions: %{}}
      end),
      opts
    )
  end

  @doc """
  Resume a suspended cell, as `ReactiveDag.ResumptionWorker` would.

  `resuming:` is what allows a cell declaring `suspends` to run at all — without
  it the cascade suspends it again on sight.
  """
  def resume(plan, cell, keys, opts \\ []) do
    ReactiveDag.Cascade.run(
      plan,
      [%{cell: to_string(cell), keys: List.wrap(keys), versions: Keyword.get(opts, :versions, %{})}],
      Keyword.put(opts, :resuming, to_string(cell))
    )
  end

  @doc "The distinct cells a report recomputed, in first-touched order."
  def cells({:ok, report}), do: ReactiveDag.Report.cells(report)
  def cells(%ReactiveDag.Report{} = report), do: ReactiveDag.Report.cells(report)

  @doc "The keys a report says one cell claimed."
  def claimed({:ok, report}, cell), do: claimed(report, cell)

  def claimed(%ReactiveDag.Report{steps: steps}, cell) do
    steps
    |> Enum.filter(&(&1.cell == to_string(cell)))
    |> Enum.flat_map(& &1.claimed)
    |> Enum.sort()
  end

  @doc "The keys a report says one cell changed."
  def changed({:ok, report}, cell), do: changed(report, cell)

  def changed(%ReactiveDag.Report{steps: steps}, cell) do
    steps
    |> Enum.filter(&(&1.cell == to_string(cell)))
    |> Enum.flat_map(& &1.changed)
    |> Enum.sort()
  end
end

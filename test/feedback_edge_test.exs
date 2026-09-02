defmodule ReactiveDag.FeedbackEdgeTest do
  @moduledoc """
  A WORKED EXAMPLE of a feedback loop, and what the engine does with it today.

  ## The domain shape

  A municipal board's minutes contain the calendar for the year ahead. So future
  meetings are derived from past ones:

      meetings ──→ minutes ──→ schedule ──┐
         ↑                                 │
         └─────────── feedback ────────────┘

  A meeting's minutes announce meetings; those meetings are meetings. The loop is
  real in the graph and never real in TIME — a resolution passed at meeting M can
  only schedule meetings strictly after M — so nothing feeds itself. The graph
  flattens time away, which is the only reason it looks circular.

  ## Keys are UUIDs

  Every key here is a minted UUID, never a natural key. That is the point: a
  scheduled meeting is a meeting the moment it is announced, with an identity
  things can attach to months before any document exists. `@known` and `@future`
  stand in for two such ids.

  ## What these tests establish

  Read in order, they say: the engine already has the machinery for this, EXCEPT
  for the runaway guard, which a loop crossing a suspending cell escapes.

  A second hole — a scoped claim for a key with no row reporting itself changed
  forever — WAS a precondition for this feature and is now closed; see
  `retire_vanished_test.exs`, "a SCOPED claim for a key with no row reports
  nothing". That one mattered most because an identity-keyed loop could not
  terminate while it stood.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.Cascade

  # Minted, not derived. Fixed literals so failures are readable.
  @known "018f2a10-0000-7000-8000-000000000001"
  @future "018f2a10-0000-7000-8000-000000000002"

  defmodule FakeRepo do
    use Agent

    def child_spec(_), do: %{id: __MODULE__, start: {__MODULE__, :start_link, [[]]}}
    def start_link(_ \\ []), do: Agent.start_link(fn -> [] end, name: __MODULE__)

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

    def query!("INSERT INTO " <> _, _params), do: %{rows: [], num_rows: 1}
    def query!("SELECT" <> _, _params), do: %{rows: [], num_rows: 0}
    def query!(_sql, _params), do: %{rows: [], num_rows: 0}
    def transaction(fun), do: {:ok, fun.()}
  end

  defmodule Ran do
    def child_spec(_), do: %{id: __MODULE__, start: {__MODULE__, :start_link, [[]]}}
    def start_link(_ \\ []), do: Agent.start_link(fn -> [] end, name: __MODULE__)
    def note(id, keys), do: Agent.update(__MODULE__, &[{id, Enum.sort(keys)} | &1])
    def all, do: Agent.get(__MODULE__, &Enum.reverse/1)
    def count(id), do: all() |> Enum.count(fn {c, _} -> c == id end)
  end

  defmodule PassThrough do
    @behaviour ReactiveDag.Op
    @impl true
    def recompute(cell, keys) do
      ReactiveDag.FeedbackEdgeTest.Ran.note(cell.id, keys)
      {:ok, keys}
    end
  end

  # The honest schedule: minutes for a meeting announce ONE future meeting, and
  # announce nothing on a second look. This is the monotone case — each pass
  # either adds a new id or reports nothing.
  defmodule AnnouncesOnce do
    @behaviour ReactiveDag.Op
    @impl true
    def recompute(cell, keys) do
      ReactiveDag.FeedbackEdgeTest.Ran.note(cell.id, keys)

      known = ReactiveDag.FeedbackEdgeTest.known()
      future = ReactiveDag.FeedbackEdgeTest.future()

      # Announced only when the KNOWN meeting's minutes moved. Asked about the
      # future meeting itself, there is nothing further to announce — which is
      # what closes the loop.
      if known in keys, do: {:ok, [future]}, else: {:ok, []}
    end
  end

  # The dishonest schedule: reports a change every time, whatever it is asked.
  # An op like this cannot converge, and is what the guard must catch.
  defmodule AnnouncesForever do
    @behaviour ReactiveDag.Op
    @impl true
    def recompute(cell, keys) do
      ReactiveDag.FeedbackEdgeTest.Ran.note(cell.id, keys)
      {:ok, [ReactiveDag.FeedbackEdgeTest.future()]}
    end
  end

  def known, do: @known
  def future, do: @future

  setup do
    start_supervised!(Ran)
    start_supervised!(FakeRepo)
    FakeRepo.install()
    :ok
  end

  defp cell(id, inputs, meta \\ %{}) do
    %ReactiveDag.Cell{
      id: id,
      op: :test,
      inputs: inputs,
      leaf?: inputs == [],
      meta: meta
    }
  end

  # meetings ──→ minutes ──→ schedule ──(feedback)──→ meetings
  #
  # `feedback_inputs` is the declaration the library does not yet honour. It is
  # written here as the proposed shape so the tests below say exactly what is
  # missing.
  defp looped_plan(schedule_op) do
    ReactiveDag.Graph.build([
      cell("docs", []),
      cell("meetings", ["docs", "schedule"], %{
        compute: PassThrough,
        feedback_inputs: ["schedule"]
      }),
      cell("minutes", ["meetings"], %{compute: PassThrough}),
      cell("schedule", ["minutes"], %{compute: schedule_op})
    ])
  end

  describe "the loop cannot be expressed today" do
    test "a back-edge raises at assembly, however it is declared" do
      # THE GAP. `build_depths` is a DFS with an on-stack set (`graph.ex:177-183`)
      # and does not consult `feedback_inputs`, so the declaration is inert and
      # the plan cannot be built at all.
      assert_raise ArgumentError, ~r/cycle at cell/, fn ->
        looped_plan(AnnouncesOnce)
      end
    end

    test "the same shape WITHOUT the back-edge assembles, so only that edge is at issue" do
      plan =
        ReactiveDag.Graph.build([
          cell("docs", []),
          cell("meetings", ["docs"], %{compute: PassThrough}),
          cell("minutes", ["meetings"], %{compute: PassThrough}),
          cell("schedule", ["minutes"], %{compute: AnnouncesOnce})
        ])

      assert plan.depths["meetings"] == 1
      assert plan.depths["schedule"] == 3
    end

    @tag :skip
    test "TODO depth comes from the NON-feedback inputs" do
      # THE TARGET. `meetings` must be ordered by `docs` alone — depth 1, where a
      # canonical record belongs — with the schedule edge present but not
      # counted. Fails today at `Graph.build`, so it is skipped rather than
      # written against a graph missing the edge under test.
      plan = looped_plan(AnnouncesOnce)

      assert plan.depths["meetings"] == 1
      assert plan.depths["schedule"] == 3
      assert "schedule" in plan.cells["meetings"].inputs,
             "the edge must remain a real input: validated, readable, propagating"
    end
  end

  describe "convergence, on the machinery that already exists" do
    test "an honest schedule settles: no change reported means nothing propagates" do
      # The rule the whole design leans on (`cascade.ex:444-447`). Demonstrated
      # on a straight line, since the loop will not assemble: the schedule
      # announces once, and says nothing when asked about what it announced.
      plan =
        ReactiveDag.Graph.build([
          cell("docs", []),
          cell("minutes", ["docs"], %{compute: PassThrough}),
          cell("schedule", ["minutes"], %{compute: AnnouncesOnce})
        ])

      {:ok, report} = Cascade.run(plan, [%{cell: "docs", keys: [@known]}])

      sched = Enum.find(report.steps, &(&1.cell == "schedule"))
      assert sched.changed == [@future], "the first pass announces the future meeting"

      # A second cascade FROM that announcement reports nothing, so a loop would
      # close here rather than going round again.
      {:ok, again} = Cascade.run(plan, [%{cell: "minutes", keys: [@future]}])
      sched2 = Enum.find(again.steps, &(&1.cell == "schedule"))

      assert sched2.changed == [],
             "asked about what it just announced, an honest schedule adds nothing"
    end

    test "a dishonest schedule never settles — and this is what must be caught" do
      # `AnnouncesForever` reports the same key changed whatever it is asked, so
      # each pass re-dirties its own consumer. On a straight line that is one
      # wasted step; around a loop it is unbounded.
      plan =
        ReactiveDag.Graph.build([
          cell("docs", []),
          cell("minutes", ["docs"], %{compute: PassThrough}),
          cell("schedule", ["minutes"], %{compute: AnnouncesForever})
        ])

      {:ok, report} = Cascade.run(plan, [%{cell: "docs", keys: [@known]}])
      sched = Enum.find(report.steps, &(&1.cell == "schedule"))

      assert sched.changed == [@future],
             "it reports a change unconditionally — the property no guard can infer"
    end
  end

  describe "the holes a review found, stated as tests" do
    @tag :skip
    test "TODO a loop crossing a suspending cell escapes the runaway guard" do
      # `max_steps` (`cascade.ex:326-341`) is per-cascade. A loop through a cell
      # declaring `suspends true` stops, commits, and ends the cascade CLEANLY;
      # the resumption starts a fresh one with a fresh counter
      # (`resumption_worker.ex:176`). So an oscillating loop trips neither
      # `max_steps` nor any per-cascade budget — it becomes an unbounded chain of
      # individually-successful jobs flipping the same rows.
      #
      # That looks exactly like normal operation, which is the failure class this
      # engine should least tolerate. The durable detector is `derived_row_versions`:
      # one row per REAL change, so a key accruing many versions in a window is
      # oscillation, visible across cascades and restarts.
      flunk("unimplemented")
    end
  end
end

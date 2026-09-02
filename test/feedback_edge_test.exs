defmodule ReactiveDag.FeedbackEdgeTest do
  @moduledoc """
  The `feedback` edge — a WORKED EXAMPLE of a declared loop, and the bounds
  that keep it honest.

  ## The domain shape

  A municipal board's minutes contain the calendar for the year ahead. So future
  meetings are derived from past ones:

      meetings ──→ minutes ──→ schedule ──┐
         ↑                                 │
         └─────────── feedback ────────────┘

  A meeting's minutes announce meetings; those meetings are meetings — same
  table, same identity. The loop is real in the graph and never real in TIME: a
  resolution passed at meeting M can only schedule meetings strictly after M,
  so nothing feeds itself. The graph flattens time away, which is the only
  reason it looks circular.

  ## Keys are UUIDs

  Every key here is a minted UUID, never a natural key. That is the point: a
  scheduled meeting is a meeting the moment it is announced, with an identity
  things can attach to months before any document exists. `@known` and `@future`
  stand in for two such ids.

  ## What these tests establish

    * the DECLARATION: a `feedback` edge assembles, takes no part in depth
      ordering, and is otherwise a real input — validated, propagating. An
      UNDECLARED back-edge still fails assembly, and a declared one that closes
      no real cycle is refused: it would leave an input silently unordered.
    * CONVERGENCE: an honest loop settles by itself, because a recompute
      reporting no change propagates nothing (`cascade.ex`, unconditional).
    * the TWO BOUNDS on a dishonest loop:
        - in one cascade, a key may cross a feedback edge `max_feedback_passes`
          times before `RunawayError` names the edge and the key;
        - ACROSS cascades — a loop through a `suspends` cell ends every cascade
          cleanly, so no in-memory counter survives — the lap count rides on
          the suspension rows (`lap` column) and `max_feedback_laps` bounds the
          chain of otherwise individually-successful resumption jobs.

  A precondition — a SCOPED claim for a key with no row reporting itself
  changed forever — was closed before this feature landed; see
  `retire_vanished_test.exs`, "a SCOPED claim for a key with no row reports
  nothing". An identity-keyed loop could not terminate while it stood.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.Cascade
  alias ReactiveDag.Test.FakeSuspensionRepo

  # Minted, not derived. Fixed literals so failures are readable.
  @known "018f2a10-0000-7000-8000-000000000001"
  @future "018f2a10-0000-7000-8000-000000000002"

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
  # An op like this cannot converge, and is what the bounds must catch.
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
    start_supervised!(FakeSuspensionRepo)
    FakeSuspensionRepo.install()
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
  # `suspend_minutes?` puts a `suspends` declaration on the loop's middle cell,
  # which is the motivating production shape: the extraction of minutes from a
  # document is the expensive step.
  defp looped_plan(schedule_op, suspend_minutes? \\ false) do
    minutes_meta =
      if suspend_minutes?,
        do: %{compute: PassThrough, suspends: %{expensive: []}},
        else: %{compute: PassThrough}

    ReactiveDag.Graph.build([
      cell("docs", []),
      cell("meetings", ["docs", "schedule"], %{
        compute: PassThrough,
        feedback_inputs: ["schedule"]
      }),
      cell("minutes", ["meetings"], minutes_meta),
      cell("schedule", ["minutes"], %{compute: schedule_op})
    ])
  end

  # The worker test hands its plan over as an MFA, the way a real Oban job does.
  def worker_plan, do: looped_plan(AnnouncesForever, true)

  describe "the declaration" do
    test "an UNDECLARED back-edge still raises at assembly" do
      # `build_depths` is a DFS with an on-stack set, and only a declared
      # feedback edge is excluded from it — so an accidental cycle fails
      # exactly as before.
      assert_raise ArgumentError, ~r/cycle at cell/, fn ->
        ReactiveDag.Graph.build([
          cell("docs", []),
          cell("meetings", ["docs", "schedule"], %{compute: PassThrough}),
          cell("minutes", ["meetings"], %{compute: PassThrough}),
          cell("schedule", ["minutes"], %{compute: AnnouncesOnce})
        ])
      end
    end

    test "depth comes from the NON-feedback inputs" do
      # THE TARGET. `meetings` is ordered by `docs` alone — depth 1, where a
      # canonical record belongs — with the schedule edge present but not
      # counted.
      plan = looped_plan(AnnouncesOnce)

      assert plan.depths["meetings"] == 1
      assert plan.depths["schedule"] == 3

      assert "schedule" in plan.cells["meetings"].inputs,
             "the edge must remain a real input: validated, readable, propagating"

      # …and PROPAGATING is literal: a schedule change claims meetings. This is
      # the difference from a `context` edge, which is excluded from parents.
      assert [{"meetings", [@future]}] = ReactiveDag.Graph.claims_for(plan, "schedule", [@future])
    end

    test "a feedback edge that closes no real cycle is refused" do
      # `minutes` is UPSTREAM of `schedule`, not derived from it. Accepting the
      # declaration would leave the input unordered — schedule could run before
      # minutes settles, silently reading stale rows — so it fails at assembly.
      assert_raise ArgumentError, ~r/closes no cycle/, fn ->
        ReactiveDag.Graph.build([
          cell("docs", []),
          cell("meetings", ["docs"], %{compute: PassThrough}),
          cell("minutes", ["meetings"], %{compute: PassThrough}),
          cell("schedule", ["minutes"], %{
            compute: AnnouncesOnce,
            feedback_inputs: ["minutes"]
          })
        ])
      end
    end

    test "feedback naming a non-input is refused" do
      # Only reachable on a hand-built cell (the DSL emits the input and the
      # meta from one entity) — but inert protection must not read as
      # protection, so the drift is loud.
      assert_raise ArgumentError, ~r/not one of its inputs/, fn ->
        ReactiveDag.Graph.build([
          cell("docs", []),
          cell("meetings", ["docs"], %{
            compute: PassThrough,
            feedback_inputs: ["schedule"]
          }),
          cell("minutes", ["meetings"], %{compute: PassThrough}),
          cell("schedule", ["minutes"], %{compute: AnnouncesOnce})
        ])
      end
    end
  end

  describe "convergence" do
    test "an honest loop settles: the second trip around reports nothing" do
      # The whole feature in one cascade: docs move for the known meeting, its
      # minutes announce the future one, the announcement travels the back-edge
      # and lands as a REAL meeting — and the loop closes because the schedule,
      # asked about the meeting it just announced, has nothing to add.
      plan = looped_plan(AnnouncesOnce)

      {:ok, report} = Cascade.run(plan, [%{cell: "docs", keys: [@known]}])

      # meetings ran twice: once for the known id (from docs), once for the
      # future id (over the feedback edge). Same cell, both identities real.
      assert Ran.count("meetings") == 2
      assert {"meetings", [@future]} in Ran.all()

      # the schedule's second run — asked about @future — announced nothing,
      # which is what ended the walk. No runaway, no budget consumed.
      schedule_runs = Enum.filter(report.steps, &(&1.cell == "schedule"))
      assert [%{changed: [@future]}, %{changed: []}] = schedule_runs
    end

    test "a straight line still shows the rule the design leans on" do
      # No loop at all: a recompute reporting no change propagates nothing.
      # Kept from the original worked example because everything above rests
      # on it staying unconditional.
      plan =
        ReactiveDag.Graph.build([
          cell("docs", []),
          cell("minutes", ["docs"], %{compute: PassThrough}),
          cell("schedule", ["minutes"], %{compute: AnnouncesOnce})
        ])

      {:ok, report} = Cascade.run(plan, [%{cell: "docs", keys: [@known]}])

      sched = Enum.find(report.steps, &(&1.cell == "schedule"))
      assert sched.changed == [@future], "the first pass announces the future meeting"

      {:ok, again} = Cascade.run(plan, [%{cell: "minutes", keys: [@future]}])
      sched2 = Enum.find(again.steps, &(&1.cell == "schedule"))

      assert sched2.changed == [],
             "asked about what it just announced, an honest schedule adds nothing"
    end

    test "a dishonest loop fails fast, naming the edge and the key" do
      # `AnnouncesForever` reports the same key changed on every pass, so the
      # loop cannot settle. The per-(edge, key) budget catches it in a handful
      # of steps — not 100,000 — and the error names what a fix needs: which
      # edge, which key, and that the op's change reporting is the defect.
      plan = looped_plan(AnnouncesForever)

      err =
        assert_raise Cascade.RunawayError, fn ->
          Cascade.run(plan, [%{cell: "docs", keys: [@known]}])
        end

      assert err.message =~ "feedback edge schedule → meetings"
      assert err.message =~ @future
      assert err.message =~ "max_feedback_passes"

      # the partial report is attached, and shows the loop's cells spinning
      spinning = err.report.steps |> Enum.map(& &1.cell) |> Enum.uniq() |> Enum.sort()
      assert ["docs", "meetings", "minutes", "schedule"] == spinning

      # a budget of 3 means the whole failure cost ~a dozen steps
      assert length(err.report.steps) < 20
    end

    test "the budget never binds on a monotone loop, however small" do
      # The honest loop above crossed the edge once per key. Run it again with
      # the tightest possible budget to pin that the bound is per (edge, KEY):
      # a loop minting new keys each pass must never be charged for them.
      plan = looped_plan(AnnouncesOnce)

      {:ok, _} =
        Cascade.run(plan, [%{cell: "docs", keys: [@known]}], max_feedback_passes: 1)
    end
  end

  # the suspension tests hand the cascade a no-op scheduler: there is no Oban
  # here, and the default scheduler's failure warning is noise, not signal.
  def no_scheduler(_point, _opts), do: :ok

  describe "the loop through a suspension" do
    # This is THE HOLE the per-cascade budget cannot see: a loop crossing a
    # `suspends` cell stops there, commits, and ends the cascade CLEANLY. The
    # resumption starts a fresh cascade with a fresh step counter, so an
    # oscillating loop becomes an unbounded chain of individually-successful
    # jobs flipping the same rows — indistinguishable from normal operation.
    # The lap count on the suspension row is what survives the chain.

    test "work that never crossed the back-edge suspends at lap 0" do
      plan = looped_plan(AnnouncesForever, true)

      {:ok, report} =
        Cascade.run(plan, [%{cell: "docs", keys: [@known]}], resumption_scheduler: &__MODULE__.no_scheduler/2)

      assert [%{reason: :expensive}] = report.suspended
      assert [%{waiting: "minutes", row_uuid: @known, lap: 0}] = FakeSuspensionRepo.recorded()
    end

    test "a resumption whose work comes back around the loop suspends one lap deeper" do
      plan = looped_plan(AnnouncesForever, true)

      # As `ResumptionWorker` would run it: `resuming:` lets the suspended cell
      # itself recompute, and `feedback_lap:` is the count read off the
      # suspensions being cleared.
      {:ok, report} =
        Cascade.run(plan, [%{cell: "minutes", keys: [@known]}],
          resuming: "minutes",
          feedback_lap: 3,
          resumption_scheduler: &__MODULE__.no_scheduler/2
        )

      # the walk went minutes → schedule → (feedback) → meetings → minutes,
      # and stopped at the suspension again — one full lap.
      assert [%{waiting: "minutes"}] = report.suspended
      assert [%{row_uuid: @future, lap: 4}] = FakeSuspensionRepo.recorded()
    end

    test "an honest loop resumes and settles with nothing left suspended" do
      plan = looped_plan(AnnouncesOnce, true)

      {:ok, report} =
        Cascade.run(plan, [%{cell: "minutes", keys: [@future]}],
          resuming: "minutes",
          feedback_lap: 1
        )

      # asked about the future meeting, the schedule announced nothing, so the
      # loop never re-reached the suspending cell: no suspension, no lap.
      assert report.suspended == []
      assert FakeSuspensionRepo.recorded() == []
    end

    test "the lap budget turns an unbounded chain into one loud failure" do
      plan = looped_plan(AnnouncesForever, true)

      err =
        assert_raise Cascade.RunawayError, fn ->
          Cascade.run(plan, [%{cell: "minutes", keys: [@known]}],
            resuming: "minutes",
            feedback_lap: 20
          )
        end

      assert err.message =~ "lap"
      assert err.message =~ "max_feedback_laps"

      # raised BEFORE recording, so the over-budget suspension does not land —
      # the cascade rolls back, the job fails, and the PREVIOUS lap's rows
      # (discharged only on success) remain as durable evidence.
      assert FakeSuspensionRepo.recorded() == []
    end

    test "a suspending cell that FEEDS the back-edge itself still advances the lap" do
      # The resumed cell's own propagation runs BEFORE the walk begins
      # (`run_outside_transaction/6`), so its crossing is the one the in-walk
      # counter never sees. If that crossing didn't mark its origins, a loop
      # whose suspending cell sits at the tail — schedule itself expensive,
      # feeding meetings directly — would reset its lap count every resumption.
      plan =
        ReactiveDag.Graph.build([
          cell("docs", []),
          cell("meetings", ["docs", "schedule"], %{
            compute: PassThrough,
            feedback_inputs: ["schedule"]
          }),
          cell("minutes", ["meetings"], %{compute: PassThrough}),
          cell("schedule", ["minutes"], %{
            compute: AnnouncesForever,
            suspends: %{expensive: []}
          })
        ])

      {:ok, _} =
        Cascade.run(plan, [%{cell: "schedule", keys: [@known]}],
          resuming: "schedule",
          feedback_lap: 7,
          resumption_scheduler: &__MODULE__.no_scheduler/2
        )

      assert [%{waiting: "schedule", row_uuid: @future, lap: 8}] =
               FakeSuspensionRepo.recorded()
    end

    test "merged work is loop work if EITHER route to it was" do
      # Two origins land on the suspending cell and MERGE before it runs — one
      # plain, one already around the loop (`looped:` is how a resumption's own
      # propagation arrives, see `run_outside_transaction/6`). If "first
      # arrival wins" decided the flag, an oscillation could hide behind a
      # merge with ordinary work and reset its lap count every time.
      plan = looped_plan(AnnouncesOnce, true)

      {:ok, _} =
        Cascade.run(
          plan,
          [
            %{cell: "minutes", keys: [@known]},
            %{cell: "minutes", keys: [@future], looped: true}
          ],
          feedback_lap: 5,
          resumption_scheduler: &__MODULE__.no_scheduler/2
        )

      # one suspension per key, both on the merged work's lap
      assert [%{lap: 6}, %{lap: 6}] = FakeSuspensionRepo.recorded()
    end

    @tag capture_log: true
    test "ResumptionWorker reads the lap off the rows and hands it back" do
      # The full production chain, minus Oban's queue: a suspension recorded at
      # lap 2 is resumed by the worker, comes back around the loop, and lands
      # at lap 3. This is the handoff that makes the count durable — remove
      # `feedback_lap:` from the worker's opts and every resumption restarts
      # at lap 0.
      point = %{tenant: "*", waiting: "minutes", resource: "meetings", row_uuid: @future}
      ReactiveDag.Suspension.record(point, "*", :expensive, 2)

      job = %Oban.Job{
        args: %{
          "tenant" => "*",
          "waiting" => "minutes",
          "resource" => "meetings",
          "row_uuid" => @future,
          "plan_mfa" => ["ReactiveDag.FeedbackEdgeTest", "worker_plan", []]
        }
      }

      assert :ok = ReactiveDag.ResumptionWorker.perform(job)

      # the lap-2 row was discharged; the new suspension is one lap deeper.
      assert [%{waiting: "minutes", lap: 3}] = FakeSuspensionRepo.recorded()
    end
  end
end

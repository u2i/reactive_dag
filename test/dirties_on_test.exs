defmodule ReactiveDag.DirtiesOnTest do
  @moduledoc """
  `dirties_on` (#39): an ordinary Ash write triggers the cascade.

  Before this, a leaf only became dirty by a host calling
  `Frontier.mark_dirty/3` by hand at every write site, or by a `Source` poll —
  and a missed call is silent staleness, the same failure class as #37 from the
  other end.

  The mark runs as an `after_action` change, so it is INSIDE the write's
  transaction: a rolled-back write leaves no dirty key, and a committed one
  always leaves one. A notifier could not promise that — Ash dispatches
  notifications after commit, so a crash in between would lose the mark.
  """
  use ReactiveDag.PostgresCase

  alias ReactiveDag.{Cascade}
  alias ReactiveDag.Fixtures.DirtiesOnGraph

  alias ReactiveDag.Fixtures.DirtiesOnGraph.{
    Attestations,
    CategoryTotals,
    Expenses,
    Quiet,
    Rollups,
    TenantedExpenses
  }

  # Stands in for a host's version resource — `ash_paper_trail` with
  # `change_tracking_mode :full_diff` writes exactly this shape. In-memory here
  # because the point under test is the REFERENCE, not the storage.
  setup_all do
    migrate!(DirtiesOnGraph.Domain)
    :ok
  end

  setup do
    pid = Ecto.Adapters.SQL.Sandbox.start_owner!(ReactiveDag.TestRepo, shared: true)
    on_exit(fn -> Ecto.Adapters.SQL.Sandbox.stop_owner(pid) end)

    migrate_library_tables!()

    # A `dirties_on` write no longer marks a table a test can read back — it
    # enqueues a cascade. This is the seam that catches those enqueues, so the
    # assertions below can ask what the write ORIGINATED without an Oban.
    ReactiveDag.Test.Pending.capture_enqueues()

    # start each test from a known-empty state — including the frontier, which
    # the destroys below would otherwise re-dirty.
    Expenses |> Ash.read!() |> Enum.each(&Ash.destroy!/1)
    CategoryTotals |> Ash.read!() |> Enum.each(&Ash.destroy!/1)
    Rollups |> Ash.read!() |> Enum.each(&Ash.destroy!/1)

    # AFTER the cleanup destroys, which enqueue too — a test must not start life
    # holding the previous test's teardown.
    ReactiveDag.Test.Pending.reset_enqueued()
    ReactiveDag.Test.Pending.reset()
    :ok
  end

  test "a TENANTED write marks in its own tenant, read off the changeset" do
    # THE SILENT FAILURE this guards. `dirties_on` is an Ash change on a write,
    # so there is no plan in scope — the tenant can only come off the changeset,
    # which is where a tenanted write already put it. Marking untenanted would
    # name work that tenant's drain never reads, and a drain that finds nothing
    # reports SUCCESS: the write succeeds, a mark exists, nothing recomputes.
    TenantedExpenses
    |> Ash.Changeset.for_create(:create, %{key: "e1", amount: 1.0}, tenant: "org_a")
    |> Ash.create!()

    assert [{_cell, _keys, opts}] = ReactiveDag.Test.Pending.enqueued()

    assert Keyword.get(opts, :tenant) == "org_a",
           "the cascade must be enqueued under the tenant the write was made under"
  end

  test "an untenanted write carries NO tenant, so the plan's own tenant decides" do
    # This asserted the literal `"*"` sentinel, because the frontier stored the
    # tenant in a NOT NULL column and needed a value meaning "all". A cascade has
    # no such column: `nil` here means the enqueue names no tenant, and
    # `CascadeWorker` then passes no `tenant:` opt at all, leaving the plan's own
    # tenant in force. Same intent — an untenanted write is not silently
    # attributed to somebody else's tenant — against the representation that
    # replaced the sentinel.
    Expenses
    |> Ash.Changeset.for_create(:create, %{key: "e1", category: "x", amount: 1.0})
    |> Ash.create!()

    assert [{_cell, _keys, opts}] = ReactiveDag.Test.Pending.enqueued()
    assert Keyword.get(opts, :tenant) == nil
  end

  test "a CREATE marks the written record's key — no host wiring" do
    Expenses
    |> Ash.Changeset.for_create(:create, %{key: "e1", category: "travel", amount: 10.0})
    |> Ash.create!()

    assert ReactiveDag.Test.Pending.enqueued_work() == [{"expenses", ["e1"]}]
  end

  test "an UPDATE marks it too" do
    e =
      Expenses
      |> Ash.Changeset.for_create(:create, %{key: "e1", category: "travel", amount: 10.0})
      |> Ash.create!()

    # The create enqueued; clear it so what follows is the UPDATE's doing alone.
    # (Under the queue this read as "the mark is gone", because a claim consumed
    # it — enqueues only accumulate, so the test says so explicitly.)
    ReactiveDag.Test.Pending.reset_enqueued()

    e |> Ash.Changeset.for_update(:revise, %{amount: 20.0}) |> Ash.update!()
    assert ReactiveDag.Test.Pending.enqueued_work() == [{"expenses", ["e1"]}]
  end

  test "a DESTROY marks the vanished key — which is exactly what downstream needs" do
    e =
      Expenses
      |> Ash.Changeset.for_create(:create, %{key: "e1", category: "travel", amount: 10.0})
      |> Ash.create!()

    # Clear the create's enqueue so the assertion below is about the DESTROY.
    ReactiveDag.Test.Pending.reset_enqueued()

    Ash.destroy!(e)

    # the row is gone, but its key is what reprices the group it left
    assert ReactiveDag.Test.Pending.enqueued_work() == [{"expenses", ["e1"]}]
  end

  test "the key is the IDENTITY serialization for a composite primary key" do
    Rollups
    |> Ash.Changeset.for_create(:create, %{fund: "gf", fy: "2025", total: 1.0})
    |> Ash.create!()

    assert ReactiveDag.Test.Pending.enqueued_work() == [{"rollups", ["gf|2025"]}]
  end

  test "OPT-IN: a resource without `dirties_on` marks nothing" do
    Quiet |> Ash.Changeset.for_create(:create, %{key: "q1"}) |> Ash.create!()
    assert ReactiveDag.Test.Pending.enqueued_work() == []
  end

  test "only the DECLARED action types mark" do
    # Rollups declares `dirties_on [:create]` only, though it HAS a destroy
    r =
      Rollups
      |> Ash.Changeset.for_create(:create, %{fund: "w", fy: "2026", total: 1.0})
      |> Ash.create!()

    assert ReactiveDag.Test.Pending.enqueued_work() == [{"rollups", ["w|2026"]}]

    ReactiveDag.Test.Pending.reset_enqueued()

    # the destroy is not in the declared list, so it enqueues nothing
    Ash.destroy!(r)
    assert ReactiveDag.Test.Pending.enqueued_work() == []
  end

  test "end to end: writing an expense makes the next drain recompute its category" do
    plan = ReactiveDag.Node.graph([Expenses, CategoryTotals])

    # NO Frontier.mark_dirty call anywhere — the write is the trigger
    Expenses
    |> Ash.Changeset.for_create(:create, %{key: "e1", category: "travel", amount: 100.0})
    |> Ash.create!()

    {:ok, report} = drain(plan)

    steps = Map.new(report.steps, &{&1.cell, &1})
    assert steps["expenses"].claimed == ["e1"]

    # `"travel"`, not `"e1"`: the parent is handed the changed ROW plus its
    # version, resolves the version to a diff, and DERIVES its own unit from
    # it. Deriving beats mapping because a mapped key is a conclusion computed
    # before the parent ran — and a row that moved between categories needs
    # both sides, which only the diff has.
    assert steps["category_totals"].claimed == ["travel"]

    assert (CategoryTotals |> Ash.get!("travel")).total == 100.0
  end

  # The write is the trigger, and a write ENQUEUES. Replaying what the writes
  # enqueued as the cascade's origins is exactly what `CascadeWorker` does with
  # the job it dequeues — the test drives the same path by hand.
  defp drain(plan) do
    for {cell, keys, opts} <- ReactiveDag.Test.Pending.enqueued() do
      ReactiveDag.Test.Pending.add(cell, keys, versions: Keyword.get(opts, :versions, %{}))
    end

    ReactiveDag.Test.Pending.reset_enqueued()
    ReactiveDag.Test.Pending.cascade(plan)
  end

  describe "frontier snapshots (#60)" do
    # `dirties_on` records the row AS IT WAS at mark time, so a parent derives
    # its claim from what the row was — the only thing that survives a delete,
    # and the only thing that names where a moved row came from.
    test "WHEN the change happened rides on the enqueue" do
      Expenses
      |> Ash.Changeset.for_create(:create, %{key: "e1", category: "travel", amount: 10.0})
      |> Ash.create!()

      # A version id used to ride here — a reference to a row in a host-owned
      # table, resolved later through a declared MFA. Now the enqueue carries
      # WHEN, and a consumer that needs to narrow reads the row as of that
      # instant and diffs it against now. Nothing is stored, so nothing can be
      # unreadable: the old failure was a reference that could not be resolved
      # silently widening the claim to the whole cell.
      assert [{"expenses", ["e1"], opts}] = ReactiveDag.Test.Pending.enqueued()
      assert %{"e1" => %DateTime{} = at} = Keyword.fetch!(opts, :versions)

      # …and the history answers what moved. Reading just BEFORE the instant is
      # the whole trick: `recorded_at` is when the new period OPENS, so reading
      # at it returns the row after the change and the diff compares a row to
      # itself.
      before = Expenses |> Ash.Query.as_of(DateTime.add(at, -1, :microsecond)) |> Ash.read!()
      now = Expenses |> Ash.read!()

      assert before == [], "nothing existed before the create"
      assert [%{category: "travel", amount: 10.0}] = now
    end

    test "a DELETED row still names its unit — the claim stays precise" do
      plan = ReactiveDag.Node.graph([Expenses, CategoryTotals])

      Expenses
      |> Ash.Changeset.for_create(:create, %{key: "e1", category: "travel", amount: 100.0})
      |> Ash.create!()

      {:ok, _} = drain(plan)
      assert (CategoryTotals |> Ash.get!("travel")).total == 100.0

      Expenses |> Ash.get!("e1") |> Ash.destroy!()
      {:ok, report} = drain(plan)

      steps = Map.new(report.steps, &{&1.cell, &1})

      # a live lookup could not name the group of a row that is gone; the
      # snapshot can, so this is ["travel"] rather than the ["*"] degradation
      assert steps["category_totals"].claimed == ["travel"]
    end

    test "a MOVED row claims BOTH units — where it went AND where it came from" do
      plan = ReactiveDag.Node.graph([Expenses, CategoryTotals])

      Expenses
      |> Ash.Changeset.for_create(:create, %{key: "e1", category: "meals", amount: 40.0})
      |> Ash.create!()

      {:ok, _} = drain(plan)
      assert (CategoryTotals |> Ash.get!("meals")).total == 40.0

      # the live row says "travel"; only the snapshot says "meals", and meals is
      # the one that would otherwise silently keep counting a row it no longer has
      Expenses
      |> Ash.get!("e1")
      |> Ash.Changeset.for_update(:recategorise, %{category: "travel"})
      |> Ash.update!()

      {:ok, report} = drain(plan)
      steps = Map.new(report.steps, &{&1.cell, &1})

      assert Enum.sort(steps["category_totals"].claimed) == ["meals", "travel"]
      assert (CategoryTotals |> Ash.get!("travel")).total == 40.0
    end

    test "two writes before a cascade claim the LATEST destination" do
      plan = ReactiveDag.Node.graph([Expenses, CategoryTotals])

      Expenses
      |> Ash.Changeset.for_create(:create, %{key: "e1", category: "meals", amount: 40.0})
      |> Ash.create!()

      {:ok, _} = drain(plan)

      # two writes before the next cascade: meals -> travel -> lodging
      e = Expenses |> Ash.get!("e1")
      e = e |> Ash.Changeset.for_update(:recategorise, %{category: "travel"}) |> Ash.update!()
      e |> Ash.Changeset.for_update(:recategorise, %{category: "lodging"}) |> Ash.update!()

      {:ok, report} = drain(plan)
      claimed = Map.new(report.steps, &{&1.cell, &1})["category_totals"].claimed

      # THIS INVERTS what the queue did, and the inversion is the point.
      #
      # The queue coalesced on `(cell, key)` and kept the EARLIEST version, so
      # two writes before a drain left it pointing at the first change only:
      # `meals` and `travel` were claimed, and `lodging` — the row's actual
      # destination — went unclaimed until something touched it again. The old
      # test asserted that limitation rather than the behaviour anyone wanted.
      #
      # Suspensions never merge, and a cascade is told what changed rather than
      # reading an aged conclusion, so the LAST write is the one that
      # propagates. `lodging` is claimed and its total is right.
      assert "lodging" in claimed
      assert "travel" in claimed

      # `meals` is not claimed here, and does not need to be: the second write
      # already moved the row out of it and recomputed that unit.
      assert (CategoryTotals |> Ash.get!("lodging")).total == 40.0
    end
  end

  describe "originating IS enqueuing (#142)" do
    # WHAT THIS BLOCK USED TO ASSERT, and why it no longer can.
    #
    # Under the queue, a write did two things: mark the frontier, and — only if
    # every origination enqueues — there is no opt-in any more, so
    # consume that mark. A host that did the first and forgot the second got
    # durable staleness, and this block existed to pin the second step down:
    # it counted calls through a `:drain_enqueuer` seam and asserted the count
    # rose per write, that a destroy counted too, that a node NOT opting in
    # counted zero, and that an enqueuer returning `{:error, _}` still let the
    # write succeed.
    #
    # There is no second step now. `MarkDirty` enqueues the cascade directly and
    # `:drain_enqueuer` is read nowhere in `lib/`, so every one of those counts
    # would be zero forever — the tests would pass while asserting nothing. The
    # property worth keeping is the one that replaced them: a declared write
    # enqueues its OWN cascade, opt-in or not, and cannot forget to.

    test "a write enqueues its own cascade — there is no second step to forget" do
      Attestations
      |> Ash.Changeset.for_create(:create, %{key: "a1", verdict: "signed"})
      |> Ash.create!()

      assert {"attestations", ["a1"]} in ReactiveDag.Test.Pending.enqueued_work()
    end

    test "a destroy enqueues too — a removal is a change downstream" do
      rec =
        Attestations
        |> Ash.Changeset.for_create(:create, %{key: "a2", verdict: "signed"})
        |> Ash.create!()

      ReactiveDag.Test.Pending.reset_enqueued()
      Ash.destroy!(rec)

      assert {"attestations", ["a2"]} in ReactiveDag.Test.Pending.enqueued_work()
    end

    test "a node that never opted into scheduling enqueues all the same" do
      # Under the queue, a node that did not opt in meant
      # "mark, and wait for whatever drains next"; it now means nothing, because
      # marking and enqueuing are the same act.
      Expenses
      |> Ash.Changeset.for_create(:create, %{key: "e9", category: "meals", amount: 1.0})
      |> Ash.create!()

      assert {"expenses", ["e9"]} in ReactiveDag.Test.Pending.enqueued_work()
    end

    test "an enqueue failure does not fail the write" do
      # Still true, and still worth pinning — but the stakes moved. The mark was
      # durable on its own, so a failed enqueue only delayed things; the cascade
      # is now the whole act, so a failure LOSES it. What must not happen either
      # way is the host's write being rejected over it.
      Application.put_env(:reactive_dag, :cascade_enqueuer, fn _cell, _keys, _opts ->
        {:error, :oban_down}
      end)

      assert %{key: "a3"} =
               Attestations
               |> Ash.Changeset.for_create(:create, %{key: "a3", verdict: "signed"})
               |> Ash.create!()
    end
  end

  @doc false
  def schedule_plan, do: ReactiveDag.Node.graph([Attestations])
end

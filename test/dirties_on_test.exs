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
  use ExUnit.Case, async: false

  alias ReactiveDag.{Cascade}

  # Stands in for a host's version resource — `ash_paper_trail` with
  # `change_tracking_mode :full_diff` writes exactly this shape. In-memory here
  # because the point under test is the REFERENCE, not the storage.
  defmodule Versions do
    def start_link, do: Agent.start_link(fn -> %{} end, name: __MODULE__)

    def record(record, changeset) do
      id = "v-" <> to_string(System.unique_integer([:positive]))
      Agent.update(__MODULE__, &Map.put(&1, id, diff(changeset, record)))
      id
    end

    def changes(version_id), do: Agent.get(__MODULE__, &Map.get(&1, version_id))

    defp diff(%Ash.Changeset{action_type: :create}, record),
      do: Map.new(dump(record), fn {k, v} -> {k, %{"to" => v}} end)

    defp diff(%Ash.Changeset{data: %{__struct__: _} = data}, record) do
      was = dump(data)

      Map.new(dump(record), fn {k, v} ->
        case Map.fetch(was, k) do
          {:ok, ^v} -> {k, %{"unchanged" => v}}
          {:ok, old} -> {k, %{"from" => old, "to" => v}}
          :error -> {k, %{"to" => v}}
        end
      end)
    end

    defp diff(_changeset, record), do: Map.new(dump(record), fn {k, v} -> {k, %{"to" => v}} end)

    defp dump(record) do
      record.__struct__
      |> Ash.Resource.Info.public_attributes()
      |> Map.new(fn a -> {to_string(a.name), Map.get(record, a.name)} end)
    end
  end

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      allow_unregistered?(true)
    end
  end

  # Marks AND schedules. Separate from `Expenses` so the default (mark only)
  # stays under test — an existing host must not start enqueueing.
  defmodule Attestations do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :verdict, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :create do
        accept([:key, :verdict])
      end
    end

    reactive do
      id(:attestations)
      leaf?(true)
      dirties_on([:create, :destroy])
      schedule_drain(true)
      payload_key(:key)
    end
  end

  defmodule Expenses do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :category, :string, public?: true
      attribute :amount, :float, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :create do
        accept([:key, :category, :amount])
      end

      update :revise do
        accept([:amount])
      end

      update :recategorise do
        accept([:category])
      end
    end

    reactive do
      id(:expenses)
      op(:source)
      leaf?(true)
      # THE FEATURE: writes here mark this cell dirty, with no host wiring
      dirties_on([:create, :update, :destroy])

      # A queue row references the version recording the change; the consumer
      # reads it back to learn WHAT moved. Both halves are the host's, because a
      # version resource is — `ash_paper_trail` is the usual supplier.
      version_id({ReactiveDag.DirtiesOnTest.Versions, :record, []})
      version_diff({ReactiveDag.DirtiesOnTest.Versions, :changes, []})
    end
  end

  # a composite-PK resource: the cell key is the identity serialization
  defmodule Rollups do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute :fund, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :fy, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :total, :float, public?: true
    end

    actions do
      # :destroy exists for test cleanup, but is NOT in `dirties_on` below —
      # which is what the "only declared action types mark" test relies on.
      defaults [:read, :destroy]

      create :create do
        accept([:fund, :fy, :total])
      end
    end

    reactive do
      id(:rollups)
      op(:source)
      leaf?(true)
      dirties_on([:create])
    end
  end

  # opt-in: no `dirties_on`, so writes mark nothing
  defmodule Quiet do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
    end

    actions do
      defaults [:read]

      create :create do
        accept([:key])
      end
    end

    reactive do
      id(:quiet)
      op(:source)
      leaf?(true)
    end
  end

  defmodule CategoryTotals do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :category, :string, public?: true
      attribute :total, :float, public?: true
      attribute :n, :integer, public?: true
    end

    identities do
      identity :by_key, [:key], pre_check_with: ReactiveDag.DirtiesOnTest.Domain
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        upsert_identity(:by_key)
        accept([:key, :category, :total, :n])
      end
    end

    reactive do
      id(:category_totals)
      op(:fold)

      recompute_by(:category, to: :expenses, from: :category)
      reduce(into: [sum: [amount: :total], count: :n])
    end
  end

  # A TENANTED write-fed leaf. `dirties_on` runs as an Ash change with no plan in
  # scope, so the tenant can only come off the changeset — which is where a
  # tenanted write already put it.
  defmodule TenantedExpenses do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
      private?(true)
    end

    multitenancy do
      strategy :attribute
      attribute :org_id
    end

    attributes do
      uuid_primary_key(:id)
      attribute :key, :string, allow_nil?: false, public?: true
      attribute :org_id, :string, public?: true
      attribute :amount, :float, public?: true
    end

    identities do
      identity :by_org_key, [:org_id, :key], pre_check_with: Domain
    end

    actions do
      defaults [:read, :destroy]

      create :create do
        accept([:key, :org_id, :amount])
      end
    end

    reactive do
      id(:tenanted_expenses)
      op(:source)
      leaf?(true)
      row_key([:org_id, :key])
      dirties_on([:create])
    end
  end

  defmodule FakeRepo do
    def start_link, do: Agent.start_link(fn -> %{} end, name: __MODULE__)

    # stores the PRIOR too, so claim can return it — the whole point of #60
    def query!("INSERT INTO " <> _, params) do
      params
      |> Enum.chunk_every(7)
      |> Enum.each(fn [cell, tenant, key, _r, _t, _held, vid] ->
        # ON CONFLICT DO NOTHING: the FIRST snapshot wins
        # ON CONFLICT: MERGE the diffs, via the library's own rule — the earliest
        # prior side and the latest `to`. `Map.put_new` modelled `DO NOTHING`,
        # which strands the unit a twice-moved row ended up in.
        Agent.update(__MODULE__, fn m ->
          # ON CONFLICT: keep the EARLIEST version id.
          Map.update(m, {tenant, cell, key}, vid, fn stored -> stored || vid end)
        end)
      end)

      %{rows: []}
    end

    @doc "The tenants marks were written under — `dirties_on` has no plan, so
    it reads the tenant off the CHANGESET, and this is what checks it did."
    def tenants,
      do: Agent.get(__MODULE__, & &1) |> Map.keys() |> Enum.map(&elem(&1, 0)) |> Enum.uniq()

    def query!("SELECT DISTINCT cell_id" <> _, _params) do
      ids = Agent.get(__MODULE__, & &1) |> Map.keys() |> Enum.map(&elem(&1, 1)) |> Enum.uniq()
      %{rows: Enum.map(ids, &[&1])}
    end

    def query!("DELETE FROM " <> _, [cell, tenant]) do
      rows =
        Agent.get_and_update(__MODULE__, fn m ->
          {mine, rest} = Enum.split_with(m, fn {{t, c, _}, _} -> t == tenant and c == cell end)
          {Enum.map(mine, fn {{_t, _c, k}, vid} -> [k, vid] end), Map.new(rest)}
        end)

      %{rows: rows}
    end

    def query!("SELECT COUNT" <> _, _params), do: %{rows: [[Agent.get(__MODULE__, &map_size/1)]]}

    # `DrainWorker` takes `Frontier.with_lock/2` — the same cluster-wide advisory
    # lock the sweep uses, so two nodes never drain at once. Always granted here:
    # there is one node in a test.
    def query!("SELECT pg_try_advisory_lock" <> _, _params), do: %{rows: [[true]]}
    def query!("SELECT pg_advisory_unlock" <> _, _params), do: %{rows: [[true]]}

    # what the frontier currently holds, for assertions
    # `{cell, key}`, tenant dropped — the shape every existing assertion uses.
    # `tenants/0` is what checks the tenant.
    def dirty,
      do: Agent.get(__MODULE__, &Map.keys/1) |> Enum.map(&{elem(&1, 1), elem(&1, 2)}) |> Enum.sort()
  end

  setup do
    start_supervised!(%{id: FakeRepo, start: {FakeRepo, :start_link, []}})
    start_supervised!(%{id: Versions, start: {Versions, :start_link, []}})
    prev_repo = Application.get_env(:reactive_dag, :repo)
    Application.put_env(:reactive_dag, :repo, FakeRepo)

    on_exit(fn ->
      Application.put_env(:reactive_dag, :repo, prev_repo)
    end)

    # A `dirties_on` write no longer marks a table a test can read back — it
    # enqueues a cascade. This is the seam that catches those enqueues, so the
    # assertions below can ask what the write ORIGINATED without an Oban.
    ReactiveDag.Test.Pending.capture_enqueues()

    # the ETS tables are shared (not `private?`, since writes may come from
    # other processes), so start each test from a known-empty state — including
    # the frontier, which the destroys below would otherwise re-dirty.
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
    test "a VERSION REFERENCE rides on the frontier row" do
      Expenses
      |> Ash.Changeset.for_create(:create, %{key: "e1", category: "travel", amount: 10.0})
      |> Ash.create!()

      # The version reference used to ride on the frontier row and come back
      # through `claim_with_diffs`. There is no row to ride on now, so it rides
      # on the enqueue itself — same reference, same job, carried by the
      # cascade's `versions:` rather than read back out of a queue table.
      assert [{"expenses", ["e1"], opts}] = ReactiveDag.Test.Pending.enqueued()
      assert %{"e1" => version_id} = Keyword.fetch!(opts, :versions)
      assert is_binary(version_id), "the cascade references the record of the change"

      # …and the record holds what moved. `%{"to" => v}` throughout here, because
      # a create had nothing before it; an update carries
      # `%{"from" => old, "to" => new}` for what moved and `%{"unchanged" => v}`
      # for the rest.
      changes = Versions.changes(version_id)

      assert changes["category"] == %{"to" => "travel"}
      assert changes["amount"] == %{"to" => 10.0}
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
    # the node opted in with `schedule_drain: true` — enqueue something to
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
      # `Expenses` declares no `schedule_drain`. Under the queue that meant
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

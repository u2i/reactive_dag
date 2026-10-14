defmodule ReactiveDag.RetireVanishedTest do
  @moduledoc """
  RECONCILE, not just upsert (issue #37). A fold writes the units it produced;
  a unit whose input rows have all gone produces nothing, so without reconcile
  its last computed value lingers forever — and a stale derived row is
  indistinguishable from a live one, which defeats the point of materializing it.

  Retirement covers BOTH sides of a node: the coordination tuple (so propagation
  carries it downstream) and the payload row (so the derived table stops showing
  it). The baseline a pass may retire against is its CLAIM — a whole-cell pass
  reconciles everything, a scoped pass only what it claimed, since reconciling
  wider would retire live units that simply were not visited.

  LIMIT — a row MOVING between units. The claim names where the row landed; the
  unit it LEFT is invisible, because nothing records which unit an input key
  previously fed (the tuple spine stores the parent's units, the frontier only
  `(cell, key, reason)`). The origin is therefore repriced by the next
  whole-cell pass, not by the scoped claim. A claim-side heuristic was tried
  and rejected: "the claimed unit is one the parent doesn't hold yet" also
  matches a genuinely NEW unit, so it would force whole-cell forever. Fixing it
  exactly needs input-key → unit provenance, which is a schema addition.
  """
  use ReactiveDag.PostgresCase

  alias ReactiveDag.{Cascade}
  alias ReactiveDag.Fixtures.ExpenseGraph
  alias ReactiveDag.Fixtures.ExpenseGraph.{CategoryTotals, Expenses}
  alias ReactiveDag.Node.Recompute

  setup_all do
    migrate!(ExpenseGraph.Domain)
    :ok
  end

  setup do
    pid = Ecto.Adapters.SQL.Sandbox.start_owner!(ReactiveDag.TestRepo, shared: true)
    on_exit(fn -> Ecto.Adapters.SQL.Sandbox.stop_owner(pid) end)

    migrate_library_tables!()
    truncate(ExpenseGraph.tables())

    for {k, cat, amt} <- [{"e1", "travel", 100.0}, {"e2", "meals", 40.0}] do
      Expenses
      |> Ash.Changeset.for_create(:create, %{key: k, category: cat, amount: amt})
      |> Ash.create!()
    end

    :ok
  end

  defp plan, do: ExpenseGraph.plan()
  defp cell, do: plan().cells["category_totals"]
  defp rows, do: CategoryTotals |> Ash.read!() |> Enum.map(&{&1.key, &1.total, &1.n}) |> Enum.sort()

  defp drain(plan),
    do: ReactiveDag.Test.Pending.cascade(plan)

  test "a DELETED input retires its unit — the row goes, and the retirement propagates" do
    c = cell()
    {:ok, _} = Recompute.recompute(c, ["*"])
    assert rows() == [{"meals", 40.0, 1}, {"travel", 100.0, 1}]

    # the last expense in :meals goes away
    Expenses |> Ash.get!("e2") |> Ash.destroy!()

    {:ok, changed} = Recompute.recompute(c, ["*"])

    # the vanished unit is REPORTED as changed, so parents recompute
    assert "meals" in changed

    # ...and its payload row is gone (the derived table shows only live units).
    # The destroy IS the retirement: the row is the unit, so there is no second
    # place a stale copy could survive.
    assert rows() == [{"travel", 100.0, 1}]
  end

  test "a scoped pass only retires within its CLAIM — live units are untouched" do
    c = cell()
    {:ok, _} = Recompute.recompute(c, ["*"])
    assert rows() == [{"meals", 40.0, 1}, {"travel", 100.0, 1}]

    # recompute ONLY :travel. :meals produced nothing in this pass, but it was
    # never claimed — retiring it here would destroy a live unit.
    {:ok, _} = Recompute.recompute(c, ["travel"])

    assert rows() == [{"meals", 40.0, 1}, {"travel", 100.0, 1}]
  end

  test "a scoped pass DOES retire a claimed unit that produced nothing" do
    c = cell()
    {:ok, _} = Recompute.recompute(c, ["*"])

    Expenses |> Ash.get!("e2") |> Ash.destroy!()

    # claim exactly the emptied unit
    {:ok, changed} = Recompute.recompute(c, ["meals"])

    assert changed == ["meals"]
    assert rows() == [{"travel", 100.0, 1}]
  end

  test "an input MOVING between units: the destination is exact, the ORIGIN needs a whole-cell pass" do
    p = plan()

    ReactiveDag.Test.Pending.add("expenses", ["*"])
    {:ok, _} = drain(p)
    assert rows() == [{"meals", 40.0, 1}, {"travel", 100.0, 1}]

    # e2 moves meals -> travel.
    Expenses
    |> Ash.get!("e2")
    |> Ash.Changeset.for_update(:recategorise, %{category: "travel"})
    |> Ash.update!()

    ReactiveDag.Test.Pending.add("expenses", ["e2"])
    {:ok, report} = drain(p)

    steps = Map.new(report.steps, &{&1.cell, &1})

    # the claim names where the row LANDED — correct, and travel is right
    assert steps["category_totals"].claimed == ["travel"]
    assert {"travel", 140.0, 2} in rows()

    # ...but nothing records which unit an input key PREVIOUSLY fed (the tuple
    # spine stores the parent's units, the frontier stores only (cell, key,
    # reason)), so the origin is invisible to a scoped claim and still counts
    # the row that left. This is the documented limit of moves without
    # provenance — see the moduledoc.
    assert {"meals", 40.0, 1} in rows()

    # a whole-cell pass reconciles it exactly: meals is empty, so it retires
    {:ok, changed} = Recompute.recompute(cell(), ["*"])
    assert "meals" in changed
    assert rows() == [{"travel", 140.0, 2}]
  end

  test "reconcile is idempotent — a re-run retires nothing and reports nothing" do
    c = cell()
    {:ok, _} = Recompute.recompute(c, ["*"])
    Expenses |> Ash.get!("e2") |> Ash.destroy!()
    {:ok, _} = Recompute.recompute(c, ["*"])

    {:ok, changed} = Recompute.recompute(c, ["*"])
    assert changed == []
    assert rows() == [{"travel", 100.0, 1}]
  end

  test "a SCOPED claim for a key with no row reports nothing — no echo" do
    # THE RETIRE ECHO. A scoped pass reconciles against its CLAIM
    # (`vanished_baseline/3`), not against the node's rows — correctly, since
    # reconciling wider would retire live units it never visited.
    #
    # But a claimed key the pass neither produced nor holds a row for was
    # reported vanished UNCONDITIONALLY: `Payload.retire`'s result was discarded
    # and the whole claim returned. In a DAG that is one wasted step downstream.
    # Around a feedback loop with identity key mapping it never terminates —
    # claim k, produce nothing, "retire" the absent k, report k changed,
    # propagate, claim k again.
    #
    # `retire/6` already answers this precisely: it filters to the keys whose row
    # it actually found and destroyed (`payload.ex:703-719`). Using its return
    # value instead of throwing it away is the whole fix.
    c = cell()
    {:ok, _} = Recompute.recompute(c, ["*"])

    # A unit that never existed, claimed explicitly.
    {:ok, changed} = Recompute.recompute(c, ["no-such-unit"])

    assert changed == [],
           "a claim for a unit with no row retired nothing, so it changed nothing"
  end

  test "a scoped claim for a key that DID have a row still reports it" do
    # The other side: real retirement must keep working, or reconcile stops
    # reconciling. `travel` has a row; deleting its input leaves the unit with
    # nothing to produce, so the claim genuinely retires it.
    c = cell()
    {:ok, _} = Recompute.recompute(c, ["*"])
    assert {"travel", _, _} = List.keyfind(rows(), "travel", 0)

    Expenses |> Ash.read!() |> Enum.each(&Ash.destroy!/1)

    {:ok, changed} = Recompute.recompute(c, ["travel"])

    assert changed == ["travel"], "a unit whose row was destroyed IS a change"
    refute List.keyfind(rows(), "travel", 0)
  end

  # This replaces "a node with a custom `upsert:` owns its own writes — the library
  # does not reconcile", which asserted that a write-elsewhere node retired
  # NOTHING: `retire_vanished/4` returned `[]` for any node supplying its own
  # `upsert:`.
  #
  # That was a hole, not a feature. The one shape whose rows the library could not
  # see was also the one it never reconciled, so its stale units lingered forever —
  # exactly the failure the five tests above exist to prevent, exempted from the
  # fix. The shape is gone (rc.39), so every derived node reconciles, and a node
  # that cannot be reconciled cannot be declared.
  test "a derived node with nowhere to write its rows is a COMPILE-TIME error" do
    alias ReactiveDag.Node.Verifiers.VerifyReactive

    defmodule Tableless do
      use Ash.Resource,
        domain: ReactiveDag.Fixtures.ExpenseGraph.Domain,
        data_layer: Ash.DataLayer.Simple,
        extensions: [ReactiveDag.Node]

      reactive do
        id :tableless
        recompute_by :category, to: :expenses, from: :category
        reduce into: fn cat, items -> %{category: cat, n: length(items)} end
      end
    end

    assert {:error, %Spark.Error.DslError{message: msg}} =
             VerifyReactive.verify(Tableless.spark_dsl_config())

    assert msg =~ "declares no attributes"
    # and it says what to do about it
    assert msg =~ "leaf? true"
    assert msg =~ "one node, one table"
  end
end

defmodule ReactiveDag.TwoNodeJoinTest do
  @moduledoc """
  `left_over:`/`right_over:` — a join whose two sides are two DIFFERENT nodes,
  each read and scoped independently.

  This shape was built once (593b291) and reverted (b93c69e). The revert is worth
  reading: it computed ONE read scope from the claim and passed it to both sides,
  so a claim naming the left's keys filtered the RIGHT table by keys that index
  nothing in it. The right read empty, the join emitted `nil` for its columns,
  and the payload upsert wrote those nils over good data:

      after seed:   {"5000", 100.0, 90.0}
      budget claim: {"5000", 111.0, nil }   <- actual destroyed
      actual claim: {"5000", nil,   95.0}   <- budget destroyed

  The revert concluded this was "the shape's natural failure mode, not an
  oversight". The failure was real; the diagnosis was one level too shallow.

  A claim key is a JOIN key, and the reverted version scoped each side by its own
  PAYLOAD key — a different column, usually a different value (an `Actuals` row
  keyed `"a1"` joins on `acct: "5000"`). So the scoped read matched nothing and
  the side came back empty. Scope each side by the column it is INDEXED BY and
  both sides read the rows the claim is about, so no nil is ever emitted.

  The test that shipped with the reverted version asserted only the side that WAS
  claimed (`.actual == 95.0`) and never the column being destroyed, so it passed
  for the whole life of the bug. Every test here asserts BOTH columns.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.{Drain, Frontier}
  alias ReactiveDag.Node.Recompute

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      allow_unregistered?(true)
    end
  end

  defmodule Budgets do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
      private?(true)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      # deliberately NOT named like the right side's column
      attribute :account_code, :string, public?: true
      attribute :amount, :float, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :create do
        accept([:key, :account_code, :amount])
      end

      update :revise do
        accept([:amount])
      end
    end

    reactive do
      id(:budgets)
      op(:source)
      leaf?(true)
    end
  end

  defmodule Actuals do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
      private?(true)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :acct, :string, public?: true
      attribute :amount, :float, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :create do
        accept([:key, :acct, :amount])
      end

      update :revise do
        accept([:amount])
      end
    end

    reactive do
      id(:actuals)
      op(:source)
      leaf?(true)
    end
  end

  defmodule Variance do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
      private?(true)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :budget, :float, public?: true
      attribute :actual, :float, public?: true
    end

    identities do
      identity :by_key, [:key], pre_check_with: ReactiveDag.TwoNodeJoinTest.Domain
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        upsert_identity(:by_key)
        accept([:key, :budget, :actual])
      end
    end

    reactive do
      id(:variance)
      op(:reconcile)

      # NO `depends_on` — `left_over:`/`right_over:` already name both inputs,
      # and the library derives the edges from them (one per side). Restating
      # them would be a second answer to one question, and duplicates the edge.
      join(
        left_over: :budgets,
        right_over: :actuals,
        left: :account_code,
        right: :acct,
        outer: true,
        into: [left: [amount: :budget], right: [amount: :actual]]
      )
    end
  end

  # The drain needs a repo for the frontier; these tests are about the JOIN, so
  # the dirty set lives in an Agent and coordination writes go nowhere.
  defmodule FakeRepo do
    def start_link, do: Agent.start_link(fn -> MapSet.new() end, name: __MODULE__)

    def query!("INSERT INTO " <> _, params) do
      params
      # 5 params per entry: cell, key, reason, enqueued_at, prior
      |> Enum.chunk_every(7)
      |> Enum.each(fn [cell, _tenant, key, _r, _t, _prior, _held] ->
        Agent.update(__MODULE__, &MapSet.put(&1, {cell, key}))
      end)

      %{rows: []}
    end

    def query!("SELECT DISTINCT cell_id" <> _, _params) do
      ids = Agent.get(__MODULE__, & &1) |> Enum.map(&elem(&1, 0)) |> Enum.uniq()
      %{rows: Enum.map(ids, &[&1])}
    end

    def query!("DELETE FROM " <> _, [cell | _tenant]) do
      keys =
        Agent.get_and_update(__MODULE__, fn set ->
          {mine, rest} = Enum.split_with(set, fn {c, _} -> c == cell end)
          {Enum.map(mine, &elem(&1, 1)), MapSet.new(rest)}
        end)

      # RETURNING key, prior — so each row is a PAIR
      %{rows: Enum.map(keys, &[&1, nil])}
    end

    def query!("SELECT COUNT" <> _, _params),
      do: %{rows: [[Agent.get(__MODULE__, &MapSet.size/1)]]}
  end

  defmodule NullWriter do
    @behaviour ReactiveDag.CoordinationWriter
    @impl true
    def put(_cell_id, _key, _opts), do: :ok
    @impl true
    def delete(_cell_id, _keys), do: :ok
  end

  setup do
    {:ok, _} = FakeRepo.start_link()
    prev_repo = Application.get_env(:reactive_dag, :repo)
    prev_writer = Application.get_env(:reactive_dag, :coordination_writer)
    Application.put_env(:reactive_dag, :repo, FakeRepo)
    Application.put_env(:reactive_dag, :coordination_writer, NullWriter)

    on_exit(fn ->
      Application.put_env(:reactive_dag, :repo, prev_repo)
      Application.put_env(:reactive_dag, :coordination_writer, prev_writer)
    end)

    for r <- [Budgets, Actuals, Variance], do: Ash.bulk_destroy!(r, :destroy, %{})

    Budgets
    |> Ash.Changeset.for_create(:create, %{key: "b1", account_code: "5000", amount: 100.0})
    |> Ash.create!()

    Budgets
    |> Ash.Changeset.for_create(:create, %{key: "b2", account_code: "6000", amount: 250.0})
    |> Ash.create!()

    Actuals
    |> Ash.Changeset.for_create(:create, %{key: "a1", acct: "5000", amount: 90.0})
    |> Ash.create!()

    # right-only: no budget declares 7000 — with `outer: true` that is a row
    Actuals
    |> Ash.Changeset.for_create(:create, %{key: "a2", acct: "7000", amount: 40.0})
    |> Ash.create!()

    :ok
  end

  defp plan, do: ReactiveDag.Node.graph([Budgets, Actuals, Variance])

  defp rows, do: Variance |> Ash.read!() |> Map.new(&{&1.key, &1})

  describe "assembly" do
    test "two inputs, and a source per side" do
      cell = plan().cells["variance"]

      assert Enum.sort(cell.inputs) == ["actuals", "budgets"]

      # each side resolved to its OWN resource — the fact the reverted version
      # had right and then ignored when scoping
      assert cell.meta.side_sources.left.resource == Budgets
      assert cell.meta.side_sources.right.resource == Actuals

      # ...and the claim rule is :group, not :identity — an input's changed keys
      # are ITS keys and must be translated to join keys before they mean
      # anything here. `:identity` passed "a1" through as a join key.
      assert cell.meta.key_rule == :group
    end
  end

  describe "a whole-cell pass" do
    test "joins two different nodes — what `over:` could never do" do
      {:ok, changed} = Recompute.recompute(plan().cells["variance"], ["*"])
      assert Enum.sort(changed) == ["5000", "6000", "7000"]

      rows = rows()

      # matched on both sides
      assert %{budget: 100.0, actual: 90.0} = rows["5000"]
      # left-only: the actual is absent, and the gap is information
      assert %{budget: 250.0, actual: nil} = rows["6000"]
      # right-only, via outer: true — an actual no budget declared
      assert %{budget: nil, actual: 40.0} = rows["7000"]
    end
  end

  describe "a one-sided claim (the reverted bug)" do
    setup do
      {:ok, _} = Recompute.recompute(plan().cells["variance"], ["*"])
      assert %{budget: 100.0, actual: 90.0} = rows()["5000"]
      :ok
    end

    test "a LEFT-side claim revises the budget and PRESERVES the actual" do
      Budgets
      |> Ash.get!("b1")
      |> Ash.Changeset.for_update(:revise, %{amount: 111.0})
      |> Ash.update!()

      {:ok, changed} = Recompute.recompute(plan().cells["variance"], ["5000"])
      assert changed == ["5000"]

      row = rows()["5000"]

      assert row.budget == 111.0, "the claimed side is written"

      # THE ASSERTION THE REVERTED VERSION'S TEST DID NOT MAKE. Under the old
      # implementation this was nil: the right side was filtered by the left's
      # keys, read empty, and `into` emitted nil for :actual.
      assert row.actual == 90.0, "the unclaimed side must not be destroyed"
    end

    test "a RIGHT-side claim revises the actual and PRESERVES the budget" do
      Actuals
      |> Ash.get!("a1")
      |> Ash.Changeset.for_update(:revise, %{amount: 95.0})
      |> Ash.update!()

      {:ok, changed} = Recompute.recompute(plan().cells["variance"], ["5000"])
      assert changed == ["5000"]

      row = rows()["5000"]

      assert row.actual == 95.0, "the claimed side is written"
      assert row.budget == 100.0, "the unclaimed side must not be destroyed"
    end

    test "the two claims in sequence leave BOTH revisions standing" do
      # The failure was symmetric, so each claim undid the other's work. Running
      # both is the shape a real drain produces when both legs move.
      Budgets
      |> Ash.get!("b1")
      |> Ash.Changeset.for_update(:revise, %{amount: 111.0})
      |> Ash.update!()

      Actuals
      |> Ash.get!("a1")
      |> Ash.Changeset.for_update(:revise, %{amount: 95.0})
      |> Ash.update!()

      {:ok, _} = Recompute.recompute(plan().cells["variance"], ["5000"])
      {:ok, _} = Recompute.recompute(plan().cells["variance"], ["5000"])

      assert %{budget: 111.0, actual: 95.0} = rows()["5000"]
    end

    test "a claim for a LEFT-ONLY key does not invent the right side" do
      # 6000 has no actual at all. A scoped pass over it must leave :actual nil
      # rather than reporting a change every time it runs.
      {:ok, changed} = Recompute.recompute(plan().cells["variance"], ["6000"])

      assert changed == [], "nothing moved, so nothing is reported"
      assert %{budget: 250.0, actual: nil} = rows()["6000"]
    end

    test "a claim for a RIGHT-ONLY key keeps the row `outer: true` created" do
      {:ok, changed} = Recompute.recompute(plan().cells["variance"], ["7000"])

      assert changed == []
      assert %{budget: nil, actual: 40.0} = rows()["7000"], "the row is not retired"
    end
  end

  describe "a side that cannot be key-scoped" do
    test "a fn side reads WHOLE, so the other side's columns survive" do
      # `side_scope/2` can only push a filter for a NAMED column. A fn side
      # computes its join key in the BEAM, so its read cannot be narrowed and it
      # reads whole — which is correct, and the case where per-side column
      # ownership would matter if the read ever came back empty.
      assert Recompute.side_scope_for_test(:account_code, ["5000"]) ==
               {:attr, :account_code, ["5000"]}

      assert Recompute.side_scope_for_test([key: :acct, where: [kind: "x"]], ["5000"]) ==
               {:attr, :acct, ["5000"]}

      assert Recompute.side_scope_for_test(fn r -> r.account_code end, ["5000"]) == nil
    end
  end

  describe "compile-time errors" do
    alias ReactiveDag.Node.Verifiers.VerifyReactive

    defp verify(mod), do: VerifyReactive.verify(mod.spark_dsl_config())

    test "a join with no input at all" do
      defmodule NoInput do
        use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Ets, extensions: [ReactiveDag.Node]
        ets(do: private?(true))
        attributes(do: attribute(:key, :string, primary_key?: true, allow_nil?: false, public?: true))
        actions(do: defaults([:read, :destroy]))

        reactive do
          id(:no_input)
          join(left: :a, right: :b, into: [left: [x: :key]])
        end
      end

      assert {:error, %Spark.Error.DslError{message: msg}} = verify(NoInput)
      assert msg =~ "declares a `join` with no input"
      # the fix is in the message: both forms, named
      assert msg =~ "over:"
      assert msg =~ "left_over:"
    end

    test "both forms at once" do
      defmodule BothForms do
        use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Ets, extensions: [ReactiveDag.Node]
        ets(do: private?(true))
        attributes(do: attribute(:key, :string, primary_key?: true, allow_nil?: false, public?: true))
        actions(do: defaults([:read, :destroy]))

        reactive do
          id(:both)

          join(
            over: :entries,
            left_over: :budgets,
            right_over: :actuals,
            left: :a,
            right: :b,
            into: [left: [x: :key]]
          )
        end
      end

      assert {:error, %Spark.Error.DslError{message: msg}} = verify(BothForms)
      assert msg =~ "declares both `over:` and `left_over:`"
      assert msg =~ "pick one"
    end

    test "only one half of a two-input join" do
      defmodule HalfJoin do
        use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Ets, extensions: [ReactiveDag.Node]
        ets(do: private?(true))
        attributes(do: attribute(:key, :string, primary_key?: true, allow_nil?: false, public?: true))
        actions(do: defaults([:read, :destroy]))

        reactive do
          id(:half)
          join(left_over: :budgets, left: :a, right: :b, into: [left: [x: :key]])
        end
      end

      assert {:error, %Spark.Error.DslError{message: msg}} = verify(HalfJoin)
      assert msg =~ "only one half of a two-input `join`"
      assert msg =~ "right_over:", "names WHICH half is missing"
    end
  end

  describe "through a real drain" do
    test "either side's change propagates through its own edge" do
      p = plan()

      Frontier.mark_dirty("budgets", ["*"], "seed")
      Frontier.mark_dirty("actuals", ["*"], "seed")
      {:ok, _} = Drain.run(p)

      assert %{budget: 100.0, actual: 90.0} = rows()["5000"]

      # touch the RIGHT side only
      Actuals
      |> Ash.get!("a1")
      |> Ash.Changeset.for_update(:revise, %{amount: 95.0})
      |> Ash.update!()

      Frontier.mark_dirty("actuals", ["a1"], "revised")
      {:ok, report} = Drain.run(p)

      steps = Map.new(report.steps, &{&1.cell, &1})
      assert steps["variance"].triggered_by == "actuals"
      assert steps["variance"].changed == ["5000"]

      row = rows()["5000"]
      assert row.actual == 95.0
      assert row.budget == 100.0, "the drain must not destroy the unclaimed side either"
    end
  end
end

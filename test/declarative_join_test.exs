defmodule ReactiveDag.DeclarativeJoinTest do
  @moduledoc """
  The Ash-first `join`: no `read:`, declarative sides — a plain attribute
  (two-column case) or `[key:, where:]` (one input split by a discriminator) —
  and declarative per-side column picks in `into:`, with absent-side nils
  giving left-join/outer gap semantics for free.
  """
  use ReactiveDag.PostgresCase

  alias ReactiveDag.Fixtures.DeclarativeJoinGraph
  alias ReactiveDag.Fixtures.DeclarativeJoinGraph.{Entries, Linked, OuterVariance, Variance}

  alias ReactiveDag.Node.Recompute

  setup_all do
    migrate!(DeclarativeJoinGraph.Domain)
    :ok
  end

  setup do
    pid = Ecto.Adapters.SQL.Sandbox.start_owner!(ReactiveDag.TestRepo, shared: true)
    on_exit(fn -> Ecto.Adapters.SQL.Sandbox.stop_owner(pid) end)

    truncate(DeclarativeJoinGraph.tables())

    for {k, kind, acct, amount} <- [
          {"b1", "budget", "A1010", 100.0},
          {"b2", "budget", "A2020", 50.0},
          {"a1", "actual", "A1010", 90.0},
          # A3030: actual with NO budget — invisible to a left join, a finding
          # to an outer one
          {"a2", "actual", "A3030", 25.0}
        ] do
      Entries
      |> Ash.Changeset.for_create(:create, %{key: k, kind: kind, acct: acct, amount: amount})
      |> Ash.create!()
    end

    :ok
  end

  test "declarative sides + picks: left join, absent right yields nils, keys prefixed" do
    plan = ReactiveDag.Node.graph([Entries, Variance])
    {:ok, changed} = Recompute.recompute(plan.cells["variance"], ["*"])

    assert Enum.sort(changed) == ["va|A1010", "va|A2020"]

    rows = Variance |> Ash.read!() |> Map.new(&{&1.key, &1})
    assert %{budget: 100.0, actual: 90.0} = rows["va|A1010"]
    # the gap is information: budget present, actual absent → nil
    assert %{budget: 50.0, actual: nil} = rows["va|A2020"]
    # right-only accounts don't emit on a left join
    refute Map.has_key?(rows, "va|A3030")
  end

  test "outer: true also emits right-only keys (nil left) — the rogue-member finding" do
    plan = ReactiveDag.Node.graph([Entries, OuterVariance])
    {:ok, changed} = Recompute.recompute(plan.cells["outer_variance"], ["*"])
    assert Enum.sort(changed) == ["A1010", "A2020", "A3030"]
  end

  test "plain-attribute sides: the two-column case (nil value = not on that side)" do
    plan = ReactiveDag.Node.graph([Entries, Linked])
    {:ok, changed} = Recompute.recompute(plan.cells["linked"], ["*"])
    # left side indexes EVERY entry by acct (last wins per key), so all accts emit
    assert Enum.sort(changed) == ["A1010", "A2020", "A3030"]
  end

  test "the verifier rejects a side keyword without key: and a bad into: pick key" do
    alias ReactiveDag.Node.Verifiers.VerifyReactive

    defmodule BadSide do
      use Ash.Resource,
        domain: ReactiveDag.Fixtures.DeclarativeJoinGraph.Domain,
        data_layer: Ash.DataLayer.Ets,
        extensions: [ReactiveDag.Node]

      ets do
      end

      attributes do
        attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
        attribute :status, :string, public?: true
      end

      actions do
        defaults [:read, :destroy]

        create :upsert do
          upsert?(true)
          accept([:key, :status])
        end
      end

      reactive do
        id :bad_side

        join(
          over: :entries,
          left: [where: [kind: "budget"]],
          right: :acct,
          into: fn k, _, _ -> %{key: k, status: "present"} end
        )
      end
    end

    assert {:error, %Spark.Error.DslError{message: msg}} =
             VerifyReactive.verify(BadSide.spark_dsl_config())

    assert msg =~ "needs `key:`"

    defmodule BadPicks do
      use Ash.Resource,
        domain: ReactiveDag.Fixtures.DeclarativeJoinGraph.Domain,
        data_layer: Ash.DataLayer.Ets,
        extensions: [ReactiveDag.Node]

      ets do
      end

      attributes do
        attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
        attribute :status, :string, public?: true
      end

      actions do
        defaults [:read, :destroy]

        create :upsert do
          upsert?(true)
          accept([:key, :status])
        end
      end

      reactive do
        id :bad_picks

        join(
          over: :entries,
          left: :acct,
          right: :acct,
          into: [left: [amount: :a], middle: [amount: :b]]
        )
      end
    end

    assert {:error, %Spark.Error.DslError{message: msg}} =
             VerifyReactive.verify(BadPicks.spark_dsl_config())

    assert msg =~ ":middle"
  end
end

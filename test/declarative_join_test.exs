defmodule ReactiveDag.DeclarativeJoinTest do
  @moduledoc """
  The Ash-first `join`: no `read:`, declarative sides — a plain attribute
  (two-column case) or `[key:, where:]` (one input split by a discriminator) —
  and declarative per-side column picks in `into:`, with absent-side nils
  giving left-join/outer gap semantics for free.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.Node.Recompute

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      allow_unregistered?(true)
    end
  end

  # ONE input holding both sides, discriminated by :kind — the fiscal
  # declared-vs-observed shape.
  defmodule Entries do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
      private?(true)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :kind, :string, public?: true
      attribute :acct, :string, public?: true
      attribute :amount, :float, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :create do
        accept([:key, :kind, :acct, :amount])
      end
    end

    reactive do
      id(:entries)
      op(:source)
      leaf?(true)
    end
  end

  # the declarative variance node: [key:, where:] sides + per-side picks.
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
      identity :by_key, [:key], pre_check_with: ReactiveDag.DeclarativeJoinTest.Domain
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
      key_rule(:all)

      join over: :entries,
           left: [key: :acct, where: [kind: "budget"]],
           right: [key: :acct, where: [kind: "actual"]],
           key_prefix: "va",
           into: [left: [amount: :budget], right: [amount: :actual]]
    end
  end

  defmodule NullWriter do
    @behaviour ReactiveDag.CoordinationWriter
    @impl true
    def put(_cell_id, _key, _opts), do: :ok
    @impl true
    def delete(_cell_id, _keys), do: :ok
  end

  setup do
    prev = Application.get_env(:reactive_dag, :coordination_writer)
    Application.put_env(:reactive_dag, :coordination_writer, NullWriter)
    on_exit(fn -> Application.put_env(:reactive_dag, :coordination_writer, prev) end)

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
    defmodule OuterVariance do
      use Ash.Resource,
        domain: ReactiveDag.DeclarativeJoinTest.Domain,
        data_layer: Ash.DataLayer.Simple,
        extensions: [ReactiveDag.Node]

      reactive do
        id(:outer_variance)
        op(:reconcile)
        key_rule(:all)
        verdict?(true)

        join over: :entries,
             left: [key: :acct, where: [kind: "budget"]],
             right: [key: :acct, where: [kind: "actual"]],
             outer: true,
             into: fn acct, budget, actual ->
               %{
                 key: acct,
                 status:
                   cond do
                     is_nil(budget) -> "undeclared"
                     is_nil(actual) -> "unspent"
                     true -> "present"
                   end
               }
             end
      end
    end

    plan = ReactiveDag.Node.graph([Entries, OuterVariance])
    {:ok, changed} = Recompute.recompute(plan.cells["outer_variance"], ["*"])
    assert Enum.sort(changed) == ["A1010", "A2020", "A3030"]
  end

  test "plain-attribute sides: the two-column case (nil value = not on that side)" do
    defmodule Linked do
      use Ash.Resource,
        domain: ReactiveDag.DeclarativeJoinTest.Domain,
        data_layer: Ash.DataLayer.Simple,
        extensions: [ReactiveDag.Node]

      reactive do
        id(:linked)
        op(:reconcile)
        key_rule(:all)
        verdict?(true)

        # every entry is on the left by acct; only actuals land on the right
        # via a fn (mixing forms is fine — each slot resolves independently)
        join over: :entries,
             left: :acct,
             right: fn e -> e.kind == "actual" && e.acct end,
             into: fn acct, _l, r ->
               %{key: acct, status: if(r, do: "observed", else: "declared-only")}
             end
      end
    end

    plan = ReactiveDag.Node.graph([Entries, Linked])
    {:ok, changed} = Recompute.recompute(plan.cells["linked"], ["*"])
    # left side indexes EVERY entry by acct (last wins per key), so all accts emit
    assert Enum.sort(changed) == ["A1010", "A2020", "A3030"]
  end

  test "the verifier rejects a side keyword without key: and a bad into: pick key" do
    alias ReactiveDag.Node.Verifiers.VerifyReactive

    defmodule BadSide do
      use Ash.Resource,
        domain: ReactiveDag.DeclarativeJoinTest.Domain,
        data_layer: Ash.DataLayer.Simple,
        extensions: [ReactiveDag.Node]

      reactive do
        id(:bad_side)
        verdict?(true)

        join over: :entries,
             left: [where: [kind: "budget"]],
             right: :acct,
             into: fn k, _, _ -> %{key: k, status: "present"} end
      end
    end

    assert {:error, %Spark.Error.DslError{message: msg}} =
             VerifyReactive.verify(BadSide.spark_dsl_config())

    assert msg =~ "needs `key:`"

    defmodule BadPicks do
      use Ash.Resource,
        domain: ReactiveDag.DeclarativeJoinTest.Domain,
        data_layer: Ash.DataLayer.Simple,
        extensions: [ReactiveDag.Node]

      reactive do
        id(:bad_picks)

        join over: :entries,
             left: :acct,
             right: :acct,
             into: [left: [amount: :a], middle: [amount: :b]],
             upsert: fn _, _ -> true end
      end
    end

    assert {:error, %Spark.Error.DslError{message: msg}} =
             VerifyReactive.verify(BadPicks.spark_dsl_config())

    assert msg =~ ":middle"
  end
end

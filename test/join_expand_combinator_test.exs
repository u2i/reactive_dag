defmodule ReactiveDag.JoinExpandCombinatorTest do
  @moduledoc """
  The `join` combinator's fn escape hatches (computed side keys, fn key with a
  prefix, fn into, upsert override) and the `expand:` slot — the group →
  many-rows shape, each row self-keyed.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.Node.Recompute

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      allow_unregistered?(true)
    end
  end

  defmodule Fiscal do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
      private?(true)
    end

    attributes do
      attribute :k, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :kind, :string, public?: true
      attribute :acct, :string, public?: true
      attribute :amount, :float, public?: true
      attribute :fy, :integer, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :create do
        accept([:k, :kind, :acct, :amount, :fy])
      end
    end

    reactive do
      id(:fiscal)
      op(:source)
      leaf?(true)
    end
  end

  # fn everything: computed side keys (the `&&` discriminator idiom), a fn key
  # with a literal prefix, a computed-columns into, an upsert capture.
  defmodule Variance do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Simple,
      extensions: [ReactiveDag.Node]

    reactive do
      id(:variance)
      op(:reconcile)
      key_rule(:all)

      join over: :fiscal,
           left: fn %{kind: k, acct: a} -> k == "budget" && a end,
           right: fn %{kind: k, acct: a} -> k == "actual" && a end,
           key: fn acct -> "va|#{acct}" end,
           into: fn _acct, budget, actual ->
             %{
               budget: budget.amount,
               actual: actual && actual.amount,
               variance: budget.amount - ((actual && actual.amount) || 0.0)
             }
           end,
           upsert: fn key, row ->
             send(ReactiveDag.JoinExpandCombinatorTest, {:upsert, key, row})
             true
           end
    end
  end

  # EXPAND: one group (a fiscal year) fans out to one row per bucket — each
  # row self-keyed, because one group → many keys.
  defmodule YearBuckets do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Simple,
      extensions: [ReactiveDag.Node]

    reactive do
      id(:year_buckets)
      op(:expand)
      key_rule(:all)

      reduce over: :fiscal,
             group_by: :fy,
             expand: fn fy, lines ->
               lines
               |> Enum.group_by(& &1.kind)
               |> Enum.map(fn {kind, ls} ->
                 %{key: "#{fy}|#{kind}", n: length(ls)}
               end)
             end,
             upsert: fn key, row ->
               send(ReactiveDag.JoinExpandCombinatorTest, {:expanded, key, row})
               true
             end
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
    Process.register(self(), ReactiveDag.JoinExpandCombinatorTest)
    prev = Application.get_env(:reactive_dag, :coordination_writer)
    Application.put_env(:reactive_dag, :coordination_writer, NullWriter)
    on_exit(fn -> Application.put_env(:reactive_dag, :coordination_writer, prev) end)

    for {k, kind, acct, amount, fy} <- [
          {"b1", "budget", "A1010", 100.0, 2025},
          {"b2", "budget", "A2020", 50.0, 2025},
          {"a1", "actual", "A1010", 90.0, 2025},
          {"a2", "actual", "A9090", 5.0, 2026}
        ] do
      Fiscal
      |> Ash.Changeset.for_create(:create, %{k: k, kind: kind, acct: acct, amount: amount, fy: fy})
      |> Ash.create!()
    end

    :ok
  end

  test "fn sides split one input; left join; computed columns; prefixed fn keys" do
    plan = ReactiveDag.Node.graph([Fiscal, Variance])
    {:ok, changed} = Recompute.recompute(plan.cells["variance"], ["*"])

    assert Enum.sort(changed) == ["va|A1010", "va|A2020"]

    assert_received {:upsert, "va|A1010", %{budget: 100.0, actual: 90.0, variance: 10.0}}
    # a missing right is a gap, not an error
    assert_received {:upsert, "va|A2020", %{budget: 50.0, actual: nil, variance: 50.0}}
    # right-only accounts don't emit on a left join
    refute_received {:upsert, "va|A9090", _}
  end

  test "expand: one group → many self-keyed rows" do
    plan = ReactiveDag.Node.graph([Fiscal, YearBuckets])
    {:ok, changed} = Recompute.recompute(plan.cells["year_buckets"], ["*"])

    assert Enum.sort(changed) == ["2025|actual", "2025|budget", "2026|actual"]
    assert_received {:expanded, "2025|budget", %{n: 2}}
    assert_received {:expanded, "2025|actual", %{n: 1}}
    assert_received {:expanded, "2026|actual", %{n: 1}}
  end

  test "an expand: row WITHOUT its own :key raises instructively" do
    defmodule KeylessExpand do
      use Ash.Resource,
        domain: ReactiveDag.JoinExpandCombinatorTest.Domain,
        data_layer: Ash.DataLayer.Simple,
        extensions: [ReactiveDag.Node]

      reactive do
        id(:keyless_expand)
        op(:expand)
        key_rule(:all)

        reduce over: :fiscal,
               group_by: :fy,
               expand: fn _fy, lines -> [%{n: length(lines)}] end,
               upsert: fn _, _ -> true end
      end
    end

    plan = ReactiveDag.Node.graph([Fiscal, KeylessExpand])

    assert_raise RuntimeError, ~r/must carry its own :key/s, fn ->
      Recompute.recompute(plan.cells["keyless_expand"], ["*"])
    end
  end
end

defmodule ReactiveDag.VerdictNodeTest do
  @moduledoc """
  A VERDICT is a row like any other.

  There used to be a second node shape for this — `verdict? true`, with no
  table, writing a status straight into the coordination tuple through a
  dedicated `status:` slot. It saved a migration when the answer was one word,
  and cost a ceiling: the tuple's schema is fixed, so the moment a verdict
  wanted company (a headroom, a breached_at) the shape had nothing to offer and
  you abandoned it entirely.

  Now a verdict node is an ordinary payload node whose row happens to carry a
  `:status` column — written by `into:` like any other column, queryable with
  ordinary Ash reads, and free to carry whatever else the domain needs.
  """
  use ExUnit.Case, async: false

  require Ash.Query

  alias ReactiveDag.Node.Recompute

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      allow_unregistered?(true)
    end
  end

  # the observed stores — a leaf node the verdict reads declaratively.
  # `verdict? true` saves you a table when the result is one word. The moment
  # it wants company — a headroom, a breached_at — that shape has nothing to
  # offer, because the coordination tuple has nowhere to put them. A node with
  # a :status column is an ordinary payload node whose row happens to be a
  # verdict, so it carries whatever it likes.

  setup do
    :ok
  end

  defmodule Totals do
    use Ash.Resource,
      domain: ReactiveDag.VerdictNodeTest.Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :total, :float, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :create do
        accept([:key, :total])
      end
    end

    reactive do
      id(:totals)
      leaf?(true)
    end
  end

  defmodule Health do
    use Ash.Resource,
      domain: ReactiveDag.VerdictNodeTest.Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :status, :string, public?: true
      # the column a tableless verdict node cannot have at all
      attribute :headroom, :float, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        accept([:key, :status, :headroom])
      end
    end

    reactive do
      id(:health)

      # a verdict WITH a table is an ordinary payload node: the status is a
      # column, written by `into:` like any other. `status:` is the tableless
      # node's slot and has no business here.
      reduce over: :totals,
             group_by: :key,
             into: fn _k, [r | _] ->
               %{
                 status: if(r.total < 1000.0, do: "present", else: "failing"),
                 headroom: 1000.0 - r.total
               }
             end
    end
  end

  setup do
    Totals |> Ash.read!() |> Enum.each(&Ash.destroy!/1)
    Health |> Ash.read!() |> Enum.each(&Ash.destroy!/1)

    for {k, t} <- [{"a", 10.0}, {"b", 5000.0}] do
      Totals |> Ash.Changeset.for_create(:create, %{key: k, total: t}) |> Ash.create!()
    end

    :ok
  end

  test "a verdict is an ordinary column — no verdict? needed" do
    plan = ReactiveDag.Node.graph([Totals, Health])

    {:ok, changed} = Recompute.recompute(plan.cells["health"], ["*"])
    assert Enum.sort(changed) == ["a", "b"]

    rows = Health |> Ash.read!() |> Map.new(&{&1.key, &1.status})
    assert rows == %{"a" => "present", "b" => "failing"}
  end

  test "the graph-wide question is an ordinary Ash read" do
    plan = ReactiveDag.Node.graph([Totals, Health])
    {:ok, _} = Recompute.recompute(plan.cells["health"], ["*"])

    failing = Health |> Ash.Query.filter(status == "failing") |> Ash.read!()

    assert Enum.map(failing, & &1.key) == ["b"]
  end

  test "the node is NOT a verdict node — it has a resource and a payload row" do
    cell = ReactiveDag.Node.graph([Totals, Health]).cells["health"]

    refute cell.meta[:verdict]
    assert cell.meta[:resource] == Health
  end

end

defmodule ReactiveDag.UnionNodeTest do
  @moduledoc """
  `union from: [...]` — the graph-wide roll-up as a node.

  A verdict-shaped node answers one question about one cell. Asking "what is
  failing ANYWHERE?" means scanning every cell separately (`Insights.summary/1`
  does exactly that, one query per cell). A union node makes that roll-up a
  NODE: one indexed table, maintained incrementally — a verdict flips, that key
  propagates, one row updates.

  It reads its inputs' ROWS (each input's own resource, via
  `ReactiveDag.Node.Rows`), so it can project any column an input has.

  It is also the first N-input combinator, and the reason it is sound where the
  cross-node join was not (reverted in #36): a join CORRELATES its inputs, so a
  claim naming one side leaves the other unread and the fold writes nulls over
  good data. A union does not correlate — each input contributes rows
  independently, and the composite key carries its own provenance, so a claim
  scopes to exactly the input that fired.
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

  # two verdict-shaped inputs — ordinary nodes, each with a :status column
  defmodule CategoryHealth do
    use Ash.Resource,
      domain: Domain,
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
      id(:category_health)
      leaf?(true)
    end
  end

  defmodule FundBalance do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :status, :string, public?: true
      # a column the tuple never had room for — the union can project it
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
      id(:fund_balance)
      leaf?(true)
    end
  end

  defmodule AllVerdicts do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      # composite PK → identity-keyed, so cell keys are "<input>|<key>"
      attribute :check, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :subject, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :status, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        accept([:check, :subject, :status])
      end
    end

    reactive do
      id(:all_verdicts)

      union from: [:category_health, :fund_balance],
            into: [check: :cell, subject: :key, status: :status]
    end
  end

  defmodule NullWriter do
    @behaviour ReactiveDag.CoordinationWriter
    @impl true
    def put(_c, _k, _o), do: :ok
    @impl true
    def delete(_c, _k), do: :ok
  end

  defp seed(resource, attrs) do
    resource |> Ash.Changeset.for_create(:upsert, attrs) |> Ash.create!()
  end

  setup do
    prev_writer = Application.get_env(:reactive_dag, :coordination_writer)
    Application.put_env(:reactive_dag, :coordination_writer, NullWriter)

    on_exit(fn -> Application.put_env(:reactive_dag, :coordination_writer, prev_writer) end)

    for r <- [AllVerdicts, CategoryHealth, FundBalance],
        row <- Ash.read!(r),
        do: Ash.destroy!(row)

    # two inputs' verdicts, as rows in their own tables
    seed(CategoryHealth, %{key: "travel", status: "failing"})
    seed(CategoryHealth, %{key: "meals", status: "present"})
    seed(FundBalance, %{key: "gf", status: "present", headroom: 250.0})

    :ok
  end

  defp plan, do: ReactiveDag.Node.graph([CategoryHealth, FundBalance, AllVerdicts])
  defp cell, do: plan().cells["all_verdicts"]
  defp rows, do: AllVerdicts |> Ash.read!() |> Enum.map(&{&1.check, &1.subject, &1.status}) |> Enum.sort()

  test "every input becomes an input edge" do
    assert Enum.sort(cell().inputs) == ["category_health", "fund_balance"]
  end

  test "a whole-cell pass unions every input's keys into one table" do
    {:ok, changed, meta} = Recompute.recompute(cell(), ["*"])

    assert Enum.sort(changed) == ["category_health|meals", "category_health|travel", "fund_balance|gf"]
    assert meta == %{inputs_read: 2}

    assert rows() == [
             {"category_health", "meals", "present"},
             {"category_health", "travel", "failing"},
             {"fund_balance", "gf", "present"}
           ]
  end

  test "the graph-wide question becomes one query" do
    {:ok, _, _} = Recompute.recompute(cell(), ["*"])

    failing = AllVerdicts |> Ash.Query.filter(status == "failing") |> Ash.read!()

    assert Enum.map(failing, &{&1.check, &1.subject}) == [{"category_health", "travel"}]
  end

  test "a SCOPED claim reads only the input that moved" do
    {:ok, _, _} = Recompute.recompute(cell(), ["*"])

    # fund_balance flips; the claim names it, so category_health is not read
    seed(FundBalance, %{key: "gf", status: "failing", headroom: 0.0})

    {:ok, changed, meta} = Recompute.recompute(cell(), ["fund_balance|gf"])

    assert changed == ["fund_balance|gf"]
    assert meta == %{inputs_read: 1}
    assert {"fund_balance", "gf", "failing"} in rows()
  end

  test "a scoped pass writes only its claimed keys, leaving the rest alone" do
    {:ok, _, _} = Recompute.recompute(cell(), ["*"])
    before = rows()

    # claim ONE of category_health's two keys
    {:ok, changed, _} = Recompute.recompute(cell(), ["category_health|meals"])

    # nothing changed, and the untouched keys are still there
    assert changed == []
    assert rows() == before
  end

  test "re-running changes nothing (the payload loop's change detection)" do
    {:ok, _, _} = Recompute.recompute(cell(), ["*"])
    {:ok, changed, _} = Recompute.recompute(cell(), ["*"])

    assert changed == []
  end

  test "a union projects any column its inputs have, not just a status" do
    # the point of reading ROWS: fund_balance carries a headroom, which the
    # coordination tuple's fixed schema had nowhere to put.
    defmodule WithHeadroom do
      use Ash.Resource,
        domain: ReactiveDag.UnionNodeTest.Domain,
        data_layer: Ash.DataLayer.Ets,
        extensions: [ReactiveDag.Node]

      ets do
      end

      attributes do
        attribute :check, :string, primary_key?: true, allow_nil?: false, public?: true
        attribute :subject, :string, primary_key?: true, allow_nil?: false, public?: true
        attribute :status, :string, public?: true
        attribute :headroom, :float, public?: true
      end

      actions do
        defaults [:read, :destroy]

        create :upsert do
          upsert?(true)
          accept([:check, :subject, :status, :headroom])
        end
      end

      reactive do
        id(:with_headroom)

        union from: [:fund_balance],
              into: [check: :cell, subject: :key, status: :status, headroom: :headroom]
      end
    end

    plan = ReactiveDag.Node.graph([FundBalance, WithHeadroom])
    {:ok, _, _} = Recompute.recompute(plan.cells["with_headroom"], ["*"])

    assert [%{check: "fund_balance", subject: "gf", status: "present", headroom: 250.0}] =
             Ash.read!(WithHeadroom)
  end

  test "a union over a node with no resource raises at ASSEMBLY, naming the input" do
    defmodule Tableless do
      use Ash.Resource,
        domain: ReactiveDag.UnionNodeTest.Domain,
        data_layer: Ash.DataLayer.Simple,
        extensions: [ReactiveDag.Node]

      reactive do
        id(:tableless)
        leaf?(true)
      end
    end

    defmodule OverTableless do
      use Ash.Resource,
        domain: ReactiveDag.UnionNodeTest.Domain,
        data_layer: Ash.DataLayer.Ets,
        extensions: [ReactiveDag.Node]

      ets do
      end

      attributes do
        attribute :check, :string, primary_key?: true, allow_nil?: false, public?: true
        attribute :subject, :string, primary_key?: true, allow_nil?: false, public?: true
      end

      actions do
        defaults [:read, :destroy]
        create :upsert, upsert?: true
      end

      reactive do
        id(:over_tableless)
        union from: [:tableless], into: [check: :cell, subject: :key]
      end
    end

    err =
      assert_raise ArgumentError, fn ->
        ReactiveDag.Node.graph([Tableless, OverTableless])
      end

    assert Exception.message(err) =~ ":tableless"
    assert Exception.message(err) =~ "reads its inputs' rows"
    assert Exception.message(err) =~ "declares no attributes"
  end
end

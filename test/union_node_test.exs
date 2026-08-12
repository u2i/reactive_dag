defmodule ReactiveDag.UnionNodeTest do
  @moduledoc """
  `union from: [...]` — the graph-wide roll-up as a node.

  A verdict-shaped node answers one question about one cell. Asking "what is
  failing ANYWHERE?" today means scanning every cell separately
  (`Insights.summary/1` does exactly that, one query per cell). A union node
  makes that roll-up a NODE: one indexed table, maintained incrementally — a
  verdict flips, that key propagates, one row updates.

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

  # two verdict-shaped inputs, whose results live in the coordination tuple
  defmodule CategoryHealth do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Simple,
      extensions: [ReactiveDag.Node]

    reactive do
      id(:category_health)
      leaf?(true)
      verdict?(true)
    end
  end

  defmodule FundBalance do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Simple,
      extensions: [ReactiveDag.Node]

    reactive do
      id(:fund_balance)
      leaf?(true)
      verdict?(true)
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

  # a real tuple table, since a union READS tuples
  defmodule FakeRepo do
    def start_link, do: Agent.start_link(fn -> %{} end, name: __MODULE__)

    def put(cell, key, status),
      do: Agent.update(__MODULE__, &Map.put(&1, {cell, key}, status))

    def query!("SELECT key, status, observed_at FROM " <> _, [cell | _]) do
      rows =
        Agent.get(__MODULE__, & &1)
        |> Enum.filter(fn {{c, _k}, _s} -> c == cell end)
        |> Enum.map(fn {{_c, k}, s} -> [k, s, nil] end)
        |> Enum.sort()

      %{rows: rows}
    end

    def query!("INSERT INTO " <> _, params) do
      params
      |> Enum.chunk_every(5)
      |> Enum.each(fn [cell, key, _r, _t, _prior] -> put(cell, key, "present") end)

      %{rows: []}
    end

    def query!("DELETE FROM " <> _, [cell]) do
      keys =
        Agent.get_and_update(__MODULE__, fn m ->
          {mine, rest} = Enum.split_with(m, fn {{c, _}, _} -> c == cell end)
          {Enum.map(mine, fn {{_, k}, _} -> k end), Map.new(rest)}
        end)

      %{rows: Enum.map(keys, &[&1, nil])}
    end

    def query!("SELECT COUNT" <> _, _), do: %{rows: [[Agent.get(__MODULE__, &map_size/1)]]}
    def query!("SELECT DISTINCT cell_id" <> _, _), do: %{rows: []}
  end

  defmodule NullWriter do
    @behaviour ReactiveDag.CoordinationWriter
    @impl true
    def put(_c, _k, _o), do: :ok
    @impl true
    def delete(_c, _k), do: :ok
  end

  setup do
    start_supervised!(%{id: FakeRepo, start: {FakeRepo, :start_link, []}})
    prev_repo = Application.get_env(:reactive_dag, :repo)
    prev_writer = Application.get_env(:reactive_dag, :coordination_writer)
    Application.put_env(:reactive_dag, :repo, FakeRepo)
    Application.put_env(:reactive_dag, :coordination_writer, NullWriter)

    on_exit(fn ->
      Application.put_env(:reactive_dag, :repo, prev_repo)
      Application.put_env(:reactive_dag, :coordination_writer, prev_writer)
    end)

    AllVerdicts |> Ash.read!() |> Enum.each(&Ash.destroy!/1)

    # two inputs' verdicts, in the tuple
    FakeRepo.put("category_health", "travel", "failing")
    FakeRepo.put("category_health", "meals", "present")
    FakeRepo.put("fund_balance", "gf", "present")

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
    FakeRepo.put("fund_balance", "gf", "failing")

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
end

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
  use ExUnit.Case, async: false

  alias ReactiveDag.{Drain, Frontier}
  alias ReactiveDag.Node.Recompute

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      allow_unregistered?(true)
    end
  end

  defmodule Expenses do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
      private?(true)
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
    end
  end

  defmodule CategoryTotals do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
      private?(true)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :category, :string, public?: true
      attribute :total, :float, public?: true
      attribute :n, :integer, public?: true
    end

    identities do
      identity :by_key, [:key], pre_check_with: ReactiveDag.RetireVanishedTest.Domain
    end

    actions do
      # NB :destroy — a node that reconciles needs a way to retire its rows
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

      recompute_by :category, to: :expenses, from: :category
      reduce into: [sum: [amount: :total], count: :n]
    end
  end

  defmodule FakeRepo do
    def start_link, do: Agent.start_link(fn -> MapSet.new() end, name: __MODULE__)

    def query!("INSERT INTO " <> _, params) do
      params
      |> Enum.chunk_every(4)
      |> Enum.each(fn [cell, key, _r, _t] -> Agent.update(__MODULE__, &MapSet.put(&1, {cell, key})) end)

      %{rows: []}
    end

    def query!("SELECT DISTINCT cell_id" <> _, _params) do
      ids = Agent.get(__MODULE__, & &1) |> Enum.map(&elem(&1, 0)) |> Enum.uniq()
      %{rows: Enum.map(ids, &[&1])}
    end

    def query!("DELETE FROM " <> _, [cell]) do
      keys =
        Agent.get_and_update(__MODULE__, fn set ->
          {mine, rest} = Enum.split_with(set, fn {c, _} -> c == cell end)
          {Enum.map(mine, &elem(&1, 1)), MapSet.new(rest)}
        end)

      %{rows: Enum.map(keys, &[&1])}
    end

    def query!("SELECT COUNT" <> _, _params), do: %{rows: [[Agent.get(__MODULE__, &MapSet.size/1)]]}
  end

  # the coordination side: records what the library PUT and DELETED, so the
  # tests can assert retirement propagates, not just that a row vanished.
  defmodule RecordingWriter do
    @behaviour ReactiveDag.CoordinationWriter

    def start_link, do: Agent.start_link(fn -> %{present: MapSet.new(), deleted: []} end, name: __MODULE__)

    @impl true
    def put(cell_id, key, _opts) do
      Agent.update(__MODULE__, &%{&1 | present: MapSet.put(&1.present, {cell_id, key})})
      :ok
    end

    @impl true
    def delete(cell_id, keys) do
      Agent.update(__MODULE__, fn s ->
        %{
          s
          | deleted: s.deleted ++ Enum.map(keys, &{cell_id, &1}),
            present: Enum.reduce(keys, s.present, &MapSet.delete(&2, {cell_id, &1}))
        }
      end)

      :ok
    end

    def deleted, do: Agent.get(__MODULE__, & &1.deleted)
    def present, do: Agent.get(__MODULE__, & &1.present) |> MapSet.to_list()
  end

  setup do
    start_supervised!(%{id: FakeRepo, start: {FakeRepo, :start_link, []}})
    start_supervised!(%{id: RecordingWriter, start: {RecordingWriter, :start_link, []}})
    prev_repo = Application.get_env(:reactive_dag, :repo)
    prev_writer = Application.get_env(:reactive_dag, :coordination_writer)
    Application.put_env(:reactive_dag, :repo, FakeRepo)
    Application.put_env(:reactive_dag, :coordination_writer, RecordingWriter)

    on_exit(fn ->
      Application.put_env(:reactive_dag, :repo, prev_repo)
      Application.put_env(:reactive_dag, :coordination_writer, prev_writer)
    end)

    for {k, cat, amt} <- [{"e1", "travel", 100.0}, {"e2", "meals", 40.0}] do
      Expenses
      |> Ash.Changeset.for_create(:create, %{key: k, category: cat, amount: amt})
      |> Ash.create!()
    end

    :ok
  end

  defp plan, do: ReactiveDag.Node.graph([Expenses, CategoryTotals])
  defp cell, do: plan().cells["category_totals"]
  defp rows, do: CategoryTotals |> Ash.read!() |> Enum.map(&{&1.key, &1.total, &1.n}) |> Enum.sort()

  defp drain(plan),
    do: Drain.run(plan, recompute: ReactiveDag.Node.Recompute, key_rule: ReactiveDag.Node.KeyRule)

  test "a DELETED input retires its unit — the row goes, and the retirement propagates" do
    c = cell()
    {:ok, _} = Recompute.recompute(c, ["*"])
    assert rows() == [{"meals", 40.0, 1}, {"travel", 100.0, 1}]

    # the last expense in :meals goes away
    Expenses |> Ash.get!("e2") |> Ash.destroy!()

    {:ok, changed} = Recompute.recompute(c, ["*"])

    # the vanished unit is REPORTED as changed, so parents recompute
    assert "meals" in changed

    # ...its payload row is gone (the derived table shows only live units)
    assert rows() == [{"travel", 100.0, 1}]

    # ...and its coordination tuple was deleted
    assert {"category_totals", "meals"} in RecordingWriter.deleted()
  end

  test "a scoped pass only retires within its CLAIM — live units are untouched" do
    c = cell()
    {:ok, _} = Recompute.recompute(c, ["*"])
    assert rows() == [{"meals", 40.0, 1}, {"travel", 100.0, 1}]

    # recompute ONLY :travel. :meals produced nothing in this pass, but it was
    # never claimed — retiring it here would destroy a live unit.
    {:ok, _} = Recompute.recompute(c, ["travel"])

    assert rows() == [{"meals", 40.0, 1}, {"travel", 100.0, 1}]
    assert RecordingWriter.deleted() == []
  end

  test "a scoped pass DOES retire a claimed unit that produced nothing" do
    c = cell()
    {:ok, _} = Recompute.recompute(c, ["*"])

    Expenses |> Ash.get!("e2") |> Ash.destroy!()

    # claim exactly the emptied unit
    {:ok, changed} = Recompute.recompute(c, ["meals"])

    assert changed == ["meals"]
    assert rows() == [{"travel", 100.0, 1}]
    assert {"category_totals", "meals"} in RecordingWriter.deleted()
  end

  test "an input MOVING between units: the destination is exact, the ORIGIN needs a whole-cell pass" do
    p = plan()

    Frontier.mark_dirty("expenses", ["*"], "seed")
    {:ok, _} = drain(p)
    assert rows() == [{"meals", 40.0, 1}, {"travel", 100.0, 1}]

    # e2 moves meals -> travel.
    Expenses
    |> Ash.get!("e2")
    |> Ash.Changeset.for_update(:recategorise, %{category: "travel"})
    |> Ash.update!()

    Frontier.mark_dirty("expenses", ["e2"], "recategorised")
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

  test "a node with a custom `upsert:` owns its own writes — the library does not reconcile" do
    defmodule CustomWrite do
      use Ash.Resource,
        domain: ReactiveDag.RetireVanishedTest.Domain,
        data_layer: Ash.DataLayer.Simple,
        extensions: [ReactiveDag.Node]

      reactive do
        id(:custom_write)
        recompute_by :category, to: :expenses, from: :category
        reduce into: fn cat, items -> %{category: cat, n: length(items)} end,
               upsert: fn _key, _row -> true end
      end
    end

    p = ReactiveDag.Node.graph([Expenses, CustomWrite])
    {:ok, changed} = Recompute.recompute(p.cells["custom_write"], ["*"])

    # only what the pass produced; nothing retired on the host's behalf
    assert Enum.sort(changed) == ["meals", "travel"]
    assert RecordingWriter.deleted() == []
  end
end

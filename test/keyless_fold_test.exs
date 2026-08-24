defmodule ReactiveDag.KeylessFoldTest do
  @moduledoc """
  A fold with NO stored key column.

  Its unit is named by `row_key` columns the row already carries — `{fund, year}`
  — so nothing needs a `"|"`-joined `rollup_key` beside them. That column was
  always a duplicate of two columns in its own row; what kept it was the LOOKUP:
  a payload write and a retire both had to find "the row for this unit", and the
  only handle they had was a key.

  With the unit's own values in hand (derived from the change, not from splitting
  a string) both find the row by its columns. This asserts the half that fails
  silently: a RETIRE, which returns false when it cannot find a row, so a stale
  derived row survives and reads exactly like a live one.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.{Drain, Frontier}

  defmodule Versions do
    def start_link, do: Agent.start_link(fn -> %{} end, name: __MODULE__)

    def record(written, prior) do
      id = "v-" <> to_string(System.unique_integer([:positive]))
      Agent.update(__MODULE__, &Map.put(&1, id, diff(prior, written)))
      id
    end

    def changes(id), do: Agent.get(__MODULE__, &Map.get(&1, id))

    defp diff(nil, w) do
      Map.new([:key, :fund, :year, :amount], &{to_string(&1), %{"to" => Map.get(w, &1)}})
    end

    defp diff(p, w) do
      Map.new([:key, :fund, :year, :amount], fn f ->
        old = Map.get(p, f)
        new = Map.get(w, f)
        {to_string(f), if(old == new, do: %{"unchanged" => new}, else: %{"from" => old, "to" => new})}
      end)
    end
  end

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false
    resources(do: allow_unregistered?(true))
  end

  defmodule Lines do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Ets, extensions: [ReactiveDag.Node]
    ets(do: private?(true))

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :fund, :string, public?: true
      attribute :year, :string, public?: true
      attribute :amount, :integer, public?: true
    end

    actions do
      defaults [:read, :destroy]
      create :upsert, upsert?: true, accept: [:key, :fund, :year, :amount]
    end

    reactive do
      id(:lines)
      leaf?(true)
      payload_key(:key)
    end
  end

  # The middle cell — GRAPH-WRITTEN through the payload loop, which is where a
  # version gets recorded. A LEAF records none (its rows arrive from outside), so a
  # fold directly over one has no change to derive from; every real host's folds sit
  # above a materializing cell for exactly this reason.
  defmodule Projected do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Ets, extensions: [ReactiveDag.Node]
    ets(do: private?(true))

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :fund, :string, public?: true
      attribute :year, :string, public?: true
      attribute :amount, :integer, public?: true
    end

    actions do
      defaults [:read, :destroy]
      create :upsert, upsert?: true, accept: [:key, :fund, :year, :amount]
    end

    reactive do
      id(:projected)
      op(:map)
      payload_key(:key)
      depends_on([:lines])
      compute(ReactiveDag.KeylessFoldTest.Project)
      version_id({ReactiveDag.KeylessFoldTest.Versions, :record, []})
      version_diff({ReactiveDag.KeylessFoldTest.Versions, :changes, []})
    end
  end

  defmodule Project do
    @behaviour ReactiveDag.Op

    @impl true
    def recompute(cell, keys, opts \\ []) do
      lines =
        case keys do
          ["*"] -> Ash.read!(Lines)
          ks -> Enum.filter(Ash.read!(Lines), &(&1.key in ks))
        end

      changed =
        for l <- lines,
            ReactiveDag.Node.Payload.upsert_row(
              Projected, cell.meta, l.key,
              %{key: l.key, fund: l.fund, year: l.year, amount: l.amount},
              opts
            ) != :unchanged do
          l.key
        end

      {:ok, changed}
    end
  end

  # THE POINT: a UUID pk, `fund`/`year` as plain columns, and NO key column.
  defmodule Rollup do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Ets, extensions: [ReactiveDag.Node]
    ets(do: private?(true))

    attributes do
      uuid_primary_key :id
      attribute :fund, :string, public?: true
      attribute :year, :string, public?: true
      attribute :total, :integer, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        accept [:fund, :year, :total]
        upsert?(true)
        upsert_identity :by_unit
      end
    end

    identities do
      identity :by_unit, [:fund, :year]
    end

    reactive do
      id(:rollup)
      op(:fold)
      # `payload_key false` — this node HAS no key column, declared rather than
      # deduced. Its row is identified by the columns below.
      payload_key(false)
      row_key([:fund, :year])
      recompute_by([fund: :fund, year: :year], to: :projected)
      reduce(into: [sum: [amount: :total]])
    end
  end

  defp plan, do: ReactiveDag.Node.graph([Lines, Projected, Rollup])

  defp drain,
    do: Drain.run(plan(), recompute: ReactiveDag.Node.Recompute, key_rule: ReactiveDag.Node.KeyRule)

  defp put_line(key, fund, year, amount) do
    Ash.create!(Lines, %{key: key, fund: fund, year: year, amount: amount}, action: :upsert)
    Frontier.mark_dirty("lines", [key], "test")
  end

  defp unit(fund, year),
    do: Ash.read!(Rollup) |> Enum.find(&(&1.fund == fund and &1.year == year))

  setup do
    start_supervised!(ReactiveDag.Test.FakeFrontierRepo)
    ReactiveDag.Test.FakeFrontierRepo.install()
    start_supervised!(%{id: Versions, start: {Versions, :start_link, []}})

    for m <- [Lines, Projected, Rollup], r <- Ash.read!(m), do: Ash.destroy!(r)
    for cell <- ["lines", "projected", "rollup"], do: Frontier.claim(cell)
    :ok
  end

  test "the fold writes its rows with no key column at all" do
    put_line("a", "gf", "2025", 10)
    put_line("b", "water", "2025", 20)
    {:ok, _} = drain()

    assert unit("gf", "2025").total == 10
    assert unit("water", "2025").total == 20

    # …and nothing on the row is a joined key.
    refute Map.has_key?(unit("gf", "2025"), :rollup_key)
  end

  test "a MOVED row's abandoned unit is RETIRED — found by its columns" do
    put_line("a", "gf", "2025", 10)
    {:ok, _} = drain()
    assert unit("gf", "2025").total == 10

    # The only line in gf|2025 moves to water|2025, so gf|2025 has no input rows
    # left and its derived row must go. The retire has no key column to find it
    # by — only the unit's own values, derived from the change.
    put_line("a", "water", "2025", 10)
    {:ok, _} = drain()

    assert unit("water", "2025").total == 10

    refute unit("gf", "2025"),
           "the abandoned unit's row survived — a retire that cannot find its row " <>
             "returns false, so a stale derived row reads exactly like a live one"
  end

  test "an untouched unit is neither repriced nor retired" do
    put_line("a", "gf", "2025", 10)
    put_line("b", "water", "2026", 99)
    {:ok, _} = drain()

    put_line("a", "gf", "2026", 10)
    {:ok, report} = drain()

    claimed = Map.new(report.steps, &{&1.cell, &1})["rollup"].claimed
    refute "water|2026" in claimed
    assert unit("water", "2026").total == 99
  end
end

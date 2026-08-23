defmodule ReactiveDag.PayloadDiffPropagationTest do
  @moduledoc """
  A GRAPH-WRITTEN row propagates from its diff, not from a read-back.

  `dirties_on` covers the other producer — a host writing a row itself, which
  attaches the diff to the mark. This covers the one every derived cell uses: a
  payload write inside a recompute, whose diff `Payload.collecting_diffs/1`
  collects and the drain hands to the key rule.

  Written because a mutation exposed the gap: removing the payload-diff channel
  from `Drain.propagate/5` failed **nothing**. The external path had tests and
  the internal one did not, which is the wrong way round — a real host writes
  every one of its cells through the payload loop.

  The case that matters is a row MOVING between units. A live read names only
  where it landed, so the unit it left keeps counting a row it no longer holds
  unless the diff names both.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.{Drain, Frontier}

  # The frontier's repo. Mirrors `dirties_on_test`'s: it must retain the DIFF a
  # mark carried, since that is half of what this file is about.
  defmodule FakeRepo do
    def start_link, do: Agent.start_link(fn -> %{} end, name: __MODULE__)

    def query!("INSERT INTO " <> _, params) do
      params
      |> Enum.chunk_every(6)
      |> Enum.each(fn [cell, tenant, key, _r, _t, prior] ->
        Agent.update(__MODULE__, fn m -> Map.put_new(m, {tenant, cell, key}, prior) end)
      end)

      %{rows: []}
    end

    def query!("SELECT DISTINCT cell_id" <> _, _params) do
      ids = Agent.get(__MODULE__, & &1) |> Map.keys() |> Enum.map(&elem(&1, 1)) |> Enum.uniq()
      %{rows: Enum.map(ids, &[&1])}
    end

    def query!("DELETE FROM " <> _, [cell, tenant]) do
      rows =
        Agent.get_and_update(__MODULE__, fn m ->
          {mine, rest} = Enum.split_with(m, fn {{t, c, _}, _} -> t == tenant and c == cell end)
          {Enum.map(mine, fn {{_t, _c, k}, prior} -> [k, prior] end), Map.new(rest)}
        end)

      %{rows: rows}
    end

    def query!("SELECT COUNT" <> _, _params), do: %{rows: [[Agent.get(__MODULE__, &map_size/1)]]}
    def query!("SELECT pg_try_advisory_lock" <> _, _params), do: %{rows: [[true]]}
    def query!("SELECT pg_advisory_unlock" <> _, _params), do: %{rows: [[true]]}
  end

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      allow_unregistered?(true)
    end
  end

  # A LEAF whose rows a source writes. Its `fund` is what the rollup groups by,
  # so changing it moves a row between units.
  defmodule Lines do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
      private?(true)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :fund, :string, public?: true
      attribute :amount, :integer, public?: true
    end

    actions do
      defaults [:read, :destroy]
      create :upsert, upsert?: true, accept: [:key, :fund, :amount]
    end

    reactive do
      id(:lines)
      leaf?(true)
      payload_key(:key)
    end
  end

  # The middle cell — GRAPH-WRITTEN, through the payload loop. Its rows are what
  # the rollup above it groups, so its diffs are what must propagate.
  defmodule Projected do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
      private?(true)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :fund, :string, public?: true
      attribute :amount, :integer, public?: true
    end

    actions do
      defaults [:read, :destroy]
      create :upsert, upsert?: true, accept: [:key, :fund, :amount]
    end

    reactive do
      id(:projected)
      op(:map)
      payload_key(:key)
      depends_on([:lines])
      compute(ReactiveDag.PayloadDiffPropagationTest.Project)
    end
  end

  defmodule Rollup do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
      private?(true)
    end

    attributes do
      attribute :fund, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :total, :integer, public?: true
    end

    actions do
      defaults [:read, :destroy]
      create :upsert, upsert?: true, accept: [:fund, :total]
    end

    reactive do
      id(:rollup)
      op(:fold)
      payload_key(:fund)
      recompute_by(:fund, to: :projected, from: :fund)
      reduce(into: [sum: [amount: :total]])
    end
  end

  # A `compute` op writing its own rows through the payload loop — which is what
  # every real host's extraction cells do.
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
              Projected,
              cell.meta,
              l.key,
              %{key: l.key, fund: l.fund, amount: l.amount},
              opts
            ) != :unchanged do
          l.key
        end

      {:ok, changed}
    end
  end

  defp plan, do: ReactiveDag.Node.graph([Lines, Projected, Rollup])

  defp drain do
    Drain.run(plan(), recompute: ReactiveDag.Node.Recompute, key_rule: ReactiveDag.Node.KeyRule)
  end

  defp put_line(key, fund, amount) do
    Ash.create!(Lines, %{key: key, fund: fund, amount: amount}, action: :upsert)
    Frontier.mark_dirty("lines", [key], "test")
  end

  setup do
    start_supervised!(%{id: FakeRepo, start: {FakeRepo, :start_link, []}})
    prev = Application.get_env(:reactive_dag, :repo)
    Application.put_env(:reactive_dag, :repo, FakeRepo)
    on_exit(fn -> Application.put_env(:reactive_dag, :repo, prev) end)

    # The ETS tables are shared, so start from a known-empty state — and drain
    # the frontier the destroys above just dirtied.
    for m <- [Lines, Projected, Rollup], r <- Ash.read!(m), do: Ash.destroy!(r)
    for cell <- ["lines", "projected", "rollup"], do: Frontier.claim(cell)
    :ok
  end

  test "the middle cell is graph-written, so its diff comes from the payload loop" do
    put_line("l1", "A", 10)
    {:ok, _} = drain()

    assert (Rollup |> Ash.get!("A")).total == 10
  end

  test "a MOVED row claims both units — from the payload write's own diff" do
    put_line("l1", "A", 10)
    {:ok, _} = drain()
    assert (Rollup |> Ash.get!("A")).total == 10

    # `fund` moves A → ES. `projected`'s payload write sees both sides; a live
    # read of the row would name only ES, leaving A counting a row it no longer
    # holds.
    put_line("l1", "ES", 10)
    {:ok, report} = drain()

    steps = Map.new(report.steps, &{&1.cell, &1})

    assert Enum.sort(steps["rollup"].claimed) == ["A", "ES"],
           "the unit it left AND the one it landed in — not `\"*\"`, and not just ES"

    assert (Rollup |> Ash.get!("ES")).total == 10

    # A was RETIRED, not zeroed — the fold produced no row for a unit with no
    # input rows left, and `retire_vanished` reconciled it away. Either outcome
    # proves the point (A was repriced rather than left holding 10); asserting a
    # zeroed row would have asserted the wrong mechanism.
    assert Ash.get(Rollup, "A") |> elem(0) == :error,
           "the unit the row left holds nothing, rather than still counting it"
  end

  test "the claim is per-unit, never the whole cell" do
    # Two funds, then move one row. Only the two affected units are claimed —
    # `"*"` would reprice GF as well, which is the over-claiming this replaces.
    put_line("l1", "A", 10)
    put_line("l2", "GF", 99)
    {:ok, _} = drain()

    put_line("l1", "ES", 10)
    {:ok, report} = drain()

    claimed = Map.new(report.steps, &{&1.cell, &1})["rollup"].claimed

    refute "*" in claimed
    refute "GF" in claimed, "an untouched unit is not repriced"
    assert (Rollup |> Ash.get!("GF")).total == 99
  end

  test "an unchanged rewrite propagates nothing" do
    put_line("l1", "A", 10)
    {:ok, _} = drain()

    # Same content. `projected`'s payload write reports `:unchanged`, so it
    # records no diff and reports no changed key — and the rollup is never
    # claimed at all.
    put_line("l1", "A", 10)
    {:ok, report} = drain()

    refute Map.has_key?(Map.new(report.steps, &{&1.cell, &1}), "rollup"),
           "nothing moved, so there was nothing to reprice"
  end
end

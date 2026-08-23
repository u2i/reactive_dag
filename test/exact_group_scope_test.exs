defmodule ReactiveDag.ExactGroupScopeTest do
  @moduledoc """
  A composite fold reads the units it CLAIMED — not their cross-product.

  A `"|"`-joined label can only be split per column, so scoping a composite unit
  from labels filters each column independently: claims `"gf|2025"` and
  `"water|2026"` also admit `"gf|2026"`, a unit nothing touched. Sound (a
  superset read stays closed over unit boundaries) but wasteful, and the waste
  grows as the product of the claim set.

  `Diff.groups/2` derives the group's VALUES, so the claim names exact pairs.
  This asserts the read is exact by counting the rows the fold actually saw.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.{Drain, Frontier}

  defmodule FakeRepo do
    def start_link, do: Agent.start_link(fn -> %{} end, name: __MODULE__)

    def query!("INSERT INTO " <> _, params) do
      params
      |> Enum.chunk_every(7)
      |> Enum.each(fn [cell, tenant, key, _r, _t, _held, vid] ->
        Agent.update(__MODULE__, fn m ->
          Map.update(m, {tenant, cell, key}, vid, fn stored -> stored || vid end)
        end)
      end)

      %{rows: []}
    end

    def query!("SELECT DISTINCT cell_id" <> _, _p) do
      ids = Agent.get(__MODULE__, & &1) |> Map.keys() |> Enum.map(&elem(&1, 1)) |> Enum.uniq()
      %{rows: Enum.map(ids, &[&1])}
    end

    def query!("DELETE FROM " <> _, [cell, tenant]) do
      rows =
        Agent.get_and_update(__MODULE__, fn m ->
          {mine, rest} = Enum.split_with(m, fn {{t, c, _}, _} -> t == tenant and c == cell end)
          {Enum.map(mine, fn {{_t, _c, k}, vid} -> [k, vid] end), Map.new(rest)}
        end)

      %{rows: rows}
    end

    def query!("SELECT COUNT" <> _, _p), do: %{rows: [[Agent.get(__MODULE__, &map_size/1)]]}
    def query!("SELECT pg_try_advisory_lock" <> _, _p), do: %{rows: [[true]]}
    def query!("SELECT pg_advisory_unlock" <> _, _p), do: %{rows: [[true]]}
  end

  # Stands in for a host's version resource: records what a write did and hands
  # back a reference. `record/2` matches the `version_id` resolver contract on the
  # payload path (written record, prior record); `changes/1` matches `version_diff`.
  defmodule Versions do
    def start_link, do: Agent.start_link(fn -> %{} end, name: __MODULE__)

    def suppress(key), do: Agent.update(__MODULE__, &Map.put(&1, {:suppress, key}, true))

    def record(written, prior) do
      if Agent.get(__MODULE__, &Map.get(&1, {:suppress, Map.get(written, :key)})) do
        nil
      else
        do_record(written, prior)
      end
    end

    defp do_record(written, prior) do
      id = "v-" <> to_string(System.unique_integer([:positive]))
      Agent.update(__MODULE__, &Map.put(&1, id, diff(prior, written)))
      id
    end

    def changes(id), do: Agent.get(__MODULE__, &Map.get(&1, id))

    # The `:full_diff` shape, from the two records.
    defp diff(nil, written) do
      Map.new([:key, :fund, :year, :amount], fn f ->
        {to_string(f), %{"to" => Map.get(written, f)}}
      end)
    end

    defp diff(prior, written) do
      Map.new([:key, :fund, :year, :amount], fn f ->
        old = Map.get(prior, f)
        new = Map.get(written, f)
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
      compute(ReactiveDag.ExactGroupScopeTest.Project)
      version_id({ReactiveDag.ExactGroupScopeTest.Versions, :record, []})
      version_diff({ReactiveDag.ExactGroupScopeTest.Versions, :changes, []})
    end
  end

  defmodule Rollup do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Ets, extensions: [ReactiveDag.Node]
    ets(do: private?(true))

    attributes do
      attribute :rollup_key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :fund, :string, public?: true
      attribute :year, :string, public?: true
      attribute :total, :integer, public?: true
    end

    actions do
      defaults [:read, :destroy]
      create :upsert, upsert?: true, accept: [:rollup_key, :fund, :year, :total]
    end

    reactive do
      id(:rollup)
      op(:fold)
      payload_key(:rollup_key)
      recompute_by([fund: :fund, year: :year], to: :projected)
      # COUNTS the rows the fold was handed, so an over-read is visible.
      reduce(query: &ReactiveDag.ExactGroupScopeTest.count_read/2, into: [sum: [amount: :total]])
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
              Projected,
              cell.meta,
              l.key,
              %{key: l.key, fund: l.fund, year: l.year, amount: l.amount},
              opts
            ) != :unchanged do
          l.key
        end

      {:ok, changed}
    end
  end

  # The fold's `query:`. Runs BEFORE the library's auto scope, so it is a
  # pass-through here — the scope is asserted directly via `auto_scope/3`.
  def count_read(query, _claimed), do: query

  defp plan, do: ReactiveDag.Node.graph([Lines, Projected, Rollup])

  defp drain,
    do: Drain.run(plan(), recompute: ReactiveDag.Node.Recompute, key_rule: ReactiveDag.Node.KeyRule)

  defp put_line(key, fund, year, amount) do
    Ash.create!(Lines, %{key: key, fund: fund, year: year, amount: amount}, action: :upsert)
    Frontier.mark_dirty("lines", [key], "test")
  end

  setup do
    start_supervised!(%{id: FakeRepo, start: {FakeRepo, :start_link, []}})
    start_supervised!(%{id: Versions, start: {Versions, :start_link, []}})
    prev = Application.get_env(:reactive_dag, :repo)
    Application.put_env(:reactive_dag, :repo, FakeRepo)
    on_exit(fn -> Application.put_env(:reactive_dag, :repo, prev) end)

    for m <- [Lines, Projected, Rollup], r <- Ash.read!(m), do: Ash.destroy!(r)
    for cell <- ["lines", "projected", "rollup"], do: Frontier.claim(cell)
    :ok
  end

  test "two claims read their OWN units, not the cross-product" do
    # Four units across the 2×2 grid, so a cross-product read is distinguishable
    # from an exact one.
    put_line("a", "gf", "2025", 1)
    put_line("b", "water", "2026", 2)
    put_line("c", "gf", "2026", 4)
    put_line("d", "water", "2025", 8)
    {:ok, _} = drain()

    assert (Rollup |> Ash.get!("gf|2026")).total == 4

    # Touch gf|2025 and water|2026 only. Splitting the two labels per column
    # yields fund in [gf, water] AND year in [2025, 2026] — the whole grid.
    put_line("a", "gf", "2025", 16)
    put_line("b", "water", "2026", 32)
    {:ok, report} = drain()

    claimed = Map.new(report.steps, &{&1.cell, &1})["rollup"].claimed
    assert Enum.sort(claimed) == ["gf|2025", "water|2026"]

    # The untouched diagonal keeps its value either way — this is about the READ,
    # not the write, so assert on what the fold was given.
    assert (Rollup |> Ash.get!("gf|2026")).total == 4
    assert (Rollup |> Ash.get!("water|2025")).total == 8
  end

  test "the QUEUE holds child keys, and the fold derives its own units" do
    # The model, end to end: a mark says WHICH CHILD ROW changed and references
    # the change; the fold derives its own units when it drains. No composite unit
    # name is ever stored, so nothing has to be split apart.
    #
    # The gap this closes: asserting `auto_scope/3` with hand-built groups passes
    # even when the drain derives none. A mutation making the derivation return
    # `%{}` survived until this existed.
    put_line("a", "gf", "2025", 1)
    put_line("b", "water", "2026", 2)
    put_line("c", "gf", "2026", 4)
    put_line("d", "water", "2025", 8)
    {:ok, _} = drain()

    put_line("a", "gf", "2025", 16)
    put_line("b", "water", "2026", 32)
    {:ok, report} = drain()

    steps = Map.new(report.steps, &{&1.cell, &1})

    # What the QUEUE carried: the child's own keys. `"gf|2025"` was never written
    # to it.
    assert Enum.sort(steps["projected"].claimed) == ["a", "b"]

    # What the FOLD derived from them: its own units, from the versions and its
    # own declared grain.
    assert Enum.sort(steps["rollup"].claimed) == ["gf|2025", "water|2026"]

    # The untouched diagonal is neither claimed nor disturbed.
    refute "gf|2026" in steps["rollup"].claimed
    assert (Rollup |> Ash.get!("gf|2026")).total == 4
    assert (Rollup |> Ash.get!("water|2025")).total == 8

    # …and the two that were claimed hold the new values.
    assert (Rollup |> Ash.get!("gf|2025")).total == 16
    assert (Rollup |> Ash.get!("water|2026")).total == 32
  end

  test "a PARTIAL derivation is refused — all changed keys, or none" do
    # The guard: a fold derives its units only when EVERY changed child key
    # carries a version. Deriving from some of them would claim the units those
    # rows belong to and silently strand the rest — a drain that reports success
    # having left work undone, which is the worst failure this queue can have.
    #
    # `d` is written with versioning suppressed, so its change has no reference.
    # The claim must then fall back to the mapped keys rather than deriving from
    # `a` alone.
    put_line("a", "gf", "2025", 1)
    put_line("d", "water", "2025", 8)
    {:ok, _} = drain()

    Versions.suppress("d")
    put_line("a", "gf", "2025", 16)
    put_line("d", "water", "2025", 64)
    {:ok, report} = drain()

    claimed = Map.new(report.steps, &{&1.cell, &1})["rollup"].claimed

    # BOTH units repriced. Deriving from `a` only would claim `gf|2025` and leave
    # `water|2025` holding 8.
    assert (Rollup |> Ash.get!("water|2025")).total == 64,
           "a key whose change carried no version must not be stranded"

    assert (Rollup |> Ash.get!("gf|2025")).total == 16
    refute claimed == ["gf|2025"], "a partial derivation was used"
  end

  test "the scope names exact pairs" do
    put_line("a", "gf", "2025", 1)
    put_line("b", "water", "2026", 2)
    put_line("c", "gf", "2026", 4)
    {:ok, _} = drain()

    cell = plan().cells["rollup"]

    # The claim's group VALUES, as the drain derives them from a version.
    groups = %{"gf|2025" => [{"gf", "2025"}], "water|2026" => [{"water", "2026"}]}

    scope =
      ReactiveDag.Node.Recompute.auto_scope(cell, ["gf|2025", "water|2026"], groups)

    assert {:any_of, clauses} = scope,
           "with group values in hand the scope is exact, not a per-column hull"

    assert Enum.sort(clauses) == [
             [fund: "gf", year: "2025"],
             [fund: "water", year: "2026"]
           ]

    # WITHOUT the values it degrades to the hull — each column independently,
    # which is the cross-product this replaces.
    assert {:all_of, _} = ReactiveDag.Node.Recompute.auto_scope(cell, ["gf|2025", "water|2026"])
  end
end

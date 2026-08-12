defmodule ReactiveDag.Node.KeysTest do
  @moduledoc """
  `ReactiveDag.Node.Keys` — one question ("which keys does this cell have?"),
  answered from wherever that cell's rows actually live.

  This is what decouples attestations from the coordination tuple. Attestations
  make three reads — the raw rows a requirement is about, the eligibility set of
  who may sign, and the subset a set-level scope selects — and **every one of
  them only ever uses `row.key`**. Nothing touches status or freshness. So the
  dependency was never on the tuple; it was on a key set, which a payload node
  can answer from its own table.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.Node.Keys

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      allow_unregistered?(true)
    end
  end

  # a PAYLOAD node: its rows are the truth about which keys it holds
  defmodule Rollups do
    use Ash.Resource,
      domain: Domain,
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
      id(:rollups)
      leaf?(true)
    end
  end

  # an IDENTITY-KEYED payload node: keys are the PK serialized in order
  defmodule FundFy do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute :fund, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :fy, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :total, :float, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :create do
        accept([:fund, :fy, :total])
      end
    end

    reactive do
      id(:fund_fy)
      leaf?(true)
    end
  end

  # a TABLELESS verdict node: the tuple is the only place its result exists
  defmodule Health do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Simple,
      extensions: [ReactiveDag.Node]

    reactive do
      id(:health)
      leaf?(true)
      verdict?(true)
    end
  end

  defmodule FakeRepo do
    def start_link, do: Agent.start_link(fn -> MapSet.new() end, name: __MODULE__)

    def put(cell, key), do: Agent.update(__MODULE__, &MapSet.put(&1, {cell, key}))

    def query!("SELECT key FROM " <> _, [cell | _]) do
      keys =
        Agent.get(__MODULE__, & &1)
        |> Enum.filter(fn {c, _k} -> c == cell end)
        |> Enum.map(&elem(&1, 1))
        |> Enum.sort()

      %{rows: Enum.map(keys, &[&1])}
    end

    def query!(_sql, _params), do: %{rows: []}
  end

  setup do
    start_supervised!(%{id: FakeRepo, start: {FakeRepo, :start_link, []}})
    prev = Application.get_env(:reactive_dag, :repo)
    Application.put_env(:reactive_dag, :repo, FakeRepo)
    on_exit(fn -> Application.put_env(:reactive_dag, :repo, prev) end)

    Rollups |> Ash.read!() |> Enum.each(&Ash.destroy!/1)
    FundFy |> Ash.read!() |> Enum.each(&Ash.destroy!/1)
    :ok
  end

  defp plan, do: ReactiveDag.Node.graph([Rollups, FundFy, Health])

  test "a PAYLOAD node's keys come from its own rows, not the tuple" do
    for {k, t} <- [{"travel", 1.0}, {"meals", 2.0}] do
      Rollups |> Ash.Changeset.for_create(:create, %{key: k, total: t}) |> Ash.create!()
    end

    # the tuple says something DIFFERENT — and is not the answer
    FakeRepo.put("rollups", "stale_key_from_the_tuple")

    keys = Keys.current(plan().cells["rollups"])

    assert Enum.sort(keys) == ["meals", "travel"]
    refute "stale_key_from_the_tuple" in keys
  end

  test "an IDENTITY-KEYED node's keys are its primary key, serialized" do
    FundFy |> Ash.Changeset.for_create(:create, %{fund: "gf", fy: "2025", total: 1.0}) |> Ash.create!()

    assert Keys.current(plan().cells["fund_fy"]) == ["gf|2025"]
  end

  test "a TABLELESS verdict node's keys come from the tuple — its only home" do
    FakeRepo.put("health", "a")
    FakeRepo.put("health", "b")

    assert Enum.sort(Keys.current(plan().cells["health"])) == ["a", "b"]
  end

  test "an unknown cell id falls back to the tuple — a host may direct-write outside its plan" do
    FakeRepo.put("not_in_the_plan", "k1")

    assert Keys.current(plan(), "not_in_the_plan") == ["k1"]
  end

  test "current/2 resolves a known id through its cell" do
    Rollups |> Ash.Changeset.for_create(:create, %{key: "travel", total: 1.0}) |> Ash.create!()
    FakeRepo.put("rollups", "ignored")

    assert Keys.current(plan(), "rollups") == ["travel"]
  end

  test "nil means CANNOT TELL, which is not the same as no keys" do
    # a resource that will not read (no repo configured for it) is unknowable;
    # returning [] would invite a caller to retire everything it holds
    Application.put_env(:reactive_dag, :repo, __MODULE__.NoSuchRepo)

    assert Keys.current(plan(), "health") == nil
  end

  describe "scoped/3" do
    setup do
      for k <- ["acme|m1", "acme|m2", "other|m3"] do
        Rollups |> Ash.Changeset.for_create(:create, %{key: k, total: 1.0}) |> Ash.create!()
      end

      :ok
    end

    test "narrows a payload node's OWN keys by a key_scope" do
      cell = plan().cells["rollups"]

      assert Enum.sort(Keys.scoped(cell, "rollups", {:prefix, "acme|%"})) ==
               ["acme|m1", "acme|m2"]
    end

    test "a nil scope is everything" do
      cell = plan().cells["rollups"]
      assert length(Keys.scoped(cell, "rollups", nil)) == 3
    end

    test "the segment scope works the same in memory as it did in SQL" do
      cell = plan().cells["rollups"]

      assert Keys.scoped(cell, "rollups", {:segment, 1, "|", "other"}) == ["other|m3"]
    end

    test "with no cell it falls back to the tuple" do
      FakeRepo.put("elsewhere", "acme|x")
      FakeRepo.put("elsewhere", "zzz|y")

      assert Keys.scoped(nil, "elsewhere", {:prefix, "acme|%"}) == ["acme|x"]
    end
  end
end

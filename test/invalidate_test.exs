defmodule ReactiveDag.InvalidateTest do
  @moduledoc """
  Reprocessing a fingerprinted node actually re-runs it.

  This is the case the whole selection chain existed for and could not finish. A
  `per_key` node skips rows whose declared inputs have not moved — and after a
  prompt change they have not. So marking the keys claimed them and the recompute
  skipped every one: the button worked and nothing happened.

  The fix is not a force flag threaded through the recompute. It is to clear the
  stored fingerprint, which makes the comparison fail *honestly*: a null
  fingerprint means "no valid prior result", and once the code that produced it
  has changed, that is precisely true.

  The test that matters is `"the action runs again"` — everything else guards
  the edges around it.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.Node.Rows

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      allow_unregistered?(true)
    end
  end

  defmodule Calls do
    def start_link, do: Agent.start_link(fn -> [] end, name: __MODULE__)
    def record(key), do: Agent.update(__MODULE__, &[key | &1])
    def all, do: Agent.get(__MODULE__, &Enum.reverse/1)
    def reset, do: Agent.update(__MODULE__, fn _ -> [] end)
  end

  defmodule Transcripts do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Ets, extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :body, :string, public?: true
      attribute :year, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]
      create :upsert, upsert?: true, accept: [:key, :body, :year]
    end

    reactive do
      id(:transcripts)
      leaf?(true)
    end
  end

  defmodule Summaries do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Ets, extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :year, :string, public?: true
      attribute :summary, :string, public?: true
      attribute :fingerprint, :string, public?: true
    end

    actions do
      defaults [:read, :destroy, update: [:summary, :fingerprint, :year]]
      create :upsert, upsert?: true, accept: [:key, :year, :summary, :fingerprint]

      action :summarise, :map do
        argument :text, :string, allow_nil?: true
        argument :year, :string, allow_nil?: true

        run fn input, _ ->
          ReactiveDag.InvalidateTest.Calls.record(input.arguments.text)
          {:ok, %{"summary" => "sum:#{input.arguments.text}", "year" => input.arguments.year}}
        end
      end
    end

    reactive do
      id(:summaries)
      recompute_by(:key, to: :transcripts, from: :key)
      slice(:year, values: ["2024", "2025"])

      per_key(:summarise,
        args: [text: :body, year: :year],
        fingerprint: [:body],
        into: [summary: :summary, year: :year]
      )
    end
  end

  defmodule FakeRepo do
    def start_link, do: Agent.start_link(fn -> MapSet.new() end, name: __MODULE__)

    def query!("INSERT INTO " <> _, p) do
      p
      |> Enum.chunk_every(7)
      |> Enum.each(fn [c, _tenant, k, _, _, _held, _vid] -> Agent.update(__MODULE__, &MapSet.put(&1, {c, k})) end)

      %{rows: []}
    end

    def query!("SELECT DISTINCT cell_id" <> _, _),
      do: %{rows: Agent.get(__MODULE__, & &1) |> Enum.map(&[elem(&1, 0)]) |> Enum.uniq()}

    def query!("DELETE FROM " <> _, [cell | _tenant]) do
      c =
        Agent.get_and_update(__MODULE__, fn s ->
          {m, r} = Enum.split_with(s, fn {x, _} -> x == cell end)
          {m, MapSet.new(r)}
        end)

      %{rows: Enum.map(c, fn {_, k} -> [k, nil] end)}
    end

    def query!("SELECT COUNT" <> _, _), do: %{rows: [[Agent.get(__MODULE__, &MapSet.size/1)]]}
  end

  setup do
    start_supervised!(%{id: Calls, start: {Calls, :start_link, []}})
    start_supervised!(%{id: FakeRepo, start: {FakeRepo, :start_link, []}})

    prev = Application.get_env(:reactive_dag, :repo)
    Application.put_env(:reactive_dag, :repo, FakeRepo)
    on_exit(fn -> Application.put_env(:reactive_dag, :repo, prev) end)

    for r <- [Transcripts, Summaries], row <- Ash.read!(r), do: Ash.destroy!(row)

    for {k, y} <- [{"t1", "2025"}, {"t2", "2024"}] do
      Transcripts
      |> Ash.Changeset.for_create(:upsert, %{key: k, body: "body-#{k}", year: y})
      |> Ash.create!()
    end

    # first pass: everything is summarised and fingerprinted
    {:ok, _, _} = ReactiveDag.Node.Recompute.recompute(plan().cells["summaries"], ["*"])
    Calls.reset()

    :ok
  end

  @doc false
  def plan, do: ReactiveDag.Node.graph([Transcripts, Summaries])

  defp cell, do: plan().cells["summaries"]

  defp run(args) do
    ReactiveDag.ReprocessWorker.perform(%Oban.Job{
      args: Map.put(args, "plan_mfa", ["ReactiveDag.InvalidateTest", "plan", []])
    })
  end

  describe "the case this exists for" do
    test "a marked key is skipped without invalidation — the bug" do
      # mark by hand, exactly as a reprocess used to: the fingerprint still
      # matches, so the action never runs
      ReactiveDag.Frontier.mark_dirty("summaries", ["t1"], "manual")

      {:ok, _} =
        ReactiveDag.Drain.run(plan(),
          recompute: ReactiveDag.Node.Recompute,
          key_rule: ReactiveDag.Node.KeyRule
        )

      assert Calls.all() == [], "the input never moved, so the fingerprint says skip"
    end

    test "the action runs again when the fingerprint is cleared first" do
      assert :ok = run(%{"cell" => "summaries", "keys" => ["t1"]})

      assert Calls.all() == ["body-t1"], "the row was re-summarised"
    end

    test "and only the selected key" do
      assert :ok = run(%{"cell" => "summaries", "keys" => ["t1"]})

      refute "body-t2" in Calls.all(), "t2 was not asked about"
    end

    test "a slice re-runs exactly its slice" do
      assert :ok = run(%{"cell" => "summaries", "where" => %{"year" => "2024"}})

      assert Calls.all() == ["body-t2"]
    end

    test "the whole cell re-runs everything" do
      assert :ok = run(%{"cell" => "summaries"})

      assert Enum.sort(Calls.all()) == ["body-t1", "body-t2"]
    end
  end

  describe "the fingerprint is restored, not abandoned" do
    test "a second reprocess is needed to run it again — the row is valid once more" do
      run(%{"cell" => "summaries", "keys" => ["t1"]})
      Calls.reset()

      # an ordinary mark now skips again, because the row was re-fingerprinted
      ReactiveDag.Frontier.mark_dirty("summaries", ["t1"], "manual")

      {:ok, _} =
        ReactiveDag.Drain.run(plan(),
          recompute: ReactiveDag.Node.Recompute,
          key_rule: ReactiveDag.Node.KeyRule
        )

      assert Calls.all() == [], "invalidation is a one-shot, not a mode"
    end
  end

  # A tenanted node, because the untenanted `Summaries` above cannot show it:
  # `invalidate/3` reads the rows it is about to clear and then UPDATES them, and
  # both halves need the tenant. Without it a reprocess of a tenanted plan raises,
  # and clearing another municipality's fingerprints would make ITS next drain
  # recompute rows nobody asked about.
  defmodule TenantedSummaries do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Ets, extensions: [ReactiveDag.Node]

    ets do
    end

    multitenancy do
      strategy :attribute
      attribute :org_id
    end

    attributes do
      uuid_primary_key(:id)
      attribute :org_id, :string, public?: true
      attribute :key, :string, allow_nil?: false, public?: true
      attribute :fingerprint, :string, public?: true
    end

    identities do
      identity :by_org_key, [:org_id, :key], pre_check_with: Domain
    end

    actions do
      defaults [:read, :destroy, update: [:fingerprint]]

      create :upsert,
        upsert?: true,
        upsert_identity: :by_org_key,
        accept: [:org_id, :key, :fingerprint]
    end

    reactive do
      id(:tenanted_summaries)
      leaf?(true)
      row_key([:org_id, :key])
    end
  end

  describe "invalidate/3 under tenancy" do
    setup do
      for org <- ["org_a", "org_b"] do
        Ash.create!(TenantedSummaries, %{key: "t1", fingerprint: "fp"},
          action: :upsert,
          tenant: org
        )
      end

      [cell: hd(ReactiveDag.Node.cells(TenantedSummaries))]
    end

    test "clears only the named tenant's rows", %{cell: cell} do
      assert Rows.invalidate(cell, :all, tenant: "org_a") == ["t1"]

      a = Ash.read!(TenantedSummaries, tenant: "org_a") |> hd()
      b = Ash.read!(TenantedSummaries, tenant: "org_b") |> hd()

      refute a.fingerprint, "the named tenant's fingerprint is cleared"

      assert b.fingerprint == "fp",
             "the OTHER tenant's is untouched — clearing it would make its next " <>
               "drain recompute rows nobody asked about"
    end

    test "a specific key is scoped too", %{cell: cell} do
      assert Rows.invalidate(cell, ["t1"], tenant: "org_b") == ["t1"]

      assert Ash.read!(TenantedSummaries, tenant: "org_a") |> hd() |> Map.get(:fingerprint) ==
               "fp"
    end
  end

  describe "invalidate/2 on its own" do
    test "clears the stored fingerprint and reports what it touched" do
      assert Rows.invalidate(cell(), ["t1"]) == ["t1"]

      row = Summaries |> Ash.read!() |> Enum.find(&(&1.key == "t1"))
      refute row.fingerprint
    end

    test ":all clears every row" do
      assert Enum.sort(Rows.invalidate(cell(), :all)) == ["t1", "t2"]
    end

    test "a key that does not exist is skipped, not an error" do
      assert Rows.invalidate(cell(), ["nope"]) == []
    end

    test "a node with no fingerprint column is a no-op" do
      # it recomputes unconditionally anyway, so there is nothing to invalidate
      [transcripts] = ReactiveDag.Node.cells(Transcripts)

      assert Rows.invalidate(transcripts, :all) == []
    end
  end

  test "telemetry reports how many rows were invalidated" do
    test_pid = self()

    :telemetry.attach(
      "invalidate-stop",
      [:reactive_dag, :reprocess, :stop],
      fn _e, m, _meta, _ -> send(test_pid, {:stop, m}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach("invalidate-stop") end)

    run(%{"cell" => "summaries", "where" => %{"year" => "2025"}})

    assert_received {:stop, m}
    assert m.invalidated == 1
    assert m.claimed == 1
  end
end

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
  use ReactiveDag.PostgresCase

  alias ReactiveDag.Fixtures.InvalidateGraph
  alias ReactiveDag.Fixtures.InvalidateGraph.{Summaries, TenantedSummaries, Transcripts}

  alias ReactiveDag.Node.Rows

  setup_all do
    migrate!(InvalidateGraph.Domain)
    :ok
  end

  setup do
    pid = Ecto.Adapters.SQL.Sandbox.start_owner!(ReactiveDag.TestRepo, shared: true)
    on_exit(fn -> Ecto.Adapters.SQL.Sandbox.stop_owner(pid) end)

    migrate_library_tables!()
    truncate(InvalidateGraph.tables())
    start_supervised!(%{id: ReactiveDag.Fixtures.Calls, start: {ReactiveDag.Fixtures.Calls, :start_link, []}})
    start_supervised!(ReactiveDag.Test.FakeSuspensionRepo)
    ReactiveDag.Test.FakeSuspensionRepo.install()

    for r <- [Transcripts, Summaries], row <- Ash.read!(r), do: Ash.destroy!(row)

    for {k, y} <- [{"t1", "2025"}, {"t2", "2024"}] do
      Transcripts
      |> Ash.Changeset.for_create(:upsert, %{key: k, body: "body-#{k}", year: y})
      |> Ash.create!()
    end

    # first pass: everything is summarised and fingerprinted
    {:ok, _, _} = ReactiveDag.Node.Recompute.recompute(plan().cells["summaries"], ["*"])
    ReactiveDag.Fixtures.Calls.reset()

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
      ReactiveDag.Test.Pending.add("summaries", ["t1"])

      {:ok, _} =
        ReactiveDag.Test.Pending.cascade(plan(),
          recompute: ReactiveDag.Node.Recompute,
          key_rule: ReactiveDag.Node.KeyRule
        )

      assert ReactiveDag.Fixtures.Calls.all() == [], "the input never moved, so the fingerprint says skip"
    end

    test "the action runs again when the fingerprint is cleared first" do
      assert :ok = run(%{"cell" => "summaries", "keys" => ["t1"]})

      assert ReactiveDag.Fixtures.Calls.all() == ["body-t1"], "the row was re-summarised"
    end

    test "and only the selected key" do
      assert :ok = run(%{"cell" => "summaries", "keys" => ["t1"]})

      refute "body-t2" in ReactiveDag.Fixtures.Calls.all(), "t2 was not asked about"
    end

    test "a slice re-runs exactly its slice" do
      assert :ok = run(%{"cell" => "summaries", "where" => %{"year" => "2024"}})

      assert ReactiveDag.Fixtures.Calls.all() == ["body-t2"]
    end

    test "the whole cell re-runs everything" do
      assert :ok = run(%{"cell" => "summaries"})

      assert Enum.sort(ReactiveDag.Fixtures.Calls.all()) == ["body-t1", "body-t2"]
    end
  end

  describe "the fingerprint is restored, not abandoned" do
    test "a second reprocess is needed to run it again — the row is valid once more" do
      run(%{"cell" => "summaries", "keys" => ["t1"]})
      ReactiveDag.Fixtures.Calls.reset()

      # an ordinary mark now skips again, because the row was re-fingerprinted
      ReactiveDag.Test.Pending.add("summaries", ["t1"])

      {:ok, _} =
        ReactiveDag.Test.Pending.cascade(plan(),
          recompute: ReactiveDag.Node.Recompute,
          key_rule: ReactiveDag.Node.KeyRule
        )

      assert ReactiveDag.Fixtures.Calls.all() == [], "invalidation is a one-shot, not a mode"
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

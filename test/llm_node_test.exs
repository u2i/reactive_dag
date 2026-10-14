defmodule ReactiveDag.LlmNodeTest do
  @moduledoc """
  STEP 0 of issue #30: an LLM node needs **no library code**.

  `ash_ai`'s `prompt/2` builds a generic Ash action, and `run :action` already
  invokes generic actions — so an LLM recompute is just the `run` rung with a
  prompt-backed action behind it. This test proves that composition end to end,
  including the `context` edge (settled data the prompt reads but is NOT
  recomputed by), which is the canonical reason that edge type exists.

  The model is injected: `prompt/2` takes `:req_llm`, "useful for testing with
  mocks", so an LLM node is as deterministic under test as any other node — no
  network, no key, no flake.

  Whatever friction shows up here is the requirements list for a first-class
  `llm` rung; see the guide section this test backs.
  """
  use ReactiveDag.PostgresCase

  alias ReactiveDag.Fixtures.LlmGraph
  alias ReactiveDag.Fixtures.LlmGraph.{Events, People, Sentiment, Transcripts}

  alias ReactiveDag.{Cascade}

  # ── the injected model ──────────────────────────────────────────────────────
  #
  # ReqLLM's structured-output entry point is `generate_object/4`; swapping the
  # module is the whole seam. This one records its calls so the test can assert
  # the prompt actually carried the claimed keys AND the context edge's data.
  defmodule FakeLLM do
    def start_link, do: Agent.start_link(fn -> [] end, name: __MODULE__)
    def calls, do: Agent.get(__MODULE__, &Enum.reverse/1)

    def generate_object(_model, context, _schema, _opts) do
      Agent.update(__MODULE__, &[rendered(context) | &1])
      {:ok, %{object: %{"summary" => "extracted"}}}
    end

    # flatten whatever context shape arrived into searchable text
    defp rendered(context) do
      context |> inspect(limit: :infinity, printable_limit: :infinity)
    end
  end

  setup_all do
    migrate!(LlmGraph.Domain)
    :ok
  end

  setup do
    pid = Ecto.Adapters.SQL.Sandbox.start_owner!(ReactiveDag.TestRepo, shared: true)
    on_exit(fn -> Ecto.Adapters.SQL.Sandbox.stop_owner(pid) end)

    migrate_library_tables!()
    truncate(LlmGraph.tables())

    # The fixture asks for its LLM rather than naming one, so point it here.
    Application.put_env(:reactive_dag, :fixture_llm, FakeLLM)

    on_exit(fn -> Application.delete_env(:reactive_dag, :fixture_llm) end)

    start_supervised!(%{id: FakeLLM, start: {FakeLLM, :start_link, []}})
    prev_repo = Application.get_env(:reactive_dag, :repo)
    Application.put_env(:reactive_dag, :repo, FakeRepo)

    on_exit(fn ->
      Application.put_env(:reactive_dag, :repo, prev_repo)
    end)

    People |> Ash.Changeset.for_create(:create, %{key: "p1", name: "Ada"}) |> Ash.create!()

    for {k, body} <- [{"t1", "first call"}, {"t2", "second call"}] do
      Transcripts |> Ash.Changeset.for_create(:create, %{key: k, body: body}) |> Ash.create!()
    end

    :ok
  end

  defp plan, do: LlmGraph.plan()

  defp drain(plan),
    do: ReactiveDag.Test.Pending.cascade(plan)

  test "an LLM node is the `run` rung — the graph wires it with no new library code" do
    p = plan()
    cell = p.cells["events"]

    # the action IS the recompute; no compute module, no combinator
    assert cell.meta.run == :extract_events
    assert cell.meta.compute == nil

    # a `ref` propagates, a `context` edge does not — both are real inputs
    assert Enum.sort(cell.inputs) == ["people", "transcripts"]
    assert p.parents["transcripts"] == ["events"]
    refute "events" in Map.get(p.parents, "people", [])
  end

  test "it runs end to end, and the prompt carries the context edge's data" do
    p = plan()

    ReactiveDag.Test.Pending.add("transcripts", ["*"])
    {:ok, report} = drain(p)

    steps = Map.new(report.steps, &{&1.cell, &1})
    assert steps["events"].triggered_by == "transcripts"
    assert Enum.sort(steps["events"].changed) == ["t1", "t2"]

    rows = Events |> Ash.read!() |> Map.new(&{&1.key, &1.summary})
    assert rows == %{"t1" => "extracted", "t2" => "extracted"}

    # the settled context reached the prompt — the reason `context` exists
    assert Enum.all?(FakeLLM.calls(), &(&1 =~ "Ada"))
    assert Enum.any?(FakeLLM.calls(), &(&1 =~ "first call"))
  end

  test "DIRTY-KEY SCOPING is what stops a whole-cell claim re-billing every key" do
    p = plan()

    ReactiveDag.Test.Pending.add("transcripts", ["*"])
    {:ok, _} = drain(p)
    assert length(FakeLLM.calls()) == 2

    # touch ONE transcript: the action receives just that key, so exactly one
    # model call is made. This is the cost discipline LLM nodes live or die by.
    before = length(FakeLLM.calls())
    ReactiveDag.Test.Pending.add("transcripts", ["t2"])
    {:ok, report} = drain(p)

    steps = Map.new(report.steps, &{&1.cell, &1})
    assert steps["events"].claimed == ["t2"]
    assert length(FakeLLM.calls()) - before == 1
  end

  test "a `context` change does NOT re-trigger the model" do
    p = plan()

    ReactiveDag.Test.Pending.add("transcripts", ["*"])
    {:ok, _} = drain(p)
    before = length(FakeLLM.calls())

    # people is a context edge: settled data, read at recompute, never a trigger
    ReactiveDag.Test.Pending.add("people", ["p1"])
    {:ok, report} = drain(p)

    refute Map.has_key?(Map.new(report.steps, &{&1.cell, &1}), "events")
    assert length(FakeLLM.calls()) == before
  end

  # ── the REAL thing: an actual ash_ai prompt/2 action ────────────────────────
  #
  # The tests above prove the `run` rung invokes a generic action. This proves
  # the specific claim in #30: an ash_ai PROMPT-BACKED action is such an action,
  # so it drops straight in. The model is swapped via `:req_llm`.

  # a thin Op that calls the prompt action per claimed key — the shape a
  # first-class `llm` rung would generate (see #30's "per-entry map").
  defmodule SentimentOp do
    @behaviour ReactiveDag.Op

    @impl true
    def recompute(_cell, keys) do
      keys =
        if keys == ["*"],
          do: ReactiveDag.Fixtures.LlmGraph.Transcripts |> Ash.read!() |> Enum.map(& &1.key),
          else: keys

      changed =
        for key <- keys do
          t = ReactiveDag.Fixtures.LlmGraph.Transcripts |> Ash.get!(key)

          {:ok, result} =
            ReactiveDag.Fixtures.LlmGraph.Sentiment
            |> Ash.ActionInput.for_action(:analyse, %{text: t.body})
            |> Ash.run_action()

          ReactiveDag.Fixtures.LlmGraph.Sentiment
          |> Ash.Changeset.for_create(:upsert, %{key: key, sentiment: result["sentiment"]})
          |> Ash.create!()

          key
        end

      {:ok, changed}
    end
  end

  test "an ash_ai prompt/2 action drops straight in — the Step 0 claim, verified" do
    start_supervised!(%{id: ReactiveDag.Fixtures.StubReqLLM, start: {ReactiveDag.Fixtures.StubReqLLM, :start_link, []}})

    p = ReactiveDag.Node.graph([Transcripts, People, Sentiment])
    {:ok, changed} = ReactiveDag.Node.Recompute.recompute(p.cells["sentiment"], ["*"])

    assert Enum.sort(changed) == ["t1", "t2"]

    rows = Sentiment |> Ash.read!() |> Map.new(&{&1.key, &1.sentiment})
    assert rows == %{"t1" => "positive", "t2" => "positive"}

    # the prompt really was rendered by ash_ai (our EEx template reached the model)
    assert Enum.any?(ReactiveDag.Fixtures.StubReqLLM.contexts(), &(&1 =~ "sentiment analyser"))
    assert Enum.any?(ReactiveDag.Fixtures.StubReqLLM.contexts(), &(&1 =~ "first call"))
  end
end

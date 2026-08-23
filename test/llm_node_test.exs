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
  use ExUnit.Case, async: false

  alias ReactiveDag.{Drain, Frontier}

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

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      allow_unregistered?(true)
    end
  end

  # the raw input the LLM reads
  defmodule Transcripts do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
      private?(true)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :body, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :create do
        accept([:key, :body])
      end
    end

    reactive do
      id(:transcripts)
      op(:source)
      leaf?(true)
    end
  end

  # settled context the prompt consults but is NOT re-triggered by
  defmodule People do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
      private?(true)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :name, :string, public?: true
    end

    actions do
      defaults [:read]

      create :create do
        accept([:key, :name])
      end
    end

    reactive do
      id(:people)
      op(:source)
      leaf?(true)
    end
  end

  defmodule Events do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
      private?(true)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :summary, :string, public?: true
    end

    identities do
      identity :by_key, [:key], pre_check_with: ReactiveDag.LlmNodeTest.Domain
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        upsert_identity(:by_key)
        accept([:key, :summary])
      end

      # THE LLM ACTION: an ordinary ash_ai prompt-backed generic action. It
      # returns the changed keys, which is `run`'s existing contract.
      action :extract_events, {:array, :string} do
        argument :keys, {:array, :string}, allow_nil?: true

        run fn input, _ctx ->
          keys =
            input.arguments[:keys] ||
              ReactiveDag.LlmNodeTest.Transcripts |> Ash.read!() |> Enum.map(& &1.key)

          # the `context` edge's data: read as settled context, exactly as a
          # host would when assembling prompt context
          people =
            ReactiveDag.LlmNodeTest.People
            |> Ash.read!()
            |> Enum.map_join(", ", & &1.name)

          changed =
            for key <- keys do
              transcript = ReactiveDag.LlmNodeTest.Transcripts |> Ash.get!(key)

              {:ok, %{object: object}} =
                ReactiveDag.LlmNodeTest.FakeLLM.generate_object(
                  "openai:gpt-4o",
                  {"You summarise transcripts. Known people: #{people}", transcript.body},
                  %{},
                  []
                )

              ReactiveDag.LlmNodeTest.Events
              |> Ash.Changeset.for_create(:upsert, %{key: key, summary: object["summary"]})
              |> Ash.create!()

              key
            end

          {:ok, changed}
        end
      end
    end

    reactive do
      id(:events)
      op(:extract)

      # THE LLM RUNG, today: a generic action + the edges that feed it.
      run(:extract_events)
      ref(:transcripts)
      context(:people)
    end
  end

  defmodule FakeRepo do
    def start_link, do: Agent.start_link(fn -> MapSet.new() end, name: __MODULE__)

    def query!("INSERT INTO " <> _, params) do
      params
      |> Enum.chunk_every(7)
      |> Enum.each(fn [cell, _tenant, key, _r, _t, _prior, _held] -> Agent.update(__MODULE__, &MapSet.put(&1, {cell, key})) end)

      %{rows: []}
    end

    def query!("SELECT DISTINCT cell_id" <> _, _params) do
      ids = Agent.get(__MODULE__, & &1) |> Enum.map(&elem(&1, 0)) |> Enum.uniq()
      %{rows: Enum.map(ids, &[&1])}
    end

    def query!("DELETE FROM " <> _, [cell | _tenant]) do
      keys =
        Agent.get_and_update(__MODULE__, fn set ->
          {mine, rest} = Enum.split_with(set, fn {c, _} -> c == cell end)
          {Enum.map(mine, &elem(&1, 1)), MapSet.new(rest)}
        end)

      %{rows: Enum.map(keys, &[&1, nil])}
    end

    def query!("SELECT COUNT" <> _, _params), do: %{rows: [[Agent.get(__MODULE__, &MapSet.size/1)]]}
  end


  setup do
    start_supervised!(%{id: FakeRepo, start: {FakeRepo, :start_link, []}})
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

  defp plan, do: ReactiveDag.Node.graph([Transcripts, People, Events])

  defp drain(plan),
    do: Drain.run(plan, recompute: ReactiveDag.Node.Recompute, key_rule: ReactiveDag.Node.KeyRule)

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

    Frontier.mark_dirty("transcripts", ["*"], "seed")
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

    Frontier.mark_dirty("transcripts", ["*"], "seed")
    {:ok, _} = drain(p)
    assert length(FakeLLM.calls()) == 2

    # touch ONE transcript: the action receives just that key, so exactly one
    # model call is made. This is the cost discipline LLM nodes live or die by.
    before = length(FakeLLM.calls())
    Frontier.mark_dirty("transcripts", ["t2"], "revised")
    {:ok, report} = drain(p)

    steps = Map.new(report.steps, &{&1.cell, &1})
    assert steps["events"].claimed == ["t2"]
    assert length(FakeLLM.calls()) - before == 1
  end

  test "a `context` change does NOT re-trigger the model" do
    p = plan()

    Frontier.mark_dirty("transcripts", ["*"], "seed")
    {:ok, _} = drain(p)
    before = length(FakeLLM.calls())

    # people is a context edge: settled data, read at recompute, never a trigger
    Frontier.mark_dirty("people", ["p1"], "renamed")
    {:ok, report} = drain(p)

    refute Map.has_key?(Map.new(report.steps, &{&1.cell, &1}), "events")
    assert length(FakeLLM.calls()) == before
  end
  # ── the REAL thing: an actual ash_ai prompt/2 action ────────────────────────
  #
  # The tests above prove the `run` rung invokes a generic action. This proves
  # the specific claim in #30: an ash_ai PROMPT-BACKED action is such an action,
  # so it drops straight in. The model is swapped via `:req_llm`.

  defmodule StubReqLLM do
    def start_link, do: Agent.start_link(fn -> [] end, name: __MODULE__)
    def contexts, do: Agent.get(__MODULE__, &Enum.reverse/1)

    def generate_object(_model, context, _schema, _opts) do
      Agent.update(__MODULE__, &[inspect(context, limit: :infinity), &1] |> then(fn [h, t] -> [h | t] end))
      {:ok, %{object: %{"sentiment" => "positive"}}}
    end
  end

  defmodule Sentiment do
    use Ash.Resource,
      domain: ReactiveDag.LlmNodeTest.Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    require AshAi.Actions
    import AshAi.Actions, only: [prompt: 2]

    ets do
      private?(true)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :sentiment, :string, public?: true
    end

    actions do
      defaults [:read]

      create :upsert do
        upsert?(true)
        accept([:key, :sentiment])
      end

      # AN ORDINARY ash_ai PROMPT ACTION — nothing reactive_dag-specific
      action :analyse, :map do
        argument :text, :string, allow_nil?: false

        run prompt("openai:gpt-4o",
              prompt: {"You are a sentiment analyser", "Analyse: <%= @input.arguments.text %>"},
              req_llm: ReactiveDag.LlmNodeTest.StubReqLLM
            )
      end
    end

    reactive do
      id(:sentiment)
      op(:analyse)
      compute(ReactiveDag.LlmNodeTest.SentimentOp)
      ref(:transcripts)
    end
  end

  # a thin Op that calls the prompt action per claimed key — the shape a
  # first-class `llm` rung would generate (see #30's "per-entry map").
  defmodule SentimentOp do
    @behaviour ReactiveDag.Op

    @impl true
    def recompute(_cell, keys) do
      keys =
        if keys == ["*"],
          do: ReactiveDag.LlmNodeTest.Transcripts |> Ash.read!() |> Enum.map(& &1.key),
          else: keys

      changed =
        for key <- keys do
          t = ReactiveDag.LlmNodeTest.Transcripts |> Ash.get!(key)

          {:ok, result} =
            ReactiveDag.LlmNodeTest.Sentiment
            |> Ash.ActionInput.for_action(:analyse, %{text: t.body})
            |> Ash.run_action()

          ReactiveDag.LlmNodeTest.Sentiment
          |> Ash.Changeset.for_create(:upsert, %{key: key, sentiment: result["sentiment"]})
          |> Ash.create!()

          key
        end

      {:ok, changed}
    end
  end

  test "an ash_ai prompt/2 action drops straight in — the Step 0 claim, verified" do
    start_supervised!(%{id: StubReqLLM, start: {StubReqLLM, :start_link, []}})

    p = ReactiveDag.Node.graph([Transcripts, People, Sentiment])
    {:ok, changed} = ReactiveDag.Node.Recompute.recompute(p.cells["sentiment"], ["*"])

    assert Enum.sort(changed) == ["t1", "t2"]

    rows = Sentiment |> Ash.read!() |> Map.new(&{&1.key, &1.sentiment})
    assert rows == %{"t1" => "positive", "t2" => "positive"}

    # the prompt really was rendered by ash_ai (our EEx template reached the model)
    assert Enum.any?(StubReqLLM.contexts(), &(&1 =~ "sentiment analyser"))
    assert Enum.any?(StubReqLLM.contexts(), &(&1 =~ "first call"))
  end
end

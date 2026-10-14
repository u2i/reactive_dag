defmodule ReactiveDag.Fixtures.LlmGraph do
  @moduledoc """
  An LLM-backed graph: transcripts in, people and events out.

  The shape a `compute` node takes when the computation is a model call — the
  node declares its op, and the call itself is injected so a test never needs a
  network. `Transcripts` is the leaf; `People` and `Events` are two
  independent consumers of it, which is what makes this a fan-out rather than a
  chain.
  """

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      resource ReactiveDag.Fixtures.LlmGraph.Transcripts
      resource ReactiveDag.Fixtures.LlmGraph.People
      resource ReactiveDag.Fixtures.LlmGraph.Events
      resource ReactiveDag.Fixtures.LlmGraph.Sentiment
    end
  end

  # the raw input the LLM reads
  defmodule Transcripts do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.LlmGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_llm_transcripts"
      repo(ReactiveDag.TestRepo)
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
      domain: ReactiveDag.Fixtures.LlmGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_llm_people"
      repo(ReactiveDag.TestRepo)
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
      domain: ReactiveDag.Fixtures.LlmGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_llm_events"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :summary, :string, public?: true
    end

    identities do
      identity :by_key, [:key], pre_check_with: ReactiveDag.Fixtures.LlmGraph.Domain
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
              ReactiveDag.Fixtures.LlmGraph.Transcripts |> Ash.read!() |> Enum.map(& &1.key)

          # the `context` edge's data: read as settled context, exactly as a
          # host would when assembling prompt context
          people =
            ReactiveDag.Fixtures.LlmGraph.People
            |> Ash.read!()
            |> Enum.map_join(", ", & &1.name)

          changed =
            for key <- keys do
              transcript = ReactiveDag.Fixtures.LlmGraph.Transcripts |> Ash.get!(key)

              {:ok, %{object: object}} =
                ReactiveDag.Fixtures.LlmGraph.llm().generate_object(
                  "openai:gpt-4o",
                  {"You summarise transcripts. Known people: #{people}", transcript.body},
                  %{},
                  []
                )

              ReactiveDag.Fixtures.LlmGraph.Events
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

  @doc "transcripts -> {people, events}."
  def plan, do: ReactiveDag.Node.graph([Transcripts, People, Events])

  defmodule Sentiment do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.LlmGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    require AshAi.Actions
    import AshAi.Actions, only: [prompt: 2]

    postgres do
      table "fx_llm_sentiment"
      repo(ReactiveDag.TestRepo)
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
              req_llm: Application.compile_env(:reactive_dag, :fixture_req_llm)
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

  @doc """
  The module that answers a model call.

  Injected rather than hard-wired so the fixture carries no reference to any
  one test's spy. A test sets this to its own recorder; nothing here reaches a
  network.
  """
  def llm, do: Application.get_env(:reactive_dag, :fixture_llm)

  # `req_llm` is read with `compile_env` rather than at runtime: `prompt/2`
  # expands it while the module is still being defined, so a function call here
  # would run before the module exists. Configured in `config/config.exs` for
  # the test env.

  def tables, do: ["fx_llm_transcripts", "fx_llm_people", "fx_llm_events", "fx_llm_sentiment"]
end

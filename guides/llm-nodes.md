# LLM nodes

An LLM recompute needs **no library code**. [ash_ai](https://hexdocs.pm/ash_ai)'s
`prompt/2` builds an ordinary generic Ash action, and the [`run`
rung](authoring-nodes.html#run-a-generic-ash-action-as-the-recompute) already
invokes generic actions — so an LLM node is the `run` rung with a prompt behind
it.

`ash_ai` is **not** a dependency of this library. Hosts that want LLM nodes add
it themselves; everything below is composition, not integration.

## The shape

```elixir
defmodule MyApp.Events do
  use Ash.Resource, data_layer: AshPostgres.DataLayer,
    extensions: [ReactiveDag.Node]

  import AshAi.Actions, only: [prompt: 2]

  attributes do
    attribute :key, :string, primary_key?: true
    attribute :summary, :string
  end

  actions do
    create :upsert do upsert?(true); accept([:key, :summary]) end

    action :extract_events, {:array, :string} do
      argument :keys, {:array, :string}, allow_nil?: true   # nil = whole cell
      run &MyApp.Events.extract/2                            # calls the prompt per key
    end

    action :summarise, :map do
      argument :text, :string, allow_nil?: false
      run prompt("openai:gpt-4o",
            prompt: {"You summarise transcripts", "Summarise: <%= @input.arguments.text %>"})
    end
  end

  reactive do
    id :events
    run :extract_events
    ref :transcripts        # a change here re-runs the model
    context :people         # read as settled context; never re-triggers
  end
end
```

Two edges, two meanings — and for LLM nodes the distinction is a **billing**
decision, not just a semantic one:

- **`ref`** — a change propagates and the model runs again.
- **`context`** — read at recompute, never a trigger. Exactly right for the
  people/positions/policy table a prompt consults: you want the *current* values
  in the prompt, but you do not want every edit to that table re-billing every
  key. This is the canonical reason the `context` edge exists.

## Cost discipline: dirty-key scoping

The `run` action receives `keys` — the claimed dirty set, or `nil` for a
whole-cell recompute. **Honour it.** Touch one transcript and exactly one model
call should follow:

```elixir
def extract(input, _ctx) do
  keys = input.arguments[:keys] || all_keys()      # nil = whole cell
  changed = for key <- keys, do: summarise_one(key)
  {:ok, changed}
end
```

An action that ignores `keys` and re-reads everything is *correct* and
*expensive* — the drain will happily hand you one key and let you bill for a
thousand. `test/llm_node_test.exs` asserts the one-key-one-call property, which
is the cheapest regression test worth having on an LLM node.

## Testing without a model

`prompt/2` takes `:req_llm`, an injectable module override. An LLM node is then
as deterministic under test as any other node — no network, no API key, no
flake:

```elixir
defmodule StubReqLLM do
  def generate_object(_model, _context, _schema, _opts),
    do: {:ok, %{object: %{"sentiment" => "positive"}}}
end

run prompt("openai:gpt-4o", prompt: {...}, req_llm: StubReqLLM)
```

Assert on the *context* the stub receives to prove your prompt actually carried
what you think it did — including the `context` edge's data.

## Known rough edges

These are the reasons a first-class `llm` rung is still open
([#30](https://github.com/u2i/reactive_dag/issues/30)); none of them block the
shape above:

- **No input fingerprinting.** A whole-cell claim re-bills every key, even where
  the inputs are byte-identical. The idiom is to hash the rendered prompt inputs
  into a column and skip the call when it is unchanged — today you write that
  yourself, inside the action.
- **One call per key, serially.** The drain is synchronous per cell, so a long
  LLM cell dominates wall-clock. Batching (N rows per prompt) and bounded
  concurrency inside the action are the levers; both live in your action for now.
- ~~No token/cost telemetry.~~ **Solved.** A recompute may return
  `{:ok, changed, meta}`, and the map rides on the drain's `%Report{}` step:

  ```elixir
  def recompute(cell, keys) do
    {changed, usage} = call_model(keys)
    {:ok, changed, %{tokens_in: usage.in, tokens_out: usage.out, cost_usd: usage.cost}}
  end
  ```

  `Report.total(report, :tokens_in)` rolls one key up across every step, and
  `ReactiveDag.Insights` carries it to a dashboard. The library never
  interprets the map — cache hits, retries and rows scanned are equally valid
  keys.
- **The per-key map is hand-written.** Reading the claimed rows, calling the
  model, and writing structured output into this resource's attributes is the
  same loop every time — which is exactly what a declarative `llm` rung would
  generate.

## Where it sits on the ladder

Between the declarative combinators and `compute Module`. An LLM node that only
maps rows → structured output is a `run` action; one that needs a bespoke
multi-input recompute, retries with backoff, or a non-Ash fetch drops to
`compute`, as ever.

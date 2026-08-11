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
defmodule MyApp.Summaries do
  use Ash.Resource, data_layer: AshPostgres.DataLayer,
    extensions: [ReactiveDag.Node]

  import AshAi.Actions, only: [prompt: 2]

  attributes do
    attribute :key, :string, primary_key?: true
    attribute :summary, :string
    attribute :fingerprint, :string        # where the input hash lives
  end

  actions do
    create :upsert do upsert?(true); accept([:key, :summary, :fingerprint]) end

    action :summarise, :map do
      argument :text, :string, allow_nil?: false
      run prompt("openai:gpt-4o",
            prompt: {"You summarise transcripts", "Summarise: <%= @input.arguments.text %>"})
    end
  end

  reactive do
    recompute_by :key, to: :transcripts, from: :key

    per_key :summarise,
      args: [text: :body],           # the row's :body becomes the `text` argument
      fingerprint: [:body],          # skip the call when :body is unchanged
      into: [summary: :summary]      # the result's "summary" → this :summary

    context :people                  # settled context; never re-triggers
  end
end
```

The library drives the loop — scope to the claimed keys, read the rows, call
the action once per row, write the structured output through the payload loop.
What you write is the *action* and the mapping.

## Fingerprinting: not paying twice for the same answer

`fingerprint:` names the input fields the result depends on. Their hash is
stored on the output row; a recompute whose hash matches **does not call the
action at all**:

```
whole-cell claim, nothing changed  →  %{called: 0, skipped: 2}
one transcript's :body edited      →  %{called: 1, skipped: 1}
a field NOT in the fingerprint     →  %{called: 0, skipped: 2}
```

That counts map rides on the drain's `%Report{}` step, so the saving is
visible rather than assumed (`Report.total(report, :skipped)`).

This is why `per_key` exists as a rung rather than a guide section. A `run`
action is **opaque by design** — the library passes keys and gets keys back, so
nothing outside it can know whether the work is worth doing. Only by driving the
loop does the library see the inputs, and only then can it decline to pay.

The fingerprint needs somewhere to live: declare a `:fingerprint` attribute (or
name another with `fingerprint_attribute`). A node that declares `fingerprint:`
with nowhere to store it is a **compile error** — a silently-never-skipping node
is exactly the expensive mistake the rung prevents.

## When to drop to `run` instead

`per_key` maps one input row to one output row. Anything else — many rows per
call, a batch prompt, a bespoke multi-input recompute — is the `run` rung, where
you own the loop.

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

- ~~No input fingerprinting.~~ **Solved** by `per_key … fingerprint:` above.
  (A `run` node still re-bills on a whole-cell claim — the library cannot see
  its inputs. That is the trade for `run`'s opacity.)
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

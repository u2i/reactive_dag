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

`fingerprint:` names what the result depends on — input fields, whose hash is
stored on the output row. A recompute whose hash matches **does not call the
action at all**:

```
whole-cell claim, nothing changed  →  %{called: 0, skipped: 2}
one transcript's :body edited      →  %{called: 1, skipped: 1}
a field NOT in the fingerprint     →  %{called: 0, skipped: 2}
```

It also takes a function, `(row -> value)`, for when "the same input" is not a
plain field comparison — a normalized body, a hash of a hash, a version folded
into a digest:

```elixir
fingerprint fn row -> :crypto.hash(:md5, normalize(row.body)) end
```

The same vocabulary declares what counts as a changed *observation* on a
source-fed leaf (see [Sources and scanning](sources.html)): one concept, one
implementation, at both rungs.

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

## Throughput: the drain is sequential, so parallelism lives here

Worth understanding before reaching for the lever: **the drain recomputes one
cell at a time, and that is not an oversight.** Depth ordering is what makes the
cascade correct — a cell may not run until everything it reads has settled — so
cells cannot be parallelised without giving that up. A long LLM cell therefore
dominates the drain's wall-clock.

Which means per-row parallelism has to live *inside* a recompute, and `per_key`
is where it goes:

```elixir
per_key :summarise,
  args: [text: :body],
  fingerprint: [:body],
  max_concurrency: 8,      # 8 rows in flight; default 1
  timeout: 60_000,         # per row; default :infinity
  into: [summary: :summary]
```

Two properties worth relying on:

- **Skipped rows never enter the stream.** Fingerprints are evaluated first, so
  slots are spent only on rows that genuinely need the call. A whole-cell claim
  over mostly-unchanged rows costs almost nothing, whatever the bound.
- **Results are applied in row order.** The changed-key list stays
  deterministic, so tests, diffs and Reports do not shuffle between runs.

A row that times out **fails the recompute** rather than being dropped — a
half-written cell that reports success is worse than a loud crash.

> **Careful with `Ash.DataLayer.Ets` and `private?: true`.** A private ETS table
> is owned by the process that created it, and each row is written from a task
> process — so writes would silently vanish. AshPostgres hosts are unaffected;
> this only bites in-memory test fixtures.

Batching (N rows per prompt) is the *other* throughput lever and is not
implemented: it changes the action's contract from one row to many, and the
result mapping from one map to results keyed by row. Tracked separately.

## Embeddings: usually not a node at all

For embeddings **on the same resource as the text**, use
[ash_ai's `vectorize`](https://hexdocs.pm/ash_ai) rather than a reactive node:

```elixir
vectorize do
  attributes description: :description_vector
  strategy :after_action          # or :manual, :ash_oban
  embedding_model MyApp.OpenAiEmbedding
end
```

It maintains an embedding **column next to the text it came from**, and it is
strictly better at that job than a DAG node would be: it hooks the changeset, so
it knows what changed without a fingerprint round-trip
(`has_vectorize_change?` checks the `used_attributes`), and `strategy:` already
offers inline, on-demand and background scheduling.

A reactive node earns its place only when the embedding is genuinely **derived
state with its own identity** — a separate resource keyed by something other
than the source row, or a vector that depends on several inputs. That is
`per_key` with an embedding action, and it needs no new rung:

```elixir
recompute_by :key, to: :transcripts, from: :key

per_key :embed,
  args: [text: :body],
  fingerprint: [:body],           # do not pay to re-embed unchanged text
  into: [vector: :vector]         # the action returns a list of floats
```

The two compose, which is the more common shape in practice: `vectorize` on a
leaf resource, and reactive nodes reading it as an ordinary input.

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

## Watching the bill

A recompute may return `{:ok, changed, meta}`, and the map rides on the drain's
`%Report{}` step:

  ```elixir
  def recompute(cell, keys) do
    {changed, usage} = call_model(keys)
    {:ok, changed, %{tokens_in: usage.in, tokens_out: usage.out, cost_usd: usage.cost}}
  end
  ```

`Report.total(report, :tokens_in)` rolls one key up across every step. The
library never interprets the map — cache hits, retries and rows scanned are
equally valid keys.

`per_key` populates it for you: `%{called: n, skipped: n}`, so the saving from
`fingerprint:` is visible rather than assumed.

For **live** cost — a budget alarm, a dashboard, a per-run log — the same numbers
arrive as telemetry while the drain is still running, rather than only in the
report at the end:

```elixir
:telemetry.attach("llm-cost", [:reactive_dag, :drain, :step], fn _e, _m, meta, _ ->
  case meta.step.meta do
    %{cost_usd: usd} -> MyApp.Budget.spend(meta.cell, usd)
    _ -> :ok
  end
end, nil)
```

The handler runs synchronously in the drain's process, so keep it cheap — record
the number and get out; do not call the billing API from inside it.

## What is still missing

One thing, and it is a contract change rather than a tuning knob:

**Batching — N rows per prompt.** `per_key` is one call per row by design, so the
fingerprint can decide per row and a failure blames one row. Feeding ten rows to
one prompt means a different action shape (an array argument, an array result,
and a story for partial failure), which is why it is not a `per_key` option.
Tracked in [#49](https://github.com/u2i/reactive_dag/issues/49).

A `run` node also still re-bills on a whole-cell claim: the library cannot see
what an opaque action depends on, so it cannot fingerprint it. That is the trade
for `run`'s freedom, and the reason `per_key` exists beside it.

## Where it sits on the ladder

Between the declarative combinators and `compute Module`. An LLM node that only
maps rows → structured output is a `run` action; one that needs a bespoke
multi-input recompute, retries with backoff, or a non-Ash fetch drops to
`compute`, as ever.

defmodule ReactiveDag.Fixtures.PerKeyGraph do
  @moduledoc """
  `per_key` — a node whose recompute runs once per key, with bounded
  concurrency.

  On Postgres rather than ETS, and that is not incidental. `max_concurrency:`
  writes each row from a TASK process, and a private ETS table is owned by the
  process that created it — so a private table silently drops those writes.
  The old fixture worked around this by declaring `ets do end` without
  `private?`, with a comment saying "real hosts on AshPostgres are unaffected;
  this is the fixture matching reality".

  It is now that reality, so the workaround is gone — along with the
  intermittent `:table_not_found` that came with it.

    * `Transcripts` — the leaf.
    * `Summaries`   — per-key, with a `fingerprint` so an unchanged body skips.
    * `Always`      — no `fingerprint:`, so every recompute calls. The opt-in
      is explicit, and this is the other side of it.
    * `Concurrent`  — exercises the concurrency bound.
    * `Embeddings`  — SKIPPED rows must never enter the stream, so slots go
      only to real calls.
  """

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      resource ReactiveDag.Fixtures.PerKeyGraph.Transcripts
      resource ReactiveDag.Fixtures.PerKeyGraph.Summaries
      resource ReactiveDag.Fixtures.PerKeyGraph.Always
      resource ReactiveDag.Fixtures.PerKeyGraph.Concurrent
      resource ReactiveDag.Fixtures.PerKeyGraph.Embeddings
    end
  end

  defmodule Transcripts do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.PerKeyGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_pk_transcripts"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :body, :string, public?: true
      # deliberately NOT in the fingerprint: changing it must not re-bill
      attribute :note, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :create do
        accept([:key, :body, :note])
      end

      update :revise do
        accept([:body, :note])
      end
    end

    reactive do
      id :transcripts
      op :source
      leaf? true
    end
  end

  defmodule Summaries do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.PerKeyGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_pk_summaries"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :summary, :string, public?: true
      # where the library stores the input hash
      attribute :fingerprint, :string, public?: true
    end

    identities do
      identity :by_key, [:key], pre_check_with: ReactiveDag.Fixtures.PerKeyGraph.Domain
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        upsert_identity(:by_key)
        accept([:key, :summary, :fingerprint])
      end

      # AN ORDINARY GENERIC ACTION taking ONE row's fields. That this could be
      # `AshAi.Actions.prompt/2` is the library's business not at all.
      action :summarise, :map do
        argument :text, :string, allow_nil?: false

        run fn input, _ctx ->
          ReactiveDag.PerKeyTest.Calls.record(input.arguments.text)
          {:ok, %{"summary" => "summary of #{input.arguments.text}"}}
        end
      end
    end

    reactive do
      id :summaries
      op :summarise

      recompute_by :key, to: :transcripts, from: :key

      per_key(:summarise,
        args: [text: :body],
        fingerprint: [:body],
        into: [summary: :summary]
      )
    end
  end

  defmodule Always do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.PerKeyGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_pk_always"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :summary, :string, public?: true
    end

    actions do
      defaults [:read]

      create :upsert do
        upsert?(true)
        accept([:key, :summary])
      end

      action :summarise, :map do
        argument :text, :string, allow_nil?: false

        run fn input, _ctx ->
          ReactiveDag.PerKeyTest.Calls.record(input.arguments.text)
          {:ok, %{"summary" => "s"}}
        end
      end
    end

    reactive do
      id :always
      recompute_by :key, to: :transcripts, from: :key
      per_key :summarise, args: [text: :body], into: [summary: :summary]
    end
  end

  defmodule Concurrent do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.PerKeyGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    # NB: NOT `private?(true)`. A private ETS table is owned by the process
    # that created it, and `max_concurrency:` writes each row from a task
    # process — so a private table would silently drop the writes. Real hosts
    # on AshPostgres are unaffected; this is the fixture matching reality.
    postgres do
      table "fx_pk_concurrent"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :summary, :string, public?: true
      attribute :fingerprint, :string, public?: true
    end

    identities do
      identity :by_key, [:key], pre_check_with: ReactiveDag.Fixtures.PerKeyGraph.Domain
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        upsert_identity(:by_key)
        accept([:key, :summary, :fingerprint])
      end

      action :summarise, :map do
        argument :text, :string, allow_nil?: false

        run fn input, _ctx ->
          ReactiveDag.PerKeyTest.Gauge.enter()
          # long enough that serial execution could never overlap
          Process.sleep(40)
          ReactiveDag.PerKeyTest.Gauge.leave()
          ReactiveDag.PerKeyTest.Calls.record(input.arguments.text)
          {:ok, %{"summary" => "s:#{input.arguments.text}"}}
        end
      end
    end

    reactive do
      id :concurrent
      recompute_by :key, to: :transcripts, from: :key

      per_key(:summarise,
        args: [text: :body],
        fingerprint: [:body],
        max_concurrency: 4,
        into: [summary: :summary]
      )
    end
  end

  defmodule Embeddings do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.PerKeyGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_pk_embeddings"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :vector, {:array, :float}, public?: true
      attribute :fingerprint, :string, public?: true
    end

    identities do
      identity :by_key, [:key], pre_check_with: ReactiveDag.Fixtures.PerKeyGraph.Domain
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        upsert_identity(:by_key)
        accept([:key, :vector, :fingerprint])
      end

      # stands in for an AshAi.EmbeddingModel call
      action :embed, :map do
        argument :text, :string, allow_nil?: false

        run fn input, _ctx ->
          ReactiveDag.PerKeyTest.Calls.record(input.arguments.text)
          {:ok, %{"vector" => [1.0, byte_size(input.arguments.text) / 1.0, 0.5]}}
        end
      end
    end

    reactive do
      id :embeddings
      recompute_by :key, to: :transcripts, from: :key

      per_key(:embed,
        args: [text: :body],
        fingerprint: [:body],
        into: [vector: :vector]
      )
    end
  end

  @doc "transcripts -> summaries."
  def plan, do: ReactiveDag.Node.graph([Transcripts, Summaries])

  def tables, do: ["fx_pk_transcripts", "fx_pk_summaries", "fx_pk_always", "fx_pk_concurrent", "fx_pk_embeddings"]
end

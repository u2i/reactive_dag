defmodule ReactiveDag.Fixtures.LeafFingerprintGraph do
  @moduledoc """
  A leaf with a `fingerprint`, and one without.

  The fingerprint is what lets a re-crawl of unchanged content cost nothing:
  the row is still written (`last_seen_at` moves) but it is not NEWS.
  """

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      resource ReactiveDag.Fixtures.LeafFingerprintGraph.Docs
      resource ReactiveDag.Fixtures.LeafFingerprintGraph.Agendas
      resource ReactiveDag.Fixtures.LeafFingerprintGraph.Plain
    end
  end

  # a crawled document: content, plus the fields that move on every poll
  defmodule Docs do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.LeafFingerprintGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_lf_docs"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :title, :string, public?: true
      attribute :content_md5, :string, public?: true
      # the two that move on their own
      attribute :last_seen_at, :utc_datetime_usec, public?: true
      attribute :etag, :string, public?: true
      attribute :fingerprint, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        accept([:key, :title, :content_md5, :last_seen_at, :etag, :fingerprint])
      end
    end

    reactive do
      id(:docs)
      leaf?(true)
      fingerprint([:content_md5])
    end
  end

  # the agenda case: the fingerprint is COMPUTED, not a field list — a re-titled
  # meeting must re-fire even though its PDF bytes are identical
  defmodule Agendas do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.LeafFingerprintGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_lf_agendas"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :title, :string, public?: true
      attribute :content_md5, :string, public?: true
      attribute :last_seen_at, :utc_datetime_usec, public?: true
      attribute :digest, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        accept([:key, :title, :content_md5, :last_seen_at, :digest])
      end
    end

    reactive do
      id(:agendas)
      leaf?(true)
      fingerprint(fn row -> "#{row.content_md5}|#{:erlang.phash2(row.title)}" end)
      fingerprint_attribute(:digest)
    end
  end

  # no fingerprint declared — the old behaviour, unchanged
  defmodule Plain do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.LeafFingerprintGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_lf_plain"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :content_md5, :string, public?: true
      attribute :last_seen_at, :utc_datetime_usec, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        accept([:key, :content_md5, :last_seen_at])
      end
    end

    reactive do
      id(:plain)
      leaf?(true)
    end
  end

  def tables, do: ["fx_lf_docs", "fx_lf_agendas", "fx_lf_plain"]
end

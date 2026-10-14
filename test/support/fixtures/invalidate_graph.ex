defmodule ReactiveDag.Fixtures.InvalidateGraph do
  @moduledoc """
  Invalidation, tenanted and not.

  `Summaries` and `TenantedSummaries` are the same node either side of
  multitenancy — a tenant lost on the way to a mark names work that tenant's
  drain never reads.
  """

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      resource ReactiveDag.Fixtures.InvalidateGraph.Transcripts
      resource ReactiveDag.Fixtures.InvalidateGraph.Summaries
      resource ReactiveDag.Fixtures.InvalidateGraph.TenantedSummaries
    end
  end

  defmodule Transcripts do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.InvalidateGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_inv_transcripts"
      repo(ReactiveDag.TestRepo)
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
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.InvalidateGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_inv_summaries"
      repo(ReactiveDag.TestRepo)
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
          ReactiveDag.Fixtures.Calls.record(input.arguments.text)
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

  # A tenanted node, because the untenanted `Summaries` above cannot show it:
  # `invalidate/3` reads the rows it is about to clear and then UPDATES them, and
  # both halves need the tenant. Without it a reprocess of a tenanted plan raises,
  # and clearing another municipality's fingerprints would make ITS next drain
  # recompute rows nobody asked about.
  defmodule TenantedSummaries do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.InvalidateGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_inv_tenanted_summaries"
      repo(ReactiveDag.TestRepo)
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

  def tables, do: ["fx_inv_transcripts", "fx_inv_summaries", "fx_inv_tenanted_summaries"]
end

defmodule ReactiveDag.Fixtures.TenantScanGraph do
  @moduledoc """
  Scanning under multitenancy — one crawl per tenant, each marking only its
  own rows.
  """

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      resource ReactiveDag.Fixtures.TenantScanGraph.Docs
    end
  end

  defmodule Docs do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.TenantScanGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_ts_docs"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        accept([:key])
      end
    end

    reactive do
      id(:docs)
      op(:source)
      leaf?(true)
      poll(ReactiveDag.Fixtures.TenantScanGraph.Crawler, every: "0 12 * * *")
    end
  end

  defmodule Crawler do
    @moduledoc """
    The poll the leaf declares.

    In the fixture because the resource NAMES it. It reports to whoever is
    polling — `send(self(), …)` — so a test still observes every call without
    the fixture knowing anything about that test.
    """
    @behaviour ReactiveDag.Source

    @impl true
    def id, do: :crawler

    @impl true
    def poll(opts) do
      send(self(), {:polled, opts})
      {:ok, Process.get(:crawler_keys, [])}
    end
  end

  def tables, do: ["fx_ts_docs"]
end

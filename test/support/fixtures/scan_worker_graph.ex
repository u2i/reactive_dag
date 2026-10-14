defmodule ReactiveDag.Fixtures.ScanWorkerGraph do
  @moduledoc """
  A SCAN: one poll feeding several leaves, each marking only its own keys.

  `Docs` and `Notices` are the two leaves a fan-out poll feeds; `Totals`
  folds one of them, so a test can watch a scan reach a consumer.
  """

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      resource ReactiveDag.Fixtures.ScanWorkerGraph.Docs
      resource ReactiveDag.Fixtures.ScanWorkerGraph.Notices
      resource ReactiveDag.Fixtures.ScanWorkerGraph.Totals
    end
  end

  defmodule Docs do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.ScanWorkerGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_sw_docs"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute(:key, :string, primary_key?: true, allow_nil?: false, public?: true)
      attribute(:category, :string, public?: true)
    end

    actions do
      defaults([:read, :destroy])
      create(:upsert, upsert?: true, accept: [:key, :category])
    end

    reactive do
      id(:docs)
      leaf?(true)
      poll(ReactiveDag.Fixtures.ScanWorkerGraph.Crawler, args: [recent: true], every: "0 * * * *")
    end
  end

  # A SECOND leaf fed by the same scanner — one crawl of one upstream that lands
  # rows in two places. This is the shape a host means by "one poll is one unit
  # of work": the source is the unit, and the cells are where its output goes.
  defmodule Notices do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.ScanWorkerGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_sw_notices"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute(:key, :string, primary_key?: true, allow_nil?: false, public?: true)
      attribute(:category, :string, public?: true)
    end

    actions do
      defaults([:read, :destroy])
      create(:upsert, upsert?: true, accept: [:key, :category])
    end

    reactive do
      id(:notices)

      # a CONSUMER of the crawl, not a second declaration of it. One scanner is
      # one node now; two nodes naming the same module means the upstream is
      # polled twice, and `verify_one_node_per_source!/1` rejects it.
      reduce(
        over: :docs,
        group_by: :key,
        expand: fn key, rows ->
          for r <- rows, r.category == "notice", do: %{key: key, category: r.category}
        end
      )
    end
  end

  defmodule Totals do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.ScanWorkerGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_sw_totals"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute(:key, :string, primary_key?: true, allow_nil?: false, public?: true)
      attribute(:category, :string, public?: true)
      attribute(:n, :integer, public?: true)
    end

    actions do
      defaults([:read, :destroy])
      create(:upsert, upsert?: true, accept: [:key, :category, :n])
    end

    reactive do
      id(:totals)
      recompute_by(:category, to: :docs, from: :category)
      reduce(group_by: :category, into: [count: :n])
    end
  end

  defmodule Crawler do
    @moduledoc """
    The poll the leaves declare — an Agent-backed recorder a test drives.

    In the fixture because the RESOURCES name it. A test still controls what
    it returns and inspects what it was called with; the fixture only needs it
    to exist by the time the graph assembles.
    """
    @behaviour ReactiveDag.Source

    def start_link, do: Agent.start_link(fn -> %{polls: [], result: nil} end, name: __MODULE__)
    def polls, do: Agent.get(__MODULE__, & &1.polls) |> Enum.reverse()
    def returns(result), do: Agent.update(__MODULE__, &%{&1 | result: result})

    def raises, do: Agent.update(__MODULE__, &%{&1 | result: :raise})

    @impl true
    def id, do: :crawler
    # NOT a `Source` callback — the behaviour has `id/0`, `origin/0` and
    # `poll/1`. This is the fan-out set a test reads directly, so it carries no
    # `@impl`.
    def leaf_cells(_g), do: ["docs", "notices"]
    @impl true
    def poll(opts) do
      Agent.update(__MODULE__, &%{&1 | polls: [opts | &1.polls]})

      case Agent.get(__MODULE__, & &1.result) do
        :raise -> raise "upstream is down"
        # a scanner reporting it CANNOT scan, rather than a poll that found
        # nothing — the distinction #122 is about
        {:error, _} = error -> error
        result -> {:ok, result || %{changed: []}}
      end
    end
  end

  def tables, do: ["fx_sw_docs", "fx_sw_notices", "fx_sw_totals"]
end

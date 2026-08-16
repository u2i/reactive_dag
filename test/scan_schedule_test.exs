defmodule ReactiveDag.ScanScheduleTest do
  @moduledoc """
  `scan … args:` / `every:` (#86) — a leaf declares what a routine poll costs and
  how often it should happen.

  The motivating shape: one crawler whose full discovery is a request per board
  per year, and whose routine check should only look at the recent slice. Two
  scanners would duplicate everything to vary which years are asked for, so it
  is one scanner with a standing default and a caller override.

  The library **never schedules**. `crontab/2` emits entries the host hands to
  its own Oban config — data, not jobs. That keeps this out of the host's
  supervision tree and lets it filter or ignore what we produce.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.Source

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      allow_unregistered?(true)
    end
  end

  # the expensive one: cheap default, affordable full pass, hourly
  defmodule Crawler do
    @behaviour Source
    def start_link, do: Agent.start_link(fn -> [] end, name: __MODULE__)
    def polls, do: Agent.get(__MODULE__, &Enum.reverse/1)

    @impl true
    def id, do: :crawler
    @impl true
    def leaf_cells(_g), do: ["agendas", "minutes"]
    @impl true
    def origin, do: %{label: "City agenda center"}
    @impl true
    def poll(opts) do
      Agent.update(__MODULE__, &[opts | &1])
      {:ok, %{changed: []}}
    end
  end

  # the cheap one: nothing to declare, so it declares nothing
  defmodule Listing do
    @behaviour Source
    def start_link, do: Agent.start_link(fn -> [] end, name: __MODULE__)
    def polls, do: Agent.get(__MODULE__, &Enum.reverse/1)

    @impl true
    def id, do: :listing
    @impl true
    def leaf_cells(_g), do: ["transcripts"]
    @impl true
    def poll(opts) do
      Agent.update(__MODULE__, &[opts | &1])
      {:ok, %{changed: []}}
    end
  end

  # a CONSUMER of the crawl — the shape that replaced "a second leaf of the same
  # crawler". cascade's agenda_center feeds two of these.
  defmodule Minutes do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute(:key, :string, primary_key?: true, allow_nil?: false, public?: true)
    end

    actions do
      defaults([:read, :destroy])
      create(:upsert, upsert?: true, accept: [:key])
    end

    reactive do
      id(:minutes)

      # a consumer of the crawl, not a second poller of it. One source is one
      # node now, so "two leaves on one scanner" is not a shape that exists.
      reduce(over: :agendas, group_by: :key, expand: fn key, _rows -> [%{key: key}] end)
    end
  end

  defmodule ZuluScan do
    @behaviour Source
    @impl true
    def id, do: :zulu_scan
    @impl true
    def poll(_opts), do: {:ok, %{changed: []}}
  end

  # sorts LAST alphabetically, so a declared order that puts it first is
  # distinguishable from the default
  defmodule Zulu do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute(:key, :string, primary_key?: true, allow_nil?: false, public?: true)
    end

    actions do
      defaults([:read, :destroy])
      create(:upsert, upsert?: true, accept: [:key])
    end

    reactive do
      id(:zulu)
      leaf?(true)
      poll(ReactiveDag.ScanScheduleTest.ZuluScan, every: "0 2 * * *")
    end
  end

  defmodule DependentScan do
    @behaviour Source
    @impl true
    def id, do: :dependent_scan
    @impl true
    def poll(_opts), do: {:ok, %{changed: []}}
  end

  # sorts FIRST alphabetically but must run LAST: only the edge says so
  defmodule Dependent do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute(:key, :string, primary_key?: true, allow_nil?: false, public?: true)
    end

    actions do
      defaults([:read, :destroy])
      create(:upsert, upsert?: true, accept: [:key])
    end

    reactive do
      id(:aaa_dependent)
      leaf?(true)
      poll(ReactiveDag.ScanScheduleTest.DependentScan, every: "0 * * * *")
      depends_on([:zulu])
    end
  end

  defmodule Agendas do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute(:key, :string, primary_key?: true, allow_nil?: false, public?: true)
    end

    actions do
      defaults([:read, :destroy])
      create(:upsert, upsert?: true, accept: [:key])
    end

    reactive do
      id(:agendas)
      leaf?(true)
      poll(ReactiveDag.ScanScheduleTest.Crawler, args: [recent: true], every: "0 * * * *")
    end
  end

  defmodule Transcripts do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute(:key, :string, primary_key?: true, allow_nil?: false, public?: true)
    end

    actions do
      defaults([:read, :destroy])
      create(:upsert, upsert?: true, accept: [:key])
    end

    reactive do
      id(:transcripts)
      leaf?(true)
      poll(ReactiveDag.ScanScheduleTest.Listing)
    end
  end

  defmodule Derived do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute(:key, :string, primary_key?: true, allow_nil?: false, public?: true)
      attribute(:n, :integer, public?: true)
    end

    actions do
      defaults([:read, :destroy])
      create(:upsert, upsert?: true, accept: [:key, :n])
    end

    reactive do
      id(:derived)
      reduce(over: :agendas, group_by: :key, into: [count: :n])
    end
  end

  setup do
    start_supervised!(%{id: Crawler, start: {Crawler, :start_link, []}})
    start_supervised!(%{id: Listing, start: {Listing, :start_link, []}})
    :ok
  end

  defp cells(entries), do: Enum.map(entries, fn {_e, _w, args: a} -> a["cell"] end)

  defp plan, do: ReactiveDag.Node.graph([Agendas, Minutes, Transcripts])

  describe "standing args" do
    test "a routine poll gets the leaf's declared default" do
      {:ok, _} = Source.poll_all(plan())

      assert [[recent: true]] = Crawler.polls()
    end

    test "the caller wins — a deliberate deep pass says so" do
      {:ok, _} = Source.poll_all(plan(), recent: false)

      assert [opts] = Crawler.polls()
      assert opts[:recent] == false
    end

    test "caller opts that don't collide are merged alongside" do
      {:ok, _} = Source.poll_all(plan(), only: [2019])

      assert [opts] = Crawler.polls()
      assert opts[:recent] == true
      assert opts[:only] == [2019]
    end

    test "a scanner declaring nothing is polled with the caller's opts alone" do
      {:ok, _} = Source.poll_all(plan(), only: ["m1"])

      assert [[only: ["m1"]]] = Listing.polls()
    end

    test "standing_args/1 exposes the defaults for a host driving one scanner" do
      args = Source.standing_args(plan())

      assert args[Crawler] == [recent: true]
      assert args[Listing] == []
    end
  end

  describe "crontab/2" do
    test "collects the declared cadence into Oban-shaped entries" do
      # keyed by CELL, not source: a source feeding several leaves is several
      # units of work, each with its own declared bound
      assert Source.crontab(plan(), MyWorker, per_cell: true) == [
               {"0 * * * *", MyWorker, args: %{"cell" => "agendas"}}
             ]
    end

    test "a leaf declaring no cadence contributes nothing" do
      # the cheap scanner gets no entry — the host schedules it however it likes,
      # and offering a cadence it never asked for would be noise
      refute Source.crontab(plan(), MyWorker, per_cell: true)
             |> Enum.any?(fn {_c, _w, [args: %{"cell" => c}]} -> c == "transcripts" end)
    end

    test "a plan with no cadence at all yields no entries" do
      assert ReactiveDag.Node.graph([Transcripts]) |> Source.crontab(MyWorker, per_cell: true) == []
    end

    test "a host can add its own standing args" do
      # a crontab entry is built once at config time, so anything it carries is
      # fixed for every firing — but a host routing crawls to their own queue
      # should not have to rebuild the list to say so
      assert Source.crontab(plan(), MyWorker, per_cell: true, args: %{"queue" => "crawls"}) == [
               {"0 * * * *", MyWorker, args: %{"cell" => "agendas", "queue" => "crawls"}}
             ]
    end

    test "and cannot retarget the job by supplying its own cell" do
      # merged UNDER the computed cell, so a stale copy-paste fails loudly at the
      # worker rather than silently polling the wrong leaf forever
      assert Source.crontab(plan(), MyWorker, per_cell: true, args: %{"cell" => "wrong"}) == [
               {"0 * * * *", MyWorker, args: %{"cell" => "agendas"}}
             ]
    end

    test "it emits DATA, not jobs — nothing was scheduled or polled" do
      Source.crontab(plan(), MyWorker, per_cell: true)

      assert Crawler.polls() == []
      assert Listing.polls() == []
    end
  end

  test "the declarations reach the cell's meta" do
    cells = plan().cells

    assert cells["agendas"].meta[:scan_args] == [recent: true]
    assert cells["agendas"].meta[:scan_every] == "0 * * * *"
    assert cells["transcripts"].meta[:scan_args] == []
    refute cells["transcripts"].meta[:scan_every]
  end

  describe "poll_cell/3 — the 'refresh this leaf' affordance" do
    test "polls the scanner feeding that cell, with its standing default" do
      {:ok, _} = Source.poll_cell(plan(), "agendas")

      assert [[recent: true]] = Crawler.polls()
      assert Listing.polls() == [], "only the named leaf's scanner runs"
    end

    test "the caller wins — the deep pass a button asks for" do
      {:ok, _} = Source.poll_cell(plan(), "agendas", recent: false)

      assert [opts] = Crawler.polls()
      assert opts[:recent] == false
    end

    test "a cell with no scanner reports it, rather than failing" do
      # a host renders this as "no refresh available", not as an error
      p = ReactiveDag.Node.graph([Agendas, Minutes, Transcripts, Derived])

      assert Source.poll_cell(p, "derived") == {:error, :no_scanner}
    end

    test "an unknown cell id is the same answer" do
      assert Source.poll_cell(plan(), "nope") == {:error, :no_scanner}
    end
  end

  describe "controls/1 — what a host needs to render a scan control" do
    test "describes each scannable cell, and omits the rest" do
      p = ReactiveDag.Node.graph([Agendas, Transcripts, Derived])
      controls = Source.controls(p)

      assert Map.keys(controls) |> Enum.sort() == ["agendas", "transcripts"]
      refute Map.has_key?(controls, "derived")
    end

    test "an expensive scanner reports its default and cadence" do
      %{"agendas" => c} = Source.controls(plan())

      assert c.source == Crawler
      assert c.args == [recent: true]
      assert c.every == "0 * * * *"
    end

    test "a cheap scanner reports empty — so a host renders a plain refresh" do
      %{"transcripts" => c} = Source.controls(plan())

      assert c.args == []
      refute c.every
    end

    test "origin rides along when the source implements it" do
      %{"agendas" => a, "transcripts" => t} = Source.controls(plan())

      assert a.origin == %{label: "City agenda center"}
      refute t.origin, "not implemented = origin unknown"
    end
  end

  describe "scan_jobs/1 — every scannable unit of work" do
    test "one entry per scanned cell, cadence or not" do
      jobs = Source.scan_jobs(plan())

      # one entry per SOURCE node. `minutes` consumes the crawl rather than
      # polling it, so it is not a unit of scannable work.
      assert Enum.map(jobs, & &1.cell) == ["agendas", "transcripts"]
    end

    test "each carries what its leaf declared" do
      [agendas, transcripts] = Source.scan_jobs(plan())

      assert agendas.source == Crawler
      assert agendas.args == [recent: true]
      assert agendas.every == "0 * * * *"

      # the cheap one declared nothing, and says so rather than being absent
      assert transcripts.source == Listing
      assert transcripts.args == []
      refute transcripts.every
    end

    test "crontab/2 is a projection of the subset that declared a cadence" do
      # ...deduplicated BY SCANNER. `agendas` and `minutes` are one crawl of one
      # site, so scheduling both would fetch it twice an hour and the second poll
      # would mark rows the first already handled.
      with_cadence = Source.scan_jobs(plan()) |> Enum.filter(& &1.every) |> Enum.map(& &1.cell)

      assert with_cadence == ["agendas"], "one source, one poll"

      assert Enum.map(Source.crontab(plan(), MyWorker, per_cell: true), fn {_c, _w, [args: a]} -> a["cell"] end) ==
               ["agendas"]
    end

    test "entries are alphabetical by default — stable, if arbitrary" do
      p = ReactiveDag.Node.graph([Agendas, Minutes, Transcripts, Zulu])

      assert cells(Source.crontab(p, MyWorker, per_cell: true)) == ["agendas", "zulu"]
    end

    test "`order:` puts a declared sequence first" do
      # for a host whose scan order carries meaning — observers before the nodes
      # that read them — alphabetical is just as arbitrary as map order, only
      # stably so (u2i/reactive_dag#123)
      p = ReactiveDag.Node.graph([Agendas, Minutes, Transcripts, Zulu])

      assert cells(Source.crontab(p, MyWorker, per_cell: true, order: [:zulu, :agendas])) == ["zulu", "agendas"]
    end

    test "anything unlisted keeps the alphabetical tail" do
      p = ReactiveDag.Node.graph([Agendas, Minutes, Transcripts, Zulu])

      assert cells(Source.crontab(p, MyWorker, per_cell: true, order: [:zulu])) == ["zulu", "agendas"]
    end

    test "`order:` also takes a comparator, for a rule rather than a list" do
      p = ReactiveDag.Node.graph([Agendas, Minutes, Transcripts, Zulu])

      by_length = fn {_every, _w, args: a} -> String.length(a["cell"]) end

      assert cells(Source.crontab(p, MyWorker, per_cell: true, order: by_length)) == ["zulu", "agendas"]
    end

    test "one source is one entry, however many nodes consume it" do
      # `minutes` reads `agendas`, so the crawl feeds two cells and is scheduled
      # once. This used to need deduplication by {scanner, cadence}; a source is
      # a node now, so there is nothing to deduplicate.
      entries = Source.crontab(plan(), MyWorker, per_cell: true)

      assert length(entries) == 1
      assert [{"0 * * * *", MyWorker, [args: %{"cell" => "agendas"}]}] = entries
    end

    test "a derived cell is not a unit of scannable work" do
      jobs = ReactiveDag.Node.graph([Agendas, Derived]) |> Source.scan_jobs()

      assert Enum.map(jobs, & &1.cell) == ["agendas"]
    end
  end

  describe "crontab/3 emits a SWEEP by default" do
    test "one entry per cadence, not one per source" do
      # the default shape: a single job that polls every source in graph order
      # and drains once. N independent cron entries cannot order themselves —
      # they fire concurrently however the list is sorted.
      p = ReactiveDag.Node.graph([Agendas, Transcripts, Zulu])

      assert [
               {"0 * * * *", MyWorker, [args: %{"sweep" => true}]},
               {"0 2 * * *", MyWorker, [args: %{"sweep" => true}]}
             ] = Source.crontab(p, MyWorker)
    end

    test "two sources on the same cadence share one entry" do
      # `agendas` and a second hourly source are one sweep, not two jobs racing
      p = ReactiveDag.Node.graph([Agendas, Transcripts])

      assert [{"0 * * * *", MyWorker, [args: %{"sweep" => true}]}] =
               Source.crontab(p, MyWorker)
    end

    test "host args ride along" do
      p = ReactiveDag.Node.graph([Agendas])

      assert [{_, MyWorker, [args: args]}] =
               Source.crontab(p, MyWorker, args: %{"queue" => "crawls"})

      assert args["queue"] == "crawls"
      assert args["sweep"] == true
    end

    test "`per_cell: true` opts back into one job per source" do
      # for a host that genuinely wants independent jobs — different queues,
      # different retry policies — rather than a sequenced sweep
      p = ReactiveDag.Node.graph([Agendas, Zulu])

      assert cells(Source.crontab(p, MyWorker, per_cell: true)) == ["agendas", "zulu"]
    end
  end

  describe "poll_all/2 — the sweep that actually sequences work" do
    test "polls in CELL order" do
      # `scanners/1` used to iterate a map, so the order real work happened in
      # was undefined — worse than in `crontab/3`, whose entries are independent
      # jobs anyway (u2i/reactive_dag#123).
      #
      # NOTE this test cannot DISTINGUISH the sort from map order: Elixir's
      # small maps happen to iterate in key order, so both give the same answer
      # at this size. The sort makes it a guarantee rather than an accident of
      # the map implementation, which is the point — but the assertion below is
      # weaker than it looks, and swapping the sort back to `Map.values/1` does
      # not fail it.
      p = ReactiveDag.Node.graph([Agendas, Transcripts, Zulu])

      assert Source.scanners(p) == [Crawler, Listing, ZuluScan]
    end

    test "a source that DEPENDS ON another is polled after it" do
      # the order comes from the graph, in the vocabulary every other edge uses.
      # `depends_on` on a source is a sequencing edge — it declares nothing
      # about rows, only that this poll needs that one to have happened.
      p = ReactiveDag.Node.graph([Agendas, Transcripts, Zulu, Dependent])

      order = Source.scanners(p)

      assert Enum.find_index(order, &(&1 == DependentScan)) >
               Enum.find_index(order, &(&1 == ZuluScan)),
             "declared downstream of zulu, so polled after it"
    end

    test "`order:` is a real happens-before here" do
      # these polls run one after another IN THIS PROCESS, so a later source
      # genuinely sees what an earlier one wrote — which is what the crontab's
      # `order:` cannot promise
      p = ReactiveDag.Node.graph([Agendas, Transcripts, Zulu])

      assert Source.scanners(p, order: [:zulu, :transcripts]) == [ZuluScan, Listing, Crawler]
    end

    test "and it drives the sweep, not just the listing" do
      p = ReactiveDag.Node.graph([Agendas, Transcripts, Zulu])

      {:ok, _} = Source.poll_all(p, order: [:zulu])

      # every scanner ran; the declared one first
      assert Crawler.polls() != []
      assert Listing.polls() != []
    end

    test "an unlisted source follows in cell order" do
      p = ReactiveDag.Node.graph([Agendas, Transcripts, Zulu])

      assert Source.scanners(p, order: [:zulu]) == [ZuluScan, Crawler, Listing]
    end

    test "naming a cell with no scanner is ignored, not an error" do
      p = ReactiveDag.Node.graph([Agendas, Transcripts, Zulu])

      assert Source.scanners(p, order: [:nope, :zulu]) == [ZuluScan, Crawler, Listing]
    end
  end
end

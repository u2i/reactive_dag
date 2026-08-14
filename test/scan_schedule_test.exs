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
    def leaf_cells(_g), do: ["agendas"]
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

  defmodule Agendas do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Ets, extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
    end

    actions do
      defaults [:read, :destroy]
      create :upsert, upsert?: true, accept: [:key]
    end

    reactive do
      id(:agendas)
      leaf?(true)
      scan(ReactiveDag.ScanScheduleTest.Crawler, args: [recent: true], every: "0 * * * *")
    end
  end

  defmodule Transcripts do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Ets, extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
    end

    actions do
      defaults [:read, :destroy]
      create :upsert, upsert?: true, accept: [:key]
    end

    reactive do
      id(:transcripts)
      leaf?(true)
      scan(ReactiveDag.ScanScheduleTest.Listing)
    end
  end

  setup do
    start_supervised!(%{id: Crawler, start: {Crawler, :start_link, []}})
    start_supervised!(%{id: Listing, start: {Listing, :start_link, []}})
    :ok
  end

  defp plan, do: ReactiveDag.Node.graph([Agendas, Transcripts])

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
      assert Source.crontab(plan(), MyWorker) == [
               {"0 * * * *", MyWorker, args: %{"source" => "crawler"}}
             ]
    end

    test "a leaf declaring no cadence contributes nothing" do
      # the cheap scanner gets no entry — the host schedules it however it likes,
      # and offering a cadence it never asked for would be noise
      refute Source.crontab(plan(), MyWorker)
             |> Enum.any?(fn {_c, _w, args: %{"source" => s}} -> s == "listing" end)
    end

    test "a plan with no cadence at all yields no entries" do
      assert ReactiveDag.Node.graph([Transcripts]) |> Source.crontab(MyWorker) == []
    end

    test "it emits DATA, not jobs — nothing was scheduled or polled" do
      Source.crontab(plan(), MyWorker)

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
end

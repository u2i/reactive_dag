defmodule ReactiveDag.DeferredScanArgsTest do
  @moduledoc """
  A zero-arity function as a standing `args:` VALUE, resolved at poll time.

  ## The bug this exists for

  `args:` is DSL data, evaluated once when the module compiles. A bound that
  depends on the clock cannot be written there as a literal — `year: 2026` is
  right on the day of the build and wrong every day after, silently, until
  something redeploys.

  The way that failed in practice was worse than a stale literal. A crawler took
  `recent: true` to mean "current and previous year" but derived "current" from
  an explicit `year:` the caller had to supply, falling back to *every year* when
  it was absent. The leaf declared `args: [recent: true]` and nothing supplied
  the anchor, so the standing bound — commented as "the standing bound" — had
  never once applied, and every routine poll crawled the full corpus.

  A literal cannot fix that and a fallback clock inside the scanner makes a pure
  function impure. Deferring the VALUE puts the clock at the layer that has one.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.Source

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      allow_unregistered?(true)
    end
  end

  # Records what it was called with, so a test can assert on the RESOLVED opts
  # rather than on the declaration.
  defmodule Crawler do
    @behaviour Source
    def start_link, do: Agent.start_link(fn -> [] end, name: __MODULE__)
    def polls, do: Agent.get(__MODULE__, &Enum.reverse/1)

    @impl true
    def id, do: :crawler
    @impl true
    def poll(opts) do
      Agent.update(__MODULE__, &[opts | &1])
      {:ok, %{changed: []}}
    end
  end

  # A clock that MOVES, which is the whole point: a deferred value must be
  # called per poll, not once at assembly. A literal or a resolve-once
  # implementation passes every other assertion in this file and fails this one.
  defmodule Clock do
    def start_link, do: Agent.start_link(fn -> 2026 end, name: __MODULE__)
    def year, do: Agent.get(__MODULE__, & &1)
    def tick, do: Agent.update(__MODULE__, &(&1 + 1))
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

      poll(ReactiveDag.DeferredScanArgsTest.Crawler,
        args: [recent: true, year: &ReactiveDag.DeferredScanArgsTest.Clock.year/0],
        every: "0 12 * * *"
      )
    end
  end

  setup do
    start_supervised!(%{id: :crawler, start: {Crawler, :start_link, []}})
    start_supervised!(%{id: :clock, start: {Clock, :start_link, []}})
    {:ok, plan: ReactiveDag.Node.graph([Agendas])}
  end

  describe "the capture survives the DSL" do
    test "assembly stamps the function itself into the cell, uncalled", %{plan: plan} do
      args = plan.cells["agendas"].meta[:scan_args]

      assert Keyword.fetch!(args, :recent) == true
      assert is_function(Keyword.fetch!(args, :year), 0)
    end
  end

  describe "poll_all/2" do
    test "calls the function and passes its result", %{plan: plan} do
      {:ok, _} = Source.poll_all(plan)

      assert [opts] = Crawler.polls()
      assert opts[:recent] == true
      assert opts[:year] == 2026
    end

    test "resolves per poll, so a moving clock moves the bound", %{plan: plan} do
      {:ok, _} = Source.poll_all(plan)
      Clock.tick()
      {:ok, _} = Source.poll_all(plan)

      assert [first, second] = Crawler.polls()
      assert first[:year] == 2026
      assert second[:year] == 2027
    end

    test "the caller's explicit value still wins over a deferred default", %{plan: plan} do
      # The escape hatch has to survive deferral: a deliberate deep pass names
      # its own year and the clock must not overwrite it.
      {:ok, _} = Source.poll_all(plan, year: 2019)

      assert [opts] = Crawler.polls()
      assert opts[:year] == 2019
    end

    test "a caller may defer too, and gets resolved like a declared default", %{plan: plan} do
      # Resolution runs on the MERGED list rather than the standing side only,
      # so `args:` is not a privileged place to defer from. Without that, this
      # arrives at the scanner as a raw function and every scanner would need to
      # handle one.
      {:ok, _} = Source.poll_all(plan, year: fn -> 1999 end)

      assert [opts] = Crawler.polls()
      assert opts[:year] == 1999
    end
  end

  describe "poll_cell/3" do
    test "resolves the same way as a sweep", %{plan: plan} do
      {:ok, _} = Source.poll_cell(plan, "agendas")

      assert [opts] = Crawler.polls()
      assert opts[:year] == 2026
    end

    test "the caller's value wins here too", %{plan: plan} do
      {:ok, _} = Source.poll_cell(plan, "agendas", year: 2019)

      assert [opts] = Crawler.polls()
      assert opts[:year] == 2019
    end
  end

  describe "describing a graph does not run the host's code" do
    # A dashboard calls `controls/1` on every render. If that resolved, it would
    # be calling the host's clock — and whatever else a host defers, which need
    # not be cheap or safe to call from a web request.
    test "controls/1 reports the function verbatim", %{plan: plan} do
      args = plan |> Source.controls() |> get_in(["agendas", :args])

      assert is_function(Keyword.fetch!(args, :year), 0)
      assert Crawler.polls() == []
    end

    test "scan_jobs/1 reports the function verbatim", %{plan: plan} do
      job = plan |> Source.scan_jobs() |> Enum.find(&(&1.cell == "agendas"))

      assert is_function(Keyword.fetch!(job.args, :year), 0)
    end

    test "standing_args/1 reports the function verbatim", %{plan: plan} do
      args = plan |> Source.standing_args() |> Map.fetch!(Crawler)

      assert is_function(Keyword.fetch!(args, :year), 0)
    end
  end

  describe "resolve_args/1" do
    test "leaves a list with no functions untouched" do
      args = [recent: true, year: 2026]
      assert Source.resolve_args(args) == args
    end

    test "leaves a non-zero-arity function alone rather than raising" do
      # A 1-arity value is not a deferred value missing its argument, it is a
      # value this does not know how to resolve. Calling it would raise
      # BadArity from inside the poll; passing it through lets the scanner
      # receive what the leaf declared and complain in its own terms.
      fun = fn x -> x end
      assert Source.resolve_args(filter: fun) == [filter: fun]
    end

    test "an anonymous zero-arity function resolves too" do
      assert Source.resolve_args(year: fn -> 1999 end) == [year: 1999]
    end

    test "resolves several deferred values independently" do
      assert Source.resolve_args(a: fn -> 1 end, b: 2, c: fn -> 3 end) == [a: 1, b: 2, c: 3]
    end
  end
end

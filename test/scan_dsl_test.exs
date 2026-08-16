defmodule ReactiveDag.ScanDslTest do
  @moduledoc """
  `scan Mod` — the scanner↔leaf pairing as a fact of the graph.

  Before this, the binding lived in two places joined by string matching: the
  scanner's own `leaf_cells/1`, and an opaque `source :atom` label the library
  never interpreted. `Source.verify!/2` existed precisely to catch the
  mismatches that invites — and only if a host remembered to call it.

  Declaring it on the leaf lets `Node.graph/2` verify the pairing itself, and
  lets `Source.poll_all/2` find scanners from the plan rather than from a list
  kept alongside it (the list being the thing that drifts).

  Scanners still run OUTSIDE the drain — external I/O has no business inside a
  depth-ordered recompute. This changes where the binding is declared, not when
  polling happens.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.Source

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      allow_unregistered?(true)
    end
  end

  # a well-behaved scanner for the :agenda_docs leaf
  defmodule Crawler do
    @behaviour ReactiveDag.Source

    def start_link, do: Agent.start_link(fn -> 0 end, name: __MODULE__)
    def polls, do: Agent.get(__MODULE__, & &1)

    @impl true
    def id, do: :crawler

    @impl true
    def leaf_cells(_graph), do: ["agenda_docs"]

    @impl true
    def poll(_opts) do
      Agent.update(__MODULE__, &(&1 + 1))
      {:ok, %{changed: ["doc-1"]}}
    end
  end

  # feeds a DIFFERENT leaf than the one that will declare it
  defmodule Disowning do
    @behaviour ReactiveDag.Source
    @impl true
    def id, do: :disowning
    @impl true
    def leaf_cells(_graph), do: ["somewhere_else"]
    @impl true
    def poll(_opts), do: {:ok, %{}}
  end

  defmodule Exploding do
    @behaviour ReactiveDag.Source
    @impl true
    def id, do: :exploding
    @impl true
    def leaf_cells(_graph), do: ["flaky"]
    @impl true
    def poll(_opts), do: raise("upstream is down")
  end

  defmodule NotASource do
    def hello, do: :world
  end

  defmodule AgendaDocs do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
    end

    actions do
      defaults [:read]
    end

    reactive do
      id(:agenda_docs)
      leaf?(true)
      # THE DECLARATION: this node's rows come from that scanner
      poll(ReactiveDag.ScanDslTest.Crawler)
    end
  end

  defmodule Flaky do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
    end

    actions do
      defaults [:read]
    end

    reactive do
      id(:flaky)
      leaf?(true)
      poll(ReactiveDag.ScanDslTest.Exploding)
    end
  end

  setup do
    start_supervised!(%{id: Crawler, start: {Crawler, :start_link, []}})
    :ok
  end

  test "the scanner lands in cell meta, so the plan knows who feeds this leaf" do
    plan = ReactiveDag.Node.graph([AgendaDocs])

    assert plan.cells["agenda_docs"].meta.scan == Crawler
    assert Source.scanners(plan) == [Crawler]
  end

  test "a source's fed cells are its children — nothing to disagree with" do
    # This replaced a test asserting that a scanner "disowning" its leaf raises.
    # That error was only possible because the pairing was written twice: `scan
    # Mod` on the leaf and `leaf_cells/1` on the module. A node declares `poll
    # Mod`, its consumers declare an ordinary edge, and the fed set is derived —
    # so the disagreement is unrepresentable rather than caught.
    plan = ReactiveDag.Node.graph([AgendaDocs])

    assert ReactiveDag.Source.cells_of(ReactiveDag.ScanDslTest.Crawler, plan) == ["agenda_docs"]
  end

  test "a `scan` naming something that isn't a Source raises, with the fix" do
    defmodule BadScan do
      use Ash.Resource,
        domain: ReactiveDag.ScanDslTest.Domain,
        data_layer: Ash.DataLayer.Ets,
        extensions: [ReactiveDag.Node]

      ets do
      end

      attributes do
        attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      end

      actions do
        defaults [:read]
      end

      reactive do
        id(:bad_scan)
        leaf?(true)
        poll(ReactiveDag.ScanDslTest.NotASource)
      end
    end

    err = assert_raise ArgumentError, fn -> ReactiveDag.Node.graph([BadScan]) end
    msg = Exception.message(err)

    assert msg =~ "does not implement"
    # points at the right rung rather than just refusing
    assert msg =~ "compute"
  end

  describe "poll_all/2" do
    test "polls every scanner the plan declares" do
      plan = ReactiveDag.Node.graph([AgendaDocs])

      assert {:ok, results} = Source.poll_all(plan)
      assert results == %{Crawler => %{changed: ["doc-1"]}}
      assert Crawler.polls() == 1
    end

    test "a scanner appears ONCE however many leaves declare it" do
      # the same module feeding two leaves must not be polled twice
      plan = ReactiveDag.Node.graph([AgendaDocs])
      assert Source.scanners(plan) == [Crawler]
    end

    test "a failing scanner is reported, not silently swallowed" do
      plan = ReactiveDag.Node.graph([Flaky])

      assert {:error, [{Exploding, reason}]} = Source.poll_all(plan)
      assert reason =~ "upstream is down"
    end

    test "one failure does not cancel the others' work" do
      plan = ReactiveDag.Node.graph([AgendaDocs, Flaky])

      assert {:error, failures} = Source.poll_all(plan)
      assert [{Exploding, _}] = failures

      # the healthy scanner still ran
      assert Crawler.polls() == 1
    end

    test "a plan with no scanners polls nothing" do
      defmodule Plain do
        use Ash.Resource,
          domain: ReactiveDag.ScanDslTest.Domain,
          data_layer: Ash.DataLayer.Ets,
          extensions: [ReactiveDag.Node]

        ets do
        end

        attributes do
          attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
        end

        actions do
          defaults [:read]
        end

        reactive do
          id(:plain)
          leaf?(true)
        end
      end

      plan = ReactiveDag.Node.graph([Plain])

      assert Source.scanners(plan) == []
      assert {:ok, %{}} = Source.poll_all(plan)
    end
  end

  test "a scanner AND a computation on one node is a contradiction, and fails" do
    # a scanner writes tuples from outside; a combinator derives them from
    # inputs. Together the poll and the drain overwrite each other.
    defmodule Src do
      use Ash.Resource,
        domain: ReactiveDag.ScanDslTest.Domain,
        data_layer: Ash.DataLayer.Ets,
        extensions: [ReactiveDag.Node]

      ets do
      end

      attributes do
        attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
        attribute :cat, :string, public?: true
      end

      actions do
        defaults [:read]
      end

      reactive do
        id(:other)
        leaf?(true)
      end
    end

    defmodule Both do
      use Ash.Resource,
        domain: ReactiveDag.ScanDslTest.Domain,
        data_layer: Ash.DataLayer.Ets,
        extensions: [ReactiveDag.Node]

      ets do
      end

      attributes do
        attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
        attribute :n, :integer, public?: true
      end

      actions do
        defaults [:read]

        create :upsert do
          upsert?(true)
          accept([:key, :n])
        end
      end

      reactive do
        id(:agenda_docs)
        poll(ReactiveDag.ScanDslTest.Crawler)
        recompute_by(:key, to: :other, from: :cat)
        reduce(into: [count: :n])
      end
    end

    err = assert_raise ArgumentError, fn -> ReactiveDag.Node.graph([Src, Both]) end
    msg = Exception.message(err)

    assert msg =~ "AND a computation"
    assert msg =~ ":reduce"
    # says what to do about it, not merely that it's wrong
    assert msg =~ "Keep the poll"
  end

  describe "one node per source" do
    # Before rc.21 the same scanner on several leaves was the FAN-OUT shape: the
    # module declared `leaf_cells/1` and wrote them all. Now everything reading
    # a source is an ordinary edge, so two nodes naming one module means the
    # upstream is polled twice — `crontab/2` emits an entry each, and nothing
    # downstream can notice.
    test "two nodes declaring the same scanner raises, with the fix" do
      err =
        assert_raise ArgumentError, fn ->
          defmodule TwiceA do
            use Ash.Resource,
              domain: ReactiveDag.ScanDslTest.Domain,
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
              id(:twice_a)
              leaf?(true)
              poll(ReactiveDag.ScanDslTest.Crawler)
            end
          end

          defmodule TwiceB do
            use Ash.Resource,
              domain: ReactiveDag.ScanDslTest.Domain,
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
              id(:twice_b)
              leaf?(true)
              poll(ReactiveDag.ScanDslTest.Crawler)
            end
          end

          ReactiveDag.Node.graph([TwiceA, TwiceB])
        end

      msg = Exception.message(err)

      assert msg =~ "twice_a, twice_b"
      assert msg =~ "polled 2 times"
      assert msg =~ "reduce over:", "the message shows the shape that replaces it"
    end

    test "one node per scanner is fine, however many read it" do
      # `agenda_docs` polls; anything consuming it is an ordinary edge
      assert %ReactiveDag.Plan{} = ReactiveDag.Node.graph([AgendaDocs])
    end
  end
end

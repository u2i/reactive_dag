defmodule ReactiveDag.SourceTest do
  @moduledoc """
  The `ReactiveDag.Source` seam, after the source became a NODE.

  This file used to test three things that no longer exist: `verify!/2` and
  `verify_cells!/2` (a source's declared leaves must be real cells) and
  `cells_of/2` resolving through a module's own `leaf_cells/1` / `leaf_cell/0`.

  All three existed because the pairing was written twice — `scan Mod` on each
  fed leaf, `leaf_cells/1` on the module — so they could disagree, and a
  verifier had to catch it. A node now declares `poll Mod`, everything reading
  it is an ordinary edge, and the cells a source feeds are its children. There
  is one declaration, so there is nothing to verify against.

  What remains is the derivation itself, and the check that a `poll` names
  something that can actually poll.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.Source

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      allow_unregistered?(true)
    end
  end

  defmodule FleetScan do
    @behaviour Source

    @impl true
    def id, do: :fleet_scan
    @impl true
    def poll(_), do: {:ok, %{changed: []}}
  end

  defmodule Machines do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :kind, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]
      create :upsert, upsert?: true, accept: [:key, :kind]
    end

    reactive do
      id :machines
      leaf? true
      poll(ReactiveDag.SourceTest.FleetScan)
    end
  end

  defmodule Diggers do
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
      defaults [:read, :destroy]
      create :upsert, upsert?: true, accept: [:key]
    end

    reactive do
      id :diggers

      reduce(
        over: :machines,
        group_by: :key,
        expand: fn key, rows ->
          for r <- rows, r.kind == "digger", do: %{key: key}
        end
      )
    end
  end

  defmodule Cranes do
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
      defaults [:read, :destroy]
      create :upsert, upsert?: true, accept: [:key]
    end

    reactive do
      id :cranes

      reduce(
        over: :machines,
        group_by: :key,
        expand: fn key, rows ->
          for r <- rows, r.kind == "crane", do: %{key: key}
        end
      )
    end
  end

  defp plan, do: ReactiveDag.Node.graph([Machines, Diggers, Cranes])

  describe "cells_of/2 — derived from the graph, not declared twice" do
    test "a source's cells are the nodes that read it" do
      assert Enum.sort(Source.cells_of(FleetScan, plan())) == ["machines"]
    end

    test "the FED cells are its children, which is graph structure" do
      # this is what `leaf_cells/1` used to restate, and what `verify_scan!/3`
      # used to police for drift
      assert Enum.sort(plan().parents["machines"]) == ["cranes", "diggers"]
    end

    test "a source nothing polls yields [] rather than raising" do
      # a module can exist before anything declares it, and a plan built from a
      # subset of resources legitimately excludes some
      defmodule Unused do
        @behaviour Source
        @impl true
        def id, do: :unused
        @impl true
        def poll(_), do: {:ok, %{changed: []}}
      end

      assert Source.cells_of(Unused, plan()) == []
    end
  end

  describe "scanners/1 and drivers off the plan" do
    test "the plan names every source it polls" do
      assert Source.scanners(plan()) == [FleetScan]
    end

    test "a source's standing args and cadence live on the source node" do
      # one poll, one cadence, one bound — not reassembled from the leaves it
      # happens to feed
      [job] = Source.scan_jobs(plan())

      assert job.cell == "machines"
      assert job.source == FleetScan
    end
  end

  describe "verify_poll!/2 — the check that survives" do
    test "a poll naming a module that cannot poll raises at assembly" do
      err =
        assert_raise ArgumentError, fn ->
          defmodule NotASource do
            def id, do: :nope
          end

          defmodule Bad do
            use Ash.Resource,
              domain: ReactiveDag.SourceTest.Domain,
              data_layer: Ash.DataLayer.Ets,
              extensions: [ReactiveDag.Node]

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
              id :bad
              leaf? true
              poll(ReactiveDag.SourceTest.NotASource)
            end
          end

          ReactiveDag.Node.graph([Bad])
        end

      msg = Exception.message(err)
      assert msg =~ "does not implement"
      assert msg =~ "poll/1"
    end
  end
end

defmodule ReactiveDag.SourceAsNodeTest do
  @moduledoc """
  SPIKE — what a scanner looks like as a node in the graph.

  Today a source sits outside the graph: leaves declare `scan Mod`, the module
  declares `leaf_cells/1` back, and `verify_scan!/3` polices the two agreeing.
  That duplication has produced four bugs in this repo alone (a crawl scheduled
  twice, standing args silently dropped, a shared origin the dashboard could not
  show, and a host omitting `scan` on one leaf to dodge the double crawl).

  If the source were a NODE, the pairing would be an ordinary edge and all of
  that machinery would be graph traversal. This file builds the two candidate
  shapes against the real thing cascade's AgendaCenter crawler does — ONE fetch
  producing documents that belong to two different leaves, where which leaf a
  document belongs to is decided FROM THE DOCUMENT (their corpus has 21 minutes
  filed in the agenda slot, so the URL cannot be trusted).

  The question this file exists to answer: does the source node hold rows, or is
  it pure coordination?

  ## Finding

  **The source should hold rows, and the fan-out should NOT be modelled as
  filtering leaves.**

  Shape A works for everything the graph is asked to do — one fetch, both leaves
  derived, the misfiled document landing by its kind, the split readable as a
  column instead of a convention buried in `reclassify/2`, and the scan history
  queryable for free (which a host otherwise hand-rolls as a `ScanResult`
  table). The scan machinery all becomes graph traversal: a source's leaves are
  `plan.parents[id]`, they cannot disagree with it, and depth ordering falls out.

  But expressing the leaves as FILTERS of the source churns permanently. A leaf
  claimed for a key it does not want writes no row, and the payload loop reads
  "claimed but nothing written" as a vanished unit — a retirement, which is a
  change. Every poll then reports every non-matching key as churn: on a real
  corpus, ~700 agenda documents perpetually "changing" minutes_docs, each one
  propagating downstream.

  Declaring the unit does not help: the claim is already per-key, which is the
  finest grain there is. The gap is that a node cannot say *"this claimed key is
  not mine"* as distinct from *"this key is gone"* — see the two tests below
  that pin exactly that.

  So either the fan-out stays a marking concern (the source marks each leaf with
  the keys that belong to it, as `refresh_source/3` does today) — in which case
  the source node is a coordination point that holds the crawl for history and
  provenance, but is not the leaves' `over:` — or the payload loop grows a way
  to decline a key.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.{Cascade}

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      allow_unregistered?(true)
    end
  end

  defmodule Fetches do
    def start_link, do: Agent.start_link(fn -> 0 end, name: __MODULE__)
    def hit, do: Agent.update(__MODULE__, &(&1 + 1))
    def count, do: Agent.get(__MODULE__, & &1)
    def reset, do: Agent.update(__MODULE__, fn _ -> 0 end)
  end

  # What one crawl of the Village site returns. Note `kind`: decided from the
  # document, not the URL — `m2` was filed in the agenda slot and is minutes
  # anyway, which is the case that makes URL-based splitting wrong.
  def crawled do
    Fetches.hit()

    [
      %{key: "a1", kind: "agenda", filed_as: "agenda", body: "agenda one"},
      %{key: "a2", kind: "agenda", filed_as: "agenda", body: "agenda two"},
      %{key: "m1", kind: "minutes", filed_as: "minutes", body: "minutes one"},
      %{key: "m2", kind: "minutes", filed_as: "agenda", body: "misfiled minutes"}
    ]
  end

  # ── Shape A: the source HOLDS the crawl ─────────────────────────────────────
  #
  # One row per discovered document, `kind` as an ordinary attribute. The leaves
  # are ordinary derived nodes that filter it.

  defmodule Documents do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :kind, :string, public?: true
      attribute :filed_as, :string, public?: true
      attribute :body, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]
      create :upsert, upsert?: true, accept: [:key, :kind, :filed_as, :body]
    end

    reactive do
      id :documents
      leaf? true
    end
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
      attribute :body, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]
      create :upsert, upsert?: true, accept: [:key, :body]
    end

    reactive do
      id :agenda_docs

      # a projection of one kind out of the crawl. `expand` because a group
      # yields zero rows or one — a document that is not this kind simply is
      # not here.
      reduce(
        over: :documents,
        group_by: :key,
        expand: fn key, rows ->
          case Enum.filter(rows, &(&1.kind == "agenda")) do
            # not this leaf's document. DECLINE it — writing nothing would read
            # as a retirement and churn on every poll.
            [] -> [{:skip, key}]
            [row | _] -> [%{key: key, body: row.body}]
          end
        end
      )
    end
  end

  defmodule MinutesDocs do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :body, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]
      create :upsert, upsert?: true, accept: [:key, :body]
    end

    reactive do
      id :minutes_docs

      # a projection of one kind out of the crawl. `expand` because a group
      # yields zero rows or one — a document that is not this kind simply is
      # not here.
      reduce(
        over: :documents,
        group_by: :key,
        expand: fn key, rows ->
          case Enum.filter(rows, &(&1.kind == "minutes")) do
            # not this leaf's document. DECLINE it — writing nothing would read
            # as a retirement and churn on every poll.
            [] -> [{:skip, key}]
            [row | _] -> [%{key: key, body: row.body}]
          end
        end
      )
    end
  end

  setup do
    # A poll or a write now ENQUEUES a cascade rather than leaving a mark,
    # so without this the library reaches for Oban and these tests fail on
    # a missing instance rather than on anything they are about.
    ReactiveDag.Test.Pending.capture_enqueues()

    start_supervised!(%{id: Fetches, start: {Fetches, :start_link, []}})
    start_supervised!(ReactiveDag.Test.FakeSuspensionRepo)
    ReactiveDag.Test.FakeSuspensionRepo.install()


    for r <- [Documents, AgendaDocs, MinutesDocs], row <- Ash.read!(r), do: Ash.destroy!(row)
    Fetches.reset()
    :ok
  end

  defp plan_a, do: ReactiveDag.Node.graph([Documents, AgendaDocs, MinutesDocs])

  defp drain(plan) do
    ReactiveDag.Test.Pending.cascade(plan)
  end

  # The poll, with the source holding rows: reconcile what the crawl found into
  # the source's own table, mark only what actually moved, drain.
  #
  # `Rows.reconcile/3` is the library's own loop — it is what makes an unchanged
  # re-poll cost nothing, and a scanner writing rows by hand would be
  # reimplementing it.
  defp poll_into_source do
    plan = plan_a()
    rows = crawled()
    by_key = Map.new(rows, &{&1.key, &1})

    {:ok, changed, _detail} =
      ReactiveDag.Node.Rows.reconcile(
        plan.cells["documents"],
        Enum.map(rows, & &1.key),
        upsert: fn key -> Map.fetch!(by_key, key) end
      )

    # ONLY when the reconcile actually changed something. Marking zero keys was
    # harmless under the queue — an empty mark left nothing for a drain to
    # claim — but an origin is not a mark: a cascade runs every origin it is
    # handed, so an empty one still produces a step for `documents` and the
    # "nothing churns" assertion below would see work that did not happen.
    # A real poll enqueues nothing when it finds nothing, which is what this
    # mirrors (see `Source.refresh/3`: `for {leaf, keys} <- by_leaf, keys != []`).
    if changed != [], do: ReactiveDag.Test.Pending.add("documents", changed)

    drain(plan)
  end

  describe "Shape A — the source holds the crawl" do
    test "one fetch, and both leaves are derived from it" do
      {:ok, _} = poll_into_source()

      assert Fetches.count() == 1

      assert Enum.map(Ash.read!(AgendaDocs), & &1.key) |> Enum.sort() == ["a1", "a2"]
      assert Enum.map(Ash.read!(MinutesDocs), & &1.key) |> Enum.sort() == ["m1", "m2"]
    end

    test "a misfiled document lands by its KIND, not its slot" do
      # m2 was filed in the agenda slot; it is minutes, and the graph says so
      {:ok, _} = poll_into_source()

      refute "m2" in Enum.map(Ash.read!(AgendaDocs), & &1.key)
      assert "m2" in Enum.map(Ash.read!(MinutesDocs), & &1.key)
    end

    test "the split is a DECLARED property, readable without the crawler" do
      {:ok, _} = poll_into_source()

      # the reason m2 is minutes is a column anyone can query, not a convention
      # buried in `reclassify/2` inside the poll
      m2 = Ash.read!(Documents) |> Enum.find(&(&1.key == "m2"))

      assert m2.kind == "minutes"
      assert m2.filed_as == "agenda"
    end

    test "an idempotent re-poll is a no-op — nothing churns" do
      {:ok, _} = poll_into_source()
      {:ok, report} = poll_into_source()

      assert Fetches.count() == 2

      # Nothing moved anywhere. The source reconciled to no change, so the
      # leaves were never claimed and the drain did no work at all.
      #
      # Before `{:skip, key}` the same second poll reported ["m1", "m2"] changed
      # in agenda_docs and ["a1", "a2"] in minutes_docs: each leaf was claimed
      # for all four keys, wrote nothing for the two that were not its kind, and
      # the payload loop read "claimed but nothing written" as a vanished unit —
      # a retirement, and therefore a change. Forever, on every poll.
      assert report.steps == []
      assert ReactiveDag.Report.changed_total(report) == 0
    end

    test "declining a key does not destroy a row that IS the other leaf's" do
      {:ok, _} = poll_into_source()
      {:ok, _} = poll_into_source()

      # the obvious way to get a quiet second pass would be to retire the
      # declined keys; that would empty each leaf on the pass after it filled it
      assert Enum.map(Ash.read!(AgendaDocs), & &1.key) |> Enum.sort() == ["a1", "a2"]
      assert Enum.map(Ash.read!(MinutesDocs), & &1.key) |> Enum.sort() == ["m1", "m2"]
    end

    test "the crawl itself is queryable — a scan history for free" do
      {:ok, _} = poll_into_source()

      # cascade hand-rolls a ScanResult table for exactly this
      assert length(Ash.read!(Documents)) == 4
    end
  end

  describe "what the graph knows without any scan machinery" do
    test "the source's leaves are its children — no leaf_cells/1, no verify" do
      plan = plan_a()

      assert Enum.sort(plan.parents["documents"]) == ["agenda_docs", "minutes_docs"]
    end

    test "and they cannot disagree with it, because it is one declaration" do
      # the error `verify_scan!/3` exists to raise — "the scanner and the leaf
      # disagree about which cells it writes" — is unrepresentable here
      plan = plan_a()

      for leaf <- ["agenda_docs", "minutes_docs"] do
        assert plan.cells[leaf].inputs == ["documents"]
      end
    end

    test "depth ordering falls out, so the drain needs no special case" do
      plan = plan_a()

      assert plan.depths["documents"] == 0
      assert plan.depths["agenda_docs"] == 1
      assert plan.depths["minutes_docs"] == 1
    end
  end

  describe "why a declined key needs its own answer" do
    test "the claim is per-key already — no unit declaration could fix this" do
      # this is why `{:skip, key}` had to exist rather than being expressible
      # with `recompute_by`: per-key is the finest claim there is, so there is
      # no unit that says "and not the ones I filter out".
      {:ok, _} = poll_into_source()

      # a genuine change to ONE agenda document
      Documents
      |> Ash.Changeset.for_create(:upsert, %{
        key: "a1",
        kind: "agenda",
        filed_as: "agenda",
        body: "EDITED"
      })
      |> Ash.create!()

      ReactiveDag.Test.Pending.add("documents", ["a1"])
      {:ok, report} = drain(plan_a())

      minutes = Enum.find(report.steps, &(&1.cell == "minutes_docs"))

      # minutes_docs was claimed for a1 — an agenda key it holds no row for —
      # and says so without retiring anything
      assert minutes.claimed == ["a1"]
      assert minutes.changed == []
      assert Enum.map(Ash.read!(MinutesDocs), & &1.key) |> Enum.sort() == ["m1", "m2"]
    end

    test "and a key that GENUINELY vanishes is still retired" do
      # the distinction has to cut both ways, or declining becomes a way to
      # leave stale rows lying around
      {:ok, _} = poll_into_source()

      Ash.read!(Documents) |> Enum.find(&(&1.key == "m1")) |> Ash.destroy!()
      ReactiveDag.Test.Pending.add("documents", ["m1"])
      {:ok, report} = drain(plan_a())

      minutes = Enum.find(report.steps, &(&1.cell == "minutes_docs"))

      assert minutes.changed == ["m1"], "gone, not declined"
      refute "m1" in Enum.map(Ash.read!(MinutesDocs), & &1.key)
    end
  end
end

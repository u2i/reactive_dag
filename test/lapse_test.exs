defmodule ReactiveDag.LapseTest do
  @moduledoc """
  `lapse`: what a machine recompute does to a HUMAN's mark.

  The default is SURVIVAL and needs no declaration — the payload write only sets
  what the computation emits, so a column the upsert action does not `accept` is
  never touched. That default is load-bearing and easy to break: the first test
  here is a regression guard on it, because the obvious implementation of lapse
  (fold the nulling into the payload upsert's attrs) requires the payload action
  to accept the human column, and would then null it on EVERY pass.

  The failure this feature prevents is the opposite one: a sign-off that
  outlives the content it was about. "I checked these figures" is a claim about
  content, and when the content moves the claim is stale — but nothing in the
  payload loop notices, so the approval silently keeps standing over numbers
  nobody approved.

  ## The test that matters most

  `when_changed: [fields]` runs its OWN comparison, narrowed to the fields
  named. That is genuinely independent of the propagate verdict: a recompute can
  be `:changed` overall (so it propagates) while the watched fields sat still,
  and the mark must then SURVIVE. Reusing the `:changed` verdict would clear
  every approval on every cosmetic edit, and an approval that lapses constantly
  stops being read as information.

  "a spelling fix leaves the approval standing" below is that case, and it is
  the one an implementation is most likely to get wrong.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.{Drain, Frontier}

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      allow_unregistered?(true)
    end
  end

  # ── the input ───────────────────────────────────────────────────────────────

  defmodule Lines do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :fy, :string, public?: true
      attribute :amount, :float, public?: true
      attribute :label, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :create do
        accept([:key, :fy, :amount, :label])
      end

      update :revise do
        accept([:amount])
      end

      update :relabel do
        accept([:label])
      end
    end

    reactive do
      id(:lines)
      op(:source)
      leaf?(true)
      dirties_on([:create, :update, :destroy])
    end
  end

  # ── SURVIVAL: no `lapse` declared ───────────────────────────────────────────

  # The default. `:approved_at` is absent from `:upsert`'s accept list, so the
  # payload write cannot touch it — and no declaration protects it.
  defmodule Survives do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :fy, :string, public?: true
      attribute :total, :float, public?: true
      attribute :approved_at, :string, public?: true
    end

    identities do
      identity :by_key, [:key], pre_check_with: ReactiveDag.LapseTest.Domain
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        upsert_identity(:by_key)
        accept([:key, :fy, :total])
      end

      update :approve do
        require_atomic? false
        accept([:approved_at])
      end
    end

    reactive do
      id(:survives)
      recompute_by :fy, to: :lines, from: :fy
      reduce into: [sum: [amount: :total]]
    end
  end

  # ── `when_changed: :any` ────────────────────────────────────────────────────

  defmodule LapsesAny do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :fy, :string, public?: true
      attribute :total, :float, public?: true
      attribute :approved_at, :string, public?: true
    end

    identities do
      identity :by_key, [:key], pre_check_with: ReactiveDag.LapseTest.Domain
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        upsert_identity(:by_key)
        accept([:key, :fy, :total])
      end

      update :approve do
        require_atomic? false
        accept([:approved_at])
      end

      # the lapse's own write — deliberately NOT the payload action
      update :lapse do
        require_atomic? false
        accept([:approved_at])
      end
    end

    reactive do
      id(:lapses_any)
      recompute_by :fy, to: :lines, from: :fy
      reduce into: [sum: [amount: :total]]

      lapse :approved_at, when_changed: :any
    end
  end

  # ── `when_changed: [fields]` — the narrow form ──────────────────────────────

  # `:total` is watched, `:label` is not. A relabel moves `:label` (so the
  # recompute is `:changed` and propagates) while `:total` sits still — and the
  # sign-off must survive that.
  defmodule LapsesNarrow do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :fy, :string, public?: true
      attribute :total, :float, public?: true
      attribute :label, :string, public?: true
      attribute :signed_off_by, :string, public?: true
    end

    identities do
      identity :by_key, [:key], pre_check_with: ReactiveDag.LapseTest.Domain
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        upsert_identity(:by_key)
        accept([:key, :fy, :total, :label])
      end

      update :sign_off do
        require_atomic? false
        accept([:signed_off_by])
      end

      update :lapse do
        require_atomic? false
        accept([:signed_off_by])
      end
    end

    reactive do
      id(:lapses_narrow)
      recompute_by :fy, to: :lines, from: :fy

      reduce group_by: [fy: :fy],
             into: fn {fy}, items ->
               %{
                 fy: fy,
                 total: items |> Enum.map(& &1.amount) |> Enum.sum(),
                 # the cosmetic column: moves on a relabel, and is NOT watched
                 label: items |> Enum.map(& &1.label) |> Enum.sort() |> Enum.join(",")
               }
             end

      lapse :signed_off_by, when_changed: [:total]
    end
  end

  # ── child rows ──────────────────────────────────────────────────────────────

  defmodule Correction do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Ets

    ets do
    end

    attributes do
      uuid_primary_key :id
      attribute :fy, :string, public?: true
      attribute :note, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :create do
        accept([:fy, :note])
      end
    end
  end

  defmodule LapsesChildren do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :fy, :string, public?: true
      attribute :total, :float, public?: true
    end

    identities do
      identity :by_key, [:key], pre_check_with: ReactiveDag.LapseTest.Domain
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        upsert_identity(:by_key)
        accept([:key, :fy, :total])
      end
    end

    reactive do
      id(:lapses_children)
      recompute_by :fy, to: :lines, from: :fy
      reduce into: [sum: [amount: :total]]

      lapse ReactiveDag.LapseTest.Correction, key: :fy, when_changed: [:total]
    end
  end

  # ── composite primary key (identity-keyed) ──────────────────────────────────

  defmodule FundLines do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :fund_code, :string, public?: true
      attribute :fy, :string, public?: true
      attribute :amount, :float, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :create do
        accept([:key, :fund_code, :fy, :amount])
      end

      update :revise do
        accept([:amount])
      end
    end

    reactive do
      id(:fund_lines)
      op(:source)
      leaf?(true)
      dirties_on([:create, :update, :destroy])
    end
  end

  # composite PK: the cell key is the identity serialization ("gf|2026")
  defmodule FundRollups do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute :fund, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :fy, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :total, :float, public?: true
      attribute :approved_at, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        accept([:fund, :fy, :total])
      end

      update :approve do
        require_atomic? false
        accept([:approved_at])
      end

      update :lapse do
        require_atomic? false
        accept([:approved_at])
      end
    end

    reactive do
      id(:fund_rollups)
      recompute_by [fund: :fund_code, fy: :fy], to: :fund_lines
      reduce into: [sum: [amount: :total]]

      lapse :approved_at, when_changed: [:total]
    end
  end

  # ── set-grain: one mark over a whole unit ───────────────────────────────────

  # `expand:` fans one unit out to many rows, so the sign-off lives on each of
  # them and every one must be cleared when any member moves.
  defmodule FySignOff do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :fy, :string, public?: true
      attribute :bucket, :string, public?: true
      attribute :total, :float, public?: true
      attribute :fy_approved_at, :string, public?: true
    end

    identities do
      identity :by_key, [:key], pre_check_with: ReactiveDag.LapseTest.Domain
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        upsert_identity(:by_key)
        accept([:key, :fy, :bucket, :total])
      end

      update :approve do
        require_atomic? false
        accept([:fy_approved_at])
      end

      update :lapse do
        require_atomic? false
        accept([:fy_approved_at])
      end
    end

    reactive do
      id(:fy_sign_off)
      recompute_by :fy, to: :lines, from: :fy

      # one unit -> many rows, each self-keyed "<fy>|<bucket>"
      reduce group_by: [fy: :fy],
             expand: fn {fy}, items ->
               items
               |> Enum.group_by(& &1.label)
               |> Enum.map(fn {bucket, rows} ->
                 %{
                   key: "#{fy}|#{bucket}",
                   fy: fy,
                   bucket: bucket,
                   total: rows |> Enum.map(& &1.amount) |> Enum.sum()
                 }
               end)
             end

      lapse :fy_approved_at, when_changed: :any, over: :fy
    end
  end

  # ── the fake frontier repo (same shape as dirties_on_test) ──────────────────

  defmodule FakeRepo do
    def start_link, do: Agent.start_link(fn -> %{} end, name: __MODULE__)

    def query!("INSERT INTO " <> _, params) do
      params
      |> Enum.chunk_every(5)
      |> Enum.each(fn [cell, key, _r, _t, prior] ->
        Agent.update(__MODULE__, fn m -> Map.put_new(m, {cell, key}, prior) end)
      end)

      %{rows: []}
    end

    def query!("SELECT DISTINCT cell_id" <> _, _params) do
      ids = Agent.get(__MODULE__, & &1) |> Map.keys() |> Enum.map(&elem(&1, 0)) |> Enum.uniq()
      %{rows: Enum.map(ids, &[&1])}
    end

    def query!("DELETE FROM " <> _, [cell]) do
      rows =
        Agent.get_and_update(__MODULE__, fn m ->
          {mine, rest} = Enum.split_with(m, fn {{c, _}, _} -> c == cell end)
          {Enum.map(mine, fn {{_c, k}, prior} -> [k, prior] end), Map.new(rest)}
        end)

      %{rows: rows}
    end

    def query!("SELECT COUNT" <> _, _params), do: %{rows: [[Agent.get(__MODULE__, &map_size/1)]]}
    def query!("SELECT pg_try_advisory_lock" <> _, _params), do: %{rows: [[true]]}
    def query!("SELECT pg_advisory_unlock" <> _, _params), do: %{rows: [[true]]}
  end

  @cells ~w(lines survives lapses_any lapses_narrow lapses_children fund_lines
            fund_rollups fy_sign_off)

  setup do
    start_supervised!(%{id: FakeRepo, start: {FakeRepo, :start_link, []}})
    prev_repo = Application.get_env(:reactive_dag, :repo)
    Application.put_env(:reactive_dag, :repo, FakeRepo)

    on_exit(fn -> Application.put_env(:reactive_dag, :repo, prev_repo) end)

    for r <- [
          Lines,
          Survives,
          LapsesAny,
          LapsesNarrow,
          LapsesChildren,
          Correction,
          FundLines,
          FundRollups,
          FySignOff
        ] do
      r |> Ash.read!() |> Enum.each(&Ash.destroy!/1)
    end

    for cell <- @cells, do: Frontier.claim(cell)
    :ok
  end

  defp drain(plan),
    do: Drain.run(plan, recompute: ReactiveDag.Node.Recompute, key_rule: ReactiveDag.Node.KeyRule)

  defp line!(key, fy, amount, label \\ "a") do
    Lines
    |> Ash.Changeset.for_create(:create, %{key: key, fy: fy, amount: amount, label: label})
    |> Ash.create!()
  end

  # ── SURVIVAL — the default, and the regression guard on it ──────────────────

  describe "survival (no `lapse` declared)" do
    test "a human column survives a :changed recompute — no declaration needed" do
      plan = ReactiveDag.Node.graph([Lines, Survives])

      line!("l1", "2026", 100.0)
      {:ok, _} = drain(plan)

      Survives
      |> Ash.get!("2026")
      |> Ash.Changeset.for_update(:approve, %{approved_at: "2026-08-01"})
      |> Ash.update!()

      # the content MOVES — this is a genuinely :changed recompute
      Lines
      |> Ash.get!("l1")
      |> Ash.Changeset.for_update(:revise, %{amount: 250.0})
      |> Ash.update!()

      {:ok, report} = drain(plan)
      steps = Map.new(report.steps, &{&1.cell, &1})
      assert steps["survives"].changed == ["2026"]

      row = Survives |> Ash.get!("2026")
      assert row.total == 250.0

      # THE DEFAULT: the payload action does not accept :approved_at, so nothing
      # in the payload loop can touch it. Break this and lapse has eaten the
      # feature it was built beside.
      assert row.approved_at == "2026-08-01"
    end
  end

  # ── `when_changed: :any` ────────────────────────────────────────────────────

  describe "when_changed: :any" do
    test "the mark is cleared when the computed content moves" do
      plan = ReactiveDag.Node.graph([Lines, LapsesAny])

      line!("l1", "2026", 100.0)
      {:ok, _} = drain(plan)

      LapsesAny
      |> Ash.get!("2026")
      |> Ash.Changeset.for_update(:approve, %{approved_at: "2026-08-01"})
      |> Ash.update!()

      assert (LapsesAny |> Ash.get!("2026")).approved_at == "2026-08-01"

      Lines
      |> Ash.get!("l1")
      |> Ash.Changeset.for_update(:revise, %{amount: 250.0})
      |> Ash.update!()

      {:ok, _} = drain(plan)

      row = LapsesAny |> Ash.get!("2026")
      assert row.total == 250.0
      # the approval was about a total that no longer holds
      assert row.approved_at == nil
    end

    test "an :unchanged recompute leaves the mark standing" do
      plan = ReactiveDag.Node.graph([Lines, LapsesAny])

      line!("l1", "2026", 100.0)
      {:ok, _} = drain(plan)

      LapsesAny
      |> Ash.get!("2026")
      |> Ash.Changeset.for_update(:approve, %{approved_at: "2026-08-01"})
      |> Ash.update!()

      # a write that does not move the content: same amount back again
      Lines
      |> Ash.get!("l1")
      |> Ash.Changeset.for_update(:revise, %{amount: 100.0})
      |> Ash.update!()

      {:ok, report} = drain(plan)
      steps = Map.new(report.steps, &{&1.cell, &1})

      # nothing propagated, and nothing lapsed
      assert steps["lapses_any"].changed == []
      assert (LapsesAny |> Ash.get!("2026")).approved_at == "2026-08-01"
    end

    test ":created never lapses — no prior record means no mark" do
      plan = ReactiveDag.Node.graph([Lines, LapsesAny])

      line!("l1", "2026", 100.0)
      {:ok, report} = drain(plan)

      steps = Map.new(report.steps, &{&1.cell, &1})
      assert steps["lapses_any"].changed == ["2026"]

      # the row was CREATED this pass; there was no prior and no mark, and the
      # lapse path must not have run at all
      assert (LapsesAny |> Ash.get!("2026")).approved_at == nil
    end
  end

  # The `:created` guard with something to LOSE. Nulling an already-null column
  # is invisible, so an attribute lapse cannot show the guard working — a child
  # lapse can. A human may annotate a key BEFORE the derived row first exists
  # (the correction is about the meeting, not about our having computed it yet),
  # and the pass that creates that row must not destroy the annotation: there
  # was no prior content, so nothing the note was about has moved.
  describe ":created (the guard, with something to lose)" do
    test "a create does not destroy child rows that predate the derived row" do
      plan = ReactiveDag.Node.graph([Lines, LapsesChildren])

      # the annotation lands FIRST — before any drain has computed the row
      Correction
      |> Ash.Changeset.for_create(:create, %{fy: "2026", note: "watch this year"})
      |> Ash.create!()

      line!("l1", "2026", 100.0)
      {:ok, report} = drain(plan)

      steps = Map.new(report.steps, &{&1.cell, &1})
      # the row was created this pass
      assert steps["lapses_children"].changed == ["2026"]

      # ...and the note survived it
      assert [%{note: "watch this year"}] = Ash.read!(Correction)
    end
  end

  # ── `when_changed: [fields]` — the narrow form ──────────────────────────────

  describe "when_changed: [fields]" do
    test "a watched field moving clears the mark" do
      plan = ReactiveDag.Node.graph([Lines, LapsesNarrow])

      line!("l1", "2026", 100.0, "water")
      {:ok, _} = drain(plan)

      LapsesNarrow
      |> Ash.get!("2026")
      |> Ash.Changeset.for_update(:sign_off, %{signed_off_by: "tom"})
      |> Ash.update!()

      # :total is watched, and this moves it
      Lines
      |> Ash.get!("l1")
      |> Ash.Changeset.for_update(:revise, %{amount: 250.0})
      |> Ash.update!()

      {:ok, _} = drain(plan)

      row = LapsesNarrow |> Ash.get!("2026")
      assert row.total == 250.0
      assert row.signed_off_by == nil
    end

    # THE MOST IMPORTANT TEST IN THIS FILE.
    #
    # The recompute is `:changed` overall — `:label` moved, so it propagates —
    # while `:total`, the field the sign-off was actually about, sat still. An
    # implementation that reuses the `:changed` verdict instead of running its
    # own narrowed comparison passes every other test here and fails this one.
    test "a spelling fix leaves the approval standing (the lapse asks its OWN question)" do
      plan = ReactiveDag.Node.graph([Lines, LapsesNarrow])

      line!("l1", "2026", 100.0, "watter")
      {:ok, _} = drain(plan)

      LapsesNarrow
      |> Ash.get!("2026")
      |> Ash.Changeset.for_update(:sign_off, %{signed_off_by: "tom"})
      |> Ash.update!()

      # the cosmetic edit: :label moves, :total does not
      Lines
      |> Ash.get!("l1")
      |> Ash.Changeset.for_update(:relabel, %{label: "water"})
      |> Ash.update!()

      {:ok, report} = drain(plan)
      steps = Map.new(report.steps, &{&1.cell, &1})

      # it DID propagate — the two grains are genuinely independent
      assert steps["lapses_narrow"].changed == ["2026"]

      row = LapsesNarrow |> Ash.get!("2026")
      assert row.label == "water"
      assert row.total == 100.0
      # ...and the sign-off survived it
      assert row.signed_off_by == "tom"
    end
  end

  # ── child rows ──────────────────────────────────────────────────────────────

  describe "child resources" do
    test "the rows attached to the lapsing key are destroyed" do
      plan = ReactiveDag.Node.graph([Lines, LapsesChildren])

      line!("l1", "2026", 100.0)
      {:ok, _} = drain(plan)

      Correction
      |> Ash.Changeset.for_create(:create, %{fy: "2026", note: "check the water line"})
      |> Ash.create!()

      assert length(Ash.read!(Correction)) == 1

      Lines
      |> Ash.get!("l1")
      |> Ash.Changeset.for_update(:revise, %{amount: 250.0})
      |> Ash.update!()

      {:ok, _} = drain(plan)

      assert Ash.read!(Correction) == []
    end

    test "MULTIPLE children on one key all go — a lapse is one-to-many" do
      plan = ReactiveDag.Node.graph([Lines, LapsesChildren])

      line!("l1", "2026", 100.0)
      {:ok, _} = drain(plan)

      for n <- 1..3 do
        Correction
        |> Ash.Changeset.for_create(:create, %{fy: "2026", note: "note #{n}"})
        |> Ash.create!()
      end

      assert length(Ash.read!(Correction)) == 3

      Lines
      |> Ash.get!("l1")
      |> Ash.Changeset.for_update(:revise, %{amount: 250.0})
      |> Ash.update!()

      {:ok, _} = drain(plan)

      # `existing/3` reads ONE row; a child lapse must not
      assert Ash.read!(Correction) == []
    end

    test "another key's children are untouched — the filter is the key" do
      plan = ReactiveDag.Node.graph([Lines, LapsesChildren])

      line!("l1", "2026", 100.0)
      line!("l2", "2027", 500.0)
      {:ok, _} = drain(plan)

      for fy <- ["2026", "2027"] do
        Correction
        |> Ash.Changeset.for_create(:create, %{fy: fy, note: "on #{fy}"})
        |> Ash.create!()
      end

      # only 2026 moves
      Lines
      |> Ash.get!("l1")
      |> Ash.Changeset.for_update(:revise, %{amount: 250.0})
      |> Ash.update!()

      {:ok, _} = drain(plan)

      assert [%{fy: "2027"}] = Ash.read!(Correction)
    end

    test "an :unchanged recompute destroys nothing" do
      plan = ReactiveDag.Node.graph([Lines, LapsesChildren])

      line!("l1", "2026", 100.0)
      {:ok, _} = drain(plan)

      Correction
      |> Ash.Changeset.for_create(:create, %{fy: "2026", note: "still true"})
      |> Ash.create!()

      Lines
      |> Ash.get!("l1")
      |> Ash.Changeset.for_update(:revise, %{amount: 100.0})
      |> Ash.update!()

      {:ok, _} = drain(plan)

      assert length(Ash.read!(Correction)) == 1
    end
  end

  # ── composite primary key ───────────────────────────────────────────────────

  describe "identity-keyed nodes (composite primary key)" do
    test "the mark lapses on the identity-keyed row too" do
      plan = ReactiveDag.Node.graph([FundLines, FundRollups])

      FundLines
      |> Ash.Changeset.for_create(:create, %{key: "f1", fund_code: "gf", fy: "2026", amount: 100.0})
      |> Ash.create!()

      {:ok, _} = drain(plan)

      [row] = Ash.read!(FundRollups)
      assert row.total == 100.0

      row
      |> Ash.Changeset.for_update(:approve, %{approved_at: "2026-08-01"})
      |> Ash.update!()

      FundLines
      |> Ash.get!("f1")
      |> Ash.Changeset.for_update(:revise, %{amount: 300.0})
      |> Ash.update!()

      {:ok, _} = drain(plan)

      [row] = Ash.read!(FundRollups)
      assert row.total == 300.0
      assert row.approved_at == nil
    end

    test "an unwatched move leaves the identity-keyed mark standing" do
      plan = ReactiveDag.Node.graph([FundLines, FundRollups])

      FundLines
      |> Ash.Changeset.for_create(:create, %{key: "f1", fund_code: "gf", fy: "2026", amount: 100.0})
      |> Ash.create!()

      {:ok, _} = drain(plan)

      [row] = Ash.read!(FundRollups)

      row
      |> Ash.Changeset.for_update(:approve, %{approved_at: "2026-08-01"})
      |> Ash.update!()

      # revise to the SAME amount: :total does not move
      FundLines
      |> Ash.get!("f1")
      |> Ash.Changeset.for_update(:revise, %{amount: 100.0})
      |> Ash.update!()

      {:ok, _} = drain(plan)

      [row] = Ash.read!(FundRollups)
      assert row.approved_at == "2026-08-01"
    end
  end

  # ── set-grain ───────────────────────────────────────────────────────────────

  describe "set-grain (`over:`)" do
    test "a mark over a unit lapses when ANY member moves" do
      plan = ReactiveDag.Node.graph([Lines, FySignOff])

      line!("l1", "2026", 100.0, "water")
      line!("l2", "2026", 50.0, "sewer")
      {:ok, _} = drain(plan)

      # one unit, two rows
      rows = FySignOff |> Ash.read!() |> Enum.filter(&(&1.fy == "2026"))
      assert length(rows) == 2

      # the sign-off covers the YEAR, so it sits on every row of it
      for row <- rows do
        row
        |> Ash.Changeset.for_update(:approve, %{fy_approved_at: "2026-08-01"})
        |> Ash.update!()
      end

      # ONE member moves
      Lines
      |> Ash.get!("l1")
      |> Ash.Changeset.for_update(:revise, %{amount: 400.0})
      |> Ash.update!()

      {:ok, _} = drain(plan)

      rows = FySignOff |> Ash.read!() |> Enum.filter(&(&1.fy == "2026"))
      assert length(rows) == 2

      # every row of the unit lapsed, including the one whose own total sat
      # still — you approved a year, and the year no longer holds
      assert Enum.all?(rows, &is_nil(&1.fy_approved_at))
    end

    test "another unit's sign-off is untouched" do
      plan = ReactiveDag.Node.graph([Lines, FySignOff])

      line!("l1", "2026", 100.0, "water")
      line!("l2", "2027", 50.0, "water")
      {:ok, _} = drain(plan)

      for row <- Ash.read!(FySignOff) do
        row
        |> Ash.Changeset.for_update(:approve, %{fy_approved_at: "signed"})
        |> Ash.update!()
      end

      Lines
      |> Ash.get!("l1")
      |> Ash.Changeset.for_update(:revise, %{amount: 400.0})
      |> Ash.update!()

      {:ok, _} = drain(plan)

      by_fy = FySignOff |> Ash.read!() |> Map.new(&{&1.fy, &1.fy_approved_at})
      assert by_fy["2026"] == nil
      assert by_fy["2027"] == "signed"
    end
  end

  # ── compile-time / assembly refusals ────────────────────────────────────────

  describe "refusals" do
    # `over:` is checkable against this node's OWN `recompute_by`, so it is a
    # COMPILE-TIME refusal rather than an assembly one. The verifier is called
    # directly (the house pattern): Spark runs verifiers from `@after_verify`,
    # which raises at a point `assert_raise` around `defmodule` cannot catch.
    test "`over:` naming a unit `recompute_by` does not declare is refused at compile time" do
      defmodule BadOver do
        use Ash.Resource,
          domain: ReactiveDag.LapseTest.Domain,
          data_layer: Ash.DataLayer.Ets,
          extensions: [ReactiveDag.Node]

        ets do
        end

        attributes do
          attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
          attribute :fy, :string, public?: true
          attribute :total, :float, public?: true
          attribute :approved_at, :string, public?: true
        end

        actions do
          defaults [:read, :destroy]

          create :upsert do
            upsert?(true)
            accept([:key, :fy, :total])
          end

          update :lapse do
            require_atomic? false
            accept([:approved_at])
          end
        end

        reactive do
          id(:bad_over)
          recompute_by :fy, to: :lines, from: :fy
          reduce into: [sum: [amount: :total]]

          # :quarter is not the unit this node recomputes by
          lapse :approved_at, when_changed: :any, over: :quarter
        end
      end

      assert {:error, %Spark.Error.DslError{message: msg}} =
               ReactiveDag.Node.Verifiers.VerifyReactive.verify(BadOver.spark_dsl_config())

      assert msg =~ "over: :quarter"
      assert msg =~ "recompute_by"
      # names what the node ACTUALLY declares, so the fix is in the message
      assert msg =~ "[:fy]"
    end

    test "a child resource with no destroy action raises at assembly" do
      defmodule Undestroyable do
        use Ash.Resource, domain: ReactiveDag.LapseTest.Domain, data_layer: Ash.DataLayer.Ets

        ets do
        end

        attributes do
          uuid_primary_key :id
          attribute :fy, :string, public?: true
        end

        actions do
          defaults [:read]

          create :create do
            accept([:fy])
          end
        end
      end

      defmodule BadChild do
        use Ash.Resource,
          domain: ReactiveDag.LapseTest.Domain,
          data_layer: Ash.DataLayer.Ets,
          extensions: [ReactiveDag.Node]

        ets do
        end

        attributes do
          attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
          attribute :fy, :string, public?: true
          attribute :total, :float, public?: true
        end

        actions do
          defaults [:read, :destroy]

          create :upsert do
            upsert?(true)
            accept([:key, :fy, :total])
          end
        end

        reactive do
          id(:bad_child)
          recompute_by :fy, to: :lines, from: :fy
          reduce into: [sum: [amount: :total]]

          lapse ReactiveDag.LapseTest.Undestroyable, key: :fy, when_changed: :any
        end
      end

      err =
        assert_raise ArgumentError, fn ->
          ReactiveDag.Node.graph([BadChild])
        end

      assert err.message =~ "needs a :destroy action"
    end

    test "`lapse` on an attribute that doesn't exist raises at assembly" do
      defmodule BadAttr do
        use Ash.Resource,
          domain: ReactiveDag.LapseTest.Domain,
          data_layer: Ash.DataLayer.Ets,
          extensions: [ReactiveDag.Node]

        ets do
        end

        attributes do
          attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
          attribute :fy, :string, public?: true
          attribute :total, :float, public?: true
        end

        actions do
          defaults [:read, :destroy]

          create :upsert do
            upsert?(true)
            accept([:key, :fy, :total])
          end
        end

        reactive do
          id(:bad_attr)
          recompute_by :fy, to: :lines, from: :fy
          reduce into: [sum: [amount: :total]]

          # neither an attribute of this resource nor a resource
          lapse :approved_at, when_changed: :any
        end
      end

      err =
        assert_raise ArgumentError, fn ->
          ReactiveDag.Node.graph([BadAttr])
        end

      assert err.message =~ "names neither an attribute of this resource nor an Ash resource"
    end

    test "a lapse action that does not accept the column raises at assembly" do
      defmodule UnacceptingLapse do
        use Ash.Resource,
          domain: ReactiveDag.LapseTest.Domain,
          data_layer: Ash.DataLayer.Ets,
          extensions: [ReactiveDag.Node]

        ets do
        end

        attributes do
          attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
          attribute :fy, :string, public?: true
          attribute :total, :float, public?: true
          attribute :approved_at, :string, public?: true
        end

        actions do
          defaults [:read, :destroy]

          create :upsert do
            upsert?(true)
            accept([:key, :fy, :total])
          end

          # accepts the WRONG column: the write would succeed and clear nothing
          update :lapse do
            require_atomic? false
            accept([:total])
          end
        end

        reactive do
          id(:unaccepting_lapse)
          recompute_by :fy, to: :lines, from: :fy
          reduce into: [sum: [amount: :total]]

          lapse :approved_at, when_changed: :any
        end
      end

      err =
        assert_raise ArgumentError, fn ->
          ReactiveDag.Node.graph([UnacceptingLapse])
        end

      assert err.message =~ "does not accept :approved_at"
    end

    test "`when_changed:` naming a field the payload never carries raises at assembly" do
      defmodule BadWatch do
        use Ash.Resource,
          domain: ReactiveDag.LapseTest.Domain,
          data_layer: Ash.DataLayer.Ets,
          extensions: [ReactiveDag.Node]

        ets do
        end

        attributes do
          attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
          attribute :fy, :string, public?: true
          attribute :total, :float, public?: true
          attribute :approved_at, :string, public?: true
        end

        actions do
          defaults [:read, :destroy]

          create :upsert do
            upsert?(true)
            accept([:key, :fy, :total])
          end

          update :lapse do
            require_atomic? false
            accept([:approved_at])
          end
        end

        reactive do
          id(:bad_watch)
          recompute_by :fy, to: :lines, from: :fy
          reduce into: [sum: [amount: :total]]

          # :vote_count is not an attribute here, so this could never fire
          lapse :approved_at, when_changed: [:total, :vote_count]
        end
      end

      err =
        assert_raise ArgumentError, fn ->
          ReactiveDag.Node.graph([BadWatch])
        end

      assert err.message =~ "[:vote_count]"
    end
  end
end

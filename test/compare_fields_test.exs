defmodule ReactiveDag.CompareFieldsTest do
  @moduledoc """
  `compare` — which of a node's columns constitute its RESULT.

  `Payload.upsert` compares every writable attribute, which is right when every
  attribute is part of the answer. A derived row often is not shaped that way: it
  carries fields that are part of the RECORD but not part of the RESULT —
  `doc_id` (which document this came from), `ordinal` (position in the source
  document), a `match_key` a downstream join builds.

  `ordinal` is the sharp one. A re-parse that reorders rows moves every single
  `ordinal` without anything having actually changed. Comparing them reports a
  change nothing made, and one spurious change re-runs every fold downstream —
  the cost that makes a cascade O(graph) instead of O(real changes).

  Before this, a host could only compare all-fields or a stored `:fingerprint`
  digest, and a digest of columns already on the row earns nothing when the
  comparison can just read them. So ten ops in one host hand-rolled the same
  private `changed?/1` over the same `@compared` module attribute.

  The property under test: **a node declaring `compare: [:a, :b]` reports a
  change when `:a` or `:b` move, and only then — every other column on the row
  is written but is not news.**

  `fingerprint` stays the answer for a LEAF, where the fields that move are the
  ones you must NOT compare and the honest witness is a hash of the ones you
  must; it wins when both are declared, which "precedence" below pins.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.Node.{Payload, Recompute, Rows}
  alias ReactiveDag.Node.Verifiers.VerifyReactive

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      allow_unregistered?(true)
    end
  end

  # the core shape: :a and :b are the result, :c rides along on the record
  defmodule Narrowed do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Ets, extensions: [ReactiveDag.Node]

    ets do
      private?(true)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :a, :string, public?: true
      attribute :b, :string, public?: true
      attribute :c, :string, public?: true
    end

    identities do
      identity :by_key, [:key], pre_check_with: ReactiveDag.CompareFieldsTest.Domain
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        upsert_identity(:by_key)
        accept([:key, :a, :b, :c])
      end
    end

    reactive do
      id(:narrowed)
      leaf?(true)
      compare([:a, :b])
    end
  end

  # the same row shape with NO `compare` — the default, which every existing
  # node relies on: every field the row carries is compared.
  defmodule Everything do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Ets, extensions: [ReactiveDag.Node]

    ets do
      private?(true)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :a, :string, public?: true
      attribute :b, :string, public?: true
      attribute :c, :string, public?: true
    end

    identities do
      identity :by_key, [:key], pre_check_with: ReactiveDag.CompareFieldsTest.Domain
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        upsert_identity(:by_key)
        accept([:key, :a, :b, :c])
      end
    end

    reactive do
      id(:everything)
      leaf?(true)
    end
  end

  # BOTH declared. The doc says the fingerprint wins; this resource exists to
  # pin it, so `compare` cannot quietly start overriding it.
  defmodule Both do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Ets, extensions: [ReactiveDag.Node]

    ets do
      private?(true)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :a, :string, public?: true
      attribute :b, :string, public?: true
      attribute :fingerprint, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        accept([:key, :a, :b, :fingerprint])
      end
    end

    reactive do
      id(:both)
      leaf?(true)
      fingerprint([:a])
      compare([:b])
    end
  end

  # a COMPOSITE-PK node: no key column, its cell key is the identity's "|"
  # serialization. It goes through `upsert_identity`, which shares `moved?/3`,
  # so `compare` must narrow there identically.
  defmodule Rollups do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Ets, extensions: [ReactiveDag.Node]

    ets do
      private?(true)
    end

    attributes do
      attribute :fund, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :fy, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :total, :float, public?: true
      attribute :ordinal, :integer, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        accept([:fund, :fy, :total, :ordinal])
      end
    end

    reactive do
      id(:rollups)
      leaf?(true)
      compare([:total])
    end
  end

  # ── the realistic host shape, through the DSL ────────────────────────────
  #
  # THE CASE THE FEATURE EXISTS FOR. Extracted line items: the amount is the
  # result; `doc_id` is provenance and `ordinal` is position in the source
  # document. A re-parse that reorders the document moves every `ordinal`
  # without changing a single amount.

  defmodule ExtractedLine do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Ets, extensions: [ReactiveDag.Node]

    ets do
      private?(true)
    end

    attributes do
      attribute :id, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :account, :string, public?: true
      attribute :amount, :float, public?: true
      attribute :doc_id, :string, public?: true
      attribute :ordinal, :integer, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        accept([:id, :account, :amount, :doc_id, :ordinal])
      end
    end

    reactive do
      id(:extracted_lines)
      op(:source)
      leaf?(true)
    end
  end

  # the derived node: one row per account, folded from the extracted lines.
  # Its `into` carries the provenance and position of the FIRST line it saw —
  # part of the record, useless as a change signal.
  defmodule AccountTotal do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Ets, extensions: [ReactiveDag.Node]

    ets do
      private?(true)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :account, :string, public?: true
      attribute :total, :float, public?: true
      attribute :doc_id, :string, public?: true
      attribute :ordinal, :integer, public?: true
    end

    identities do
      identity :by_key, [:key], pre_check_with: ReactiveDag.CompareFieldsTest.Domain
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        upsert_identity(:by_key)
        accept([:key, :account, :total, :doc_id, :ordinal])
      end
    end

    reactive do
      id(:account_totals)
      op(:fold)
      key_rule(:all)

      # `account` and `total` ARE the answer; doc_id/ordinal are the record.
      compare([:account, :total])

      reduce over: :extracted_lines,
             group_by: &ReactiveDag.CompareFieldsTest.group/1,
             key: &ReactiveDag.CompareFieldsTest.key/1,
             into: &ReactiveDag.CompareFieldsTest.into/2
    end
  end

  # the same fold with no `compare` — the regression guard's DSL twin: a
  # reorder DOES propagate here, which is the behaviour being fixed.
  defmodule AccountTotalWide do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Ets, extensions: [ReactiveDag.Node]

    ets do
      private?(true)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :account, :string, public?: true
      attribute :total, :float, public?: true
      attribute :doc_id, :string, public?: true
      attribute :ordinal, :integer, public?: true
    end

    identities do
      identity :by_key, [:key], pre_check_with: ReactiveDag.CompareFieldsTest.Domain
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        upsert_identity(:by_key)
        accept([:key, :account, :total, :doc_id, :ordinal])
      end
    end

    reactive do
      id(:account_totals_wide)
      op(:fold)
      key_rule(:all)

      reduce over: :extracted_lines,
             group_by: &ReactiveDag.CompareFieldsTest.group/1,
             key: &ReactiveDag.CompareFieldsTest.key/1,
             into: &ReactiveDag.CompareFieldsTest.into/2
    end
  end

  def group(line), do: line.account
  def key(account), do: account

  def into(account, lines) do
    first = Enum.min_by(lines, & &1.ordinal)

    %{
      key: account,
      account: account,
      total: lines |> Enum.map(& &1.amount) |> Enum.sum(),
      doc_id: first.doc_id,
      ordinal: first.ordinal
    }
  end

  # ── an AGGREGATE node: the third threading site ──────────────────────────
  #
  # `Recompute.Aggregate` writes through its own `lapse_opts/1`, a different
  # line from the reduce/join path's.
  #
  # Note what it takes to make `compare` load-bearing here. The aggregate's
  # `project/3` builds the payload from the key column plus each aggregate's
  # `dest` and NOTHING else — a column merely sitting on the resource never
  # reaches `attrs`, so narrowing past it would be a no-op. The narrowing only
  # bites when the node declares TWO aggregates and only one is the result:
  # `avg_flow` is the answer, `reading_count` is bookkeeping about the source
  # (how many rows the parse happened to emit), and a re-parse that splits one
  # reading into two without moving the average must not propagate.
  defmodule Reading do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Ets

    ets do
      private?(true)
    end

    attributes do
      attribute :id, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :pm_key, :string, public?: true
      attribute :flow, :float, public?: true
    end

    actions do
      defaults [:read, :destroy]
      create :create do accept([:id, :pm_key, :flow]) end
    end
  end

  defmodule PlantMonth do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Ets, extensions: [ReactiveDag.Node]

    ets do
      private?(true)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :avg_flow, :float, public?: true
      # bookkeeping: how many rows the source happened to emit
      attribute :reading_count, :integer, public?: true
    end

    identities do
      identity :by_key, [:key], pre_check_with: ReactiveDag.CompareFieldsTest.Domain
    end

    relationships do
      has_many :readings, ReactiveDag.CompareFieldsTest.Reading do
        source_attribute :key
        destination_attribute :pm_key
      end
    end

    actions do
      defaults [:read, :destroy]
      create :seed do accept([:key]) end

      create :upsert do
        upsert?(true)
        upsert_identity(:by_key)
        accept([:key, :avg_flow, :reading_count])
      end
    end

    reactive do
      id(:plant_months)
      op(:fold)
      key_rule(:all)
      compare([:avg_flow])

      aggregate over: :readings, avg: [flow: :avg_flow], count: :reading_count
    end
  end

  # the same aggregate with NO `compare` — the count moving DOES propagate here
  defmodule PlantMonthWide do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Ets, extensions: [ReactiveDag.Node]

    ets do
      private?(true)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :avg_flow, :float, public?: true
      attribute :reading_count, :integer, public?: true
    end

    identities do
      identity :by_key, [:key], pre_check_with: ReactiveDag.CompareFieldsTest.Domain
    end

    relationships do
      has_many :readings, ReactiveDag.CompareFieldsTest.Reading do
        source_attribute :key
        destination_attribute :pm_key
      end
    end

    actions do
      defaults [:read, :destroy]
      create :seed do accept([:key]) end

      create :upsert do
        upsert?(true)
        upsert_identity(:by_key)
        accept([:key, :avg_flow, :reading_count])
      end
    end

    reactive do
      id(:plant_months_wide)
      op(:fold)
      key_rule(:all)

      aggregate over: :readings, avg: [flow: :avg_flow], count: :reading_count
    end
  end

  setup do
    for r <- [
          Narrowed,
          Everything,
          Both,
          Rollups,
          ExtractedLine,
          AccountTotal,
          AccountTotalWide,
          PlantMonth,
          PlantMonthWide,
          Reading
        ],
        row <- Ash.read!(r),
        do: Ash.destroy!(row)

    :ok
  end

  defp cell(mod) do
    [c] = ReactiveDag.Node.cells(mod)
    c
  end

  # ── the core property ────────────────────────────────────────────────────

  describe "the core property: only the declared columns are the result" do
    test "moving a column OUTSIDE `compare` is not a change" do
      row = fn c -> %{key: "k1", a: "1", b: "2", c: c} end
      opts = [compare: [:a, :b]]

      assert Payload.upsert(Narrowed, :key, "k1", row.("first"), :upsert, opts) == :created
      assert Payload.upsert(Narrowed, :key, "k1", row.("MOVED"), :upsert, opts) == :unchanged

      # ...and it WAS written — the row is current, it just isn't news
      assert [%{c: "MOVED"}] = Ash.read!(Narrowed)
    end

    test "moving a column INSIDE `compare` IS a change" do
      opts = [compare: [:a, :b]]

      assert Payload.upsert(Narrowed, :key, "k1", %{key: "k1", a: "1", b: "2", c: "x"}, :upsert, opts) ==
               :created

      assert Payload.upsert(Narrowed, :key, "k1", %{key: "k1", a: "MOVED", b: "2", c: "x"}, :upsert, opts) ==
               :changed
    end

    test "every declared column counts, not just the first" do
      opts = [compare: [:a, :b]]

      assert Payload.upsert(Narrowed, :key, "k1", %{key: "k1", a: "1", b: "2"}, :upsert, opts) == :created
      assert Payload.upsert(Narrowed, :key, "k1", %{key: "k1", a: "1", b: "MOVED"}, :upsert, opts) == :changed
    end

    test "a declared column moving to nil is still a change" do
      # narrowing must not turn into "ignore falsy" — an amount that vanished is
      # the loudest news a derived row has.
      opts = [compare: [:a, :b]]

      assert Payload.upsert(Narrowed, :key, "k1", %{key: "k1", a: "1", b: "2"}, :upsert, opts) == :created
      assert Payload.upsert(Narrowed, :key, "k1", %{key: "k1", a: nil, b: "2"}, :upsert, opts) == :changed
    end

    test "the DSL slot reaches the cell's meta" do
      assert cell(Narrowed).meta[:compare] == [:a, :b]
      refute cell(Everything).meta[:compare]
    end
  end

  describe "no `compare` — the default every existing node relies on" do
    test "every field the row carries is compared" do
      row = fn c -> %{key: "k1", a: "1", b: "2", c: c} end

      assert Payload.upsert(Everything, :key, "k1", row.("first")) == :created
      # the very move `Narrowed` calls :unchanged
      assert Payload.upsert(Everything, :key, "k1", row.("MOVED")) == :changed
    end

    test "an identical re-write is still unchanged" do
      row = %{key: "k1", a: "1", b: "2", c: "x"}

      assert Payload.upsert(Everything, :key, "k1", row) == :created
      assert Payload.upsert(Everything, :key, "k1", row) == :unchanged
    end
  end

  describe "precedence: `fingerprint` wins when both are declared" do
    test "a field inside `compare` but outside the fingerprint does not fire" do
      # fingerprint [:a], compare [:b] — :b moves. If `compare` won, this would
      # be :changed. The doc says the fingerprint wins, so it is not.
      opts = [fingerprint: [:a], compare: [:b]]

      assert Payload.upsert(Both, :key, "k1", %{key: "k1", a: "1", b: "2"}, :upsert, opts) == :created
      assert Payload.upsert(Both, :key, "k1", %{key: "k1", a: "1", b: "MOVED"}, :upsert, opts) == :unchanged
    end

    test "the fingerprinted field still fires" do
      opts = [fingerprint: [:a], compare: [:b]]

      assert Payload.upsert(Both, :key, "k1", %{key: "k1", a: "1", b: "2"}, :upsert, opts) == :created
      assert Payload.upsert(Both, :key, "k1", %{key: "k1", a: "MOVED", b: "2"}, :upsert, opts) == :changed
    end

    test "a nil digest falls through to `compare`, not to comparing everything" do
      # `fingerprint_attr/2` yields nil when the fn cannot determine a value. The
      # fallback then reads the next answer down, which is `compare` when one is
      # declared — narrowed, as declared, rather than silently widened back out.
      opts = [fingerprint: fn _row -> nil end, compare: [:b]]

      assert Payload.upsert(Both, :key, "k1", %{key: "k1", a: "1", b: "2"}, :upsert, opts) == :created

      # :a moved and is outside `compare`; with no fingerprint to consult, the
      # narrowed comparison is what answers.
      assert Payload.upsert(Both, :key, "k1", %{key: "k1", a: "MOVED", b: "2"}, :upsert, opts) ==
               :unchanged
    end

    test "both slots reach meta" do
      assert cell(Both).meta[:fingerprint] == [:a]
      assert cell(Both).meta[:compare] == [:b]
    end
  end

  describe "upsert_identity — a composite-PK node shares moved?/3" do
    test "a column outside `compare` moving is not a change" do
      row = fn ord -> %{fund: "gf", fy: "2025", total: 10.0, ordinal: ord} end
      opts = [compare: [:total]]

      assert Payload.upsert_identity(Rollups, [:fund, :fy], row.(1), :upsert, opts) == :created
      assert Payload.upsert_identity(Rollups, [:fund, :fy], row.(99), :upsert, opts) == :unchanged

      assert [%{ordinal: 99}] = Ash.read!(Rollups)
    end

    test "the compared column moving IS a change" do
      opts = [compare: [:total]]

      assert Payload.upsert_identity(Rollups, [:fund, :fy], %{fund: "gf", fy: "2025", total: 10.0}, :upsert, opts) ==
               :created

      assert Payload.upsert_identity(Rollups, [:fund, :fy], %{fund: "gf", fy: "2025", total: 11.0}, :upsert, opts) ==
               :changed
    end

    test "without `compare` the same bookkeeping move DOES report a change" do
      row = fn ord -> %{fund: "water", fy: "2026", total: 10.0, ordinal: ord} end

      assert Payload.upsert_identity(Rollups, [:fund, :fy], row.(1)) == :created
      assert Payload.upsert_identity(Rollups, [:fund, :fy], row.(99)) == :changed
    end

    test "narrowing does not merge distinct identities" do
      # `Map.take` narrows the COMPARISON, never the identity lookup: two rows
      # with the same total under different identities are two rows.
      opts = [compare: [:total]]

      assert Payload.upsert_identity(Rollups, [:fund, :fy], %{fund: "gf", fy: "2025", total: 10.0}, :upsert, opts) ==
               :created

      assert Payload.upsert_identity(Rollups, [:fund, :fy], %{fund: "gf", fy: "2026", total: 10.0}, :upsert, opts) ==
               :created

      assert length(Ash.read!(Rollups)) == 2
    end
  end

  # ── through the DSL ──────────────────────────────────────────────────────

  describe "through the DSL: a re-parse that only reorders the document" do
    # THE CASE THE FEATURE EXISTS FOR. Same amounts, same accounts, every
    # `ordinal` moved because the source document was re-parsed in a different
    # order. Nothing about the result changed, so nothing may propagate.
    #
    # This goes through a real `reactive` block → `Node.graph/1` →
    # `Recompute.recompute/2`, so the meta stamping and the reduce/join
    # threading site (`recompute.ex` `writer_fn/2`) are what answer.
    defp lines(order) do
      for row <- Ash.read!(ExtractedLine), do: Ash.destroy!(row)

      for {{id, account, amount}, ordinal} <- Enum.with_index(order, 1) do
        ExtractedLine
        |> Ash.Changeset.for_create(:upsert, %{
          id: id,
          account: account,
          amount: amount,
          doc_id: "doc-1",
          ordinal: ordinal
        })
        |> Ash.create!()
      end
    end

    @forward [{"l1", "4000", 10.0}, {"l2", "4000", 5.0}, {"l3", "5000", 7.0}]
    @reordered [{"l3", "5000", 7.0}, {"l2", "4000", 5.0}, {"l1", "4000", 10.0}]

    defp fold(mod) do
      graph = ReactiveDag.Node.graph([ExtractedLine, mod])
      cell = graph.cells[to_string(ReactiveDag.Node.cells(mod) |> hd() |> Map.fetch!(:id))]
      {:ok, changed} = Recompute.recompute(cell, ["*"])
      changed
    end

    test "a reorder propagates nothing — the ordinals moved, the result did not" do
      lines(@forward)
      assert Enum.sort(fold(AccountTotal)) == ["4000", "5000"]

      # the same document, re-parsed in a different order: every ordinal shifts
      lines(@reordered)

      assert fold(AccountTotal) == [],
             "a re-parse that reordered the source must not re-run every fold downstream"

      # ...and the reordered provenance WAS written — the row is current
      rows = AccountTotal |> Ash.read!() |> Map.new(&{&1.key, &1})
      assert rows["4000"].ordinal == 2
      assert rows["5000"].ordinal == 1
      assert rows["4000"].total == 15.0
    end

    test "the same reorder DOES propagate on a node without `compare`" do
      # the behaviour being fixed, pinned so the fix is visibly load-bearing
      lines(@forward)
      assert Enum.sort(fold(AccountTotalWide)) == ["4000", "5000"]

      lines(@reordered)
      assert Enum.sort(fold(AccountTotalWide)) == ["4000", "5000"]
    end

    test "an amount that actually moved still propagates, and only its key" do
      lines(@forward)
      assert Enum.sort(fold(AccountTotal)) == ["4000", "5000"]

      lines([{"l1", "4000", 11.0}, {"l2", "4000", 5.0}, {"l3", "5000", 7.0}])

      assert fold(AccountTotal) == ["4000"]
    end

    test "an identical re-run is still a no-op" do
      lines(@forward)
      assert Enum.sort(fold(AccountTotal)) == ["4000", "5000"]
      assert fold(AccountTotal) == []
    end

    test "the meta stamping survives graph assembly, not just to_cell" do
      graph = ReactiveDag.Node.graph([ExtractedLine, AccountTotal])
      assert graph.cells["account_totals"].meta[:compare] == [:account, :total]
      refute graph.cells["extracted_lines"].meta[:compare]
    end
  end

  describe "through the DSL: the leaf reconcile path (rows.ex)" do
    # A different threading site from the fold above: `Rows.reconcile` builds its
    # own opts. A leaf whose row-returning `upsert:` moves only a bookkeeping
    # column reports no changed keys.
    test "a reconcile whose row moves only an uncompared column reports no keys" do
      cell = cell(Narrowed)
      row = fn c -> %{key: "k1", a: "1", b: "2", c: c} end

      {:ok, changed, _} = Rows.reconcile(cell, ["k1"], upsert: fn _ -> row.("first") end)
      assert changed == ["k1"]

      {:ok, changed, _} = Rows.reconcile(cell, ["k1"], upsert: fn _ -> row.("MOVED") end)
      assert changed == []

      assert [%{c: "MOVED"}] = Ash.read!(Narrowed)
    end

    test "a compared column moving through the same path DOES report the key" do
      cell = cell(Narrowed)

      {:ok, _, _} = Rows.reconcile(cell, ["k1"], upsert: fn _ -> %{key: "k1", a: "1", b: "2"} end)

      {:ok, changed, _} = Rows.reconcile(cell, ["k1"], upsert: fn _ -> %{key: "k1", a: "MOVED", b: "2"} end)

      assert changed == ["k1"]
    end

    test "the same reconcile on a node without `compare` reports the key" do
      cell = cell(Everything)
      row = fn c -> %{key: "k1", a: "1", b: "2", c: c} end

      {:ok, _, _} = Rows.reconcile(cell, ["k1"], upsert: fn _ -> row.("first") end)
      {:ok, changed, _} = Rows.reconcile(cell, ["k1"], upsert: fn _ -> row.("MOVED") end)

      assert changed == ["k1"]
    end
  end

  describe "through the DSL: the aggregate path (recompute/aggregate.ex)" do
    # The third threading site, with its own `lapse_opts/1`.
    #
    # The move under test: SPLITTING a reading in two without changing the mean.
    # `north` goes from [1.0, 3.0] to [1.0, 3.0, 3.0, 1.0] — same average (2.0),
    # `reading_count` 2 → 4. The count is bookkeeping about how the source was
    # parsed; the average is the result. Only the average may propagate.
    defp seed_groups(mod) do
      for k <- ["north|2024-01", "south|2024-01"],
          do: mod |> Ash.Changeset.for_create(:seed, %{key: k}) |> Ash.create!()
    end

    defp readings(rows) do
      for row <- Ash.read!(Reading), do: Ash.destroy!(row)

      for {id, pm, f} <- rows,
          do: Reading |> Ash.Changeset.for_create(:create, %{id: id, pm_key: pm, flow: f}) |> Ash.create!()
    end

    @two [{"a", "north|2024-01", 1.0}, {"b", "north|2024-01", 3.0}, {"c", "south|2024-01", 2.0}]
    @split [
      {"a", "north|2024-01", 1.0},
      {"b", "north|2024-01", 3.0},
      {"a2", "north|2024-01", 3.0},
      {"b2", "north|2024-01", 1.0},
      {"c", "south|2024-01", 2.0}
    ]

    test "a split that moves only the count reports nothing" do
      seed_groups(PlantMonth)
      readings(@two)
      cell = cell(PlantMonth)

      {:ok, changed} = Recompute.recompute(cell, ["*"])
      assert Enum.sort(changed) == ["north|2024-01", "south|2024-01"]

      rows = PlantMonth |> Ash.read!() |> Map.new(&{&1.key, &1})
      assert rows["north|2024-01"].avg_flow == 2.0
      assert rows["north|2024-01"].reading_count == 2

      # the re-parse: same mean, twice the rows
      readings(@split)

      {:ok, changed} = Recompute.recompute(cell, ["*"])

      assert changed == [],
             "the count is bookkeeping about the parse; the average did not move"

      # ...and the new count WAS written — the row is current, it just isn't news
      rows = PlantMonth |> Ash.read!() |> Map.new(&{&1.key, &1})
      assert rows["north|2024-01"].reading_count == 4
      assert rows["north|2024-01"].avg_flow == 2.0
    end

    test "the same split DOES propagate on an aggregate without `compare`" do
      # the behaviour being fixed, on this path too
      seed_groups(PlantMonthWide)
      readings(@two)
      cell = cell(PlantMonthWide)

      {:ok, changed} = Recompute.recompute(cell, ["*"])
      assert Enum.sort(changed) == ["north|2024-01", "south|2024-01"]

      readings(@split)

      {:ok, changed} = Recompute.recompute(cell, ["*"])
      assert changed == ["north|2024-01"]
    end

    test "a moved average still propagates, and only its group" do
      seed_groups(PlantMonth)
      readings(@two)
      cell = cell(PlantMonth)
      {:ok, _} = Recompute.recompute(cell, ["*"])

      readings(@two ++ [{"d", "south|2024-01", 8.0}])

      {:ok, changed} = Recompute.recompute(cell, ["*"])
      assert changed == ["south|2024-01"]
    end

    test "an identical re-run is still a no-op" do
      seed_groups(PlantMonth)
      readings(@two)
      cell = cell(PlantMonth)

      {:ok, _} = Recompute.recompute(cell, ["*"])
      {:ok, second} = Recompute.recompute(cell, ["*"])
      assert second == []
    end

    test "the aggregate node's meta carries `compare`" do
      assert cell(PlantMonth).meta[:compare] == [:avg_flow]
      refute cell(PlantMonthWide).meta[:compare]
    end
  end

  # ── the verifier ─────────────────────────────────────────────────────────
  #
  # Runtime-defined modules report diagnostics rather than raising, so these
  # call `VerifyReactive.verify/1` on the DSL config directly — the same idiom
  # as `verifier_covers_both_forms_test.exs`.

  describe "the verifier" do
    test "`compare []` is refused — a node that can never report a change" do
      defmodule ComparesNothing do
        use Ash.Resource, domain: ReactiveDag.CompareFieldsTest.Domain, data_layer: Ash.DataLayer.Ets, extensions: [ReactiveDag.Node]

        ets do
          private?(true)
        end

        attributes do
          attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
          attribute :a, :string, public?: true
        end

        actions do
          defaults [:read]
        end

        reactive do
          id(:compares_nothing)
          leaf?(true)
          compare([])
        end
      end

      assert {:error, %Spark.Error.DslError{message: msg}} =
               VerifyReactive.verify(ComparesNothing.spark_dsl_config())

      assert msg =~ "compares no columns"
      assert msg =~ "stale forever"
      # the fix is in the message
      assert msg =~ "omit `compare`"
    end

    test "a name that is not an attribute is refused, and the message names it" do
      defmodule ComparesGhost do
        use Ash.Resource, domain: ReactiveDag.CompareFieldsTest.Domain, data_layer: Ash.DataLayer.Ets, extensions: [ReactiveDag.Node]

        ets do
          private?(true)
        end

        attributes do
          attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
          attribute :amount, :float, public?: true
        end

        actions do
          defaults [:read]
        end

        reactive do
          id(:compares_ghost)
          leaf?(true)
          # :ammount is a typo; `Map.take` would silently omit it, narrowing the
          # comparison further than the declaration reads
          compare([:amount, :ammount])
        end
      end

      assert {:error, %Spark.Error.DslError{message: msg}} =
               VerifyReactive.verify(ComparesGhost.spark_dsl_config())

      assert msg =~ ":ammount", "the message must name the offending field"
      refute msg =~ "[:amount, :ammount]", "only the missing name is the offender"
      assert msg =~ "no attribute"
      # ...and it lists what IS declared, so the fix needs no second lookup
      assert msg =~ "amount"
    end

    test "a valid `compare` passes" do
      assert :ok = VerifyReactive.verify(Narrowed.spark_dsl_config())
      assert :ok = VerifyReactive.verify(AccountTotal.spark_dsl_config())
      assert :ok = VerifyReactive.verify(Rollups.spark_dsl_config())
    end

    test "declaring no `compare` is untouched by the check" do
      assert :ok = VerifyReactive.verify(Everything.spark_dsl_config())
    end
  end
end

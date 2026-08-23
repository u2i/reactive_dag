defmodule ReactiveDag.AshTenancyTest do
  @moduledoc """
  When a node's resource declares Ash `multitenancy`, a tenanted plan's writes
  and reads honour it.

  Deliberately no new DSL. A host says `multitenancy do strategy :attribute end`
  once, in Ash's own vocabulary, and the library sets the tenant on the changeset
  and the query — Ash itself then reads the attribute name, applies the
  `parse_attribute` MFA and forces the value (`Ash.Actions.Create` /
  `Ash.Actions.Read`, `handle_attribute_multitenancy/1`). So the library never
  learns the column name, which is the mistake that would have broken any host
  using a non-identity `parse_attribute`.

  Both are no-ops when the resource declares nothing: Ash's own guard is
  `strategy == :attribute`, so an untenanted host behaves exactly as before.

  ## The dangerous one

  `retire_vanished` reconciles what a pass PRODUCED against what the cell HOLDS.
  If the write is tenant-scoped and the read is not, a tenanted drain destroys
  every other tenant's rows — the same partial-read-against-total-baseline shape
  as the two-node join bug, except it deletes rather than nils. That is the first
  test here, and it is the reason this cannot ship half-done.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.Node.{Payload, Rows}

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      allow_unregistered?(true)
    end
  end

  # A TENANTED node: `municipality_id` is the tenant attribute, and Ash — not
  # this library — is what puts a value in it.
  defmodule Meeting do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
      private?(true)
    end

    multitenancy do
      strategy :attribute
      attribute :municipality_id
    end

    # A UUID PRIMARY KEY, and this is forced rather than chosen. The tenant
    # attribute cannot be in the primary key: Spark refuses a nullable PK, and
    # `for_create` validates required attributes BEFORE `Ash.create` applies the
    # tenant — so a non-nullable tenant attribute fails validation while the
    # changeset already carries the tenant. With a UUID PK the tenant is an
    # ordinary column and uniqueness moves to an identity.
    attributes do
      uuid_primary_key(:id)
      attribute :key, :string, allow_nil?: false, public?: true
      attribute :municipality_id, :string, public?: true
      attribute :title, :string, public?: true
    end

    identities do
      identity :by_tenant_key, [:municipality_id, :key], pre_check_with: Domain
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        # by IDENTITY, not the primary key: the PK is a fresh UUID every time, so
        # conflicting on it would insert a duplicate per write.
        upsert_identity(:by_tenant_key)
        accept([:key, :municipality_id, :title])
      end
    end

    reactive do
      id(:meeting)
      op(:source)
      leaf?(true)
      # REQUIRED: `derived_payload_key/1` returns the single-attribute PK, which
      # here is `:id` — without this the library writes cell keys into the UUID
      # column. See ADR-003.
      payload_key(:key)
    end
  end

  # An UNTENANTED node, in the same graph. Declares no multitenancy at all, so
  # every path must behave exactly as it did before this feature existed.
  defmodule Shared do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
      private?(true)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :title, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        accept([:key, :title])
      end
    end

    reactive do
      id(:shared)
      op(:source)
      leaf?(true)
    end
  end

  setup do
    for r <- [Meeting, Shared] do
      r |> Ash.read!(tenant: "a") |> Enum.each(&Ash.destroy!(&1, tenant: "a"))
      r |> Ash.read!(tenant: "b") |> Enum.each(&Ash.destroy!(&1, tenant: "b"))
    end

    :ok
  end

  # The tenant is the PLAN's, so a read takes it as an option — a cell carries
  # none, which is what keeps every tenant's plan identical.
  defp meeting_cell, do: ReactiveDag.Node.graph([Meeting]).cells["meeting"]

  describe "Ash does the column work" do
    test "the library never names the attribute — Ash forces it from the tenant" do
      Payload.upsert(Meeting, :key, "m1", %{title: "budget"}, :upsert, tenant: "a")

      # read WITHOUT a tenant to see the raw row: Ash filled municipality_id in
      [row] = Ash.read!(Meeting, tenant: "a")
      assert row.municipality_id == "a"
      assert row.title == "budget"
    end

    test "two tenants' rows coexist under the same key" do
      Payload.upsert(Meeting, :key, "m1", %{title: "village"}, :upsert, tenant: "a")
      Payload.upsert(Meeting, :key, "m1", %{title: "town"}, :upsert, tenant: "b")

      assert [%{title: "village"}] = Ash.read!(Meeting, tenant: "a")
      assert [%{title: "town"}] = Ash.read!(Meeting, tenant: "b")
    end

    test "change detection is per tenant" do
      # B writing the same key must not make A's next write look unchanged.
      assert Payload.upsert(Meeting, :key, "m1", %{title: "x"}, :upsert, tenant: "a") == :created

      assert Payload.upsert(Meeting, :key, "m1", %{title: "x"}, :upsert, tenant: "b") ==
               :created,
             "B's row is new, even though A already holds this key"

      assert Payload.upsert(Meeting, :key, "m1", %{title: "x"}, :upsert, tenant: "a") ==
               :unchanged
    end
  end

  describe "reads are scoped" do
    test "`Rows.all/1` sees only its own tenant" do
      Payload.upsert(Meeting, :key, "m1", %{title: "v"}, :upsert, tenant: "a")
      Payload.upsert(Meeting, :key, "m2", %{title: "t"}, :upsert, tenant: "b")

      assert Rows.all(meeting_cell(), tenant: "a") |> Enum.map(& &1.key) == ["m1"]
      assert Rows.all(meeting_cell(), tenant: "b") |> Enum.map(& &1.key) == ["m2"]
    end

    test "`Rows.key_count/1` counts only its own tenant" do
      Payload.upsert(Meeting, :key, "m1", %{title: "v"}, :upsert, tenant: "a")
      Payload.upsert(Meeting, :key, "m2", %{title: "t"}, :upsert, tenant: "b")

      assert Rows.key_count(meeting_cell(), tenant: "a") == 1
    end
  end

  describe "retirement — the destructive case" do
    test "retiring in one tenant does NOT delete another's rows" do
      Payload.upsert(Meeting, :key, "m1", %{title: "v"}, :upsert, tenant: "a")
      Payload.upsert(Meeting, :key, "m1", %{title: "t"}, :upsert, tenant: "b")

      Payload.retire(Meeting, :key, nil, ["m1"], :destroy, tenant: "a")

      assert Ash.read!(Meeting, tenant: "a") == []

      assert [%{title: "t"}] = Ash.read!(Meeting, tenant: "b"),
             "B's row must survive A's retirement of the same key"
    end
  end

  describe "retirement through a real recompute — the destructive path" do
    # `retire_vanished` subtracts what a pass PRODUCED from what the cell HOLDS.
    # The baseline comes from `Rows.all/2`, so an unscoped read there retires
    # every other tenant's keys. Mutation testing found this untested: scoping
    # only the retire LOOKUP passed every test while the BASELINE stayed global.
    defmodule Rollup do
      use Ash.Resource,
        domain: Domain,
        data_layer: Ash.DataLayer.Ets,
        extensions: [ReactiveDag.Node]

      ets do
        private?(true)
      end

      multitenancy do
        strategy :attribute
        attribute :municipality_id
      end

      attributes do
        uuid_primary_key(:id)
        attribute :key, :string, allow_nil?: false, public?: true
        attribute :municipality_id, :string, public?: true
        attribute :total, :integer, public?: true
      end

      identities do
        identity :by_tenant_key, [:municipality_id, :key], pre_check_with: Domain
      end

      actions do
        defaults [:read, :destroy]

        create :upsert do
          upsert?(true)
          upsert_identity(:by_tenant_key)
          accept([:key, :municipality_id, :total])
        end
      end

      reactive do
        id(:rollup)
        op(:fold)
        payload_key(:key)
        # A `reduce` is what RETIRES: `compute Mod` writes its own rows and never
        # reaches `materialize/4`. The `over` leaf holds nothing, so this pass
        # produces nothing and every key the cell holds is retired.
        reduce(over: :meeting, group_by: :key, into: [count: :total])
      end
    end

    test "a fold WRITES its rows under the plan's tenant" do
      # The write half. `writer_fn/2` merges the tenant into the payload opts, so
      # a fold's output row lands in the right tenant — without it the row is
      # written untenanted and that tenant's next read cannot see it.
      Payload.upsert(Meeting, :key, "m1", %{title: "x"}, :upsert, tenant: "a")

      cell = ReactiveDag.Node.graph([Meeting, Rollup]).cells["rollup"]
      {:ok, changed} = ReactiveDag.Node.Recompute.recompute(cell, ["*"], tenant: "a")

      assert changed == ["m1"], "the fold produced a row"

      assert [%{key: "m1", municipality_id: "a"}] = Ash.read!(Rollup, tenant: "a"),
             "the output row carries the plan's tenant"

      assert Ash.read!(Rollup, tenant: "b") == [], "and nothing landed in B"
    end

    test "a whole-cell pass producing nothing retires only ITS tenant's rows" do
      # Seed both tenants directly, then recompute A whole-cell with an op that
      # produces NOTHING. Everything A holds must be retired; nothing of B's.
      for t <- ["a", "b"] do
        Rollup
        |> Ash.Changeset.for_create(:upsert, %{key: "r1", total: 1}, tenant: t)
        |> Ash.create!()
      end

      assert length(Ash.read!(Rollup, tenant: "a")) == 1
      assert length(Ash.read!(Rollup, tenant: "b")) == 1

      cell = ReactiveDag.Node.graph([Meeting, Rollup]).cells["rollup"]

      {:ok, _changed} = ReactiveDag.Node.Recompute.recompute(cell, ["*"], tenant: "a")

      assert Ash.read!(Rollup, tenant: "a") == [], "A's vanished row was retired"

      assert length(Ash.read!(Rollup, tenant: "b")) == 1,
             "B's row must survive A's whole-cell retirement"
    end
  end

  describe "per_key and aggregate — the two paths that were unthreaded" do
    # Both delegate to submodules that write rows, and both were shipped with a
    # TODO. `PerKey` has FOUR tenant points (the over-read, the existing-
    # fingerprint read, the action write, and the payload upsert); `Aggregate`
    # has two. A gap in any one writes a row into the wrong tenant or reads
    # another tenant's fingerprints as this tenant's.
    defmodule Summary do
      use Ash.Resource,
        domain: Domain,
        data_layer: Ash.DataLayer.Ets,
        extensions: [ReactiveDag.Node]

      ets do
        private?(true)
      end

      multitenancy do
        strategy :attribute
        attribute :municipality_id
      end

      attributes do
        uuid_primary_key(:id)
        attribute :key, :string, allow_nil?: false, public?: true
        attribute :municipality_id, :string, public?: true
        attribute :summary, :string, public?: true
        attribute :fingerprint, :string, public?: true
      end

      identities do
        identity :by_tenant_key, [:municipality_id, :key], pre_check_with: Domain
      end

      actions do
        defaults [:read, :destroy]

        create :upsert do
          upsert?(true)
          upsert_identity(:by_tenant_key)
          accept([:key, :municipality_id, :summary, :fingerprint])
        end

        action :summarise, :map do
          argument :text, :string
          run(fn input, _ -> {:ok, %{"summary" => "sum:" <> (input.arguments.text || "")}} end)
        end
      end

      reactive do
        id(:summary)
        op(:map)
        payload_key(:key)

        recompute_by(:key, to: :meeting, from: :key)

        per_key(:summarise,
          args: [text: :title],
          fingerprint: [:title],
          into: [summary: :summary]
        )
      end
    end

    # An `aggregate` node: the datastore groups over a has_many, and the library
    # writes each parent's aggregate values. Both the read and the write need the
    # tenant — the read so it does not fold another tenant's children in, the
    # write so the row lands in the right tenant.
    defmodule Reading do
      use Ash.Resource,
        domain: Domain,
        data_layer: Ash.DataLayer.Ets,
        extensions: [ReactiveDag.Node]

      ets do
        private?(true)
      end

      multitenancy do
        strategy :attribute
        attribute :municipality_id
      end

      attributes do
        uuid_primary_key(:id)
        attribute :key, :string, allow_nil?: false, public?: true
        attribute :municipality_id, :string, public?: true
        attribute :plant_key, :string, public?: true
        attribute :flow, :integer, public?: true
      end

      actions do
        defaults [:read, :destroy]

        create :create do
          accept([:key, :municipality_id, :plant_key, :flow])
        end
      end

      reactive do
        id(:readings)
        op(:source)
        leaf?(true)
      end
    end

    defmodule Plant do
      use Ash.Resource,
        domain: Domain,
        data_layer: Ash.DataLayer.Ets,
        extensions: [ReactiveDag.Node]

      ets do
        private?(true)
      end

      multitenancy do
        strategy :attribute
        attribute :municipality_id
      end

      attributes do
        uuid_primary_key(:id)
        attribute :key, :string, allow_nil?: false, public?: true
        attribute :municipality_id, :string, public?: true
        attribute :total, :integer, public?: true
      end

      identities do
        identity :by_tenant_key, [:municipality_id, :key], pre_check_with: Domain
      end

      relationships do
        has_many :readings, ReactiveDag.AshTenancyTest.Reading do
          source_attribute :key
          destination_attribute :plant_key
        end
      end

      actions do
        defaults [:read, :destroy]

        create :upsert do
          upsert?(true)
          upsert_identity(:by_tenant_key)
          accept([:key, :municipality_id, :total])
        end
      end

      reactive do
        id(:plants)
        op(:fold)
        payload_key(:key)
        aggregate(over: :readings, sum: [flow: :total])
      end
    end

    test "`aggregate` folds only its own tenant's children" do
      # Same parent key in both tenants, different children. If the read is
      # unscoped the fold sums BOTH tenants' readings into one total — a wrong
      # number rather than an error, which is the worst kind.
      for {t, flow} <- [{"a", 10}, {"b", 99}] do
        Plant
        |> Ash.Changeset.for_create(:upsert, %{key: "p1", total: 0}, tenant: t)
        |> Ash.create!()

        Reading
        |> Ash.Changeset.for_create(:create, %{key: "r-" <> t, plant_key: "p1", flow: flow}, tenant: t)
        |> Ash.create!()
      end

      cell = ReactiveDag.Node.graph([Reading, Plant]).cells["plants"]
      {:ok, _changed} = ReactiveDag.Node.Recompute.recompute(cell, ["*"], tenant: "a")

      assert [%{total: 10}] = Ash.read!(Plant, tenant: "a"),
             "A's total is A's reading alone, not 109"

      assert [%{total: 0}] = Ash.read!(Plant, tenant: "b"), "B was not recomputed at all"
    end

    test "`per_key` writes into the plan's tenant, and skips per tenant" do
      Payload.upsert(Meeting, :key, "m1", %{title: "a-title"}, :upsert, tenant: "a")
      Payload.upsert(Meeting, :key, "m1", %{title: "b-title"}, :upsert, tenant: "b")

      cell = ReactiveDag.Node.graph([Meeting, Summary]).cells["summary"]

      {:ok, changed, meta} = ReactiveDag.Node.Recompute.recompute(cell, ["*"], tenant: "a")
      assert changed == ["m1"]
      assert meta.called == 1

      assert [%{summary: "sum:a-title", municipality_id: "a"}] =
               Ash.read!(Summary, tenant: "a")

      assert Ash.read!(Summary, tenant: "b") == [], "nothing landed in B"

      # B's own pass must CALL, not skip: A's fingerprint is not B's. An
      # unscoped fingerprint read would find A's and skip the work.
      {:ok, _changed, meta_b} = ReactiveDag.Node.Recompute.recompute(cell, ["*"], tenant: "b")

      assert meta_b.called == 1, "B's row is unseen work, not a cache hit"
      assert [%{summary: "sum:b-title"}] = Ash.read!(Summary, tenant: "b")

      # ...and A re-running now SKIPS, because its own fingerprint matches.
      {:ok, _c, meta_a2} = ReactiveDag.Node.Recompute.recompute(cell, ["*"], tenant: "a")
      assert meta_a2.skipped == 1
    end
  end

  describe "`\"*\"` is a TENANT, not an absence" do
    test "an untenanted plan can still read a tenanted resource" do
      # The state EVERY existing host is in: a plan with no tenant, and a
      # resource that has just declared multitenancy. `"*"` is what the frontier
      # writes when no tenant is named, so it is a tenant VALUE — but the tenant
      # helpers treated it as "no tenant" and passed nothing, which Ash refuses
      # on a tenanted resource. So adopting tenancy on one resource broke every
      # untenanted read of it.
      Payload.upsert(Meeting, :key, "m1", %{title: "x"}, :upsert, tenant: "*")

      assert Rows.all(meeting_cell(), tenant: "*") |> Enum.map(& &1.key) == ["m1"]
      assert Rows.key_count(meeting_cell(), tenant: "*") == 1
    end

    test "and its rows are separate from a named tenant's" do
      Payload.upsert(Meeting, :key, "m1", %{title: "untenanted"}, :upsert, tenant: "*")
      Payload.upsert(Meeting, :key, "m1", %{title: "a"}, :upsert, tenant: "a")

      assert [%{title: "untenanted"}] = Ash.read!(Meeting, tenant: "*")
      assert [%{title: "a"}] = Ash.read!(Meeting, tenant: "a")
    end
  end

  describe "an untenanted resource is untouched" do
    test "writes and reads work with no tenant at all" do
      assert Payload.upsert(Shared, :key, "s1", %{title: "x"}, :upsert) == :created
      assert Payload.upsert(Shared, :key, "s1", %{title: "x"}, :upsert) == :unchanged

      assert [%{key: "s1"}] = Ash.read!(Shared)
    end

    test "...and passing a tenant to one is harmless" do
      # Ash's own guard is `strategy == :attribute`; a resource declaring nothing
      # ignores the tenant rather than raising.
      assert Payload.upsert(Shared, :key, "s1", %{title: "x"}, :upsert, tenant: "a") == :created
      assert [%{key: "s1"}] = Ash.read!(Shared)
    end
  end
end

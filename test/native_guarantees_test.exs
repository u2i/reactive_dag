defmodule ReactiveDag.NativeGuaranteesTest do
  @moduledoc """
  Model each real portal GUARANTEE SHAPE as a native `ReactiveDag.Node` resource
  (one resource per guarantee), and assert the library supports everything the
  compliance model needs. Shapes taken from the live catalog:

    * op-kinds as the set: reconcile / relation / product / fold / union / bind / analysis
    * leaves with domain metadata: observed / declared / workflow
      (strength / source / check / attest_for / cadence)
    * guarantee-level features: addresses, shape, population, conformance (metadata);
      rests_on / depends_on (guarantee→guarantee edges); over (second-order);
      for_each (generator → N instances)

  Each test is "author it in a resource; assert it lowers to the right cells +
  carries the domain metadata". Gaps become library work.
  """
  use ExUnit.Case, async: true

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false
    resources do
      allow_unregistered?(true)
    end
  end

  defp cellsof(mod), do: ReactiveDag.Node.cells(mod)
  defp by_id(cells), do: Map.new(cells, &{&1.id, &1})

  # ── shared named nodes the guarantees ref ────────────────────────────────────
  defmodule Stores do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Simple, extensions: [ReactiveDag.Node]
    reactive do
      id :stores
      op :leaf
      leaf? true
    end
  end

  # ── SHAPE 1: reconcile (declared ⟗ observed) — store_encrypted ────────────────
  defmodule StoreEncrypted do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Simple, extensions: [ReactiveDag.Node]

    reactive do
      id :"g:store_encrypted"
      op :guarantee
      key_rule :all
      # guarantee-level compliance metadata — must survive to meta.
      meta claim: "every store is encrypted as declared", addresses: [:C1_1], shape: :delta

      compose :reconcile do
        as :"g:store_encrypted/set"
        ref :stores
        # an observed leaf WITH domain binding metadata.
        compose :leaf do
          as :"g:store_encrypted/set/1"
          leaf? true
          meta strength: :measured, source: :probe, check: "store_config"
        end
      end
    end
  end

  test "SHAPE reconcile: lowers g → set(reconcile) → [ref, observed-leaf]; metadata survives" do
    cells = cellsof(StoreEncrypted) |> by_id()

    assert cells["g:store_encrypted"].op == :guarantee
    assert cells["g:store_encrypted"].meta.claim == "every store is encrypted as declared"
    assert cells["g:store_encrypted"].meta.addresses == [:C1_1]

    set = cells["g:store_encrypted/set"]
    assert set.op == :reconcile
    assert Enum.sort(set.inputs) == ["g:store_encrypted/set/1", "stores"]

    leaf = cells["g:store_encrypted/set/1"]
    assert leaf.leaf?
    assert leaf.meta.source == :probe
    assert leaf.meta.check == "store_config"
    assert leaf.meta.strength == :measured
  end

  # ── SHAPE 2: product (axis × axis over a coverage fn) — strong_auth ───────────
  defmodule People do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Simple, extensions: [ReactiveDag.Node]
    reactive do id(:people); op(:leaf); leaf?(true) end
  end

  defmodule Systems do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Simple, extensions: [ReactiveDag.Node]
    reactive do id(:systems); op(:leaf); leaf?(true) end
  end

  defmodule StrongAuth do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Simple, extensions: [ReactiveDag.Node]

    reactive do
      id :"g:strong_auth"
      op :guarantee
      key_rule :all
      meta claim: "every principal authenticates strongly on every system", addresses: [:CC6_1]

      compose :product do
        as :"g:strong_auth/set"
        ref :people        # axis a
        ref :systems       # axis b
        compose :leaf do   # coverage fn leaf
          as :"g:strong_auth/set/2"
          leaf? true
          meta source: :probe, check: "mfa"
        end
      end
    end
  end

  test "SHAPE product: g → set(product) over two axis refs + a coverage-fn leaf" do
    cells = cellsof(StrongAuth) |> by_id()
    set = cells["g:strong_auth/set"]
    assert set.op == :product
    assert Enum.sort(set.inputs) == ["g:strong_auth/set/2", "people", "systems"]
    assert cells["g:strong_auth/set/2"].meta.check == "mfa"
  end

  # ── SHAPE 3: relation with an fn leg (members judged by fn) — duty_has_owner ──
  defmodule DutyHasOwner do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Simple, extensions: [ReactiveDag.Node]

    reactive do
      id :"g:duty_has_owner"
      op :guarantee
      key_rule :all

      compose :relation do
        as :"g:duty_has_owner/set"
        ref :duties          # members
        compose :leaf do     # fn leg (the performer assignment)
          as :"g:duty_has_owner/set/1"
          leaf? true
          meta strength: :attested, attest_for: "CC1_3", cadence: :annual
        end
      end
    end
  end

  test "SHAPE relation+fn: fn-leaf carries workflow/attestation metadata" do
    cells = cellsof(DutyHasOwner) |> by_id()
    assert cells["g:duty_has_owner/set"].op == :relation
    fn_leaf = cells["g:duty_has_owner/set/1"]
    assert fn_leaf.meta.attest_for == "CC1_3"
    assert fn_leaf.meta.cadence == :annual
  end

  # ── SHAPE 4: guarantee→guarantee edges (rests_on / depends_on) ────────────────
  # the assurance ladder: a guarantee discharged by / dependent on other guarantees.
  defmodule HireScreenedBeforeAccess do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Simple, extensions: [ReactiveDag.Node]

    reactive do
      id :"g:hire_screened_before_access"
      op :guarantee
      key_rule :all
      meta rests_on: [:"g:hire_screened"], depends_on: [:"g:access_removed_on_leave"]

      compose :fold do
        as :"g:hire_screened_before_access/set"
        ref :hire_events
      end
    end
  end

  test "SHAPE guarantee-edges: rests_on/depends_on survive as guarantee metadata" do
    g = cellsof(HireScreenedBeforeAccess) |> by_id() |> Map.get("g:hire_screened_before_access")
    assert g.meta.rests_on == [:"g:hire_screened"]
    assert g.meta.depends_on == [:"g:access_removed_on_leave"]
  end

  # ── SHAPE 5: second-order (over: computed from the graph) ─────────────────────
  defmodule Readiness do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Simple, extensions: [ReactiveDag.Node]
    reactive do
      id :"g:readiness"
      op :guarantee
      over :findings
      compute FakeReadinessOp   # second-order needs a host recompute, not a set-expr
    end
  end

  test "SHAPE second-order: over: rides in meta for the host hook" do
    g = cellsof(Readiness) |> by_id() |> Map.get("g:readiness")
    assert g.meta.over == :findings
    assert g.meta.compute == FakeReadinessOp
  end

  # ── SHAPE 6: generator (for_each → N instances) ───────────────────────────────
  defmodule ConcernDischarged do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Simple, extensions: [ReactiveDag.Node]

    reactive do
      id :"g:concern_discharged"
      op :guarantee
      key_rule :all
      for_each :walk_risks

      # a generator's sub-tree uses POSITIONAL ids (no absolute `as:`) so it
      # RE-ROOTS per member: g:concern_discharged.<m>/0, …/0/0. (Absolute `as:` is
      # for stable, cross-ref'd names — incompatible with per-member re-rooting.)
      compose :fold do
        compose :leaf do
          leaf? true
          meta source: :probe, check: "risk_posture", strength: :examined
        end
      end
    end
  end

  test "SHAPE generator: for_each expands to one instance sub-tree per member, stamped + re-rooted" do
    members = [%{id: "r1", meta: %{probe_filter: "r1"}}, %{id: "r2", meta: %{probe_filter: "r2"}}]
    cells = ReactiveDag.Node.cells(ConcernDischarged, fn :walk_risks -> members end) |> by_id()

    # per member: a full sub-tree re-rooted under g:concern_discharged.<m> — NO template.
    refute Map.has_key?(cells, "g:concern_discharged")
    assert cells["g:concern_discharged.r1"].op == :guarantee
    assert cells["g:concern_discharged.r2"].op == :guarantee

    # the positional sub-tree re-rooted per member (…​.r1/0 = the fold, …​.r1/0/0 = its leaf).
    assert cells["g:concern_discharged.r1/0"].op == :fold
    r1_leaf = cells["g:concern_discharged.r1/0/0"] || raise "re-rooted instance leaf missing"
    assert r1_leaf.leaf?
    # the member stamp reached the instance's probe leaf; the r2 leaf is DISTINCT.
    assert r1_leaf.meta.probe_filter == "r1"
    assert r1_leaf.meta.check == "risk_posture"
    assert cells["g:concern_discharged.r2/0/0"].meta.probe_filter == "r2"
  end
end

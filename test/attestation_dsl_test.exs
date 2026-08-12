defmodule ReactiveDag.AttestationDslTest do
  @moduledoc """
  The attestation DSL → graph shape: a requirement declared ONCE on the raw
  node, consumed by name from an `attested` view node and from `gate:`d edges —
  both lowering to the same interposed-cell shape, with the eligibility cell
  and the store leaf as REAL input edges (authority changes propagate; lineage
  sees the join). Plus the vacuity lint: a verdict whose every evidence path is
  gated on one requirement has swallowed its own denominator.
  """
  use ExUnit.Case, async: true

  alias ReactiveDag.Attestation.Requirement

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      allow_unregistered?(true)
    end
  end

  # the RAW cell — owns its data AND the requirement's policy.
  defmodule Machines do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Simple,
      extensions: [ReactiveDag.Node]

    def holder(_scope, _elig_key), do: nil

    reactive do
      op(:source)
      leaf?(true)

      attestation :machine_ownership do
        signers(:machine_holders)
        join(&Machines.holder/2)
        quorum(:any)
        tolerance(days: 180)
      end
    end
  end

  # the ELIGIBILITY cell — who may sign is data.
  defmodule MachineHolders do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Simple,
      extensions: [ReactiveDag.Node]

    reactive do
      op(:source)
      leaf?(true)
    end
  end

  # the declared ATTESTED VIEW: both cells exist — raw list AND signed list.
  defmodule ConfirmedMachines do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Simple,
      extensions: [ReactiveDag.Node]

    reactive do
      id(:confirmed_machines)
      attested(over: :machines, requirement: :machine_ownership)
    end
  end

  # a consumer whose DENOMINATOR is the raw cell and whose other leg is gated.
  defmodule OwnershipVerdict do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Simple,
      extensions: [ReactiveDag.Node]

    reactive do
      id(:ownership_verdict)
      verdict?(true)
      op(:reconcile)
      compute(FakeReconcile)
      depends_on([:machines, {:machines, gate: :machine_ownership}])
    end
  end

  defp plan,
    do: ReactiveDag.Node.graph([Machines, MachineHolders, ConfirmedMachines, OwnershipVerdict])

  test "a declared attested node resolves: requirement struct + eligibility + store edges" do
    cell = plan().cells["confirmed_machines"]

    assert %{over: "machines", requirement: %Requirement{} = req} = cell.meta.attested
    assert req.name == :machine_ownership
    # `on` is filled from the DECLARING node at assembly — records live under it.
    assert req.on == "machines"
    assert Requirement.tolerance_seconds(req) == 180 * 86_400

    assert Enum.sort(cell.inputs) ==
             Enum.sort(["machines", "machine_holders", ReactiveDag.Attestation.leaf_cell()])

    assert cell.meta.compute == ReactiveDag.Attestation.Op
    assert cell.meta.key_rule == :all
  end

  test "a gate: interposes an anonymous attested cell of the same shape" do
    id = ReactiveDag.Node.gated_id("machines", :machine_ownership)
    cell = plan().cells[id]

    assert cell.op == :attested
    assert %{requirement: %Requirement{name: :machine_ownership}} = cell.meta.attested

    assert Enum.sort(cell.inputs) ==
             Enum.sort(["machines", "machine_holders", ReactiveDag.Attestation.leaf_cell()])

    # the consumer's gated edge points AT the interposed cell; its raw edge stays raw.
    verdict = plan().cells["ownership_verdict"]
    assert id in verdict.inputs
    assert "machines" in verdict.inputs
  end

  describe "tolerance units" do
    # regression: an unsupported unit used to contribute 0 seconds, silently
    # expiring every signature immediately (presents as policy, not as a bug).
    test "the full unit set normalizes, and units combine" do
      assert Requirement.tolerance_seconds(%Requirement{tolerance: [weeks: 2]}) == 2 * 604_800
      assert Requirement.tolerance_seconds(%Requirement{tolerance: [minutes: 30]}) == 1_800

      assert Requirement.tolerance_seconds(%Requirement{tolerance: [days: 1, hours: 2, seconds: 3]}) ==
               86_400 + 7_200 + 3
    end

    test "an unknown unit raises instead of counting as zero" do
      assert_raise ArgumentError, ~r/unsupported unit.*months/s, fn ->
        Requirement.tolerance_seconds(%Requirement{name: :req, tolerance: [months: 6]})
      end
    end

    test "validate_tolerance (the DSL schema's custom type) rejects the bad shapes" do
      assert {:ok, nil} = Requirement.validate_tolerance(nil)
      assert {:ok, 3600} = Requirement.validate_tolerance(3600)
      assert {:ok, [minutes: 5]} = Requirement.validate_tolerance(minutes: 5)

      assert {:error, _} = Requirement.validate_tolerance(months: 6)
      assert {:error, _} = Requirement.validate_tolerance(days: -1)
      assert {:error, _} = Requirement.validate_tolerance([])
      assert {:error, _} = Requirement.validate_tolerance(-30)
      assert {:error, _} = Requirement.validate_tolerance("90d")
    end

    test "a requirement declaring an unsupported unit fails at the DSL schema" do
      assert_raise Spark.Error.DslError, ~r/unsupported unit/, fn ->
        defmodule BadTolerance do
          use Ash.Resource,
            domain: Domain,
            data_layer: Ash.DataLayer.Simple,
            extensions: [ReactiveDag.Node]

          def holder(_scope, _elig_key), do: nil

          reactive do
            op(:source)
            leaf?(true)

            attestation :aging do
              signers(:machine_holders)
              join(&BadTolerance.holder/2)
              tolerance(months: 6)
            end
          end
        end
      end
    end
  end

  test "the store surfaces as ONE leaf cell — signing propagates like a scan" do
    store = plan().cells[ReactiveDag.Attestation.leaf_cell()]
    assert store.leaf?
    assert store.meta.attestation_store

    # it is below every attested cell, so a leaf write drains through them.
    assert ReactiveDag.Attestation.leaf_cell() in plan().cells["confirmed_machines"].inputs
  end

  test "signing dirties the attested views: store + eligibility are PROPAGATING parents" do
    p = plan()
    gated = ReactiveDag.Node.gated_id("machines", :machine_ownership)

    for parent_of <- [ReactiveDag.Attestation.leaf_cell(), "machine_holders", "machines"] do
      assert "confirmed_machines" in p.parents[parent_of]
      assert gated in p.parents[parent_of]
    end
  end

  test "an unknown requirement fails assembly, naming what IS declared" do
    defmodule Orphan do
      use Ash.Resource,
        domain: Domain,
        data_layer: Ash.DataLayer.Simple,
        extensions: [ReactiveDag.Node]

      reactive do
        id(:orphan)
        attested(over: :machines, requirement: :no_such_requirement)
      end
    end

    err =
      assert_raise ArgumentError, fn ->
        ReactiveDag.Node.graph([Machines, MachineHolders, Orphan])
      end

    assert err.message =~ "no_such_requirement"
    assert err.message =~ "machine_ownership"
  end

  test "a requirement must be consumed against the cell it is declared on" do
    defmodule OtherLeaf do
      use Ash.Resource,
        domain: Domain,
        data_layer: Ash.DataLayer.Simple,
        extensions: [ReactiveDag.Node]

      reactive do
        id(:other_leaf)
        op(:source)
        leaf?(true)
      end
    end

    defmodule Mismatched do
      use Ash.Resource,
        domain: Domain,
        data_layer: Ash.DataLayer.Simple,
        extensions: [ReactiveDag.Node]

      reactive do
        id(:mismatched)
        attested(over: :other_leaf, requirement: :machine_ownership)
      end
    end

    err =
      assert_raise ArgumentError, fn ->
        ReactiveDag.Node.graph([Machines, MachineHolders, OtherLeaf, Mismatched])
      end

    assert err.message =~ "declared on"
  end

  test "VACUITY LINT: a verdict whose every evidence path is gated on one requirement raises" do
    defmodule AllGated do
      use Ash.Resource,
        domain: Domain,
        data_layer: Ash.DataLayer.Simple,
        extensions: [ReactiveDag.Node]

      reactive do
        id(:all_gated)
        verdict?(true)
        op(:reconcile)
        compute(FakeReconcile)
        # BOTH legs through the same gate: the rows the gate withholds are
        # invisible to the very join meant to expose them.
        depends_on([{:machines, gate: :machine_ownership}])
      end
    end

    err =
      assert_raise ArgumentError, fn ->
        ReactiveDag.Node.graph([Machines, MachineHolders, AllGated])
      end

    assert err.message =~ "structurally vacuous"
    assert err.message =~ "machine_ownership"
  end

  test "the lint passes when a denominator leg consumes the raw cell ungated" do
    # OwnershipVerdict has exactly that shape — assembly succeeds.
    assert %ReactiveDag.Plan{} = plan()
  end

  test "a verdict node that IS the attested view is exempt from the lint" do
    # first-class coverage: the view writes a row for EVERY raw row
    # (covered/pending/refused), so it cannot swallow its own denominator.
    defmodule AttestedVerdict do
      use Ash.Resource,
        domain: Domain,
        data_layer: Ash.DataLayer.Simple,
        extensions: [ReactiveDag.Node]

      reactive do
        id(:attested_verdict)
        verdict?(true)
        attested(over: :machines, requirement: :machine_ownership)
      end
    end

    p = ReactiveDag.Node.graph([Machines, MachineHolders, AttestedVerdict])
    assert p.cells["attested_verdict"].meta.verdict == true
    assert p.cells["attested_verdict"].meta.attested.requirement.name == :machine_ownership
  end

  test "a graph with no attestation vocabulary is untouched (no store leaf appears)" do
    p = ReactiveDag.Node.graph([Machines, MachineHolders])
    refute Map.has_key?(p.cells, ReactiveDag.Attestation.leaf_cell())
  end

  describe "non-blocking mode (:annotate)" do
    defmodule BestEffortReport do
      use Ash.Resource,
        domain: Domain,
        data_layer: Ash.DataLayer.Simple,
        extensions: [ReactiveDag.Node]

      reactive do
        id(:best_effort_report)
        verdict?(true)
        op(:report)
        compute(FakeReport)
        # NON-BLOCKING: everything flows, signed distinguished from unsigned.
        depends_on([{:machines, gate: :machine_ownership, mode: :annotate}])
      end
    end

    test "an annotate gate interposes its OWN cell, distinct from the blocking one" do
      p = ReactiveDag.Node.graph([Machines, MachineHolders, BestEffortReport, OwnershipVerdict])

      blocking = ReactiveDag.Node.gated_id("machines", :machine_ownership)
      annotate = ReactiveDag.Node.gated_id("machines", :machine_ownership, :annotate)

      assert annotate == "machines@machine_ownership~annotate"
      refute blocking == annotate

      # two projections → two cells, same three inputs, same requirement.
      assert p.cells[annotate].meta.attested.mode == :annotate
      assert p.cells[blocking].meta.attested.mode == :require
      assert p.cells[annotate].inputs == p.cells[blocking].inputs
      assert annotate in p.cells["best_effort_report"].inputs
    end

    test "an all-annotate-gated verdict is NOT vacuous — nothing is withheld" do
      # the same shape that raises under :require passes under :annotate,
      # because unsigned rows flow (as `unsigned`) and remain countable.
      assert %ReactiveDag.Plan{} =
               ReactiveDag.Node.graph([Machines, MachineHolders, BestEffortReport])
    end

    test "a declared attested node takes mode: :annotate" do
      defmodule AnnotatedMachines do
        use Ash.Resource,
          domain: Domain,
          data_layer: Ash.DataLayer.Simple,
          extensions: [ReactiveDag.Node]

        reactive do
          id(:annotated_machines)
          attested(over: :machines, requirement: :machine_ownership, mode: :annotate)
        end
      end

      p = ReactiveDag.Node.graph([Machines, MachineHolders, AnnotatedMachines])
      assert p.cells["annotated_machines"].meta.attested.mode == :annotate
    end

    test "spine_status: the one place the modes differ is the unsigned row" do
      statuses =
        ReactiveDag.Attestation.Requirement.statuses(%ReactiveDag.Attestation.Requirement{})

      for {state, require_status, annotate_status} <- [
            {:affirmed, "covered", "covered"},
            {:pending, "pending", "unsigned"},
            # a rejection bites in BOTH modes: data someone said is wrong is not
            # "best effort" data — passing it through would launder the objection.
            {:refused, "refused", "refused"}
          ] do
        assert ReactiveDag.Attestation.Op.spine_status(state, :require, statuses) ==
                 require_status

        assert ReactiveDag.Attestation.Op.spine_status(state, :annotate, statuses) ==
                 annotate_status
      end
    end
  end

  test "gated refs nested inside compose legs get their interposed cell too" do
    defmodule NestedGate do
      use Ash.Resource,
        domain: Domain,
        data_layer: Ash.DataLayer.Simple,
        extensions: [ReactiveDag.Node]

      reactive do
        id(:nested_gate)
        op(:union)
        compute(FakeUnion)
        ref(:machines)

        compose :inner do
          as(:inner)
          ref(:machines, gate: :machine_ownership)
        end
      end
    end

    p = ReactiveDag.Node.graph([Machines, MachineHolders, NestedGate])
    id = ReactiveDag.Node.gated_id("machines", :machine_ownership)

    assert p.cells[id].op == :attested
    assert id in p.cells["inner"].inputs
  end
  test "an attested view declaring payload attributes raises at assembly" do
    # the same half-state `verdict?` refuses: Attestation.Op writes the
    # ADMISSION to the coordination tuple and never touches this resource, so
    # these columns would silently stay empty forever.
    defmodule AttestedWithColumns do
      use Ash.Resource,
        domain: ReactiveDag.AttestationDslTest.Domain,
        data_layer: Ash.DataLayer.Ets,
        extensions: [ReactiveDag.Node]

      ets do
      end

      attributes do
        attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
        attribute :serial, :string, public?: true
      end

      actions do
        defaults [:read]
      end

      reactive do
    id(:attested_with_columns)
    attested(over: :machines, requirement: :ownership)
      end
    end

    # raised at ASSEMBLY (extra_meta), like verdict?'s equivalent guard
    err =
      assert_raise RuntimeError, fn ->
        ReactiveDag.Node.graph([Machines, MachineHolders, AttestedWithColumns])
      end

    msg = Exception.message(err)

    assert msg =~ ":serial"
    assert msg =~ "never be written"
    # says what to do instead, rather than only refusing
    assert msg =~ "joining back to"
  end
end

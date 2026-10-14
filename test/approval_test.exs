defmodule ReactiveDag.ApprovalTest do
  @moduledoc """
  Signing off on a VERSION, not on a row.

  The property everything here turns on: whether a sign-off applies is a
  COMPARISON, not a stored fact.

      approved?(row) = row.approval.recorded_at == row.recorded_at

  A flag would need something to clear it on every path that writes the row,
  and a path that forgets leaves a row reading as reviewed when nobody reviewed
  *that content* — silent, and in the direction of falsely claiming review. An
  equality that stops holding cannot be forgotten, which is why this is thirty
  lines rather than a state machine.

  The other half is what the library refuses to know. A scheme with two
  reviewers, a recorded reason, or an expiry is a different host resource, not
  a different option — so these tests use two deliberately different shapes and
  assert the library cannot tell them apart.
  """
  use ReactiveDag.PostgresCase

  alias ReactiveDag.Fixtures.ApprovalGraph
  alias ReactiveDag.Fixtures.ApprovalGraph.{DualControlApproval, Events, SimpleApproval}

  alias ReactiveDag.Approval

  @simple [via: :approval_id, resource: SimpleApproval]
  @dual [via: :approval_id, resource: DualControlApproval]

  setup_all do
    migrate!(ApprovalGraph.Domain)
    :ok
  end

  setup do
    pid = Ecto.Adapters.SQL.Sandbox.start_owner!(ReactiveDag.TestRepo, shared: true)
    on_exit(fn -> Ecto.Adapters.SQL.Sandbox.stop_owner(pid) end)

    truncate(ApprovalGraph.tables())
    :ok
  end

  defp row(attrs) do
    Events
    |> Ash.Changeset.for_create(:upsert, attrs)
    |> Ash.create!()
  end

  defp approve(mod, attrs) do
    mod
    |> Ash.Changeset.for_create(:upsert, attrs)
    |> Ash.create!()
  end

  describe "currency is derived" do
    test "an approval covering the row's current version applies" do
      approve(SimpleApproval, %{id: "a1", recorded_at: ~U[2026-01-01 00:00:01.000000Z], approved_by: "tom"})
      r = row(%{key: "k1", outcome: "carried", recorded_at: ~U[2026-01-01 00:00:01.000000Z], approval_id: "a1"})

      assert Approval.approved?(r, @simple)
    end

    test "the row moving on lapses the sign-off, with nothing erasing it" do
      approve(SimpleApproval, %{id: "a1", recorded_at: ~U[2026-01-01 00:00:01.000000Z], approved_by: "tom"})
      r = row(%{key: "k1", outcome: "carried", recorded_at: ~U[2026-01-01 00:00:01.000000Z], approval_id: "a1"})
      assert Approval.approved?(r, @simple)

      # The content changed. NOTHING ran to clear anything — the approval row is
      # untouched and still says `v-1`. The equality simply stopped holding.
      moved = row(%{key: "k1", outcome: "defeated", recorded_at: ~U[2026-01-01 00:00:02.000000Z], approval_id: "a1"})

      refute Approval.approved?(moved, @simple)

      assert %{recorded_at: ~U[2026-01-01 00:00:01.000000Z]} = Ash.get!(SimpleApproval, "a1"),
             "the approval is a RECORD of what someone signed; nothing may rewrite it"
    end

    test "re-approving the new version restores it, and the old approval stands" do
      approve(SimpleApproval, %{id: "a1", recorded_at: ~U[2026-01-01 00:00:01.000000Z], approved_by: "tom"})
      approve(SimpleApproval, %{id: "a2", recorded_at: ~U[2026-01-01 00:00:02.000000Z], approved_by: "tom"})

      r = row(%{key: "k1", outcome: "defeated", recorded_at: ~U[2026-01-01 00:00:02.000000Z], approval_id: "a2"})
      assert Approval.approved?(r, @simple)

      # History without designing history: the previous approval is still there,
      # naming the version it covered.
      assert %{recorded_at: ~U[2026-01-01 00:00:01.000000Z]} = Ash.get!(SimpleApproval, "a1")
    end
  end

  describe "not approved, in every form it takes" do
    test "no reference at all" do
      refute Approval.approved?(row(%{key: "k", recorded_at: ~U[2026-01-01 00:00:01.000000Z]}), @simple)
    end

    test "a reference pointing at nothing" do
      r = row(%{key: "k", recorded_at: ~U[2026-01-01 00:00:01.000000Z], approval_id: "gone"})

      refute Approval.approved?(r, @simple),
             "a dangling reference IS unapproved — the safe reading, and it must not raise"
    end

    test "an approval covering a version the row never had" do
      approve(SimpleApproval, %{id: "a1", recorded_at: ~U[2026-01-01 00:00:09.000000Z]})
      r = row(%{key: "k", recorded_at: ~U[2026-01-01 00:00:01.000000Z], approval_id: "a1"})

      refute Approval.approved?(r, @simple)
    end

    test "a row with no version cannot be approved" do
      # Both nil would compare EQUAL, which would read as approved — the one
      # false positive this comparison could produce, and the worst direction.
      approve(SimpleApproval, %{id: "a1", recorded_at: nil})
      r = row(%{key: "k", recorded_at: nil, approval_id: "a1"})

      refute Approval.approved?(r, @simple),
             "nil == nil must not read as a sign-off"
    end
  end

  describe "the library knows nothing about the scheme" do
    test "a two-reviewer approval works identically" do
      approve(DualControlApproval, %{
        id: "d1",
        recorded_at: ~U[2026-01-01 00:00:01.000000Z],
        first_reviewer: "tom",
        second_reviewer: "sam",
        decision: :approved,
        expires_at: ~U[2030-01-01 00:00:00Z]
      })

      r = row(%{key: "k1", recorded_at: ~U[2026-01-01 00:00:01.000000Z], approval_id: "d1"})

      assert Approval.approved?(r, @dual)
    end

    test "the library does not read `decision` — a rejection is the host's to not write" do
      approve(DualControlApproval, %{id: "d1", recorded_at: ~U[2026-01-01 00:00:01.000000Z], decision: :rejected})
      r = row(%{key: "k1", recorded_at: ~U[2026-01-01 00:00:01.000000Z], approval_id: "d1"})

      assert Approval.approved?(r, @dual),
             "the contract is EXISTENCE plus a matching version. A host that models " <>
               "rejection writes no row, or filters before referencing one — reading " <>
               "`decision` here would make the host's vocabulary the library's business"
    end

    test "an expired approval is likewise the host's business" do
      approve(DualControlApproval, %{
        id: "d1",
        recorded_at: ~U[2026-01-01 00:00:01.000000Z],
        expires_at: ~U[2000-01-01 00:00:00Z]
      })

      r = row(%{key: "k1", recorded_at: ~U[2026-01-01 00:00:01.000000Z], approval_id: "d1"})

      assert Approval.approved?(r, @dual),
             "expiry is a rule about WHO may approve and for how long — the same " <>
               "category as who may sign at all, and equally not read here"
    end
  end

  describe "covering/2" do
    test "returns the approval even when it has lapsed" do
      approve(SimpleApproval, %{id: "a1", recorded_at: ~U[2026-01-01 00:00:01.000000Z], approved_by: "tom"})
      moved = row(%{key: "k1", recorded_at: ~U[2026-01-01 00:00:02.000000Z], approval_id: "a1"})

      refute Approval.approved?(moved, @simple)

      assert %{approved_by: "tom", recorded_at: ~U[2026-01-01 00:00:01.000000Z]} = Approval.covering(moved, @simple),
             "a reviewer whose sign-off lapsed needs to see who approved WHAT — " <>
               "which is precisely the case approved?/2 answers false for"
    end

    test "nil when there is nothing to show" do
      assert is_nil(Approval.covering(row(%{key: "k", recorded_at: ~U[2026-01-01 00:00:00.000000Z]}), @simple))
    end
  end
end

defmodule ReactiveDag.Fixtures.ApprovalGraph do
  @moduledoc """
  Approvals: a human sign-off that is itself a node, timestamped against the
  state it approved — so a later change makes the approval stale.
  """

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      resource ReactiveDag.Fixtures.ApprovalGraph.SimpleApproval
      resource ReactiveDag.Fixtures.ApprovalGraph.DualControlApproval
      resource ReactiveDag.Fixtures.ApprovalGraph.Events
    end
  end

  # A one-signature scheme.
  defmodule SimpleApproval do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.ApprovalGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: []

    postgres do
      table "fx_ap_simple_approval"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :id, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :recorded_at, :utc_datetime_usec, public?: true
      attribute :approved_by, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]
      create :upsert, upsert?: true, accept: [:id, :recorded_at, :approved_by]
    end
  end

  # A two-reviewer scheme with a decision and an expiry — a DIFFERENT shape,
  # sharing only `recorded_at`. If the library could tell these apart, it would
  # be constraining a host's rules about who may approve what.
  defmodule DualControlApproval do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.ApprovalGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: []

    postgres do
      table "fx_ap_dual_control_approval"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :id, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :recorded_at, :utc_datetime_usec, public?: true
      attribute :first_reviewer, :string, public?: true
      attribute :second_reviewer, :string, public?: true
      attribute :decision, :atom, public?: true
      attribute :expires_at, :utc_datetime, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :upsert,
        upsert?: true,
        accept: [:id, :recorded_at, :first_reviewer, :second_reviewer, :decision, :expires_at]
    end
  end

  defmodule Events do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.ApprovalGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: []

    postgres do
      table "fx_ap_events"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :outcome, :string, public?: true
      attribute :recorded_at, :utc_datetime_usec, public?: true
      attribute :approval_id, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]
      create :upsert, upsert?: true, accept: [:key, :outcome, :recorded_at, :approval_id]
    end
  end

  def tables, do: ["fx_ap_simple_approval", "fx_ap_dual_control_approval", "fx_ap_events"]
end

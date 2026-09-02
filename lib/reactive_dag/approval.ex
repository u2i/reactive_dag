defmodule ReactiveDag.Approval do
  @moduledoc """
  SIGNING OFF ON A VERSION — two columns and one comparison.

  A row points at the approval that covers it. The approval says which version
  it covered. Whether the sign-off still applies is a comparison:

      approved?(row) = row.<reference>.version_id == row.version_id

  The content moves, the versions stop matching, the sign-off lapses. Nothing
  had to erase it.

  ## Why the approval carries the version, and not the row a flag

  A flag needs something to clear it on every path that writes the row — a
  hook, an invalidation pass, a trigger — and the failure mode of a missed
  clear is a row that reads as reviewed when nobody reviewed *that content*.
  Silent, and in the direction of falsely claiming review.

  An equality that stops holding cannot be forgotten. That is the whole
  argument, and it is why this module is thirty lines rather than a state
  machine.

  ## The library knows nothing about approval schemes

  One signature, two reviewers in agreement, a decision with a recorded reason,
  an approval that expires — these are different HOST RESOURCES, not different
  configurations here. The contract is:

    * the approving resource has a `version_id`;
    * the approved node has a column pointing at it.

  Nothing else is read, so nothing else can be constrained. Different cells may
  therefore use entirely different schemes, and neither is aware of the other.

  **This is a constraint on future changes, not just a description.** The
  pressure to add `approved_by`, `approved_at`, `approval_note` to a
  library-owned notion will be constant, and every one of them would make a
  host's scheme the library's business.

  ## History arrives without being designed

  Approvals are records with their own identity. A re-approval writes a NEW one
  and the reference moves; the previous approval stays exactly where it was,
  naming the version it covered and whoever made it. Ordering them by time is
  the whole audit trail.

  ## What this does not do

  It does not release a suspension. A `:approval` suspension is cleared by
  `ReactiveDag.Suspension.discharge/1` and a resumption, the same as any other
  — see `release/3` below, which is the small amount of glue that connects the
  two.
  """

  require Logger

  alias ReactiveDag.{Cascade, Suspension}

  @doc """
  Is this row's sign-off current?

  `false` when the row has no approval reference, when the reference points at
  nothing, or when the approval covered a version the row has since moved past.
  Those three are deliberately one answer: from a consumer's side, "never
  approved" and "approved, then changed" are both *not approved now*.

  `spec` is `[via: :approval_id, resource: MyApp.Approval]` — the column
  holding the reference, and what it points at.
  """
  @spec approved?(struct(), keyword()) :: boolean()
  def approved?(row, spec) do
    with {:ok, ref} <- reference(row, spec),
         {:ok, approval} <- fetch(ref, spec) do
      version_of(approval) == version_of(row) and not is_nil(version_of(row))
    else
      _ -> false
    end
  end

  @doc """
  The approval covering this row, or `nil`.

  Returns it whether or not it is CURRENT — a reviewer looking at a row whose
  sign-off lapsed needs to see who approved what, which is exactly the case
  `approved?/2` answers `false` for.
  """
  @spec covering(struct(), keyword()) :: struct() | nil
  def covering(row, spec) do
    with {:ok, ref} <- reference(row, spec),
         {:ok, approval} <- fetch(ref, spec) do
      approval
    else
      _ -> nil
    end
  end

  @doc """
  Release a `:approval` suspension, and cascade onward from the approved row.

  This is the glue between an approval and the graph, and it is deliberately
  thin: approving is the HOST's action, on the host's own resource, with the
  host's own rules about who may do it. What the library contributes is what
  happens next — the suspension is discharged and the change it held resumes.

  Checks the sign-off is actually current before releasing. Approving version A
  and then releasing a row that has moved to version B would propagate content
  nobody signed, which is the one thing a gate exists to prevent — and it is a
  plausible mistake, because the two happen in different requests.

  Returns `{:ok, report}`, or `{:error, :not_approved}` when the sign-off is
  missing or lapsed.
  """
  @spec release(ReactiveDag.Plan.t(), map(), keyword()) ::
          {:ok, ReactiveDag.Report.t()} | {:error, :not_approved}
  def release(plan, point, opts) do
    row = Keyword.fetch!(opts, :row)
    spec = Keyword.fetch!(opts, :spec)

    if approved?(row, spec) do
      suspensions = Suspension.at(point)
      ids = Enum.map(suspensions, & &1.id)

      # `skip_gate` names the cell whose gate this clears. Without it the
      # cascade reaches the same gated cell and suspends again immediately —
      # the released change would produce a new suspension and nothing would
      # ever propagate.
      cell = Keyword.fetch!(opts, :cell)

      # No `feedback_lap:` handoff here, unlike `ResumptionWorker` —
      # deliberately. A release runs on a person's sign-off, and a human action
      # is an EXTERNAL event: the same thing that resets the loop accounting
      # everywhere else. An approval gate sitting inside a declared feedback
      # loop is bounded by the person at it — each lap costs a signature, which
      # is a stronger brake than any counter.

      result =
        Cascade.run(
          plan,
          [%{cell: cell, keys: keys_for(point), versions: versions_for(suspensions, point)}],
          Keyword.take(opts, [:tenant, :plan_mfa]) ++ [skip_gate: cell]
        )

      case result do
        {:ok, report} ->
          Suspension.transaction(fn -> Suspension.discharge(ids) end)
          {:ok, report}

        other ->
          other
      end
    else
      Logger.warning(
        "reactive_dag: refusing to release #{point.waiting} for #{point.row_uuid} — " <>
          "its sign-off is missing or covers a version the row has moved past. " <>
          "Releasing anyway would propagate content nobody approved."
      )

      {:error, :not_approved}
    end
  end

  # ---- reading the two columns, and nothing else ----

  defp reference(row, spec) do
    case Map.get(row, Keyword.fetch!(spec, :via)) do
      nil -> :error
      ref -> {:ok, ref}
    end
  end

  # A read failure is `false`, not a raise. A dashboard rendering a hundred rows
  # should not fall over because one reference dangles — and a dangling
  # reference IS "not approved", which is the safe reading.
  defp fetch(ref, spec) do
    resource = Keyword.fetch!(spec, :resource)

    case Ash.get(resource, ref, authorize?: false) do
      {:ok, approval} ->
        {:ok, approval}

      {:error, _} ->
        Logger.debug(fn ->
          "reactive_dag: approval #{inspect(ref)} on #{inspect(resource)} could not " <>
            "be read; treating the row as unapproved"
        end)

        :error
    end
  end

  defp version_of(record), do: Map.get(record, :version_id)

  defp keys_for(%{row_uuid: "*"}), do: ["*"]
  defp keys_for(%{row_uuid: uuid}), do: [uuid]

  defp versions_for(_suspensions, %{row_uuid: "*"}), do: %{}

  defp versions_for(suspensions, %{row_uuid: uuid}) do
    case Enum.find(suspensions, &(&1.version_id != "*")) do
      nil -> %{}
      %{version_id: vid} -> %{uuid => vid}
    end
  end
end

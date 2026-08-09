defmodule ReactiveDag.Attestation.Op do
  @moduledoc """
  The recompute of an ATTESTED cell — the derived view an `attested` combinator
  or a `gate:`d edge lowers to. Thin DB glue: read the three inputs, run the
  pure `ReactiveDag.Attestation.Evaluation`, write ordinary spine rows.

  The cell's meta carries `attested: %{over: raw_cell_id, requirement:
  %Requirement{}}` (resolved at graph assembly). Inputs are `[raw, eligibility,
  store leaf]` — so a scan, a role change, or a signing all dirty this cell and
  the drain recomputes it the same way.

  ## What it writes

  One spine row per admission, in the requirement's status vocabulary (default
  `covered` / `pending` / `refused`) — so verdict rollup, first-class coverage,
  and the freshness spine apply to attested views unchanged. Affirmed rows are
  put with `strength: "attested"` in the writer opts: the spine-only default
  writer drops it (strength is a host extension column), a host writer stamps
  it — which keeps "the machinery assigns the strength" inside the existing
  `CoordinationWriter` seam.

  Rows vanished from the raw cell are retired (delete): an attested view has no
  claim about data that no longer exists — the attestation RECORD survives in
  the store (append-only history), only the projection row goes.
  """
  @behaviour ReactiveDag.Op

  alias ReactiveDag.{Attestation, Op, Tuple}
  alias ReactiveDag.Attestation.{Evaluation, Requirement}

  @impl true
  def recompute(cell, _keys) do
    %{over: over, requirement: %Requirement{scope: :key} = req} = attested_meta(cell)

    raw_rows = Tuple.rows(over)
    stances = Attestation.stances(over)
    eligibility = Tuple.all_keys(to_string(req.signers))
    statuses = Requirement.statuses(req)

    admissions = Evaluation.evaluate(raw_rows, stances, eligibility, req, DateTime.utc_now())

    by_key = Map.new(admissions, fn %{scope: {:key, k}} = a -> {k, a} end)

    Tuple.reconcile(to_string(cell_id(cell)), Map.keys(by_key),
      upsert: fn key ->
        %{state: state} = by_key[key]

        opts =
          case state do
            :affirmed -> [status: statuses.affirmed, strength: "attested"]
            :pending -> [status: statuses.pending]
            :refused -> [status: statuses.refused]
          end

        # a writer that reports the changed boolean keeps propagation
        # O(real changes); with the spine-only default (:ok) every key
        # propagates — correct, just less scoped.
        Op.put(cell, key, opts) in [true, :ok]
      end
    )
  end

  defp attested_meta(%{meta: %{attested: a}}), do: a

  defp attested_meta(cell) do
    raise ArgumentError,
          "#{inspect(cell_id(cell))} reached Attestation.Op without attested meta — " <>
            "was the graph assembled by ReactiveDag.Node.graph/2 (which resolves requirements)?"
  end

  defp cell_id(%{id: id}), do: id
  defp cell_id(id) when is_binary(id), do: id
end

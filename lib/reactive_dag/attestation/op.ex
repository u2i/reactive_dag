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

  One spine row per admission, in the requirement's status vocabulary — so
  verdict rollup, first-class coverage, and the freshness spine apply to
  attested views unchanged. The mapping depends on the view's MODE
  (`spine_status/3`):

    * `:require` (blocking, the default) — `covered` / `pending` / `refused`.
      A not-yet-signed row is withheld: consumers of the signed set read
      `covered` and see nothing for it.
    * `:annotate` (non-blocking) — `covered` / `unsigned` / `refused`. Best
      effort: an unsigned row FLOWS, distinguished from signed rather than
      withheld. A rejection still bites — data someone said is WRONG is a
      different thing from data nobody has vouched for, and passing it through
      as best-effort would launder the objection.

  Affirmed rows are put with `strength: "attested"` in the writer opts: the
  spine-only default writer drops it (strength is a host extension column), a
  host writer stamps it — which keeps "the machinery assigns the strength"
  inside the existing `CoordinationWriter` seam.

  Rows vanished from the raw cell are retired (delete): an attested view has no
  claim about data that no longer exists — the attestation RECORD survives in
  the store (append-only history), only the projection row goes.
  """
  @behaviour ReactiveDag.Op

  alias ReactiveDag.{Attestation, Op, Tuple}
  alias ReactiveDag.Attestation.{Evaluation, Requirement}

  @impl true
  def recompute(cell, _keys) do
    meta = attested_meta(cell)
    %{over: over, requirement: %Requirement{scope: :key} = req} = meta
    mode = Map.get(meta, :mode, :require)

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
            :affirmed -> [status: spine_status(state, mode, statuses), strength: "attested"]
            _ -> [status: spine_status(state, mode, statuses)]
          end

        # a writer that reports the changed boolean keeps propagation
        # O(real changes); with the spine-only default (:ok) every key
        # propagates — correct, just less scoped.
        Op.put(cell, key, opts) in [true, :ok]
      end
    )
  end

  @doc """
  The admission → spine-status projection, per mode. Pure — the one place the
  blocking/non-blocking distinction lives (force evaluation is identical in
  both; a mode only changes what a not-yet-signed row projects to).
  """
  @spec spine_status(:affirmed | :pending | :refused, :require | :annotate, map()) :: String.t()
  def spine_status(:affirmed, _mode, statuses), do: statuses.affirmed
  def spine_status(:pending, :require, statuses), do: statuses.pending
  def spine_status(:pending, :annotate, statuses), do: statuses.unsigned
  def spine_status(:refused, _mode, statuses), do: statuses.refused

  defp attested_meta(%{meta: %{attested: a}}), do: a

  defp attested_meta(cell) do
    raise ArgumentError,
          "#{inspect(cell_id(cell))} reached Attestation.Op without attested meta — " <>
            "was the graph assembled by ReactiveDag.Node.graph/2 (which resolves requirements)?"
  end

  defp cell_id(%{id: id}), do: id
  defp cell_id(id) when is_binary(id), do: id
end

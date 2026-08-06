defmodule ReactiveDag.Verdict do
  @moduledoc """
  The generic READ layer over the coordination-tuple spine — a cell's live verdict
  and its failing-sample, rolled up from the tuple `status` histogram. This is the
  engine piece a host used to hand-write (the portal's `ModelEval.Verdict`); it's
  domain-neutral, so it lives here and the host keeps only its own addressing
  sugar (`for_guarantee`, `for_control`, typed `detail` joins) on top.

  A verdict is:

      %{status: :green | :findings | :pending | :unknown,
        failing: non_neg_integer(), pending: non_neg_integer(), sample: [key]}

  computed from the cell's `status_histogram` + a failing-key sample, both over
  the shared `ReactiveDag.Tuple` spine (so it honors the configured `tuple_table`
  and the `:key_scope` selector — a host scopes to an app/tenant with the same
  selector shapes the tuple reads accept).
  """

  @type verdict :: %{
          status: :green | :findings | :pending | :unknown,
          failing: non_neg_integer(),
          pending: non_neg_integer(),
          sample: [String.t()]
        }

  @doc """
  The live verdict for `cell_id`. Options:

    * `:key_scope` — a `t:ReactiveDag.Tuple.key_scope/0` narrowing to a subset of
      the cell's keys (e.g. one app/tenant); nil = the whole cell.
    * `:sample_limit` — cap on the failing-key sample (default 5).

  `total == 0` → `:unknown` (nothing evaluated — distinct from green). Any failing
  → `:findings`; else any pending/stale → `:pending`; else `:green`.
  """
  @spec for_cell(String.t(), keyword()) :: verdict()
  def for_cell(cell_id, opts \\ []) do
    scope = Keyword.get(opts, :key_scope)
    limit = Keyword.get(opts, :sample_limit, 5)

    counts = ReactiveDag.Tuple.status_histogram(cell_id, key_scope: scope)
    sample = ReactiveDag.Tuple.keys_by_status(cell_id, ["failing"], limit: limit, key_scope: scope)

    rollup(counts, sample)
  end

  @doc """
  Roll a `%{status => count}` histogram + a failing sample into a verdict — the
  status decision, exposed so a host can roll up counts it gathered another way.
  """
  @spec rollup(%{String.t() => non_neg_integer()}, [String.t()]) :: verdict()
  def rollup(counts, sample) do
    failing = Map.get(counts, "failing", 0)
    pending = Map.get(counts, "pending", 0) + Map.get(counts, "stale", 0)
    total = counts |> Map.values() |> Enum.sum()

    status =
      cond do
        total == 0 -> :unknown
        failing > 0 -> :findings
        pending > 0 -> :pending
        true -> :green
      end

    %{status: status, failing: failing, pending: pending, sample: sample}
  end

  @doc """
  Roll a MAP of per-cell verdicts into a single status (e.g. a control over its
  guarantees): findings if any has findings, else pending if any pending, else
  green if any is green, else unknown. The dual of `for_cell` at the group level.
  """
  @spec roll_group([verdict()]) :: :green | :findings | :pending | :unknown
  def roll_group(verdicts) do
    statuses = Enum.map(verdicts, & &1.status)

    cond do
      :findings in statuses -> :findings
      :pending in statuses -> :pending
      :green in statuses -> :green
      true -> :unknown
    end
  end
end

defmodule ReactiveDag.Verdict do
  @moduledoc """
  A cell's live verdict: the one-word answer, rolled up from its rows' statuses.

  This is the engine piece a host used to hand-write (the portal's
  `ModelEval.Verdict`); it's domain-neutral, so it lives here and the host keeps
  only its own addressing sugar (`for_guarantee`, `for_control`, typed `detail`
  joins) on top.

  A verdict is:

      %{status: :green | :findings | :pending | :unknown,
        failing: non_neg_integer(), pending: non_neg_integer(), sample: [key]}

  computed from the cell's status histogram plus a failing-key sample, both read
  from the node's own resource through `ReactiveDag.Node.Rows`.

  ## Scoping to a subset

  This used to read the coordination tuple, which offered a `:key_scope`
  selector — prefix and glob shapes over the serialized cell key — because a
  key was all the tuple had to filter on. A resource has columns, so scoping is
  now an ordinary Ash read: filter the rows yourself and hand the result to
  `rollup/2`.

      MyApp.CategoryHealth
      |> Ash.Query.filter(tenant_id == ^tenant)
      |> Ash.read!()
      |> Enum.frequencies_by(& &1.status)
      |> ReactiveDag.Verdict.rollup(sample)

  That is more expressive than the selector was — any column, not just a key
  prefix — and it honors policies, which a raw tuple query never did.
  """

  alias ReactiveDag.Node.Rows

  @type verdict :: %{
          status: :green | :findings | :pending | :unknown,
          failing: non_neg_integer(),
          pending: non_neg_integer(),
          sample: [String.t()]
        }

  @doc """
  The live verdict for `cell`. Options:

    * `:sample_limit` — cap on the failing-key sample (default 5).

  `total == 0` → `:unknown` (nothing evaluated — distinct from green). Any failing
  → `:findings`; else any pending/stale → `:pending`; else `:green`.

  Takes a `%ReactiveDag.Cell{}` rather than a cell id: the rows live in the
  node's resource, and the cell is what knows which resource that is. Get one
  from `plan.cells[id]`.
  """
  @spec for_cell(ReactiveDag.Cell.t(), keyword()) :: verdict()
  def for_cell(%ReactiveDag.Cell{} = cell, opts \\ []) do
    limit = Keyword.get(opts, :sample_limit, 5)

    rollup(
      Rows.status_histogram(cell),
      Rows.keys_by_status(cell, ["failing"], limit: limit)
    )
  end

  @doc """
  Roll a `%{status => count}` histogram + a failing sample into a verdict — the
  status decision, exposed so a host can roll up counts it gathered another way.
  """
  @spec rollup(%{(String.t() | nil) => non_neg_integer()}, [String.t()]) :: verdict()
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

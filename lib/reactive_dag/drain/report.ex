defmodule ReactiveDag.Drain.Report do
  @moduledoc """
  What a drain ACTUALLY did — the processing trace, returned by
  `ReactiveDag.Drain.run/2`.

  The drain already knows everything worth recording as it works: which cell
  it claimed, what the recompute reported changed, which cell's propagation
  caused the work, how long each step took. This struct is that knowledge kept
  instead of discarded — one entry per recompute step, in execution
  (topological) order, plus run-level totals.

    * `steps` — one `t:step/0` per cell recompute, in execution order:
        * `:cell` — the cell id that recomputed
        * `:pass` — the drain-loop iteration the step ran in
        * `:claimed` — the dirty keys claimed (`["*"]` = whole cell)
        * `:changed` — the keys the recompute reported as actually changed
        * `:triggered_by` — the cell whose propagation dirtied this one
          (`nil` for the seeded frontier), reconstructing the causal tree
        * `:duration_us` — microseconds the recompute took
    * `passes` — drain-loop iterations (≥ `length(steps)`; a pass with an
      empty claim recomputes nothing)
    * `duration_us` — wall time of the whole drain

  Persistence is deliberately NOT here: the report is a value. A host that
  wants a durable processing log stores it where its runs already live (an
  Oban job's meta, a run table) — the library reports; the host records.
  """

  @type step :: %{
          cell: String.t(),
          pass: non_neg_integer(),
          claimed: [String.t()],
          changed: [String.t()],
          triggered_by: String.t() | nil,
          duration_us: non_neg_integer()
        }

  @type t :: %__MODULE__{
          steps: [step()],
          passes: non_neg_integer(),
          duration_us: non_neg_integer()
        }

  defstruct steps: [], passes: 0, duration_us: 0

  @doc "The distinct cells the drain recomputed, in first-touched order."
  @spec cells(t()) :: [String.t()]
  def cells(%__MODULE__{steps: steps}), do: steps |> Enum.map(& &1.cell) |> Enum.uniq()

  @doc "Total keys reported changed across every step."
  @spec changed_total(t()) :: non_neg_integer()
  def changed_total(%__MODULE__{steps: steps}),
    do: steps |> Enum.map(&length(&1.changed)) |> Enum.sum()

  @doc """
  The causal tree as `%{cell => triggered_by}` — `nil` roots are the seeded
  frontier. A cell recomputed more than once keeps its LAST cause (matching
  the drain's own bookkeeping).
  """
  @spec causes(t()) :: %{String.t() => String.t() | nil}
  def causes(%__MODULE__{steps: steps}),
    do: Map.new(steps, &{&1.cell, &1.triggered_by})
end

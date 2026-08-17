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
        * `:meta` — whatever the recompute strategy reported about the work
          (`%{}` when it reported nothing). The library never interprets it:
          token/cost counts for an LLM node, cache hits, retries and rows
          scanned are all just keys. A strategy opts in by returning
          `{:ok, changed, meta}`. A count may be reported flat
          (`tokens_in: 1600`) or broken down (`tokens_in: %{"model-a" => 1200,
          "model-b" => 400}`) — see `total/2` and `by/2`.
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
          duration_us: non_neg_integer(),
          op: atom() | nil,
          depth: non_neg_integer() | nil,
          meta: map()
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

  @doc """
  Merge one key out of every step's `meta`, summing numeric values — the
  roll-up a cost line wants (`total(report, :tokens_in)`). Steps whose meta
  lacks the key contribute nothing.

  A step may report the key **broken down** instead of flat, as a map of
  `%{bucket => number}`:

      %{tokens_in: %{"claude-haiku-4-5" => 1200, "claude-sonnet-4-6" => 400}}

  Those still total to a single number here, so a cost line does not have to
  know which shape a node reports. `by/2` returns the breakdown instead.

  The library does not interpret the buckets — a bucket is a model name only
  because a host chose to key by one. Mixing shapes across steps is fine: a
  graph where one node reports per-model tokens and another reports a bare
  count totals correctly rather than refusing to show a number.

  The arithmetic is `ReactiveDag.Rollup`, shared with
  `ReactiveDag.Source.detail_total/2` — a scan and a drain answer "what did this
  cost" identically because it is one fold, not two that must agree.
  """
  @spec total(t(), atom()) :: number()
  def total(%__MODULE__{steps: steps}, key),
    do: steps |> Enum.map(& &1[:meta]) |> ReactiveDag.Rollup.total(key)

  @doc """
  One key out of every step's `meta`, summed **per bucket** — the breakdown
  behind `total/2`.

      Report.by(report, :tokens_in)
      #=> %{"claude-haiku-4-5" => 1200, "claude-sonnet-4-6" => 400}

  This is what a cost line needs that a single number cannot give: models
  differ in price by an order of magnitude, so one summed token count cannot be
  turned into a cost, nor say which model is driving spend.

  Steps reporting the key as a bare number are collected under `:unattributed`
  rather than dropped — a node that reports tokens without saying which model
  produced them is a gap worth SEEING, and silently omitting it would make the
  breakdown disagree with `total/2` for no visible reason.

      Report.by(report, :tokens_in)
      #=> %{"claude-haiku-4-5" => 1200, unattributed: 90}

  Sums of the returned values always equal `total/2` for the same key. Returns
  `%{}` when no step reported the key at all.
  """
  @spec by(t(), atom()) :: %{optional(String.t() | atom()) => number()}
  def by(%__MODULE__{steps: steps}, key),
    do: steps |> Enum.map(& &1[:meta]) |> ReactiveDag.Rollup.by(key)

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

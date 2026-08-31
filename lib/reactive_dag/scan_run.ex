defmodule ReactiveDag.ScanRun do
  @moduledoc """
  What one scan DID — the poll and the drain it triggered, as a single value.

  A scan is two phases that always happen together and were reported
  separately. The poll says what it fetched, what it could not reach and what it
  cost; the drain returns a `ReactiveDag.Report` saying what recomputed
  downstream. A host wanting "what happened when this ran" had to collect both
  from one telemetry payload and know which half answered which question.

  This is the pair, named. `ReactiveDag.ScanWorker` builds one per scan and puts
  it on `[:reactive_dag, :scan, :stop]`, so a broadcast, a durable row or a log
  line takes one value instead of reassembling it.
  `ReactiveDag.Insights.record/1` retains it whole for the same reason — a host
  unwrapping to `run.report` throws away the poll, which is most of what the
  scan did and all of what it could not reach.

  ## It does not merge the phases

  Deliberately. A poll is fallible network I/O that must not abort a sweep when
  one upstream is down; a drain is pure recomputation that must RAISE rather
  than mark keys clean over work that did not happen. Those two rules cannot
  live in one loop, and nothing here tries to make them.

  What was genuinely duplicated was the reporting: two vocabularies for "what
  did this cost", answered by `ReactiveDag.Rollup` for both. This is the same
  move one level up — one value for "what did this run do".

  ## The drain may be absent

  `report` is `nil` for a scan that never drained: an unscannable source (no
  credential, integration not enabled) is a completed scan that found nothing,
  and a host recording scan results still wants the row. `drained?/1` says
  which, rather than making every caller test for nil.
  """

  alias ReactiveDag.Report

  @typedoc """
  The poll half — what `ReactiveDag.Source.refresh/3` returned, plus the cell it
  belongs to.
  """
  @type t :: %__MODULE__{
          cell: String.t() | :sweep,
          changed: [String.t()],
          unreachable: [{String.t(), term()}],
          detail: map(),
          report: Report.t() | nil,
          not_scannable: term() | nil,
          duration_us: non_neg_integer()
        }

  defstruct cell: nil,
            changed: [],
            unreachable: [],
            detail: %{},
            report: nil,
            not_scannable: nil,
            duration_us: 0

  @doc """
  Did the poll find anything?

  Distinct from `drained?/1`: a poll can change keys whose recompute produced
  nothing downstream, and a drain can run over marks another source left.
  """
  @spec changed?(t()) :: boolean()
  def changed?(%__MODULE__{changed: changed}), do: changed != []

  @doc """
  Did a drain run at all?

  `false` for an unscannable source, which completes without draining. A host
  rendering "0 passes" for that would be reporting a drain that never happened.
  """
  @spec drained?(t()) :: boolean()
  def drained?(%__MODULE__{report: %Report{}}), do: true
  def drained?(%__MODULE__{}), do: false

  @doc """
  Could the poll see everything it meant to?

  The honest-gap discipline in one predicate: a scan that could not look must
  never render as a scan that found nothing.
  """
  @spec complete?(t()) :: boolean()
  def complete?(%__MODULE__{unreachable: []}), do: true
  def complete?(%__MODULE__{}), do: false

  @doc """
  Sum one cost key across BOTH phases.

      ScanRun.total(run, :tokens_in)

  This is the question a scan's cost line actually asks, and until now it took
  two calls and an addition: the crawl's own spend lives in `detail` and its
  downstream recomputes' spend lives in the report's steps. A crawler that
  classifies documents with a model and feeds nodes that summarise them with
  another spends in both places, and neither number alone is the bill.

  `ReactiveDag.Rollup` does the arithmetic, so a flat count and a per-model
  breakdown both total, mixed freely across the two phases.
  """
  @spec total(t(), atom()) :: number()
  def total(%__MODULE__{} = run, key), do: run |> metas() |> ReactiveDag.Rollup.total(key)

  @doc """
  One cost key across both phases, summed **per bucket**.

      ScanRun.by(run, :tokens_in)
      #=> %{"claude-haiku-4-5" => 900, "openai/gpt-5.6-luna" => 3000}

  The breakdown behind `total/2`, and the only form that answers "which model is
  driving this" when the poll and the drain use different ones — which is the
  normal case, since a classifier and a summariser are chosen separately.
  """
  @spec by(t(), atom()) :: %{optional(String.t() | atom()) => number()}
  def by(%__MODULE__{} = run, key), do: run |> metas() |> ReactiveDag.Rollup.by(key)

  # The poll's `detail` and every drain step's `meta`, as one list — the two
  # places a scan can report cost from. Both are plain meta maps, which is what
  # lets one fold serve both.
  defp metas(%__MODULE__{detail: detail, report: report}) do
    [detail | steps_meta(report)]
  end

  defp steps_meta(%Report{steps: steps}), do: Enum.map(steps, & &1[:meta])
  defp steps_meta(_), do: []
end

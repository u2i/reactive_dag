defmodule ReactiveDag.PopOrderTest do
  @moduledoc """
  WHICH PENDING CELL RUNS NEXT, and why it is a correctness question.

  The rule: never pop a cell while a cell it transitively depends on is still
  pending. `Cascade.shallowest/2` asks exactly that, over inputs minus feedback
  edges.

  It used to ask for the minimum `plan.depths` instead. Depth is a valid witness
  — it strictly increases along every non-feedback edge — but not the rule, and
  the difference is measurable: on the host's 35-cell plan, of 595 cell pairs
  only 123 are genuinely comparable, and depth imposed an order on 388 of the
  472 that are not. Seven cells shared depth 1, ordered by map iteration.

  The case that makes this correctness rather than tidiness is a DERIVED context
  provider, which `context_edge_test.exs` does not cover — its provider is a
  leaf, where nothing can run out of order.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.{Cascade, Cell, Graph}

  defmodule Ran do
    def child_spec(_), do: %{id: __MODULE__, start: {__MODULE__, :start_link, [[]]}}
    def start_link(_ \\ []), do: Agent.start_link(fn -> [] end, name: __MODULE__)
    def note(id), do: Agent.update(__MODULE__, &[id | &1])
    def order, do: Agent.get(__MODULE__, &Enum.reverse/1)
  end

  defmodule Op do
    @behaviour ReactiveDag.Op
    @impl true
    def recompute(cell, keys) do
      ReactiveDag.PopOrderTest.Ran.note(cell.id)
      {:ok, keys}
    end
  end

  # A loop tail that ANSWERS ONCE. Echoing keys instead — as `Op` does — makes
  # the loop dishonest, and the feedback budget correctly refuses it; this op is
  # about pop ORDER, so it settles on the second ask.
  defmodule AnswersOnce do
    @behaviour ReactiveDag.Op
    @impl true
    def recompute(cell, keys) do
      ReactiveDag.PopOrderTest.Ran.note(cell.id)
      if "k1" in keys, do: {:ok, ["k2"]}, else: {:ok, []}
    end
  end

  setup do
    start_supervised!(Ran)
    start_supervised!(ReactiveDag.Test.FakeSuspensionRepo)
    ReactiveDag.Test.FakeSuspensionRepo.install()
    :ok
  end

  defp c(id, inputs, meta \\ %{}),
    do: %Cell{id: id, op: :test, inputs: inputs, leaf?: inputs == [], meta: meta}

  test "a DERIVED context provider settles before its reader — the case depth was hiding" do
    # leaf ──→ provider ──(context)──→ reader
    #   └────────────────────────────→ reader
    #
    # The context edge is an INPUT of reader but propagates NOTHING: provider's
    # parents are empty, so a provider change claims nobody. If reader ran first
    # it would read a pre-cascade provider and never be re-queued — a stale value
    # baked into a derived row with nothing to correct it.
    #
    # Ordering is the whole of what prevents that, which is why the pop rule is
    # a correctness concern and not a performance one.
    plan =
      Graph.build([
        c("leaf", []),
        c("provider", ["leaf"], %{compute: Op}),
        c("reader", ["leaf", "provider"], %{compute: Op, context_inputs: ["provider"]})
      ])

    assert Map.get(plan.parents, "provider", []) == [],
           "a context edge must not propagate — otherwise this test proves nothing"

    {:ok, _} = Cascade.run(plan, [%{cell: "leaf", keys: ["k1"]}])

    assert Ran.order() == ["provider", "reader"],
           "the reader must not run before the provider it reads: #{inspect(Ran.order())}"
  end

  test "cells the graph leaves INCOMPARABLE pop in a reproducible order" do
    # Any order among incomparable cells is CORRECT — nothing requires either to
    # precede the other — but it must be the same every run, or a cascade's step
    # trace cannot be compared between runs.
    #
    # This needs more than 32 pending cells to mean anything. Erlang maps below
    # that size are flat and iterate in sorted key order, so a smaller graph is
    # reproducible whether or not the tie-break exists; above it they become
    # hash tries and iteration follows the hash. (Measured: 8 keys iterate
    # sorted, 40 keys start "cell_19", "cell_9", "cell_40".) A prior claim that
    # step order was already unreproducible on the host's 35-cell plan was
    # therefore wrong for its 7-cell levels — only a wide cascade reaches the
    # threshold, and this test is the one that does.
    names = for i <- 1..40, do: "cell_#{i}"

    plan =
      Graph.build([c("leaf", []) | for(n <- names, do: c(n, ["leaf"], %{compute: Op}))])

    {:ok, _} = Cascade.run(plan, [%{cell: "leaf", keys: ["k1"]}])

    assert Ran.order() == Enum.sort(names),
           "40 incomparable cells must pop in cell-id order, not hash order"
  end

  test "a diamond's apex still runs ONCE, with both legs settled" do
    # The property depth was originally introduced for, restated over the rule
    # that actually implies it.
    plan =
      Graph.build([
        c("leaf", []),
        c("left", ["leaf"], %{compute: Op}),
        c("right", ["leaf"], %{compute: Op}),
        c("apex", ["left", "right"], %{compute: Op})
      ])

    {:ok, _} = Cascade.run(plan, [%{cell: "leaf", keys: ["k1"]}])

    assert Enum.count(Ran.order(), &(&1 == "apex")) == 1
    assert List.last(Ran.order()) == "apex"
  end

  test "a feedback edge does NOT block its own loop entry" do
    # The exclusion, stated as behaviour: counting the back-edge would make the
    # loop entry wait for a cell derived from itself, and nothing would ever run.
    plan =
      Graph.build([
        c("docs", []),
        c("entry", ["docs", "tail"], %{compute: Op, feedback_inputs: ["tail"]}),
        c("mid", ["entry"], %{compute: Op}),
        c("tail", ["mid"], %{compute: AnswersOnce})
      ])

    {:ok, _} = Cascade.run(plan, [%{cell: "docs", keys: ["k1"]}])

    # One trip round: docs → entry → mid → tail announces k2 → entry again.
    # The entry is never blocked BY the tail, and it keeps its place at the head
    # of the loop rather than being ordered below the cell it feeds.
    assert Ran.order() == ["entry", "mid", "tail", "entry", "mid", "tail"]
  end
end

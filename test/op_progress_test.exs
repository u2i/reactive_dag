defmodule ReactiveDag.OpProgressTest do
  @moduledoc """
  `Op.progress/3` — the only signal from inside one recompute.

  The drain-side counterpart to `Source.progress/3`, and it exists for the same
  reason: an op is opaque to the library, so a cell extracting 34 meetings through
  an LLM emits ONE `:drain, :step` and it fires when the work is already over.

  `:cell_start` says a cell BEGAN, which names the slow cell. This says how far
  through it is — the difference between "recomputing meeting_events" for four
  minutes and "meeting_events · 12/34 meetings".
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.Op

  setup do
    parent = self()

    :telemetry.attach(
      "op-progress-test",
      [:reactive_dag, :cascade, :progress],
      fn _e, measurements, metadata, _ -> send(parent, {:progress, measurements, metadata}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach("op-progress-test") end)
    :ok
  end

  test "a count, with the cell it belongs to" do
    # A drain runs many cells; a consumer showing per-row progress needs to know
    # whose progress this is.
    Op.progress(12, 34, cell: "meeting_events", label: "meetings")

    assert_receive {:progress, %{done: 12, total: 34},
                    %{cell: "meeting_events", label: "meetings"}}
  end

  test "a total that is not yet known" do
    # An op still discovering how much there is. A count without a denominator is
    # still better than silence.
    Op.progress(3)

    assert_receive {:progress, %{done: 3, total: nil}, _}
  end

  test "a PHASE — the label alone, no count" do
    # Same shape as `Source.progress/3`'s phase: for the parts of a recompute with
    # no denominator.
    Op.progress(nil, nil, cell: "meeting_events", label: "loading transcripts")

    assert_receive {:progress, %{done: nil, total: nil}, %{label: "loading transcripts"}}
  end

  test "emitting with no handler attached is not an error" do
    # Per UNIT, not per batch — so this runs once per meeting in a real extraction
    # and must cost nothing when nobody is listening.
    :telemetry.detach("op-progress-test")

    assert :ok = Op.progress(1, 1)
  end
end

defmodule ReactiveDag.SourceProgressTest do
  @moduledoc """
  `Source.progress/3` — the only signal from INSIDE one poll.

  A scanner is opaque to the library: handed options, returns a result, and
  everything between is the host's. So a crawl of 700 documents emits one
  `:source_stop`, and it fires when the crawl is already over — the wrong end for
  anything a person is waiting on.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.Source

  setup do
    handler = "progress-test-#{System.unique_integer([:positive])}"
    test = self()

    :telemetry.attach(
      handler,
      [:reactive_dag, :scan, :progress],
      fn _e, measurements, metadata, _ -> send(test, {:progress, measurements, metadata}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)
    :ok
  end

  test "reports done and total" do
    Source.progress(34, 721, cell: "meeting_docs", label: "documents")

    assert_receive {:progress, %{done: 34, total: 721},
                    %{cell: "meeting_docs", label: "documents"}}
  end

  test "total may be nil — a crawl still discovering pages has a count, not a ratio" do
    # Better than silence: "fetched 34 so far" is progress even without a
    # denominator, and a scanner that waited until it knew the total would report
    # nothing during the discovery it is doing.
    Source.progress(34)

    assert_receive {:progress, %{done: 34, total: nil}, _}
  end

  test "the cell is optional but load-bearing when present" do
    # A sweep polls several sources at once; a consumer showing progress on a row
    # needs to know whose progress it is.
    Source.progress(1, 2, cell: "wwtp_docs")

    assert_receive {:progress, _, %{cell: "wwtp_docs"}}
  end

  test "a PHASE reports without a count — the label is the whole message" do
    # The parts of a poll with no denominator: a crawl counts documents while it
    # fetches, then reclassifies and writes each leaf with nothing to count.
    #
    # Reporting only the countable part leaves a page holding a frozen `34/34`
    # through the slowest phase, which reads as a hang — the number says the work is
    # finished and the poll is plainly still running.
    Source.progress(nil, nil, cell: "meeting_docs", label: "writing 3 leaves")

    assert_receive {:progress, %{done: nil, total: nil},
                    %{cell: "meeting_docs", label: "writing 3 leaves"}}
  end

  test "a label rides with a counted event too" do
    # So a consumer can say "34/721 documents" rather than "34/721".
    Source.progress(34, 721, cell: "meeting_docs", label: "documents")

    assert_receive {:progress, %{done: 34, total: 721}, %{label: "documents"}}
  end

  test "emitting with no handler attached is not an error" do
    # Per DOCUMENT, not per batch — so this runs 700 times in a real crawl and
    # must cost nothing when nobody is listening.
    :telemetry.detach("progress-test-none")

    assert :ok = Source.progress(1, 1)
  end
end

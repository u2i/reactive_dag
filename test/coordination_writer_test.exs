defmodule ReactiveDag.CoordinationWriterTest do
  @moduledoc """
  The coordination-write seam: an op calls ReactiveDag.Op.put/tombstone/delete,
  which route to the host-configured CoordinationWriter. Tested with a fake
  writer that records calls (no DB — the spine SQL is proven by the host suites).
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.{Cell, Op}

  defmodule FakeWriter do
    @behaviour ReactiveDag.CoordinationWriter
    @impl true
    def put(cell, key, opts), do: send(self(), {:put, cell.id, key, opts})
    @impl true
    def delete(cell, keys), do: send(self(), {:delete, cell.id, keys})
    @impl true
    def tombstone(cell, keys), do: send(self(), {:tombstone, cell.id, keys})
  end

  # a writer WITHOUT tombstone/2 — Op.tombstone must fall back to delete.
  defmodule DeleteOnlyWriter do
    @behaviour ReactiveDag.CoordinationWriter
    @impl true
    def put(cell, key, _opts), do: send(self(), {:put, cell.id, key})
    @impl true
    def delete(cell, keys), do: send(self(), {:delete_fallback, cell.id, keys})
  end

  setup do
    prev = Application.get_env(:reactive_dag, :coordination_writer)
    on_exit(fn -> Application.put_env(:reactive_dag, :coordination_writer, prev) end)
    :ok
  end

  defp cell, do: %Cell{id: "budget_vs_actual", op: :fold}

  test "Op.put routes to the configured writer, carrying the cell id + host opts" do
    Application.put_env(:reactive_dag, :coordination_writer, FakeWriter)
    Op.put(cell(), "A1010|2025", source_ref: %{"va" => "A1010|2025"})
    assert_received {:put, "budget_vs_actual", "A1010|2025", source_ref: %{"va" => "A1010|2025"}}
  end

  test "Op.delete routes to the writer" do
    Application.put_env(:reactive_dag, :coordination_writer, FakeWriter)
    Op.delete(cell(), ["k1", "k2"])
    assert_received {:delete, "budget_vs_actual", ["k1", "k2"]}
  end

  test "Op.tombstone uses the writer's tombstone when it has one" do
    Application.put_env(:reactive_dag, :coordination_writer, FakeWriter)
    Op.tombstone(cell(), ["gone"])
    assert_received {:tombstone, "budget_vs_actual", ["gone"]}
  end

  test "Op.tombstone falls back to delete when the writer has no tombstone/2" do
    Application.put_env(:reactive_dag, :coordination_writer, DeleteOnlyWriter)
    Op.tombstone(cell(), ["gone"])
    assert_received {:delete_fallback, "budget_vs_actual", ["gone"]}
  end

  test "the default writer is the spine-only ReactiveDag.Tuple.Writer" do
    Application.delete_env(:reactive_dag, :coordination_writer)
    assert ReactiveDag.CoordinationWriter.writer() == ReactiveDag.Tuple.Writer
  end
end

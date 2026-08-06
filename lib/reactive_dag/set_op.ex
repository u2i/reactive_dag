defmodule ReactiveDag.SetOp do
  @moduledoc """
  A generic `RecomputeStrategy` for SET-BASED ops — the layering for hosts whose
  recompute is set algebra over the coordination tuple (the compliance portal),
  the counterpart to `ReactiveDag.Node.Recompute` for per-key/BEAM hosts.

  The insight the portal-skin spike surfaced: a set-based host's recompute is a
  MECHANICAL dispatch — `cell.op` → a SQL template taking the cell id + its input
  cell ids + the dirty keys — where only the SQL bodies are domain-specific. This
  module owns that dispatch frame (scope dirty keys, handle leaves, look up the
  op-kind's template, call it); the host supplies a TEMPLATE REGISTRY, so its
  `RecomputeStrategy` collapses from a hand-written dispatch table to config + the
  templates it already has.

  A template is `(cell, dirty_keys | nil) -> {:ok, [changed_key]}` — the host's
  set-based SQL (e.g. the portal's `Recompute.reconcile/product/…`) wrapped to
  take the cell (so it can read `cell.inputs` + `cell.meta`). The registry is
  `%{op_atom => template_fun}`, supplied via
  `config :reactive_dag, set_op_templates: MyApp.templates()`.

  Why the SQL stays host-side: the templates read/write the host's tuple TABLE
  with its EXTENSION columns (`strength`, …) — the codomain law (proving → SQL).
  The library owns *which* template runs and the leaf/scope handling; the host
  owns *what the SQL is*.
  """
  @behaviour ReactiveDag.RecomputeStrategy

  require Logger
  alias ReactiveDag.Cell

  @impl true
  def recompute(%Cell{leaf?: true} = cell, keys) do
    # a leaf's tuples were written by its source; its changed keys ARE its dirty
    # keys (or, whole-cell, its current keys via the shared spine).
    if "*" in keys, do: {:ok, ReactiveDag.Tuple.all_keys(cell.id)}, else: {:ok, keys}
  end

  def recompute(%Cell{op: op} = cell, keys) do
    scoped = if "*" in keys, do: nil, else: keys

    case Map.get(templates(), op) do
      fun when is_function(fun, 2) ->
        fun.(cell, scoped)

      nil ->
        Logger.warning("reactive_dag: no set_op template for op #{inspect(op)}; no-op")
        {:ok, []}
    end
  end

  @doc "The host's op-kind → template registry (`%{op => (cell, dirty | nil) -> {:ok, changed}}`)."
  @spec templates() :: %{atom() => (Cell.t(), [String.t()] | nil -> {:ok, [String.t()]})}
  def templates, do: Application.get_env(:reactive_dag, :set_op_templates, %{})
end

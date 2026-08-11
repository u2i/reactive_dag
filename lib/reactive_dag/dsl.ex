defmodule ReactiveDag.Dsl do
  @moduledoc """
  The DSL compile pipeline over the flat `Cell` IR — the resolve→lower→validate
  machinery both host DSLs share, parameterized by app hooks so each keeps its
  own domain vocabulary.

  A host compiles by handing `compile/2` its **named roots** (each a `{id, node}`
  where `node` is a nested op-expression in the host's own node structs) plus a
  `hooks` map:

    * the `Lowering` callbacks (`classify / legs / leg_id / ref_id / to_cell`) —
      how to walk a node into cells. `ref_id` is where BY-NAME resolution lives:
      a ref returns the id of the node it points at (the portal's register faces
      resolve `ref(:people, role: :rows)` → `"reg:people/rows"`). No object
      inlining, no shared-identity dedup.

    * `validate` (optional) — `(cells -> :ok | {:error, message})` for DOMAIN
      checks the substrate can't know (the portal's guarantee/scenario/addresses
      id checks). Runs after the structural checks below.

  `compile/2` lowers every root via `Lowering.walk`, unions the cells (a shared
  node appears once — it's a named root; refs are edges; a walk yielding the
  same id twice is DEDUPED, first wins), then validates:

    1. structural — every input/ref names a real cell; acyclic (delegated to
       `Graph.build`, which raises on a dangle/cycle). The duplicate-id check
       bites only on `validate_cells/2`'s hand-built lists — `compile/2` has
       already collapsed duplicates by design.
    2. domain — the host's `validate` hook.

  Returns `{:ok, cells}` or `{:error, message}`; a host transformer turns an
  error into a compile-time `Spark.Error.DslError`.
  """

  alias ReactiveDag.{Graph, Lowering}

  @type root :: {String.t(), term()}
  @type hooks :: %{
          required(:lowering) => Lowering.callbacks(),
          optional(:validate) => (list() -> :ok | {:error, String.t()})
        }

  @spec compile([root()], hooks()) :: {:ok, [term()]} | {:error, String.t()}
  def compile(roots, hooks) do
    cb = Map.fetch!(hooks, :lowering)

    cells =
      roots
      |> Enum.flat_map(fn {id, node} -> elem(Lowering.walk(id, node, cb), 1) end)
      |> dedup_by_id()

    validate_cells(cells, hooks[:validate])
  end

  @doc """
  Validate an ALREADY-assembled cell list (for a host that builds cells itself
  rather than handing op-expression roots): structural checks (unique ids +
  inputs resolve + acyclic) then the optional domain hook. `{:ok, cells}` |
  `{:error, message}`.
  """
  @spec validate_cells([term()], (list() -> :ok | {:error, String.t()}) | nil) ::
          {:ok, [term()]} | {:error, String.t()}
  def validate_cells(cells, domain_validate \\ nil) do
    with :ok <- validate_structural(cells),
         :ok <- run_domain(cells, domain_validate) do
      {:ok, cells}
    end
  end

  # A named node reached as a ref from several parents is still emitted once (as
  # its own root); if a host's walk yields the same id twice, keep the first.
  defp dedup_by_id(cells) do
    {kept, _seen} =
      Enum.reduce(cells, {[], MapSet.new()}, fn cell, {acc, seen} ->
        id = cell_id(cell)
        if MapSet.member?(seen, id), do: {acc, seen}, else: {[cell | acc], MapSet.put(seen, id)}
      end)

    Enum.reverse(kept)
  end

  # Structural validation: unique ids + inputs resolve + acyclic. Graph.build
  # re-checks dangling/cycle and raises ArgumentError → surface as {:error, …}.
  defp validate_structural(cells) do
    ids = Enum.map(cells, &cell_id/1)

    with :ok <- unique_ids(ids) do
      try do
        _ = Graph.build(cells)
        :ok
      rescue
        e in ArgumentError -> {:error, e.message}
      end
    end
  end

  defp unique_ids(ids) do
    case ids -- Enum.uniq(ids) do
      [] -> :ok
      dupes -> {:error, "duplicate cell ids: #{inspect(Enum.uniq(dupes))}"}
    end
  end

  defp run_domain(_cells, nil), do: :ok
  defp run_domain(cells, fun) when is_function(fun, 1), do: fun.(cells)

  # cells may be ReactiveDag.Cell or a host's own struct — both expose `.id`.
  defp cell_id(%{id: id}), do: id
end

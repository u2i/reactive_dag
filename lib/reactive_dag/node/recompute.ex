defmodule ReactiveDag.Node.Recompute do
  @moduledoc """
  A GENERIC `ReactiveDag.RecomputeStrategy` for graphs declared with
  `ReactiveDag.Node`. Because `Node` standardizes where a cell's op module lives
  — `cell.meta.compute`, a `ReactiveDag.Op` — the dispatch is uniform and the
  host no longer hand-writes it:

      ReactiveDag.Drain.run(plan,
        recompute: ReactiveDag.Node.Recompute,
        key_rule:  ReactiveDag.Node.KeyRule)

  This is the per-key-Elixir shape (the op is a behaviour module the host
  supplies). A host whose recompute is set-based SQL keyed by `cell.op` (the
  compliance portal) still writes its own strategy; this generic one serves the
  common "meta.compute is a ReactiveDag.Op module" case, of which cascade is the
  archetype.

  A LEAF (or a cell with no compute) passes its claimed keys through as changed —
  a leaf's tuples were written by its source; if it reaches recompute at all, its
  claimed keys already ARE its changes.
  """
  @behaviour ReactiveDag.RecomputeStrategy

  require Logger
  alias ReactiveDag.Cell

  @impl true
  def recompute(%Cell{leaf?: true}, keys), do: {:ok, keys}

  # a declarative `reduce` combinator (the common fold) — run it generically:
  # read `over` → group_by → into → upsert each (host writes payload + reports
  # changed) → Op.put the changed keys. The author wrote only group/reduce/upsert.
  def recompute(%Cell{meta: %{reduce: %{} = r}} = cell, _keys) do
    changed =
      r.read.(r.over)
      |> Enum.group_by(r.group_by)
      |> Enum.flat_map(fn {group, items} ->
        key = r.key.(group)
        row = r.into.(group, items)

        if r.upsert.(key, row) do
          ReactiveDag.Op.put(cell, key)
          [key]
        else
          []
        end
      end)

    {:ok, changed}
  end

  def recompute(%Cell{meta: %{compute: nil}, id: id}, keys) do
    Logger.warning("reactive_dag: node #{inspect(id)} has no compute module; passing keys through")
    {:ok, keys}
  end

  def recompute(%Cell{meta: %{compute: op}} = cell, keys) when is_atom(op) and not is_nil(op) do
    op.recompute(cell, keys)
  end

  # a cell whose meta carries no :compute key at all (e.g. a non-Node plan) —
  # treat like a leaf: pass through.
  def recompute(%Cell{}, keys), do: {:ok, keys}
end

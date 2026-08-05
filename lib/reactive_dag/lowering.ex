defmodule ReactiveDag.Lowering do
  @moduledoc """
  Shared machinery for lowering a NESTED op-expression into a flat cell list —
  the recursion both host DSLs independently grew (cascade's `Lower.resolve_legs`,
  the portal's `Graph.build_node`). Same algorithm: walk an op's legs, recurse
  into each, a `ref` resolves to an existing cell-id (no new cell), a nested op
  becomes an intermediate cell whose inputs are the recursed leg ids.

  The generic walk is parameterized by three host callbacks so each DSL keeps its
  own id-naming and domain fields:

    * `leg_id.(parent_id, index, leg)` → the id for a nested leg at `index`.
      (portal: `"\#{parent}/\#{i}"`; cascade: `"\#{parent}$\#{type}\#{i}"` or an
      explicit `:as`.)
    * `ref_id.(ref_leg)` → the cell-id a `ref` points at (no new cell emitted).
    * `to_cell.(id, node, input_ids)` → build the host's cell for a node given
      its resolved input ids. Returns whatever cell struct the host uses — the
      walk never inspects it, only collects it — so a host that keeps its own
      Cell type (cascade) works as well as one using `ReactiveDag.Cell`. For a
      leaf, `input_ids` is `[]`.

  A leg is classified by `classify.(leg)` → `:ref | :op | :leaf`. `:op` legs
  recurse; `:ref`/`:leaf` are terminal (a leaf still gets a cell; a ref does not).

  Returns `{root_id, cells}` — the expression's root cell-id and every cell it
  emitted (intermediates + the root, in dependency order). The host collects
  these across all its named nodes and hands the union to `Graph.build/1`.
  """

  @type node_kind :: :ref | :op | :leaf

  @type cell :: term()
  @type callbacks :: %{
          classify: (term() -> node_kind()),
          legs: (term() -> [term()]),
          leg_id: (String.t(), non_neg_integer(), term() -> String.t()),
          ref_id: (term() -> String.t()),
          to_cell: (String.t(), term(), [String.t()] -> cell())
        }

  @doc """
  Lower one node rooted at `id` into `{root_id, cells}`, recursing through its
  op-legs per the host `cb` callbacks. `ref` legs resolve to an existing id and
  emit no cell; `op` legs recurse; `leaf` legs emit a terminal cell.
  """
  @spec walk(String.t(), term(), callbacks()) :: {String.t(), [cell()]}
  def walk(id, node, cb) do
    case cb.classify.(node) do
      :ref ->
        {cb.ref_id.(node), []}

      :leaf ->
        {id, [cb.to_cell.(id, node, [])]}

      :op ->
        legs = cb.legs.(node)

        {input_ids, sub} =
          legs
          |> Enum.with_index()
          |> Enum.map_reduce([], fn {leg, i}, acc ->
            leg_id = cb.leg_id.(id, i, leg)
            {lid, lsub} = walk(leg_id, leg, cb)
            {lid, acc ++ lsub}
          end)

        {id, sub ++ [cb.to_cell.(id, node, input_ids)]}
    end
  end
end

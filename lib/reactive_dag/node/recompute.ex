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

  # a declarative REDUCE combinator — read `over` → group_by → into each group.
  # `into` returns ONE row (a fold) or a LIST of rows (a group → many "expand").
  # A single row's key comes from `key.(group)`; list rows must carry their own
  # `:key` (see key resolution in rows_with_keys/3).
  def recompute(%Cell{meta: %{reduce: %{} = r}} = cell, _keys) do
    pairs =
      r.read.(r.over)
      |> Enum.group_by(r.group_by)
      |> Enum.flat_map(fn {group, items} ->
        group |> r.into.(items) |> rows_with_keys(r, group)
      end)

    {:ok, materialize(cell, pairs, r.upsert)}
  end

  # a declarative JOIN combinator — read `over` into a LEFT and RIGHT index
  # (both `%{join_key => item}`), then for each left key emit `into.(jk, left,
  # right_or_nil)` (a left join; right may be absent). One row per left key.
  def recompute(%Cell{meta: %{join: %{} = j}} = cell, _keys) do
    items = j.read.(j.over)
    left = index(items, j.left)
    right = index(items, j.right)

    pairs =
      Enum.flat_map(left, fn {jk, litem} ->
        j.into.(jk, litem, Map.get(right, jk)) |> rows_with_keys(j, jk)
      end)

    {:ok, materialize(cell, pairs, j.upsert)}
  end

  # a PURE-ASH-QUERY aggregate — the datastore groups + aggregates the `over`
  # relationship. The node's resource (`meta.resource`) is the group's resource: read it
  # with the relationship aggregates loaded (ONE Ash query; Postgres does the
  # GROUP BY), and each parent row's aggregate values become its payload. This is a
  # WHOLE-CELL recompute (a GROUP BY reprices every group; there's no per-dirty-key
  # scoping), so the changed set is every group whose aggregate value moved.
  def recompute(%Cell{meta: %{aggregate: %{} = agg, resource: resource}} = cell, _keys)
      when not is_nil(resource) do
    {:ok, ReactiveDag.Node.Recompute.Aggregate.recompute(cell, resource, agg)}
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

  # normalize an `into` result (one row or a list) into `[{key, row}]`. Key
  # resolution: a row that carries its own `:key` field SELF-IDENTIFIES (expand
  # rows must, since one group → many keys); otherwise the spec's `key` fn is
  # applied to the group term (the fold case, one row per group). Row-key wins so
  # the `length == 1` ambiguity (a group that expands to exactly one row) is moot.
  defp rows_with_keys(row_or_rows, spec, group_term) do
    key_fn = Map.get(spec, :key)

    row_or_rows
    |> List.wrap()
    |> Enum.map(fn row ->
      key =
        cond do
          is_map(row) and is_map_key(row, :key) -> row.key
          is_function(key_fn, 1) -> key_fn.(group_term)
          true -> raise "reactive_dag: cannot resolve a coordination key for row #{inspect(row)}"
        end

      {key, row}
    end)
  end

  # write each {key, row}, Op.put the changed keys, return them. Shared by
  # reduce/expand + join. Three write modes, in precedence order:
  #   * VERDICT node (`verdict? true`) — no payload; the row's `:status`/`:strength`
  #     go straight into the tuple (Op.put opts). The result IS the coordination row.
  #   * host `upsert:` callback (override) — `(key, row) -> changed?`.
  #   * the LIB closing the loop into the node's OWN resource (`meta.resource`).
  defp materialize(%Cell{meta: %{verdict: true}} = cell, pairs, _upsert) do
    # a verdict-only node: the computed row lives in the tuple, not a payload table.
    Enum.map(pairs, fn {key, row} ->
      ReactiveDag.Op.put(cell, key, verdict_opts(row))
      key
    end)
  end

  defp materialize(cell, pairs, upsert) do
    write = writer_fn(cell, upsert)

    Enum.flat_map(pairs, fn {key, row} ->
      if write.(key, row) do
        ReactiveDag.Op.put(cell, key)
        [key]
      else
        []
      end
    end)
  end

  # a verdict row's tuple opts — status/strength if the combinator set them.
  defp verdict_opts(row) when is_map(row) do
    [status: row[:status], strength: row[:strength]]
    |> Enum.reject(fn {_k, v} -> is_nil(v) end)
  end

  defp verdict_opts(_), do: []

  # `(key, row) -> changed?`. An explicit `upsert:` wins; otherwise the row is
  # written into the node's own resource (the unified "resource IS payload" shape).
  defp writer_fn(_cell, upsert) when is_function(upsert, 2), do: upsert

  defp writer_fn(%Cell{meta: meta, id: id}, nil) do
    case meta[:resource] do
      nil ->
        raise """
        reactive_dag: node #{inspect(id)} has a reduce/join with no `upsert:`, no
        backing resource, and is not `verdict? true`. Either give the node an
        AshPostgres resource (its rows ARE its payload), mark it `verdict? true` (its
        result lives in the coordination tuple), or supply an explicit `upsert:`.
        """

      resource ->
        key_attr = meta[:payload_key] || :key
        action = meta[:payload_action] || :upsert
        fn key, row -> ReactiveDag.Node.Payload.upsert(resource, key_attr, key, row, action) == :changed end
    end
  end

  # index items by a side's key fn; `key_fn.(item)` returns the join key, or nil
  # for an item not on this side (so a single input can be split into left/right).
  defp index(items, key_fn) do
    for item <- items, jk = key_fn.(item), not is_nil(jk), into: %{}, do: {jk, item}
  end
end

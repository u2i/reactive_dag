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
  alias ReactiveDag.Node.Recompute.{Declarative, Read}

  @impl true
  def recompute(%Cell{leaf?: true}, keys), do: {:ok, keys}

  # a declarative REDUCE combinator — read `over` → group_by → into each group.
  # `into` returns ONE row (a fold) or a LIST of rows (a group → many "expand").
  # A single row's key comes from `key.(group)`; list rows must carry their own
  # `:key` (see key resolution in rows_with_keys/3).
  #
  # SCOPING: `read` may be arity-1 (`over -> items`, whole-cell) or arity-2
  # (`over, dirty_keys -> items`) so a host can scope the datastore read to the
  # claimed keys — e.g. `read: fn :fiscal_lines, keys -> FiscalDoc |> filter(keys) end`.
  # `keys` is the claimed dirty set, or `nil` for a whole-cell recompute (`"*"`).
  def recompute(%Cell{meta: %{reduce: %{} = r}} = cell, keys) do
    group_by = Declarative.group_fn(r.group_by)
    key_fn = Declarative.key_fn(r.key, r.key_prefix)

    # which slot emits a group's [{key, row}] pairs — the verifier guarantees
    # exactly the right one is declared for the node's shape.
    emit =
      cond do
        is_function(r.status, 2) ->
          fn group, items -> [{key_fn.(group), status_row(r.status.(group, items))}] end

        is_function(r.expand, 2) ->
          fn group, items -> expand_pairs(r.expand.(group, items), cell.id) end

        true ->
          into = Declarative.into_fn(r.into, r.group_by)
          fn group, items -> into_pair(into.(group, items), key_fn, group) end
      end

    pairs =
      Read.items(cell.meta[:over_source], r.over, r.query, scope(keys), auto_scope(cell, keys))
      |> Enum.group_by(group_by)
      |> Enum.flat_map(fn {group, items} -> emit.(group, items) end)

    {:ok, materialize(cell, pairs, r.upsert)}
  end

  # a declarative JOIN combinator — read `over` into a LEFT and RIGHT index
  # (both `%{join_key => item}`), then for each left key emit `into.(jk, left,
  # right_or_nil)` (a left join; right may be absent). With `outer: true`,
  # right-only keys ALSO emit (`into.(jk, nil, right)`) — the full-outer
  # reconcile shape, where an undeclared right-side member is a finding.
  # `read` may be arity-2 for dirty-key scoping (see `reduce` above).
  def recompute(%Cell{meta: %{join: %{} = j}} = cell, keys) do
    items = Read.items(cell.meta[:over_source], j.over, j.query, scope(keys), auto_scope(cell, keys))
    left = index(items, Declarative.side_fn(j.left))
    right = index(items, Declarative.side_fn(j.right))
    key_fn = Declarative.key_fn(j.key, j.key_prefix)

    emit =
      if is_function(j.status, 3) do
        fn jk, l, r -> [{key_fn.(jk), status_row(j.status.(jk, l, r))}] end
      else
        into = Declarative.join_into_fn(j.into)
        fn jk, l, r -> into_pair(into.(jk, l, r), key_fn, jk) end
      end

    left_pairs =
      Enum.flat_map(left, fn {jk, litem} -> emit.(jk, litem, Map.get(right, jk)) end)

    right_only_pairs =
      if Map.get(j, :outer, false) do
        Enum.flat_map(right, fn {jk, ritem} ->
          if Map.has_key?(left, jk), do: [], else: emit.(jk, nil, ritem)
        end)
      else
        []
      end

    {:ok, materialize(cell, left_pairs ++ right_only_pairs, j.upsert)}
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

  # the ASH-NATIVE escape hatch: a GENERIC action on the node's own resource
  # (`run :recompute_keys`). The action does its own DOMAIN writes and returns
  # the changed keys; the library passes only the arguments the action declares
  # (keys / cell_id) and closes the coordination loop with Op.put — an action
  # has no %Cell{} to put through, so coordination stays the library's job,
  # exactly as in the payload loop. MUST precede the compute-nil clause: a run
  # node's meta carries compute: nil too.
  def recompute(%Cell{meta: %{run: action, resource: resource}} = cell, keys)
      when is_atom(action) and not is_nil(action) and not is_nil(resource) do
    params =
      %{keys: scope(keys), cell_id: cell.id}
      |> Map.take(declared_args(resource, action))

    case resource |> Ash.ActionInput.for_action(action, params) |> Ash.run_action() do
      {:ok, changed} when is_list(changed) ->
        Enum.each(changed, &ReactiveDag.Op.put(cell, &1))
        {:ok, changed}

      {:ok, other} ->
        raise "reactive_dag: run action #{inspect(action)} on #{inspect(resource)} must " <>
                "return the changed keys (`{:array, :string}`), got: #{inspect(other)}"

      {:error, error} ->
        raise "reactive_dag: run action #{inspect(action)} on #{inspect(resource)} failed: " <>
                Exception.message(error)
    end
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

  # the arguments a `run` action declares — the library passes only these.
  defp declared_args(resource, action) do
    case Ash.Resource.Info.action(resource, action) do
      %{arguments: args} -> Enum.map(args, & &1.name)
      nil -> []
    end
  end

  # the library's automatic read scope, by the cell's key-rule GRAIN:
  #   :identity       → the claimed keys ARE the over's keys: filter payload_key.
  #   {:bucket, kind} → the claimed keys are bucket labels: filter the over's
  #                     date attribute to the claimed buckets' range HULL (a
  #                     superset read is still closed over groups — extra
  #                     buckets recompute and change-detect to nothing). Needs
  #                     the over's group calculation to be a Calendar bucket of
  #                     the same kind (stamped as over_source.bucket_scopes);
  #                     otherwise no auto scope.
  #   anything else   → no auto scope: a grain-changing host rule's claims must
  #                     not filter the child-grain read; `query:` still receives
  #                     them for host-grain scoping.
  defp auto_scope(%Cell{meta: meta}, keys) do
    case meta[:key_rule] do
      :identity -> keyed_scope(keys)
      nil -> keyed_scope(keys)
      {:bucket, kind} -> bucket_hull(meta[:over_source], kind, scope(keys))
      :group -> group_attr_scope(meta, scope(keys))
      _ -> nil
    end
  end

  # `:group` claims are group labels; when assembly proved the group is one
  # plain string attribute with default keys (over_source.group_scope_attr),
  # the labels ARE that attribute's values (minus any key_prefix) — filter it.
  defp group_attr_scope(_meta, nil), do: nil

  defp group_attr_scope(meta, labels) do
    with %{group_scope_attr: attr} when not is_nil(attr) <- meta[:over_source] do
      prefix = get_in(meta, [:reduce]) |> then(&(&1 && Map.get(&1, :key_prefix)))

      values =
        case prefix do
          nil -> labels
          p -> Enum.map(labels, &String.replace_prefix(&1, p <> "|", ""))
        end

      {:attr, attr, values}
    else
      _ -> nil
    end
  end

  defp keyed_scope(keys) do
    case scope(keys) do
      nil -> nil
      claimed -> {:keys, claimed}
    end
  end

  defp bucket_hull(_source, _kind, nil), do: nil

  defp bucket_hull(%{bucket_scopes: scopes}, kind, labels) when is_map(scopes) do
    with attr when not is_nil(attr) <- scopes[kind],
         ranges = Enum.map(labels, &ReactiveDag.Calendar.range(kind, &1)),
         false <- :error in ranges do
      {froms, tos} = Enum.unzip(ranges)
      {:range, attr, Enum.min(froms, Date), Enum.max(tos, Date)}
    else
      _ -> nil
    end
  end

  defp bucket_hull(_source, _kind, _labels), do: nil

  # the claimed keys as a read scope: `nil` for a whole-cell recompute (`"*"`),
  # else the specific dirty keys a scoped `read` can filter its query to.
  defp scope(keys) do
    cond do
      is_nil(keys) -> nil
      "*" in keys -> nil
      true -> keys
    end
  end

  # a `status:` result → the verdict row (`{status, strength}` when the host's
  # tuple carries strength).
  defp status_row({status, strength}), do: %{status: status, strength: strength}
  defp status_row(status), do: %{status: status}

  # an `into:` result is ONE row: its own `:key` wins (a self-identified row),
  # else the key derives from the group term / join key. A list here is the
  # old expand overload — point at the `expand:` slot.
  defp into_pair(rows, _key_fn, _term) when is_list(rows) do
    raise "reactive_dag: `into:` returns ONE row per group — the group → many-rows " <>
            "shape is the `expand:` slot (each row carrying its own :key)"
  end

  defp into_pair(row, key_fn, term) when is_map(row) do
    key = if is_map_key(row, :key), do: row.key, else: key_fn.(term)
    [{key, row}]
  end

  # `expand:` rows fan one group out to many keys, so each row must self-key.
  defp expand_pairs(rows, cell_id) when is_list(rows) do
    Enum.map(rows, fn
      %{key: key} = row ->
        {key, row}

      row ->
        raise "reactive_dag: #{cell_id}: an `expand:` row must carry its own :key " <>
                "(one group fans out to many keys) — got #{inspect(row)}"
    end)
  end

  defp expand_pairs(other, cell_id) do
    raise "reactive_dag: #{cell_id}: `expand:` must return a LIST of rows, got #{inspect(other)}"
  end

  # write each {key, row}, Op.put the changed keys, return them. Shared by
  # reduce/expand + join. Three write modes, in precedence order:
  #   * VERDICT node (`verdict? true`) — no payload; the row's `:status`/`:strength`
  #     go straight into the tuple (Op.put opts). The result IS the coordination row.
  #   * host `upsert:` callback (override) — `(key, row) -> changed?`.
  #   * the LIB closing the loop into the node's OWN resource (`meta.resource`).
  defp materialize(%Cell{meta: %{verdict: true}} = cell, pairs, _upsert) do
    # a verdict-only node: the computed row lives in the tuple, not a payload
    # table — so the tuple write IS the change detection. A writer reporting
    # the boolean CHANGED signal (the default Tuple.Writer does) scopes
    # propagation to real flips; a bare-:ok writer propagates everything
    # (correct, just less scoped).
    Enum.flat_map(pairs, fn {key, row} ->
      case ReactiveDag.Op.put(cell, key, verdict_opts(row)) do
        false -> []
        _ok_or_true -> [key]
      end
    end)
  end

  defp materialize(cell, pairs, upsert) do
    write = writer_fn(cell, upsert)
    coord = cell.meta[:coordination_opts]

    Enum.flat_map(pairs, fn {key, row} ->
      if write.(key, row) do
        # the coordination write goes through Op.put → the host CoordinationWriter,
        # so extension columns (source_ref, fingerprint, …) a node declares via
        # `coordination_opts:` are applied in the payload loop — not just the spine.
        ReactiveDag.Op.put(cell, key, coord_opts(coord, key, row))
        [key]
      else
        []
      end
    end)
  end

  defp coord_opts(nil, _key, _row), do: []
  defp coord_opts(fun, key, row) when is_function(fun, 2), do: fun.(key, row)

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

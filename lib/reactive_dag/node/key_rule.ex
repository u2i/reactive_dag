defmodule ReactiveDag.Node.KeyRule do
  @moduledoc """
  THE propagation rule: how a change reaches a parent, decided by what the parent
  DECLARED. `ReactiveDag.Drain` calls this — there is nothing to configure.

  `Node` records each node's `key_rule` in `cell.meta`; this reads it:

    * `:all`      — any input change escalates to a whole-cell recompute (the
      drain turns this into `"*"`). For aggregate / cross-range (reduce) cells.
    * `:identity` — a changed input key maps to the same output key (pass through).
      For key-local (map) cells. The default.

  A `recompute_by` unit lowers to the `:group` forms below, which are richer than
  either: they map a changed CHILD key to the parent UNIT it belongs to, so a
  fold reprices one group rather than escalating to the whole cell.

  `rule/3` takes the specific child input, not just the parent — so a node whose
  legs propagate differently (a change to the *members* leg passing keys through
  while a change to the *fn* leg escalates) is expressible without a bespoke
  module. That was the last thing a host wrote its own rule for.
  """
  alias ReactiveDag.Cell

  def rule(%Cell{meta: %{key_rule: :all}}, _child, _changed), do: :all

  # `{:group, from: :key}` — pure resolution: the key's leading `|`-segments carry
  # the group's INPUT fields in `group_by` order. No query, and deletion-safe: a
  # vanished key still names the group it left. A key that violates the grammar
  # degrades the propagation to `:all` — correctness over precision.
  #
  # The DIFF path answers the same question without a key grammar, and answers a
  # move better (both units, not one), so this rung is the narrow case where a
  # host wants resolution with no read at all.
  def rule(%Cell{meta: %{key_rule: {:group, _opts}} = meta}, _child, changed) do
    pure_group_claims(meta, changed)
  end

  # `:group` — a changed child ROW claims its group: the mapping is the very
  # `group_by`/side fields the combinator already declares, evaluated by
  # reading the changed rows (one scoped query per propagation). A changed key
  # the lookup can't find — a deleted row — degrades the propagation to :all:
  # vanish must reprice everything it might have left.
  def rule(%Cell{meta: %{key_rule: :group, side_sources: %{} = sides} = meta}, child, changed) do
    two_node_join_claims(meta[:join], sides, child, changed)
  end

  def rule(%Cell{meta: %{key_rule: :group} = meta}, _child, changed) do
    group_claims(meta[:reduce] || meta[:join], meta[:over_source], changed)
  end

  def rule(_parent, _child, changed), do: {:keys, changed}

  @doc """
  `rule/3` with the DIFF each changed key was marked with — both sides of the
  change, from whichever writer produced it.

  A diff survives its row and names where it went, so a `:group` claim stays
  precise in the two cases a live lookup cannot handle at all:

    * the row was **deleted** — nothing to read, but the diff still names the
      unit it belonged to;
    * the row **moved** between units — the live row names only where it landed,
      and BOTH units need repricing.

  There is no live read on this path. An earlier version carried only the prior
  side, so a move had to union the snapshot with a lookup of where the row landed
  — and that lookup was itself what degraded to `:all` on a delete.

  Keys with no diff (a source-fed leaf has no Ash row behind it) fall back to
  `rule/3`, so the two paths coexist.
  """
  @spec rule(Cell.t(), Cell.id(), [String.t()], %{String.t() => map()}) ::
          ReactiveDag.KeyRule.result()
  def rule(parent, child, changed, diffs) when map_size(diffs) == 0,
    do: rule(parent, child, changed)

  def rule(%Cell{meta: %{key_rule: :group} = meta} = parent, child, changed, diffs) do
    spec = meta[:reduce] || meta[:join]

    case diff_claims(spec, meta, changed, diffs) do
      :none ->
        rule(parent, child, changed)

      {:ok, from_diffs} ->
        # NO live read. A snapshot said only where a row WAS, so a move had to
        # union it with a lookup of where the row landed — and that lookup was
        # the thing that degraded to `:all` on a deleted row. A diff carries both
        # sides, so the union has nothing to add and the query is pure cost.
        {:keys, from_diffs}
    end
  end

  def rule(parent, child, changed, _diffs), do: rule(parent, child, changed)

  @doc """
  `rule/4` with the plan's OPTS — currently `:tenant`.

  A `:group` claim resolves by READING the changed rows, and that read has to be
  scoped: a tenanted resource refuses an unscoped one outright, so `:group`
  propagation raised for any host running a graph per tenant. `:identity` and
  `:all` never read, which is why this went unnoticed.

  A fifth arity rather than widening `rule/4`, for the reason `rule/4` itself was
  added rather than widening `rule/3`: the seam is public and hosts implement it.
  `ReactiveDag.Graph` calls the widest arity a module exports.
  """
  @spec rule(Cell.t(), Cell.id(), [String.t()], %{String.t() => map()}, keyword()) ::
          ReactiveDag.KeyRule.result()
  def rule(parent, child, changed, diffs, opts) do
    # The read scope travels in the process, not the argument list. Every clause
    # below reaches the two reads through several private functions that exist to
    # express the GROUPING, and threading a tenant through each would put the
    # plan's identity into a dozen signatures that are otherwise about fields.
    #
    # Set-and-restore rather than set-and-leave: `rule/5` is called once per
    # propagation edge on the drain's own process, which also runs recomputes.
    prev = Process.get(__MODULE__)
    Process.put(__MODULE__, opts)

    try do
      rule(parent, child, changed, diffs)
    after
      if prev, do: Process.put(__MODULE__, prev), else: Process.delete(__MODULE__)
    end
  end

  # The scope a `:group` lookup reads under — the plan's tenant, put there by
  # `rule/5`. Empty when a caller used an older arity, which is correct for an
  # untenanted host and is what every existing caller does.
  defp scope, do: Process.get(__MODULE__, [])

  # Derive each snapshotted key's unit by running the combinator's own grouping
  # against the stored row. Returns the keys derived, plus the changed keys that
  # had no snapshot (which still need the live path).
  defp diff_claims(nil, _meta, _changed, _diffs), do: :none

  # The DIFF each changed key was written with — both sides of the change, from
  # the payload write that produced it (`Payload.collecting_diffs/1`).
  #
  # This replaces a jsonb snapshot carried on the queue row, and answers strictly
  # more: a snapshot said where a row WAS, so a move needed it unioned with a live
  # read of where the row landed. A diff carries both, so there is nothing to read
  # and nothing to fail — see `ReactiveDag.Node.Diff`.
  #
  # FOUR routes still fall back to the live read, and each is a real case rather
  # than a gap left unfinished. Measured on a real host: 3 of its 4 `:group`
  # cells take the diff path, and the fourth needs the first of these.
  #
  #   * a `{:calc, _}` in the grain — a calculation is an Ash `expr` the
  #     datastore evaluates. The diff holds the ATTRIBUTES it derives from (a
  #     host's `side` comes from `kind`), but evaluating an expr in the BEAM
  #     against a bare map is a different capability from this module's.
  #   * a `%Join{}` rather than a `%Reduce{}` — its sides are picked by
  #     `side_fn/1`, not by a group plan, so the grain is not a field list.
  #   * a key with NO diff — a source-fed leaf has no Ash row behind it, so
  #     nothing captured one.
  #   * a diff that yields no unit — a nil in the row's own grain. Falling back
  #     wholesale beats claiming a partial set and stranding the rest.
  defp diff_claims(spec, meta, changed, diffs) do
    alias ReactiveDag.Node.Recompute.Declarative

    with %ReactiveDag.Node.Reduce{} = r <- spec,
         plan when is_list(plan) <- meta[:over_source][:group_key_plan],
         true <- Enum.all?(plan, &match?({:attr, _, _}, &1)) do
      key_fn = Declarative.key_fn(Map.get(r, :key), Map.get(r, :key_prefix))
      grain = Enum.map(plan, fn {:attr, name, _string?} -> name end)
      with_diffs = Enum.filter(changed, &Map.has_key?(diffs, &1))

      keys =
        with_diffs
        |> Enum.flat_map(&ReactiveDag.Node.Diff.units(diffs[&1], grain, key_fn))
        |> Enum.uniq()

      # A key whose diff yields no unit — a nil in its own grain — is no better
      # than no diff at all: fall back wholesale rather than claim a partial set
      # and strand the rest.
      if keys == [] and with_diffs != [], do: :none, else: {:ok, keys}
    else
      _ -> :none
    end
  end


  # A TWO-INPUT join: the changed keys belong to ONE side, so translate them
  # through THAT side only. Which side is the `child` that propagated — the same
  # edge the claim will be scoped by, so the translation and the read agree.
  #
  # Reading the other side's rows here would be wrong twice: its key column is a
  # different column, and its rows did not change.
  defp two_node_join_claims(nil, _sides, _child, _changed), do: :all

  defp two_node_join_claims(j, sides, child, changed) do
    case side_of(j, child) do
      nil ->
        :all

      side ->
        spec = if side == :left, do: j.left, else: j.right
        source = sides[side]

        # `Payload.lookup/1`, not `payload_key` directly. A keyless node's
        # `payload_key` is `false`, not nil — so a nil check passed it through and
        # the read built a filter from the literal `false`
        # ("No such field false for resource …", #223). This was the LAST place
        # consulting `payload_key` without the `false` case; every other path
        # already asked `lookup/1`.
        case source && ReactiveDag.Node.Payload.lookup(Map.to_list(source)) do
          nil -> :all
          {:key, attr} -> side_join_keys(spec, source, j, changed, attr)
          {:identity, fields} -> side_join_keys_by_fields(spec, source, j, changed, fields)
        end
    end
  end

  # Which side propagated. `nil` for a child that is NEITHER side — the caller
  # degrades to `:all` rather than guessing, since translating through the wrong
  # side would claim keys the change has nothing to do with.
  defp side_of(%{left_over: l, right_over: r}, child) when not is_nil(child) do
    c = to_string(child)

    cond do
      not is_nil(l) and to_string(l) == c -> :left
      not is_nil(r) and to_string(r) == c -> :right
      true -> nil
    end
  end

  defp side_of(_j, _child), do: nil

  # Read the changed rows of ONE side and map each to its join key. A key the
  # lookup cannot find is a DELETED row, and a vanished row must reprice
  # everything it might have left — the same `:all` degradation `group_claims/3`
  # makes, for the same reason.
  defp side_join_keys(spec, source, j, changed, key_attr) do
    source.resource
    |> Ash.Query.do_filter([{key_attr, [in: changed]}])
    |> read_side(source)
    |> side_claims(spec, j, changed)
  end

  # A KEYLESS side: no key column, so the changed keys are read back through the
  # columns that identify the row — the same decomposition `group_claims_by_fields/4`
  # performs, and the inverse of the join that built the key.
  #
  # Degrading to `:all` here would be sound and is what #223 proposed as the minimal
  # fix, but it reprices the whole cell because one row moved. A join side that
  # declares `row_key` has said exactly how to find its rows; this uses it.
  defp side_join_keys_by_fields(spec, source, j, changed, fields) do
    cols = Enum.reject(fields, &(&1 == tenant_attribute(source)))
    decoded = Enum.map(changed, &decode_key(&1, cols))

    if Enum.any?(decoded, &is_nil/1) do
      :all
    else
      filter = Enum.map(decoded, fn pairs -> [and: Enum.map(pairs, &[&1])] end)

      source.resource
      |> Ash.Query.do_filter(or: filter)
      |> read_side(source)
      |> side_claims(spec, j, changed)
    end
  end

  defp read_side(query, source) do
    query
    |> load_calcs(Map.get(source, :load, []))
    |> Ash.read!(scope())
  end

  defp side_claims(rows, spec, j, changed) do
    alias ReactiveDag.Node.Recompute.Declarative

    if length(rows) < length(changed) do
      :all
    else
      key_fn = Declarative.key_fn(Map.get(j, :key), Map.get(j, :key_prefix))
      side_fn = Declarative.side_fn(spec)

      keys =
        rows
        |> Enum.map(side_fn)
        |> Enum.reject(&(&1 in [nil, false]))
        |> Enum.map(key_fn)
        |> Enum.uniq()

      {:keys, keys}
    end
  end

  defp group_claims(nil, _source, _changed), do: :all
  defp group_claims(_spec, nil, _changed), do: :all

  # an identity-keyed over has no key column to look changed keys up by
  defp group_claims(_spec, %{payload_key: nil}, _changed), do: :all

  defp group_claims(spec, source, changed) do
    alias ReactiveDag.Node.Recompute.Declarative

    # A key column to filter by, or `:all`.
    #
    # `payload_key` is not enough on its own: it falls back to the resource's
    # single primary key, so a node identified by `row_key` COLUMNS rather than a
    # key column yields `:id` — and filtering a UUID column by a composite cell
    # key raises `InvalidFilterValue` (`id == "|FY25/26||"`). A host's reprocess
    # found exactly that.
    #
    # Such a node degrades to `:all` here rather than reading: this path is the
    # LIVE-READ fallback, and the precise answer for these nodes comes from the
    # diff path (`rule/4`), which needs no key column at all.
    case ReactiveDag.Node.Payload.lookup(Map.to_list(source)) do
      {:key, attr} ->
        group_claims_by_key(spec, source, changed, attr)

      {:identity, fields} ->
        # A KEYLESS over: no key column, so the changed keys are read back through
        # the columns they were built from. `:all` would be sound here and is what
        # an earlier revision did — but it reprices the whole cell because one row
        # moved, which is exactly the widening `recompute_by` exists to avoid, and a
        # host's test caught it.
        group_claims_by_fields(spec, source, changed, fields)
    end
  end

  # Read the changed rows by splitting each cell key across the columns that
  # identify the row — the same decomposition `Payload.retire/6` performs, and the
  # inverse of the join that built the key.
  #
  # A key whose arity does not match the columns is not this node's, and a row the
  # read cannot find is a DELETED row: both degrade to `:all`, since a vanished row
  # may have left any group.
  defp group_claims_by_fields(spec, source, changed, fields) do
    cols = Enum.reject(fields, &(&1 == tenant_attribute(source)))
    decoded = Enum.map(changed, &decode_key(&1, cols))

    if Enum.any?(decoded, &is_nil/1) do
      :all
    else
      filter = Enum.map(decoded, fn pairs -> [and: Enum.map(pairs, &[&1])] end)

      rows =
        source.resource
        |> Ash.Query.do_filter(or: filter)
        |> load_calcs(Map.get(source, :load, []))
        |> Ash.read!(scope())

      if length(rows) < length(changed), do: :all, else: claims_from(spec, rows)
    end
  end

  defp decode_key(key, cols) when is_binary(key) do
    values = String.split(key, "|")
    if length(values) == length(cols), do: Enum.zip(cols, values), else: nil
  end

  defp decode_key(_key, _cols), do: nil

  defp tenant_attribute(source) do
    case source[:resource] do
      nil ->
        nil

      resource ->
        if Ash.Resource.Info.multitenancy_strategy(resource) == :attribute,
          do: Ash.Resource.Info.multitenancy_attribute(resource)
    end
  end

  defp group_claims_by_key(spec, source, changed, key_attr) do
    alias ReactiveDag.Node.Recompute.Declarative

    rows =
      source.resource
      |> Ash.Query.do_filter([{key_attr, [in: changed]}])
      |> load_calcs(Map.get(source, :load, []))
      |> Ash.read!(scope())

    if length(rows) < length(changed), do: :all, else: claims_from(spec, rows)
  end

  # The parent UNITS the changed rows belong to — each row through the combinator's
  # own grouping, then named by its key fn. Shared by the keyed and keyless reads:
  # only HOW the rows are found differs, never what they mean.
  defp claims_from(spec, rows) do
    alias ReactiveDag.Node.Recompute.Declarative

    key_fn = Declarative.key_fn(Map.get(spec, :key), Map.get(spec, :key_prefix))

    keys =
      case spec do
        %ReactiveDag.Node.Reduce{} = r ->
          group_fn = Declarative.group_fn(r.group_by)
          Enum.map(rows, &key_fn.(group_fn.(&1)))

        %ReactiveDag.Node.Join{} = j ->
          left = Declarative.side_fn(j.left)
          right = Declarative.side_fn(j.right)

          rows
          |> Enum.flat_map(&[left.(&1), right.(&1)])
          |> Enum.reject(&(&1 in [nil, false]))
          |> Enum.map(key_fn)
      end

    {:keys, Enum.uniq(keys)}
  end

  defp load_calcs(query, []), do: query
  defp load_calcs(query, loads), do: Ash.Query.load(query, loads)

  defp pure_group_claims(meta, changed) do
    alias ReactiveDag.Node.Recompute.Declarative

    with %{} = spec <- meta[:reduce],
         %{group_key_plan: plan} when is_list(plan) <- meta[:over_source] do
      prefix = Map.get(spec, :key_prefix)

      # how reconstructed group values become THIS node's key: an
      # identity-keyed parent serializes its identity fields in primary-key
      # order (values arrive dest-named); else the default group-order join.
      to_key =
        case meta[:identity_fields] do
          fields when is_list(fields) ->
            dests = Declarative.group_dests(spec.group_by)
            id_key = Declarative.identity_key_fn(fields, prefix)
            fn values -> id_key.(Map.new(Enum.zip(dests, values))) end

          _ ->
            key_fn = Declarative.key_fn(nil, prefix)
            fn values -> key_fn.(List.to_tuple(values)) end
        end

      labels = Enum.map(changed, &key_from_segments(plan, &1, to_key))

      if :error in labels, do: :all, else: {:keys, Enum.uniq(labels)}
    else
      _ -> :all
    end
  end

  defp key_from_segments(plan, key, to_key) do
    segs = String.split(key, "|")

    if length(segs) < length(plan) do
      :error
    else
      values =
        plan
        |> Enum.zip(segs)
        |> Enum.map(fn
          {{:attr, _name, _string?}, seg} ->
            seg

          {{:calc, _name}, _seg} ->
            # an opaque calculation can't be evaluated from a segment
            :error
        end)

      if :error in values, do: :error, else: to_key.(values)
    end
  end
end

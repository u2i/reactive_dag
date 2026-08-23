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

  # `{:group, from: :key}` — pure resolution: the key's leading `|`-segments
  # carry the group's INPUT fields in group_by order (a plain attribute's
  # value; a Calendar calculation's raw date, relabeled through the calc's own
  # bucket). No query, and deletion-safe: a vanished key still names the group
  # it left. A key that violates the grammar degrades the propagation to :all
  # — correctness over precision.
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
  `rule/3` with the SNAPSHOTS the changed keys were marked with — the child rows
  as they were, which `ReactiveDag.Frontier` captured at mark time.

  A snapshot survives its row, so a `:group` claim stays precise in the two
  cases a live lookup cannot handle:

    * the row was **deleted** — nothing to read, but the snapshot still names
      the unit it belonged to;
    * the row **moved** between units — the live row names where it landed, the
      snapshot names where it came from, and BOTH need repricing.

  Keys with no snapshot (a source-fed leaf has no Ash row behind it) fall back
  to `rule/3` exactly as before, so the two paths coexist.
  """
  @spec rule(Cell.t(), Cell.id(), [String.t()], %{String.t() => map()}) ::
          ReactiveDag.KeyRule.result()
  def rule(parent, child, changed, priors) when map_size(priors) == 0,
    do: rule(parent, child, changed)

  def rule(%Cell{meta: %{key_rule: :group} = meta} = parent, child, changed, priors) do
    spec = meta[:reduce] || meta[:join]

    case snapshot_claims(spec, meta, changed, priors) do
      :none ->
        rule(parent, child, changed)

      {:ok, from_snapshots} ->
        # The snapshot names where each row WAS; the live lookup names where it
        # is NOW. A move needs both, so the live path still runs over every
        # changed key and the two sets are unioned.
        #
        # The live lookup degrading to :all no longer forces :all overall: that
        # degradation means "a row vanished and I cannot name its group", which
        # is exactly what the snapshot just answered.
        case rule(parent, child, changed) do
          :all -> {:keys, from_snapshots}
          {:keys, looked_up} -> {:keys, Enum.uniq(from_snapshots ++ looked_up)}
        end
    end
  end

  def rule(parent, child, changed, _priors), do: rule(parent, child, changed)

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
  def rule(parent, child, changed, priors, opts) do
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
      rule(parent, child, changed, priors)
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
  defp snapshot_claims(nil, _meta, _changed, _priors), do: :none

  defp snapshot_claims(spec, meta, changed, priors) do
    alias ReactiveDag.Node.Recompute.Declarative

    with %ReactiveDag.Node.Reduce{} = r <- spec,
         plan when is_list(plan) <- meta[:over_source][:group_key_plan] do
      key_fn = Declarative.key_fn(Map.get(r, :key), Map.get(r, :key_prefix))
      snapshotted = Enum.filter(changed, &Map.has_key?(priors, &1))

      keys =
        snapshotted
        |> Enum.map(&unit_from_snapshot(plan, priors[&1], key_fn))
        |> Enum.reject(&(&1 == :error))

      # a snapshot we cannot derive from is no better than none: fall back
      # wholesale rather than claim a partial set
      if length(keys) < length(snapshotted), do: :none, else: {:ok, keys}
    else
      _ -> :none
    end
  end

  # The snapshot is jsonb, so its keys are STRINGS and a Calendar bucket's
  # source is a date that round-tripped as a string — parse it back through the
  # calculation rather than comparing the wrong thing.
  defp unit_from_snapshot(plan, prior, key_fn) do
    values =
      Enum.map(plan, fn
        {:attr, name, _string?} ->
          Map.get(prior, to_string(name), :error)

        {:calendar, kind, of} ->
          case Map.get(prior, to_string(of)) do
            nil -> :error
            raw -> calendar_label(kind, raw)
          end

        {:calc, _name} ->
          # an opaque calculation cannot be evaluated from stored attributes
          :error
      end)

    if :error in values, do: :error, else: key_fn.(List.to_tuple(values))
  end

  defp calendar_label(kind, raw) when is_binary(raw) do
    case Date.from_iso8601(raw) do
      {:ok, date} -> ReactiveDag.Calendar.label(kind, date)
      _ -> :error
    end
  end

  defp calendar_label(kind, %Date{} = date), do: ReactiveDag.Calendar.label(kind, date)
  defp calendar_label(_kind, _raw), do: :error

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

        if is_nil(source) or is_nil(source[:payload_key]) do
          :all
        else
          side_join_keys(spec, source, j, changed)
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
  defp side_join_keys(spec, source, j, changed) do
    alias ReactiveDag.Node.Recompute.Declarative

    rows =
      source.resource
      |> Ash.Query.do_filter([{source.payload_key, [in: changed]}])
      |> load_calcs(Map.get(source, :load, []))
      |> Ash.read!(scope())

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

    rows =
      source.resource
      |> Ash.Query.do_filter([{source.payload_key, [in: changed]}])
      |> load_calcs(Map.get(source, :load, []))
      |> Ash.read!(scope())

    if length(rows) < length(changed) do
      :all
    else
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

          {{:calendar, kind, _of}, seg} ->
            case ReactiveDag.Calendar.parse(seg) do
              {_child_kind, first} -> ReactiveDag.Calendar.label(kind, first)
              :error -> :error
            end

          {{:calc, _name}, _seg} ->
            # an opaque calculation can't be evaluated from a segment
            :error
        end)

      if :error in values, do: :error, else: to_key.(values)
    end
  end
end

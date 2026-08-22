defmodule ReactiveDag.Node.Recompute do
  @moduledoc """
  THE engine: how a cell recomputes, decided by what its node DECLARED.

  `ReactiveDag.Drain` calls this — there is nothing to configure and no strategy
  to pass. Dispatch reads the `reactive` block's own shape, in this order:

  | the node declares | this runs |
  |---|---|
  | `leaf? true` | pass the claimed keys through (its source wrote the rows) |
  | `reduce` | read the scoped slice of `over`, group, fold each group into a row |
  | `join` | reconcile one input's two sides by key |
  | `per_key` | one call per key, with the input-fingerprint skip |
  | `union` | the union of its inputs' keys |
  | `aggregate` | a datastore `GROUP BY` — no rows enter the BEAM |
  | `run :action` | a generic Ash action on the node's own resource |
  | `compute Mod` | the escape hatch: a `ReactiveDag.Op` module |
  | nothing | pass through, LOUDLY (see below) |

  The combinators match BEFORE `compute`, so a node declaring both gets its
  combinator — which is why the verifier refuses that pair at compile time.

  ## One engine

  This was once one of two shipped strategies, passed in as `recompute:`, with a
  set-based sibling that dispatched `cell.op` to a SQL template from config. Both
  hosts ended up passing this one: what varied between them turned out to be
  DATA the DSL can declare — a module named in `compute`, a combinator, a key
  rule — not control flow. A pluggable engine everyone plugs the same thing into
  is an indirection, so the plug went and the DSL kept the declaring.

  `compute Mod` remains the escape hatch for work Ash cannot express (an LLM
  call, a PDF parse). The distinction that matters: it is declared IN the node
  and checked by the verifier, rather than looked up in a config map.

  A LEAF (or a cell with no compute) passes its claimed keys through as changed —
  a leaf's tuples were written by its source; if it reaches recompute at all, its
  claimed keys already ARE its changes.
  """

  require Logger
  alias ReactiveDag.Cell
  alias ReactiveDag.Node.Recompute.{Declarative, Read}

  @doc """
  Recompute `keys` of `cell` (or `[\"*\"]` for the whole cell), returning
  `{:ok, changed}` — the subset whose output actually changed. Only those
  propagate, which is what keeps a cascade O(real changes) rather than O(graph).

  May return `{:ok, changed, meta}`: an arbitrary map the drain carries onto its
  `%Report{}` step without interpreting. Token counts, cache hits and retries are
  all just keys, so `ReactiveDag.Insights` and a dashboard can show what the work
  cost.

  `{:error, reason}` is a CONTAINED failure — the drain rolls that cell back and
  carries on with the rest. It must be RETURNED, not raised: an exception inside
  a nested transaction aborts the outer one, so only a value can be isolated by a
  savepoint.
  """
  # 2-arity kept for direct callers (tests, a host driving recompute itself).
  def recompute(cell, keys), do: recompute(cell, keys, [])

  def recompute(%Cell{leaf?: true}, keys, _opts), do: {:ok, keys}

  # a declarative REDUCE combinator — read `over` → group_by → into each group.
  # `into` returns ONE row (a fold) or a LIST of rows (a group → many "expand").
  # A single row's key comes from `key.(group)`; list rows must carry their own
  # `:key` (see key resolution in rows_with_keys/3).
  #
  # SCOPING: `read` may be arity-1 (`over -> items`, whole-cell) or arity-2
  # (`over, dirty_keys -> items`) so a host can scope the datastore read to the
  # claimed keys — e.g. `read: fn :fiscal_lines, keys -> FiscalDoc |> filter(keys) end`.
  # `keys` is the claimed dirty set, or `nil` for a whole-cell recompute (`"*"`).
  def recompute(%Cell{meta: %{reduce: %{} = r}} = cell, keys, opts) do
    group_by = Declarative.group_fn(r.group_by)
    key_fn = Declarative.key_fn(r.key, r.key_prefix)
    keyer = row_keyer(cell, r, key_fn)

    # which slot emits a group's [{key, row}] pairs — the verifier guarantees
    # exactly the right one is declared for the node's shape.
    emit =
      cond do
        is_function(r.expand, 2) ->
          fn group, items -> expand_pairs(r.expand.(group, items), cell.id) end

        true ->
          into = Declarative.into_fn(r.into, r.group_by)
          fn group, items -> into_pair(into.(group, items), keyer, group) end
      end

    pairs =
      Read.items(cell.meta[:over_source], r.over, r.query, scope(keys), auto_scope(cell, keys), opts)
      |> Enum.group_by(group_by)
      |> Enum.flat_map(fn {group, items} -> emit.(group, items) end)

    {:ok, materialize(cell, pairs, scope(keys), opts)}
  end

  # a declarative JOIN combinator — read `over` into a LEFT and RIGHT index
  # (both `%{join_key => item}`), then for each left key emit `into.(jk, left,
  # right_or_nil)` (a left join; right may be absent). With `outer: true`,
  # right-only keys ALSO emit (`into.(jk, nil, right)`) — the full-outer
  # reconcile shape, where an undeclared right-side member is a finding.
  # `read` may be arity-2 for dirty-key scoping (see `reduce` above).
  def recompute(%Cell{meta: %{join: %{} = j}} = cell, keys, opts) do
    {left_items, right_items} = join_items(cell, j, keys, opts)

    left = index(left_items, Declarative.side_fn(j.left))
    right = index(right_items, Declarative.side_fn(j.right))
    key_fn = Declarative.key_fn(j.key, j.key_prefix)
    keyer = row_keyer(cell, j, key_fn)

    into = Declarative.join_into_fn(j.into)

    emit = fn jk, l, r ->
      jk |> into.(l, r) |> into_pair(keyer, jk)
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

    pairs = left_pairs ++ right_only_pairs

    # RETIREMENT is scoped to what this pass could SEE. A two-input pass that
    # read one side must not reconcile the other side's keys out of existence:
    # they were not produced because they were not read, which is the honest-gap
    # distinction the library draws everywhere else (`Rows.observed: :partial`).
    {:ok, materialize(cell, pairs, retire_scope(keys), opts)}
  end

  # a PER-KEY map — for each claimed input row, call a generic action with that
  # row and write its structured output here. The library drives the loop, which
  # is what lets it FINGERPRINT the inputs and skip the call when nothing the
  # result depends on has moved (a `run` action is opaque, so nothing outside it
  # could). Reports `%{called:, skipped:}` so the saving is visible.
  # TODO(tenancy): `PerKey.recompute/3` writes rows through the payload loop, so
  # it needs the tenant. Not threaded yet — ADR-003.
  def recompute(%Cell{meta: %{per_key: %{} = spec}} = cell, keys, _opts) do
    {changed, meta} =
      ReactiveDag.Node.Recompute.PerKey.recompute(cell, spec, scope(keys))

    {:ok, changed, meta}
  end

  # a UNION — one row per (input cell, key) across N inputs. The claim carries
  # its provenance ("<input>|<key>"), so a scoped pass reads only the input that
  # moved; nothing correlates across inputs, which is what makes N of them safe
  # here where a cross-node join was not.
  def recompute(%Cell{meta: %{union: %{} = spec}} = cell, keys, opts) do
    claimed = scope(keys)

    {pairs, meta} =
      ReactiveDag.Node.Recompute.Union.pairs(spec, cell.meta[:union_sources] || %{}, claimed)

    {:ok, materialize(cell, pairs, claimed, opts), meta}
  end

  # a PURE-ASH-QUERY aggregate — the datastore groups + aggregates the `over`
  # relationship. The node's resource (`meta.resource`) is the group's resource: read it
  # with the relationship aggregates loaded (ONE Ash query; Postgres does the
  # GROUP BY), and each parent row's aggregate values become its payload. This is a
  # WHOLE-CELL recompute (a GROUP BY reprices every group; there's no per-dirty-key
  # scoping), so the changed set is every group whose aggregate value moved.
  # TODO(tenancy): `Aggregate.recompute/3` writes rows too — same gap.
  def recompute(%Cell{meta: %{aggregate: %{} = agg, resource: resource}} = cell, _keys, _opts)
      when not is_nil(resource) do
    {:ok, ReactiveDag.Node.Recompute.Aggregate.recompute(cell, resource, agg)}
  end

  # the ASH-NATIVE escape hatch: a GENERIC action on the node's own resource
  # (`run :recompute_keys`). The action does its own DOMAIN writes and returns
  # the changed keys, which are what propagates. MUST precede the compute-nil
  # clause: a run node's meta carries compute: nil too.
  def recompute(%Cell{meta: %{run: action, resource: resource}} = cell, keys, _opts)
      when is_atom(action) and not is_nil(action) and not is_nil(resource) do
    params =
      %{keys: scope(keys), cell_id: cell.id}
      |> Map.take(declared_args(resource, action))

    case resource |> Ash.ActionInput.for_action(action, params) |> Ash.run_action() do
      {:ok, changed} when is_list(changed) ->
        {:ok, changed}

      {:ok, other} ->
        raise "reactive_dag: run action #{inspect(action)} on #{inspect(resource)} must " <>
                "return the changed keys (`{:array, :string}`), got: #{inspect(other)}"

      {:error, error} ->
        raise "reactive_dag: run action #{inspect(action)} on #{inspect(resource)} failed: " <>
                Exception.message(error)
    end
  end

  # A DSL-authored node cannot reach here — `VerifyReactive` rejects a block with
  # no computation at compile time. This is the hand-assembled path: a host that
  # builds `%Cell{}` structs itself has no verifier, so the pass-through stays,
  # loudly, rather than becoming a crash in the one place the substrate is
  # deliberately unopinionated.
  def recompute(%Cell{meta: %{compute: nil}, id: id}, keys, _opts) do
    Logger.warning(
      "reactive_dag: node #{inspect(id)} declares no computation; passing its keys " <>
        "through unchanged, so anything downstream recomputes against inputs that never " <>
        "moved. A hand-assembled cell needs `meta.compute`; a DSL-authored one is caught " <>
        "at compile time."
    )

    {:ok, keys}
  end

  # `compute Mod` and `run` are escape hatches: the op does its own writes, so the
  # tenant is the host's to handle — the library has no changeset to set it on.
  def recompute(%Cell{meta: %{compute: op}} = cell, keys, _opts)
      when is_atom(op) and not is_nil(op) do
    op.recompute(cell, keys)
  end

  # a cell whose meta carries no :compute key at all (e.g. a non-Node plan) —
  # treat like a leaf: pass through.
  def recompute(%Cell{}, keys, _opts), do: {:ok, keys}

  # the arguments a `run` action declares — the library passes only these.
  defp declared_args(resource, action) do
    case Ash.Resource.Info.action(resource, action) do
      %{arguments: args} -> Enum.map(args, & &1.name)
      nil -> []
    end
  end

  # the library's automatic read scope, by the cell's key-rule GRAIN:
  #   :identity        → the claimed keys ARE the over's keys: filter payload_key.
  #   :group (any form) → the claimed keys are group labels: invert them through
  #                     assembly's group_key_plan — a plain string attribute
  #                     filters by equality; a Calendar bucket by its date-range
  #                     HULL (a superset read is still closed over groups —
  #                     extra groups recompute and change-detect to nothing).
  #   anything else    → no auto scope: a grain-changing host rule's claims must
  #                     not filter the child-grain read; `query:` still receives
  #                     them for host-grain scoping.
  @doc false
  # public for tests: the scope the library derives from a claim set.
  def auto_scope(%Cell{meta: meta}, keys) do
    case meta[:key_rule] do
      :identity -> keyed_scope(keys)
      nil -> keyed_scope(keys)
      :group -> group_scope(meta, scope(keys))
      {:group, _opts} -> group_scope(meta, scope(keys))
      _ -> nil
    end
  end

  # `:group` claims are group labels; when assembly's group_key_plan proves a
  # SINGLE-entry group, the labels invert to a data predicate: a plain string
  # attribute's values (`attr in claims`), or a Calendar bucket's date-range
  # HULL. Multi-entry groups and opaque calculations don't invert — the read
  # stays whole (or `query:`-scoped) rather than guessing wrong.
  defp group_scope(_meta, nil), do: nil

  defp group_scope(meta, labels) do
    prefix = meta[:reduce] |> then(&(&1 && Map.get(&1, :key_prefix)))

    values =
      case prefix do
        nil -> labels
        p -> Enum.map(labels, &String.replace_prefix(&1, p <> "|", ""))
      end

    case meta[:over_source] do
      %{group_key_plan: [{:attr, attr, true}]} ->
        {:attr, attr, values}

      %{group_key_plan: [{:calendar, kind, attr}]} ->
        ranges = Enum.map(values, &ReactiveDag.Calendar.range(kind, &1))

        if :error in ranges do
          nil
        else
          {froms, tos} = Enum.unzip(ranges)
          {:range, attr, Enum.min(froms, Date), Enum.max(tos, Date)}
        end

      # a COMPOSITE unit: each claim is its columns joined with "|", so split
      # them back apart and scope each column by the values seen at its
      # position. For SEVERAL claims this is a cross-product SUPERSET (claims
      # "gf|2025" and "water|2026" also admit "gf|2026") — still sound, because
      # a superset read stays closed over unit boundaries, and far tighter than
      # reading the whole table. Only plain-string columns invert; a mixed plan
      # scopes by the string ones it can and leaves the rest to the fold.
      %{group_key_plan: [_, _ | _] = plan} ->
        composite_scope(plan, values)

      _ ->
        nil
    end
  end

  defp composite_scope(plan, labels) do
    segments = Enum.map(labels, &String.split(&1, "|"))

    if Enum.any?(segments, &(length(&1) != length(plan))) do
      # a key that doesn't match the plan's arity isn't ours to invert
      nil
    else
      plan
      |> Enum.with_index()
      |> Enum.flat_map(fn
        {{:attr, attr, true}, i} ->
          [{:attr, attr, segments |> Enum.map(&Enum.at(&1, i)) |> Enum.uniq()}]

        _other ->
          []
      end)
      |> case do
        [] -> nil
        [one] -> one
        many -> {:all_of, many}
      end
    end
  end

  defp keyed_scope(keys) do
    case scope(keys) do
      nil -> nil
      claimed -> {:keys, claimed}
    end
  end

  # the claimed keys as a read scope: `nil` for a whole-cell recompute (`"*"`),
  # else the specific dirty keys a scoped `read` can filter its query to.
  defp scope(keys) do
    cond do
      is_nil(keys) -> nil
      "*" in keys -> nil
      true -> keys
    end
  end

  # tuple carries strength).
  # an `into:` result is ONE row: its own `:key` wins (a self-identified row),
  # else the cell keyer derives it — from the row's IDENTITY fields for a
  # composite-primary-key node, else from the group term / join key. A list
  # here is the old expand overload — point at the `expand:` slot.
  defp into_pair(rows, _keyer, _term) when is_list(rows) do
    raise "reactive_dag: `into:` returns ONE row per group — the group → many-rows " <>
            "shape is the `expand:` slot (each row carrying its own :key)"
  end

  defp into_pair(row, keyer, term) when is_map(row) do
    key = if is_map_key(row, :key), do: row.key, else: keyer.(row, term)
    [{key, row}]
  end

  # how a payload row gets its cell key: an IDENTITY-KEYED node (composite
  # primary key) serializes the row's identity fields in primary-key order —
  # the key IS the identity, not a stored column; anything else derives from
  # the group term / join key as ever.
  defp row_keyer(%Cell{meta: %{identity_fields: fields}}, spec, _key_fn)
       when is_list(fields) do
    id_key = Declarative.identity_key_fn(fields, Map.get(spec, :key_prefix))
    fn row, _term -> id_key.(row) end
  end

  defp row_keyer(_cell, _spec, key_fn), do: fn _row, term -> key_fn.(term) end

  # `expand:` rows fan one group out to many keys, so each row must self-key.
  defp expand_pairs(rows, cell_id) when is_list(rows) do
    Enum.map(rows, fn
      # "this claimed key is not mine" — distinct from "this key is gone", which
      # is what returning nothing for it would mean. See `materialize/4`.
      {:skip, key} ->
        {key, :skip}

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

  # write each {key, row} and return the changed keys. Shared by reduce/expand +
  # join. ONE write mode: the library closes the loop into the node's own resource
  # (`meta.resource`). A host `upsert:` override used to take precedence; it is
  # gone, because a node owns its rows.
  defp materialize(cell, pairs, claimed, opts) do
    write = writer_fn(cell, opts)

    {declined, pairs} = Enum.split_with(pairs, &match?({_key, :skip}, &1))

    changed = Enum.flat_map(pairs, fn {key, row} -> if write.(key, row), do: [key], else: [] end)

    # A DECLINED key is not this node's — it was claimed because an input moved,
    # and this node has nothing to say about it. That is not the same as a key
    # whose rows have gone, and it must not be reconciled as one: subtracting it
    # from the claim would retire it, report it changed, and do so again on every
    # pass forever. So it leaves the baseline entirely.
    kept = Enum.map(declined, &elem(&1, 0))
    produced = Enum.map(pairs, &elem(&1, 0))

    changed ++ retire_vanished(cell, produced ++ kept, claimed, opts)
  end

  # RECONCILE, not just upsert. A fold writes the units it produced; a unit whose
  # input rows have all gone produces nothing, so without this its last computed
  # value would linger forever — and a stale derived row is indistinguishable
  # from a live one, which defeats the point of materializing it.
  #
  # `want` is what this pass produced. The baseline it is subtracted from is the
  # CLAIM: a whole-cell pass reconciles every key the cell has, a scoped pass
  # only the units it claimed (reconciling wider would retire live units that
  # simply weren't visited). Destroying the row IS the retirement — the key is
  # returned as changed, so propagation carries it downstream.
  #
  # A write-elsewhere node used to return early here — `[]`, unconditionally, for
  # any node supplying its own `upsert:`. So the one shape that could not be
  # reconciled was also the one whose rows the library could not see, and its stale
  # units lingered forever: exactly the failure the paragraph above describes,
  # exempted from the fix. Removing the shape removes the exemption.
  defp retire_vanished(%Cell{meta: meta} = cell, want, claimed, opts) do
    want_set = MapSet.new(want)

    case vanished_baseline(cell, claimed, opts) do
      nil ->
        []

      baseline ->
        case Enum.reject(baseline, &MapSet.member?(want_set, &1)) do
          [] ->
            []

          vanished ->
            if resource = meta[:resource] do
              ReactiveDag.Node.Payload.retire(
                resource,
                meta[:payload_key] || :key,
                meta[:identity_fields],
                vanished,
                meta[:payload_destroy] || :destroy,
                Keyword.take(opts, [:tenant])
              )
            end

            vanished
        end
    end
  end

  # what a pass is entitled to retire: the claimed units for a scoped pass, and
  # everything the node currently HOLDS for a whole-cell one. `nil` = don't
  # reconcile (a node whose current key set we cannot enumerate).
  defp vanished_baseline(cell, nil, opts), do: current_keys(cell, opts)
  defp vanished_baseline(cell, ["*"], opts), do: current_keys(cell, opts)
  defp vanished_baseline(_cell, claimed, _opts) when is_list(claimed), do: claimed

  # a node's OWN ROWS are the truth about which units it currently holds.
  #
  # `nil` means "cannot enumerate — do not reconcile", and is NOT the same as
  # `[]`, which would claim the node holds nothing and retire every key it has.
  # Every derived node has rows of its own — the verifier refuses one that does
  # not — so the rescue is the guard that matters: an unreadable resource must not
  # be read as an empty one.
  # TENANT-SCOPED, and this is the destructive path: the baseline is what gets
  # subtracted from, so an unscoped read retires every OTHER tenant's keys.
  defp current_keys(%Cell{meta: meta, id: id}, opts) do
    %Cell{id: id, meta: meta}
    |> ReactiveDag.Node.Rows.all(opts)
    |> Enum.map(& &1.key)
  rescue
    _ -> nil
  end

  # `(key, row) -> changed?`. The row goes into the node's OWN resource — one node,
  # one table. There used to be an `upsert:` slot that took a closure writing
  # somewhere else, and a raise here for the node that declared neither; both are
  # gone, and `VerifyReactive.verify_owns_rows/2` refuses at COMPILE time what this
  # raised at drain time.
  defp writer_fn(%Cell{meta: meta, id: id}, tenant_opts) do
    case meta[:resource] do
      nil ->
        # A hand-assembled `%Cell{}` bypasses the verifier, so this stays as the
        # backstop for the one path that can still reach it.
        raise """
        reactive_dag: node #{inspect(id)} computes rows but its cell carries no
        resource, so there is nowhere to write them. A DSL-authored node is caught
        at compile time; a hand-built %Cell{} needs `meta.resource`.
        """

      resource ->
        action = meta[:payload_action] || :upsert
        # what a recompute CLEARS: the human marks whose watched fields this
        # write moves. Nil when the node declares no `lapse`, and the payload
        # path then behaves exactly as before — survival, for free.
        opts =
          Keyword.merge(
            [lapse: meta[:lapse], compare: meta[:compare]],
            Keyword.take(tenant_opts, [:tenant])
          )

        case meta[:identity_fields] do
          fields when is_list(fields) ->
            fn _key, row ->
              ReactiveDag.Node.Payload.upsert_identity(resource, fields, row, action, opts) !=
                :unchanged
            end

          _ ->
            key_attr = meta[:payload_key] || :key

            fn key, row ->
              ReactiveDag.Node.Payload.upsert(resource, key_attr, key, row, action, opts) !=
                :unchanged
            end
        end
    end
  end

  # index items by a side's key fn; `key_fn.(item)` returns the join key, or nil
  # for an item not on this side (so a single input can be split into left/right).
  defp index(items, key_fn) do
    for item <- items, jk = key_fn.(item), not is_nil(jk), into: %{}, do: {jk, item}
  end

  # The two sides' items, and WHICH sides this pass actually read.
  #
  # One input: both sides come from one read, so both are always "read".
  #
  # Two inputs: each side is read with ITS OWN source and ITS OWN scope. The
  # reverted implementation computed one `claimed`/`auto` pair and passed it to
  # both sides, so a left-side claim filtered the right table by keys that index
  # nothing in it, read empty, and the join wrote nils over good data. A side is
  # read only when the claim concerns it — whole-cell reads both.
  defp join_items(%Cell{meta: %{side_sources: %{} = sides}}, j, keys, opts) do
    claimed = scope(keys)

    read = fn side, over, spec ->
      Read.items(sides[side], over, j.query, claimed, side_scope(spec, claimed), opts)
    end

    {read.(:left, j.left_over, j.left), read.(:right, j.right_over, j.right)}
  end

  defp join_items(cell, j, keys, opts) do
    items =
      Read.items(cell.meta[:over_source], j.over, j.query, scope(keys), auto_scope(cell, keys), opts)

    {items, items}
  end

  # A side's own read scope.
  #
  # The claimed keys are JOIN keys, not the side's payload keys — those are
  # different columns and usually different values. Scoping `Budgets` by its
  # `payload_key` (`:key`, e.g. `"b1"`) with a claim of `"5000"` (an
  # `account_code`) matches nothing, so the side reads empty: the same
  # read-nothing failure the reverted version had, arrived at from the other
  # direction. A side filters the column it is INDEXED BY.
  #
  # Only a plain-attribute side (`left: :account_code`) or the `[key: …]` form
  # names a filterable column. A fn side computes its join key in the BEAM, so
  # there is nothing to push into the query and the side reads whole — correct,
  # and no worse than the one-input form, which also reads whole for a fn side.
  @doc false
  # public for tests: the read scope a side derives from a claim set.
  def side_scope_for_test(spec, claimed), do: side_scope(spec, claimed)

  defp side_scope(_spec, nil), do: nil

  defp side_scope(spec, claimed) do
    case join_key_attr(spec) do
      nil -> nil
      attr -> {:attr, attr, claimed}
    end
  end

  defp join_key_attr(attr) when is_atom(attr) and not is_nil(attr), do: attr
  defp join_key_attr(spec) when is_list(spec), do: Keyword.get(spec, :key)
  defp join_key_attr(_fn_or_nil), do: nil

  # What this pass may reconcile: its claim, both forms alike. A two-input pass
  # reads BOTH sides for every claim — each scoped to its own join-key column —
  # so every claimed key was genuinely looked for and a key that produced no row
  # really has no row.
  defp retire_scope(keys), do: scope(keys)
end

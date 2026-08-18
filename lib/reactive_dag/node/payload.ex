defmodule ReactiveDag.Node.Payload do
  @moduledoc """
  Closes the payload loop for a resource-backed node: writes a combinator's output
  row into the node's OWN resource (`cell.meta.resource`), keyed by the cell key.

  This is what makes "the resource IS the node, and its rows ARE its payload" true
  in the code, not just the docs. A `reduce`/`join` whose `into` returns a row and
  that omits an explicit `upsert:` has its row written HERE — an Ash upsert into the
  node's resource, with change-detection — so the common case needs no host write
  callback, and writing into a *different* resource becomes the explicit deviation
  (a custom `upsert:`), not the default.

  ## The key attribute

  The cell key (a string) maps to one resource attribute — the payload key. It
  defaults to `:key`; a resource whose primary key is named otherwise declares
  `payload_key :flow_key` in its `reactive` block. The row is written with that
  attribute set to the cell key; a `:key` field on the row itself is dropped (it's
  the cell key, not a payload column).

  ## Change detection

  `upsert/5` reads the existing row first and compares the writable attributes; it
  returns `:changed` only when a create or a real value change happened, so the
  drain's parent-dirty only fires for genuine changes (a no-op recompute
  stays a no-op). Requires an upsert action on the resource — by default the
  action named `:upsert` with `upsert?: true`; override with `payload_action`.

  ## Lapsing a human's mark

  The upsert also holds the prior record long enough for `lapse/5` to ask its
  OWN question of it: did the fields a human's mark was about move? A `lapse`
  declaration then nulls that column, or destroys the child rows, as a SEPARATE
  write ordered after the create.

  Separate deliberately. Folding it into the create's attrs would need the
  payload action to `accept` the human column — and it would then null it on
  every pass, destroying the default this whole loop gives for free: a column
  the payload action does not accept is never touched, so a mark survives
  recomputes with no declaration at all.
  """

  require Ash.Query

  alias ReactiveDag.Node.Fingerprint

  @doc """
  Upsert `row` into `resource` under `cell_key` (written to `key_attr`), via
  `action`. `row`'s `:key` field is dropped before writing.

  Returns `:created` (no row existed), `:changed` (a row existed and moved) or
  `:unchanged`. The first two both mean "propagate"; they are distinguished
  because a caller reporting *what a scan did* wants them apart, and this is the
  only place that knows — one row of the check it already performs.

  ## Options

    * `:fingerprint` — compare ONE value instead of every attribute: a field
      list (hashed) or `(row -> value)`. The value is written to
      `:fingerprint_attribute` (default `:fingerprint`), so the next call has
      something to compare against.

      This is what a SOURCE-FED LEAF needs. Comparing every attribute is right
      for a derived node, where every attribute is part of the result — but a
      leaf's row carries fields that move on every observation without the
      observation having changed: a `last_seen_at` by definition, an `etag` a
      server may re-issue for identical bytes. Comparing those reports a change
      on every poll and re-runs the whole cascade.

    * `:fingerprint_attribute` — where the value is stored (default
      `:fingerprint`).

    * `:lapse` — the human marks this write clears when the content moves (the
      assembled `lapse` entities, from `cell.meta[:lapse]`). Applied AFTER the
      create, as its own write, and only where the watched fields actually
      moved. See `lapse/5`.
  """
  @spec upsert(module(), atom(), String.t(), map(), atom(), keyword()) :: :created | :changed | :unchanged
  def upsert(resource, key_attr, cell_key, row, action \\ :upsert, opts \\ []) do
    attrs =
      row
      |> Map.drop([:key])
      |> Map.put(key_attr, cell_key)
      |> stamp_fingerprint(row, resource, opts)

    # The PRIOR record, not just the verdict: a lapse asks its own question of
    # it (did the WATCHED fields move?), which the verdict cannot answer — a
    # recompute is `:changed` overall the moment any column moves, and the mark
    # must survive when the ones it was about sat still.
    prior = existing(resource, key_attr, cell_key)

    verdict =
      case prior do
        nil -> :created
        record -> if moved?(record, attrs, opts), do: :changed, else: :unchanged
      end

    {:ok, _} =
      resource
      |> Ash.Changeset.for_create(action, attrs)
      |> Ash.create()

    lapse(prior, attrs, resource, cell_key, opts)

    verdict
  end

  @doc """
  Upsert an IDENTITY-KEYED row (a composite-primary-key node): the row carries
  its identity fields, the upsert conflicts on the primary key (Ash's default
  for `upsert? true`), and no key column exists — the cell key is the
  identity's serialization, derived elsewhere. Returns `:changed` | `:unchanged`
  with the same read-compare change detection as `upsert/6`, and the same
  `:fingerprint` / `:fingerprint_attribute` / `:lapse` options.
  """
  @spec upsert_identity(module(), [atom()], map(), atom(), keyword()) :: :created | :changed | :unchanged
  def upsert_identity(resource, identity_fields, row, action \\ :upsert, opts \\ []) do
    attrs = row |> Map.drop([:key]) |> stamp_fingerprint(row, resource, opts)

    prior = existing_by(resource, Map.take(attrs, identity_fields))

    verdict =
      case prior do
        nil -> :created
        record -> if moved?(record, attrs, opts), do: :changed, else: :unchanged
      end

    {:ok, _} =
      resource
      |> Ash.Changeset.for_create(action, attrs)
      |> Ash.create()

    # An identity-keyed node has no key COLUMN — its cell key is the identity's
    # `"|"` serialization, in primary-key order — so a child lapse's key value is
    # reconstructed here rather than read off the row.
    lapse(prior, attrs, resource, identity_key(attrs, identity_fields), opts)

    verdict
  end

  @doc """
  Clear the human marks a recompute invalidated: null the declared attributes,
  destroy the declared child rows.

  `prior` is the record as it was BEFORE this pass (nil on a create). `attrs` is
  what was just written. Each `lapse` spec fires only when the fields IT watches
  moved between the two — its own comparison, deliberately not the propagate
  verdict, because a recompute is `:changed` the moment any column moves and a
  mark about the totals must survive a spelling fix.

  `nil` prior never lapses: no prior record means no mark, and there is nothing
  to compare against.

  ## Never raises

  This runs inside the drain's per-cell savepoint, where a raise would abort the
  OUTER transaction and roll back a recompute that was perfectly good. So a
  failure to clear is logged and contained. That is safe only because everything
  checkable without writing — a missing attribute, an absent lapse action, a
  child with no destroy — already raised at ASSEMBLY, which is off the hot path.
  What can still fail here is a genuine write failure, and losing the recompute
  as well would not make the mark any more correct.
  """
  @spec lapse(struct() | nil, map(), module(), String.t() | nil, keyword()) :: :ok
  def lapse(prior, attrs, resource, cell_key, opts)
  def lapse(nil, _attrs, _resource, _cell_key, _opts), do: :ok

  def lapse(prior, attrs, resource, cell_key, opts) do
    case opts[:lapse] do
      specs when is_list(specs) and specs != [] ->
        Enum.each(specs, &apply_lapse(&1, prior, attrs, resource, cell_key))

      _ ->
        :ok
    end
  end

  defp apply_lapse(spec, prior, attrs, resource, cell_key) do
    if lapse_moved?(prior, attrs, spec.when_changed) do
      do_lapse(spec, prior, attrs, resource, cell_key)
    end
  rescue
    e ->
      require Logger

      Logger.error(
        "reactive_dag: lapse #{inspect(lapse_label(spec))} on #{inspect(resource)} failed " <>
          "and was skipped — the human's mark is still standing over content that moved. " <>
          "#{Exception.message(e)}"
      )

      :ok
  end

  defp lapse_label(%{kind: :attribute, attribute: a}), do: a
  defp lapse_label(%{kind: :child, resource: r}), do: r

  # The lapse's OWN comparison. `:any` reuses the whole-attrs comparison the
  # payload already performs; a field list narrows it with `Map.take`, which is
  # the entire mechanism — the same question asked of fewer columns.
  #
  # NOT the fingerprint: `Fingerprint.put/5` treats nil as a silent no-op, which
  # inverts the safe direction here. For change detection a nil fingerprint
  # falling back to "compare everything" over-reports a change, which is safe.
  # For a lapse the same nil would read as "nothing moved" — the mark survives
  # content it should not — so lapse compares fields directly and never routes
  # through the fingerprint.
  defp lapse_moved?(prior, attrs, :any), do: differs?(prior, attrs)

  defp lapse_moved?(prior, attrs, fields) when is_list(fields),
    do: differs?(prior, Map.take(attrs, fields))

  # An ATTRIBUTE lapse nulls the column. A separate UPDATE, ordered after the
  # payload create: folding it into the create's attrs would need the payload
  # action to accept the human column, and it would then null it on every pass.
  defp do_lapse(%{kind: :attribute, attribute: attr, action: action, over: nil}, prior, _attrs, _resource, _cell_key) do
    # Already clear: nothing to withdraw, and skipping the write keeps a
    # recompute from touching rows no human ever marked.
    unless is_nil(Map.get(prior, attr)) do
      prior
      |> Ash.Changeset.for_update(action, %{attr => nil})
      |> Ash.update!()
    end

    :ok
  end

  # SET-GRAIN: the mark lives over a whole unit, so every row of that unit
  # carries it and every one must be cleared. The unit's value comes off the ROW
  # just written — `over:` names a `recompute_by` unit, and a declarative group's
  # parent columns are on the row by construction — rather than from parsing the
  # key string, which for an `expand:` node is host-defined and carries no
  # grammar the library may rely on.
  defp do_lapse(%{kind: :attribute, attribute: attr, action: action, over: unit}, _prior, attrs, resource, _cell_key) do
    case Map.fetch(attrs, unit) do
      {:ok, value} ->
        resource
        |> Ash.Query.do_filter([{unit, value}])
        |> Ash.read!()
        |> Enum.reject(&is_nil(Map.get(&1, attr)))
        |> Enum.each(fn record ->
          record |> Ash.Changeset.for_update(action, %{attr => nil}) |> Ash.update!()
        end)

      :error ->
        raise ArgumentError,
              "the row this node wrote carries no #{inspect(unit)} column, so the unit " <>
                "`over: #{inspect(unit)}` names cannot be read off it. A set-grain lapse " <>
                "reads the unit from the row, which a declarative `group_by` puts there by " <>
                "construction; an `expand:`/`into:` fn must emit it too."
    end

    :ok
  end

  # A CHILD lapse destroys the rows attached to the lapsing key — one-to-many, so
  # every matching row goes, unlike the single-row `existing/3` the payload path
  # uses. Rows under other keys are untouched: the filter is the key, which is
  # why `key:` is required rather than guessed.
  defp do_lapse(%{kind: :child, resource: child, key: key_col, action: action} = spec, _prior, attrs, _resource, cell_key) do
    value = child_key_value(spec, cell_key, attrs)

    child
    |> Ash.Query.do_filter([{key_col, value}])
    |> Ash.read!()
    |> Enum.each(&(&1 |> Ash.Changeset.for_destroy(action) |> Ash.destroy!()))

    :ok
  end

  # Row-grain: the child's key column holds this node's CELL KEY, threaded in by
  # the caller — it is the one fact the row itself may not carry (an
  # identity-keyed node has no key column at all, its cell key being the
  # identity's serialization).
  #
  # Set-grain: it holds the UNIT instead, read off the row just written.
  defp child_key_value(%{over: nil}, cell_key, _attrs), do: cell_key
  defp child_key_value(%{over: unit}, _cell_key, attrs), do: Map.fetch!(attrs, unit)

  # the identity-keyed node's cell key: its identity fields, in primary-key
  # order, joined with "|" — the same canonical serialization the rest of the
  # library derives such keys with.
  defp identity_key(attrs, fields),
    do: Enum.map_join(fields, "|", &to_string(Map.get(attrs, &1)))

  @doc """
  RETIRE a vanished unit's row: the unit's input rows are gone, so the derived
  row must go too — a derived table whose whole point is that you query it
  cannot keep rows for units that no longer exist (a stale row is
  indistinguishable from a live one).

  `keys` are cell keys; for an identity-keyed node each is the identity's `"|"`
  serialization, split back into its fields. Rows already absent are skipped, so
  this is idempotent. Returns the keys whose row was actually destroyed.

  Requires a destroy action (default `:destroy`); a resource without one raises
  with the fix, since silently keeping the row would defeat the reconcile.
  """
  @spec retire(module(), atom() | nil, [atom()] | nil, [String.t()], atom()) :: [String.t()]
  def retire(resource, key_attr, identity_fields, keys, action \\ :destroy)
  def retire(_resource, _key_attr, _identity_fields, [], _action), do: []

  def retire(resource, key_attr, identity_fields, keys, action) do
    ensure_destroy!(resource, action)

    Enum.filter(keys, fn key ->
      case find_row(resource, key_attr, identity_fields, key) do
        nil ->
          false

        record ->
          record |> Ash.Changeset.for_destroy(action) |> Ash.destroy!()
          true
      end
    end)
  end

  defp find_row(resource, _key_attr, fields, key) when is_list(fields) do
    values = String.split(key, "|")

    if length(values) == length(fields) do
      existing_by(resource, fields |> Enum.zip(values) |> Map.new())
    else
      nil
    end
  end

  defp find_row(resource, key_attr, _fields, key), do: existing(resource, key_attr, key)

  defp ensure_destroy!(resource, action) do
    case Ash.Resource.Info.action(resource, action) do
      %{type: :destroy} ->
        :ok

      _ ->
        raise ArgumentError,
              "reactive_dag: #{inspect(resource)} needs a #{inspect(action)} action to retire " <>
                "vanished units' rows (a unit whose input rows are gone must not linger in " <>
                "the derived table). Add `defaults [:destroy]`, or name another with " <>
                "`payload_destroy`."
    end
  end

  defp existing_by(resource, identity_map) do
    resource
    |> Ash.Query.do_filter(Enum.to_list(identity_map))
    |> Ash.read_one()
    |> case do
      {:ok, record} -> record
      _ -> nil
    end
  end

  defp existing(resource, key_attr, cell_key) do
    resource
    |> Ash.Query.do_filter([{key_attr, cell_key}])
    |> Ash.read_one()
    |> case do
      {:ok, record} -> record
      _ -> nil
    end
  end

  # a writable attr differs between the stored record and the row we'd write.
  defp differs?(record, attrs) do
    Enum.any?(attrs, fn {attr, value} -> Map.get(record, attr) != value end)
  end

  # a declared fingerprint REPLACES the all-attribute comparison rather than
  # adding to it — the point is to ignore the fields that move on their own.
  #
  # A nil fingerprint (the node declares none, or a `(row -> value)` fn returned
  # nil because the source could not determine it) falls back to comparing
  # everything. That is the safe direction: it may report a change that did not
  # happen, where trusting a nil would miss one that did.
  defp moved?(record, attrs, opts) do
    case fingerprint_attr(opts, attrs) do
      {attr, value} when not is_nil(value) -> Map.get(record, attr) != value
      _ -> differs?(record, attrs)
    end
  end

  defp fingerprint_attr(opts, attrs) do
    case opts[:fingerprint] do
      nil ->
        nil

      _ ->
        attr = opts[:fingerprint_attribute] || Fingerprint.default_attribute()
        {attr, Map.get(attrs, attr)}
    end
  end

  defp stamp_fingerprint(attrs, row, resource, opts) do
    case opts[:fingerprint] do
      nil ->
        attrs

      spec ->
        Fingerprint.put(
          attrs,
          opts[:fingerprint_attribute] || Fingerprint.default_attribute(),
          Fingerprint.of(spec, row),
          resource,
          "fingerprint:"
        )
    end
  end
end

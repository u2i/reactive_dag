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

    * `:compare` — compare ONLY these fields, storing nothing. For a DERIVED row
      that carries fields which are part of the record but not part of the
      result: `doc_id` (provenance), `ordinal` (position in the source document),
      a `match_key` a join builds. A re-parse that reorders rows moves every
      `ordinal`, and comparing them reports a change nothing made — which then
      re-runs every fold downstream.

      Prefer it to `:fingerprint` for a derived node: a fingerprint needs a column
      to hold the digest, and a digest of fields already on the row earns nothing
      when the comparison can just read them. `:fingerprint` remains the answer
      for a leaf, where the fields that move are the ones you must NOT compare and
      the honest witness is a hash of the ones you must.

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
    prior = existing(resource, key_attr, cell_key, opts)

    verdict =
      case prior do
        nil -> :created
        record -> if moved?(record, attrs, opts), do: :changed, else: :unchanged
      end

    {:ok, written} =
      resource
      |> Ash.Changeset.for_create(action, attrs, tenant_opts(opts))
      |> Ash.create()

    # This tail is `write/6`'s, duplicated — `upsert/6` predates the rung split
    # and never moved onto it. Worth collapsing, but not in the same change as
    # adding the diff: the two have different `find_prior` shapes and merging
    # them is its own piece of work with its own tests.
    record_diff(cell_key, prior, attrs, verdict, written, opts)
    lapse(prior, attrs, resource, cell_key, opts)

    verdict
  end

  @doc """
  Upsert a row, finding it by the node's declared `row_key`.

  ONE entry point for all three rungs, so a caller never decides which lookup
  applies — the node already declared it:

    * `:uuid` — the cell key IS the row's id
    * `[:col, :col]` — find by those columns' values, taken off the row
    * `(cell_key, attrs, opts -> row | nil)` — the resolver decides

  Falls back to `upsert/6` (the `payload_key` path) when the node declares no
  `row_key`, so this is additive: an existing node behaves exactly as before.

  `opts` carries `:tenant`, `:lapse` and `:compare` as `upsert/6` does, and is
  passed to a resolver so it can scope its own read.
  """
  @spec upsert_row(module(), map(), String.t(), map(), keyword()) ::
          :created | :changed | :unchanged
  def upsert_row(resource, meta, cell_key, row, opts \\ []) do
    action = meta[:payload_action] || :upsert

    opts =
      Keyword.merge(
        [
          lapse: meta[:lapse],
          compare: meta[:compare],
          payload_update: meta[:payload_update],
          # so the write can record a version for the mark it will cause
          version_id: meta[:version_id]
        ],
        opts
      )

    case meta[:row_key] do
      nil ->
        upsert(resource, meta[:payload_key] || :key, cell_key, row, action, opts)

      :uuid ->
        # The key IS the id. It goes in as an attribute like any other, so the
        # host's upsert conflicts on the primary key — which is what it already
        # does by default.
        write(
          resource,
          row |> Map.drop([:key]) |> Map.put(primary_key!(resource), cell_key),
          fn attrs -> existing_by(resource, Map.take(attrs, [primary_key!(resource)]), opts) end,
          action,
          cell_key,
          opts
        )

      fields when is_list(fields) ->
        write(
          resource,
          Map.drop(row, [:key]),
          fn attrs -> existing_by(resource, Map.take(attrs, fields), opts) end,
          action,
          cell_key,
          opts
        )

      resolver when is_function(resolver, 3) ->
        resolve_write(resource, Map.drop(row, [:key]), resolver, action, cell_key, opts)
    end
  end

  # RUNG 3. The resolver names a row by judgement, and that row's identity need
  # not match anything this `attrs` map would conflict on — a meeting matched
  # "within an hour" has a DIFFERENT `starts_at`. So the matched row is updated
  # in place rather than upserted and hoped about.
  #
  # The update action is the node's `payload_update` or Ash's `:update`. A
  # resolver rung on a resource with no update action is a declaration that
  # cannot work, and says so.
  defp resolve_write(resource, attrs, resolver, action, cell_key, opts) do
    attrs = stamp_fingerprint(attrs, attrs, resource, opts)
    prior = resolver.(cell_key, attrs, opts)

    case prior do
      nil ->
        {:ok, _} =
          resource
          |> Ash.Changeset.for_create(action, attrs, tenant_opts(opts))
          |> Ash.create()

        lapse(nil, attrs, resource, cell_key, opts)
        :created

      record ->
        verdict = if moved?(record, attrs, opts), do: :changed, else: :unchanged

        if verdict == :changed do
          {:ok, _} =
            record
            |> Ash.Changeset.for_update(update_action!(resource, opts), attrs)
            |> Ash.update()
        end

        lapse(record, attrs, resource, cell_key, opts)
        verdict
    end
  end

  defp update_action!(resource, opts) do
    name = Keyword.get(opts, :payload_update) || :update

    if Ash.Resource.Info.action(resource, name) do
      name
    else
      raise ArgumentError,
            "reactive_dag: a `row_key` RESOLVER matched an existing row, so " <>
              "#{inspect(resource)} needs an update action to revise it — it has no " <>
              "#{inspect(name)}. Add one, or name it with `payload_update:`."
    end
  end

  # The shared tail: stamp, find the prior by whatever the rung says, decide the
  # verdict, write, lapse. Only the FINDING differs between rungs, which is the
  # whole point of declaring it.
  defp write(resource, attrs, find_prior, action, cell_key, opts) do
    attrs = stamp_fingerprint(attrs, attrs, resource, opts)
    prior = find_prior.(attrs)

    verdict =
      case prior do
        nil -> :created
        record -> if moved?(record, attrs, opts), do: :changed, else: :unchanged
      end

    {:ok, written} =
      resource
      |> Ash.Changeset.for_create(action, attrs, tenant_opts(opts))
      |> Ash.create()

    record_diff(cell_key, prior, attrs, verdict, written, opts)
    lapse(prior, attrs, resource, cell_key, opts)

    verdict
  end

  @diffs __MODULE__.Diffs
  @doc """
  WHICH LOOKUP identifies a node's row, from its declarations.

  One function because there were three copies of this decision —
  `Recompute.writer_fn/2`, `Rows.write/3` and `retire/6` — each dispatching on
  `identity_fields` alone. A node identified by `row_key` COLUMNS rather than a key
  column then fell through to `payload_key`, whose default is the resource's single
  primary key: a surrogate UUID. The write put the unit's name into `:id` and the
  lookup filtered a UUID column by `"|FY25/26||"`.

  Returns:

    * `{:identity, fields}` — find by those columns' values, taken off the row.
      Either a composite primary key (`identity_fields`) or a `row_key` column list
      on a node with no key column of its own.
    * `{:key, attr}` — find by that column, which holds the cell key.
  """
  @spec lookup(map()) :: {:identity, [atom()]} | {:key, atom()}
  def lookup(meta) do
    cond do
      is_list(meta[:identity_fields]) ->
        {:identity, meta[:identity_fields]}

      keyless_row_key?(meta) ->
        {:identity, meta[:row_key]}

      true ->
        {:key, meta[:payload_key] || :key}
    end
  end

  # A node that DECLARED it has no key column (`payload_key false`), and named the
  # columns its row is identified by instead.
  #
  # Declared, not inferred. Deducing it from "has a `row_key` and a surrogate
  # primary key" reclassified an existing node the moment it declared a `row_key`
  # — its writes went from the key column to identity, and its lookups filtered a
  # composite cell key against a UUID. `row_key` says how to FIND a row; whether a
  # key column exists is a separate fact only the node can state.
  defp keyless_row_key?(meta) do
    meta[:declared_payload_key] == false and is_list(meta[:row_key])
  end

  @versions __MODULE__.Versions

  @doc """
  Collect the diffs a block of payload writes produced, keyed by cell key.

  A propagating consumer needs to know which UNIT a changed row belonged to
  BEFORE the write as well as after — a row that moved between units affects
  both, and a row that was deleted affects the one it left. This is the only
  place both sides are in hand: the prior record and the new attrs, at the moment
  of the write.

      {changed, diffs} = Payload.collecting_diffs(fn -> materialize(...) end)

  ## TRANSITIONAL — this is a process-dictionary channel

  The diff should travel in the return value, not in the process. Doing that
  properly means widening `upsert/6`, `upsert_row/5` and `upsert_identity/5` from
  a verdict atom to `{verdict, diff}`, which is a breaking change to the one part
  of this library hosts call directly — a real host has 16 such call sites, each
  reading `!= :unchanged`.

  So: collected here for now, on the explicit understanding that it is a
  scaffold. The exit is one release that widens those three returns and deletes
  this section; until then `collecting_diffs/1` is the only supported way to read
  them, so the channel has exactly one entry and one exit rather than becoming
  ambient state anything can reach into.
  """
  @spec collecting_diffs((-> result)) :: {result, %{optional(String.t()) => map()}}
        when result: term()
  def collecting_diffs(fun) when is_function(fun, 0) do
    {result, diffs, _versions} = collecting_diffs_and_versions(fun)
    {result, diffs}
  end

  @doc """
  `collecting_diffs/1`, and the VERSIONS those writes recorded.

  Two returns rather than one because they answer different questions: the diffs
  are what this drain uses NOW to derive its parents' claims, and the versions are
  the durable references it attaches to the marks it writes, so a LATER drain can
  derive units from the same change.

  A version appears only for a cell whose node declares `version_id`, keyed by the
  cell key the write was made under.
  """
  @spec collecting_diffs_and_versions((-> result)) ::
          {result, %{optional(String.t()) => map()}, %{optional(String.t()) => String.t()}}
        when result: term()
  def collecting_diffs_and_versions(fun) when is_function(fun, 0) do
    prev = Process.get(@diffs)
    prev_v = Process.get(@versions)
    Process.put(@diffs, %{})
    Process.put(@versions, %{})

    try do
      result = fun.()
      # Returned as VALUES, not left in the process dictionary: `after` restores
      # the caller's dict, and the drain's propagation runs outside this block.
      {result, Process.get(@diffs, %{}), Process.get(@versions, %{})}
    after
      if prev, do: Process.put(@diffs, prev), else: Process.delete(@diffs)
      if prev_v, do: Process.put(@versions, prev_v), else: Process.delete(@versions)
    end
  end

  # Outside a `collecting_diffs/1` block this is a no-op, so every existing
  # caller — a host op writing its own rows, a test driving `upsert/6` — behaves
  # exactly as before and pays nothing.
  defp record_diff(_cell_key, _prior, _attrs, :unchanged, _written, _opts), do: :ok

  defp record_diff(cell_key, prior, attrs, _verdict, written, opts) do
    case Process.get(@diffs) do
      nil ->
        :ok

      acc ->
        Process.put(@diffs, Map.put(acc, cell_key, diff(prior, attrs)))
        record_version(cell_key, written, prior, opts)
    end
  end

  # The VERSION this write produced, if the node declared how to record one.
  #
  # Why the payload path needs this and not just `dirties_on`: a graph-written row
  # propagates to its parents, and a propagated mark can only REFERENCE a change
  # if the write that caused it recorded one. Without this the mark carries a NULL
  # version, and the parent falls back to splitting its `"|"` key — the round-trip
  # `Diff.groups/2` exists to remove.
  #
  # Same MFA/fun contract as `MarkDirty`'s resolver, so a host declares
  # `version_id` once and both producers honour it. The arguments differ by
  # necessity: the payload loop builds attrs rather than accepting a caller's
  # changeset, so a resolver here receives the written record and the prior one.
  defp record_version(cell_key, written, prior, opts) do
    with resolver when not is_nil(resolver) <- opts[:version_id],
         acc when not is_nil(acc) <- Process.get(@versions),
         id when is_binary(id) <- resolve_version(resolver, written, prior) do
      Process.put(@versions, Map.put(acc, cell_key, id))
    else
      _ -> :ok
    end
  end

  defp resolve_version(resolver, record, prior) do
    case resolver do
      {m, f, a} -> apply(m, f, [record, prior | a])
      fun when is_function(fun, 2) -> fun.(record, prior)
    end
  rescue
    e ->
      require Logger

      Logger.warning(
        "reactive_dag: version_id resolver failed on a payload write " <>
          "(#{Exception.message(e)}); the propagated mark will carry no reference"
      )

      nil
  end

  # The `:full_diff` shape `ash_paper_trail` writes, and which
  # `ReactiveDag.Node.Diff` reads — one vocabulary for "what moved",
  # whether it came from a version row or from here.
  #
  # STRING keys, because that is what a jsonb column round-trips to and this must
  # not read differently depending on where the diff was born.
  defp diff(nil, attrs) do
    Map.new(attrs, fn {k, v} -> {to_string(k), %{"to" => v}} end)
  end

  defp diff(prior, attrs) do
    Map.new(attrs, fn {k, v} ->
      case Map.get(prior, k) do
        ^v -> {to_string(k), %{"unchanged" => v}}
        was -> {to_string(k), %{"from" => was, "to" => v}}
      end
    end)
  end

  defp primary_key!(resource) do
    case Ash.Resource.Info.primary_key(resource) do
      [single] ->
        single

      other ->
        raise ArgumentError,
              "reactive_dag: `row_key :uuid` needs a single-attribute primary key to map " <>
                "the cell key onto; #{inspect(resource)} has #{inspect(other)}."
    end
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

    prior = existing_by(resource, Map.take(attrs, identity_fields), opts)

    verdict =
      case prior do
        nil -> :created
        record -> if moved?(record, attrs, opts), do: :changed, else: :unchanged
      end

    {:ok, _} =
      resource
      |> Ash.Changeset.for_create(action, attrs, tenant_opts(opts))
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
  @spec retire(module(), atom() | nil, [atom()] | nil, [String.t()], atom(), keyword()) ::
          [String.t()]
  def retire(resource, key_attr, identity_fields, keys, action \\ :destroy, opts \\ [])
  def retire(_resource, _key_attr, _identity_fields, [], _action, _opts), do: []

  def retire(resource, key_attr, identity_fields, keys, action, opts) do
    ensure_destroy!(resource, action)

    # `groups:` — the unit's own column VALUES, when the caller derived them from a
    # change (`Drain.derive_units/4`). With them a row is found by its columns and
    # needs no key column at all, which is what lets a node drop the stored
    # `"|"`-joined key it used to be found by.
    #
    # Without them the lookup falls back to `key_attr`/`identity_fields` below —
    # the pre-existing behaviour for a node that still stores its key.
    groups = opts[:groups] || %{}
    fields = opts[:row_key_fields]

    Enum.filter(keys, fn key ->
      # The lookup is TENANT-SCOPED, and that is the whole safety of this
      # function under tenancy: an unscoped find would hand back another
      # tenant's row for the same key and destroy it. Reconciling a partial read
      # against a total baseline is the shape that produced the two-node join
      # bug; here it deletes rather than nils.
      case find_by_group(resource, fields, groups[key], opts) ||
             find_by_key_fields(resource, fields, key, opts) ||
             find_row(resource, key_attr, identity_fields, key, opts) do
        nil ->
          false

        record ->
          record |> Ash.Changeset.for_destroy(action) |> Ash.destroy!()
          true
      end
    end)
  end

  # Find a row by the unit's own column values — no key column, no `"|"` split.
  #
  # `fields` is the node's `row_key` minus the tenant column (which travels in
  # opts, not in the grain), and `values` is one group tuple. A mismatch in arity
  # means these are not this node's fields, so it declines rather than guessing.
  defp find_by_group(_resource, nil, _values, _opts), do: nil
  defp find_by_group(_resource, _fields, nil, _opts), do: nil
  defp find_by_group(_resource, _fields, [], _opts), do: nil

  defp find_by_group(resource, fields, [group | _], opts) do
    values = if is_tuple(group), do: Tuple.to_list(group), else: [group]

    if length(values) == length(fields) do
      existing_by(resource, fields |> Enum.zip(values) |> Map.new(), opts)
    else
      nil
    end
  end

  # A keyless node with NO group values to hand — a whole-cell pass, or a claim
  # whose change carried no version. The unit's name is still the join of its
  # `row_key` columns, so it splits back across them.
  #
  # This is the one place the `"|"` form survives, and it survives as a FALLBACK
  # rather than as storage: nothing writes it, nothing reads it off a row. Without
  # it the lookup fell through to filtering the whole composite against the
  # surrogate primary key — `id == "|FY25/26||"` — which raised
  # `InvalidFilterValue` on a UUID column. A host's reprocess found that.
  defp find_by_key_fields(_resource, nil, _key, _opts), do: nil

  defp find_by_key_fields(resource, fields, key, opts) do
    values = String.split(key, "|")

    if length(values) == length(fields) do
      existing_by(resource, fields |> Enum.zip(values) |> Map.new(), opts)
    else
      nil
    end
  end

  defp find_row(resource, _key_attr, fields, key, opts) when is_list(fields) do
    values = String.split(key, "|")

    if length(values) == length(fields) do
      existing_by(resource, fields |> Enum.zip(values) |> Map.new(), opts)
    else
      nil
    end
  end

  defp find_row(resource, key_attr, _fields, key, opts),
    do: existing(resource, key_attr, key, opts)

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

  defp existing_by(resource, identity_map, opts) do
    resource
    |> Ash.Query.do_filter(Enum.to_list(identity_map))
    |> scoped(opts)
    |> Ash.read_one()
    |> case do
      {:ok, record} -> record
      _ -> nil
    end
  end

  defp existing(resource, key_attr, cell_key, opts) do
    resource
    |> Ash.Query.do_filter([{key_attr, cell_key}])
    |> scoped(opts)
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
  # Three answers to "did this move?", in precedence order:
  #
  #   * a declared FINGERPRINT — compare the one stored digest. For a source-fed
  #     leaf, whose row carries fields that move on every observation without the
  #     observation having changed.
  #   * a declared COMPARE list — compare those columns and no others. For a
  #     derived row carrying fields that are part of the RECORD but not part of
  #     the RESULT: provenance (which document this came from), position (an
  #     `ordinal` that shifts when a re-parse reorders rows), anything a consumer
  #     never folds on. Comparing them reports a change nothing made.
  #   * neither — compare every field the row carries, which is right when every
  #     field is part of the result.
  #
  # `:compare` narrows with `Map.take`, exactly as `lapse_moved?/3` does. Both are
  # the same question asked of fewer columns, and neither routes through the
  # fingerprint: `Fingerprint.put/5` treats nil as a no-op, and a nil digest
  # falling back to "compare everything" is the safe direction for change
  # detection but not for a narrowed comparison, which must mean what it says.
  defp moved?(record, attrs, opts) do
    case fingerprint_attr(opts, attrs) do
      {attr, value} when not is_nil(value) ->
        Map.get(record, attr) != value

      _ ->
        case opts[:compare] do
          fields when is_list(fields) and fields != [] -> differs?(record, Map.take(attrs, fields))
          _ -> differs?(record, attrs)
        end
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

  # THE tenant seam. `opts[:tenant]` is the PLAN's tenant; everything below hands
  # it to Ash and lets Ash decide what it means.
  #
  # Ash's own `handle_attribute_multitenancy/1` (create.ex, read.ex) reads the
  # resource's `multitenancy` block, applies its `parse_attribute` MFA and forces
  # the column — so this library never learns the attribute name. That matters:
  # naming the column here would break any host whose `parse_attribute` is not
  # the identity function, and would be a second copy of a fact the resource
  # already declares.
  #
  # A resource declaring no multitenancy ignores the tenant entirely (Ash guards
  # on `strategy == :attribute`), so passing it is always safe.
  defp tenant_opts(opts) do
    case Keyword.get(opts, :tenant) do
      nil -> []
      tenant -> [tenant: tenant]
    end
  end

  defp scoped(query, opts) do
    case Keyword.get(opts, :tenant) do
      nil -> query
      tenant -> Ash.Query.set_tenant(query, tenant)
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

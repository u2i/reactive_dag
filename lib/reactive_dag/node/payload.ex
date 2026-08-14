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
  """

  require Ash.Query

  alias ReactiveDag.Node.Fingerprint

  @doc """
  Upsert `row` into `resource` under `cell_key` (written to `key_attr`), via
  `action`. Returns `:changed` (created, or a writable attr differs) or
  `:unchanged`. `row`'s `:key` field is dropped before writing.

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
  """
  @spec upsert(module(), atom(), String.t(), map(), atom(), keyword()) :: :changed | :unchanged
  def upsert(resource, key_attr, cell_key, row, action \\ :upsert, opts \\ []) do
    attrs =
      row
      |> Map.drop([:key])
      |> Map.put(key_attr, cell_key)
      |> stamp_fingerprint(row, resource, opts)
      |> stamp_live(opts)

    changed? =
      case existing(resource, key_attr, cell_key) do
        nil -> true
        record -> moved?(record, attrs, opts)
      end

    {:ok, _} =
      resource
      |> Ash.Changeset.for_create(action, attrs)
      |> Ash.create()

    if changed?, do: :changed, else: :unchanged
  end

  @doc """
  Upsert an IDENTITY-KEYED row (a composite-primary-key node): the row carries
  its identity fields, the upsert conflicts on the primary key (Ash's default
  for `upsert? true`), and no key column exists — the cell key is the
  identity's serialization, derived elsewhere. Returns `:changed` | `:unchanged`
  with the same read-compare change detection as `upsert/6`, and the same
  `:fingerprint` / `:fingerprint_attribute` options.
  """
  @spec upsert_identity(module(), [atom()], map(), atom(), keyword()) :: :changed | :unchanged
  def upsert_identity(resource, identity_fields, row, action \\ :upsert, opts \\ []) do
    attrs = row |> Map.drop([:key]) |> stamp_fingerprint(row, resource, opts) |> stamp_live(opts)

    changed? =
      case existing_by(resource, Map.take(attrs, identity_fields)) do
        nil -> true
        record -> moved?(record, attrs, opts)
      end

    {:ok, _} =
      resource
      |> Ash.Changeset.for_create(action, attrs)
      |> Ash.create()

    if changed?, do: :changed, else: :unchanged
  end

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

  @doc """
  MARK `keys` retired rather than destroying their rows — the retain-if-vanish
  policy a node declares with `retain_if_vanished`.

  Sets the status attribute to the declared `retired` value and, when the node
  named one, stamps the timestamp. Rows already absent are skipped, so this is
  idempotent. Returns the keys whose row was actually marked.

  The row survives, which is the point: the upstream withdrew the item but the
  artifact you fetched is still yours. `reconcile/3` then leaves it out of its
  baseline, so it is not retired again on the next poll.
  """
  @spec mark_retired(module(), atom() | nil, [atom()] | nil, [String.t()], map(), atom()) ::
          [String.t()]
  def mark_retired(_resource, _key_attr, _fields, [], _policy, _action), do: []

  def mark_retired(resource, key_attr, fields, keys, policy, action) do
    now = DateTime.utc_now()

    Enum.filter(keys, fn key ->
      case find_row(resource, key_attr, fields, key) do
        nil ->
          false

        record ->
          attrs =
            %{policy.status => policy.retired}
            |> then(&if policy[:at], do: Map.put(&1, policy.at, now), else: &1)

          record
          |> Ash.Changeset.for_update(update_action(resource, action), attrs)
          |> Ash.update!()

          true
      end
    end)
  end

  # a node's `:upsert` is a create action, so marking goes through an UPDATE:
  # the primary one when it exists, else Ash's built-in.
  defp update_action(resource, _action) do
    case Ash.Resource.Info.primary_action(resource, :update) do
      %{name: name} -> name
      _ -> :update
    end
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
    revived?(record, opts) or
      case fingerprint_attr(opts, attrs) do
        {attr, value} when not is_nil(value) -> Map.get(record, attr) != value
        _ -> differs?(record, attrs)
      end
  end

  # COMING BACK is a change, even when the bytes did not move. A retired row that
  # reappears carries its old fingerprint — its liveness changed, not its
  # content — so a fingerprint comparison alone would report "unchanged" and the
  # revival would never propagate. The prior record is already in hand for that
  # comparison, so seeing this costs no extra read.
  defp revived?(record, opts) do
    case opts[:retain_if_vanished] do
      %{status: attr, retired: retired} -> Map.get(record, attr) == retired
      _ -> false
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

  # An observed row is LIVE by definition — otherwise a revived row would be
  # written back still marked retired, and `reconcile/3` would leave it out of
  # its own baseline forever. An explicit status in the row wins: the host may
  # have its own vocabulary beyond live/retired.
  defp stamp_live(attrs, opts) do
    case opts[:retain_if_vanished] do
      %{status: attr, live: live} -> Map.put_new(attrs, attr, live)
      _ -> attrs
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

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
  the coordination key, not a payload column).

  ## Change detection

  `upsert/5` reads the existing row first and compares the writable attributes; it
  returns `:changed` only when a create or a real value change happened, so the
  drain's `Op.put`/parent-dirty only fires for genuine changes (a no-op recompute
  stays a no-op). Requires an upsert action on the resource — by default the
  action named `:upsert` with `upsert?: true`; override with `payload_action`.
  """

  require Ash.Query

  @doc """
  Upsert `row` into `resource` under `cell_key` (written to `key_attr`), via
  `action`. Returns `:changed` (created, or a writable attr differs) or
  `:unchanged`. `row`'s `:key` field is dropped before writing.
  """
  @spec upsert(module(), atom(), String.t(), map(), atom()) :: :changed | :unchanged
  def upsert(resource, key_attr, cell_key, row, action \\ :upsert) do
    attrs = row |> Map.drop([:key]) |> Map.put(key_attr, cell_key)

    changed? =
      case existing(resource, key_attr, cell_key) do
        nil -> true
        record -> differs?(record, attrs)
      end

    {:ok, _} =
      resource
      |> Ash.Changeset.for_create(action, attrs)
      |> Ash.create()

    if changed?, do: :changed, else: :unchanged
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
end

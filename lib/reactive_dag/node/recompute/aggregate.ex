defmodule ReactiveDag.Node.Recompute.Aggregate do
  @moduledoc """
  Runs a pure-Ash-query `aggregate` node: the datastore groups + aggregates the
  node's `over` relationship in ONE query (a relationship aggregate — Postgres does
  the `GROUP BY`), and each parent row's aggregate values become its payload.

  The node's resource is the group's own resource — ONE row per group, aggregating
  the finer rows reached through `over`. The runner:

    1. reads that resource with the relationship aggregates loaded under temporary
       names (`Ash.Query.aggregate(q, tmp, kind, over, field: src)`),
    2. for each row, projects the loaded aggregates onto the resource's own
       attributes (the `dest` in each mapping), upserts the row (via
       `ReactiveDag.Node.Payload`), and returns the changed keys.

  It is a WHOLE-CELL recompute: a `GROUP BY` reprices every group, so the changed
  set is every group whose aggregate value actually moved (Payload's change-
  detection filters the no-ops).
  """
  require Ash.Query

  @doc "Recompute an aggregate node; returns the changed keys."
  @spec recompute(ReactiveDag.Cell.t(), module(), map()) :: [String.t()]
  def recompute(cell, resource, agg) do
    loads = load_specs(agg)             # [{tmp_name, kind, over, src, dest}]
    action = cell.meta[:payload_action] || :upsert
    {key_attr, key_of} = keying(cell)

    resource
    |> load_aggregates(agg.over, loads)
    |> Ash.read!()
    |> Enum.flat_map(fn row ->
      cell_key = key_of.(row)
      payload = project(row, key_attr, identity_fields(cell), loads)

      case write(resource, cell, key_attr, cell_key, payload, action) do
        v when v in [:created, :changed] -> [cell_key]
        :unchanged -> []
      end
    end)
  end

  # KEYS ARE ASH KEYS here too (the same rule `reduce` follows): a COMPOSITE
  # primary key means the row IS its identity — no key column, and the cell key
  # is the identity serialized in primary-key order. Otherwise the payload key
  # (derived from a single-attribute PK, or declared) names the column.
  defp keying(%{meta: %{identity_fields: fields}}) when is_list(fields) do
    key_fn = ReactiveDag.Node.Recompute.Declarative.identity_key_fn(fields, nil)
    {nil, key_fn}
  end

  defp keying(cell) do
    key_attr = cell.meta[:payload_key] || :key
    {key_attr, fn row -> row |> Map.fetch!(key_attr) |> to_string() end}
  end

  defp write(resource, %{meta: %{identity_fields: fields}} = cell, _key_attr, _cell_key, payload, action)
       when is_list(fields) do
    ReactiveDag.Node.Payload.upsert_identity(resource, fields, payload, action, lapse_opts(cell))
  end

  defp write(resource, cell, key_attr, cell_key, payload, action) do
    ReactiveDag.Node.Payload.upsert(resource, key_attr, cell_key, payload, action, lapse_opts(cell))
  end

  # what a recompute CLEARS — nil unless the node declares a `lapse`, so the
  # aggregate path is untouched for every node that does not.
  defp lapse_opts(%{meta: meta}), do: [lapse: meta[:lapse]]

  # add each aggregate to the query under its temp name, then LOAD them so they're
  # computed + present on the result rows (aggregate/5 defines; load selects).
  defp load_aggregates(resource, over, loads) do
    Enum.reduce(loads, Ash.Query.new(resource), fn {tmp, kind, _over, src, _dest}, q ->
      opts = if kind == :count, do: [], else: [field: src]
      Ash.Query.aggregate(q, tmp, kind, over, opts)
    end)
  end

  defp identity_fields(%{meta: %{identity_fields: fields}}) when is_list(fields), do: fields
  defp identity_fields(_cell), do: nil

  # the row's payload = the columns that IDENTIFY it (the identity fields for a
  # composite-PK node, else the key attribute) + each aggregate on its dest.
  # Query-added aggregates land in the row's `.aggregates` map (keyed by our
  # temp name), NOT as struct fields.
  defp project(row, key_attr, identity_fields, loads) do
    aggs = Map.get(row, :aggregates) || %{}

    base =
      case identity_fields do
        fields when is_list(fields) -> Map.new(fields, &{&1, Map.fetch!(row, &1)})
        _ -> %{key_attr => Map.fetch!(row, key_attr)}
      end

    Enum.reduce(loads, base, fn {tmp, _kind, _over, _src, dest}, acc ->
      Map.put(acc, dest, Map.get(aggs, tmp))
    end)
  end

  # flatten the Aggregate struct into `[{tmp_name, kind, over, src_field, dest_attr}]`.
  # `count: :day_count` → src nil, dest :day_count.
  # `avg: [flow: :avg_flow]` → src :flow, dest :avg_flow.
  defp load_specs(%{over: over} = agg) do
    for kind <- ReactiveDag.Node.Recompute.Declarative.fold_kinds(),
        spec = Map.get(agg, kind),
        not is_nil(spec),
        {src, dest} <- normalize(kind, spec) do
      {:"__rd_#{kind}_#{dest}", kind, over, src, dest}
    end
  end

  defp normalize(:count, dest) when is_atom(dest), do: [{nil, dest}]
  defp normalize(_kind, mapping) when is_list(mapping), do: mapping
  defp normalize(_kind, dest) when is_atom(dest), do: [{dest, dest}]
end

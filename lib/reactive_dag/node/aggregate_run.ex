defmodule ReactiveDag.Node.AggregateRun do
  @moduledoc """
  Runs a pure-Ash-query `aggregate` node: the datastore groups + aggregates the
  node's `over` relationship in ONE query (a relationship aggregate — Postgres does
  the `GROUP BY`), and each parent-grain row's aggregate values become its payload.

  The node's resource IS the group grain (one row per group). The runner:

    1. reads that resource with the relationship aggregates loaded under temporary
       names (`Ash.Query.aggregate(q, tmp, kind, over, field: src)`),
    2. for each row, projects the loaded aggregates onto the resource's own
       attributes (the `dest` in each mapping), upserts the row (via
       `ReactiveDag.Node.Payload`), and `Op.put`s the changed keys.

  It is a WHOLE-CELL recompute: a `GROUP BY` reprices every group, so the changed
  set is every group whose aggregate value actually moved (Payload's change-
  detection filters the no-ops).
  """
  require Ash.Query

  @doc "Recompute an aggregate node; returns the changed keys."
  @spec recompute(ReactiveDag.Cell.t(), module(), map()) :: [String.t()]
  def recompute(cell, resource, agg) do
    loads = load_specs(agg)             # [{tmp_name, kind, over, src, dest}]
    key_attr = cell.meta[:payload_key] || :key
    action = cell.meta[:payload_action] || :upsert

    resource
    |> load_aggregates(agg.over, loads)
    |> Ash.read!()
    |> Enum.flat_map(fn row ->
      cell_key = Map.fetch!(row, key_attr) |> to_string()
      payload = project(row, key_attr, loads)

      case ReactiveDag.Node.Payload.upsert(resource, key_attr, cell_key, payload, action) do
        :changed -> ReactiveDag.Op.put(cell, cell_key) && [cell_key]
        :unchanged -> []
      end
    end)
  end

  # add each aggregate to the query under its temp name, then LOAD them so they're
  # computed + present on the result rows (aggregate/5 defines; load selects).
  defp load_aggregates(resource, over, loads) do
    Enum.reduce(loads, Ash.Query.new(resource), fn {tmp, kind, _over, src, _dest}, q ->
      opts = if kind == :count, do: [], else: [field: src]
      Ash.Query.aggregate(q, tmp, kind, over, opts)
    end)
  end

  # the row's payload = its key attr + each aggregate projected onto its dest attr.
  # Query-added aggregates land in the row's `.aggregates` map (keyed by our temp
  # name), NOT as struct fields.
  defp project(row, key_attr, loads) do
    aggs = Map.get(row, :aggregates) || %{}
    base = %{key_attr => Map.fetch!(row, key_attr)}

    Enum.reduce(loads, base, fn {tmp, _kind, _over, _src, dest}, acc ->
      Map.put(acc, dest, Map.get(aggs, tmp))
    end)
  end

  # flatten the Aggregate struct into `[{tmp_name, kind, over, src_field, dest_attr}]`.
  # `count: :day_count` → src nil, dest :day_count.
  # `avg: [flow: :avg_flow]` → src :flow, dest :avg_flow.
  defp load_specs(%{over: over} = agg) do
    for kind <- [:count, :sum, :avg, :min, :max, :first],
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

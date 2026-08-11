defmodule ReactiveDag.Node.Recompute.Read do
  @moduledoc """
  Executes a combinator's READ — always an Ash read of the over node's
  resource (its primary read action, or the `:read` action `read:` names),
  shaped by the optional `query:` transformer, and ALWAYS scoped by the
  library: when the recompute claimed specific dirty keys, the over's payload
  key is filtered to them (the transformer cannot un-scope; scoping is the
  substrate's correctness concern, not policy).

  The over node's resource is a cross-node fact stamped into
  `cell.meta.over_source` at graph assembly (`ReactiveDag.Node.graph/2`); a
  cell built via `to_cell/1` alone has no stamp and raises here with guidance.
  """

  @doc """
  The over node's items: `source` is `cell.meta[:over_source]` (or nil),
  `query_fn` the combinator's `query:` (or nil), `scope` the claimed dirty
  keys or nil for whole-cell.
  """
  @spec items(map() | nil, atom(), ({Ash.Query.t(), [String.t()] | nil} -> Ash.Query.t()) | nil, [String.t()] | nil) ::
          [term()]
  def items(source, over, query_fn, scope)

  def items(nil, over, _query_fn, _scope) do
    raise ArgumentError,
          "reactive_dag: the read over #{inspect(over)} is resolved at graph assembly — " <>
            "build the plan with ReactiveDag.Node.graph/2 (a to_cell/1-only cell cannot " <>
            "know the over node's resource)"
  end

  def items(%{resource: resource, payload_key: pk, read_action: action}, _over, query_fn, scope) do
    base =
      case action do
        nil -> Ash.Query.new(resource)
        name -> Ash.Query.for_read(resource, name)
      end

    base
    |> apply_query(query_fn, scope)
    |> scope_query(pk, scope)
    |> Ash.read!()
  end

  defp apply_query(query, nil, _scope), do: query
  defp apply_query(query, fun, scope) when is_function(fun, 2), do: fun.(query, scope)

  defp scope_query(query, _pk, nil), do: query
  defp scope_query(query, pk, keys), do: Ash.Query.do_filter(query, [{pk, [in: keys]}])
end

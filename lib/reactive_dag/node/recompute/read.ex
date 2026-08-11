defmodule ReactiveDag.Node.Recompute.Read do
  @moduledoc """
  Resolves a combinator's READ. The fn escape hatches behave as ever (the host
  owns the read and its scoping); the Ash-first declarative forms — `read:`
  omitted (primary read) or naming a `:read` action — have the LIBRARY read
  the over node's resource, automatically scoped to the claimed dirty keys by
  filtering the over's payload key.

  Declarative reads need the over node's resource, a cross-node fact stamped
  into `cell.meta.over_source` at graph assembly (`ReactiveDag.Node.graph/2`);
  a cell built via `to_cell/1` alone has no stamp and raises here with
  guidance.
  """

  @doc """
  The over node's items for a recompute: `read` is the combinator's `read:`
  value (fn, atom, or nil), `source` is `cell.meta[:over_source]` (or nil),
  `scope` is the claimed dirty keys or nil for whole-cell.
  """
  @spec items(term(), atom(), map() | nil, [String.t()] | nil) :: [term()]
  def items(read, over, _source, scope) when is_function(read, 2), do: read.(over, scope)
  def items(read, over, _source, _scope) when is_function(read, 1), do: read.(over)

  def items(read, over, nil, _scope) do
    raise ArgumentError,
          "reactive_dag: a declarative read (#{inspect(read)}) over #{inspect(over)} is " <>
            "resolved at graph assembly — build the plan with ReactiveDag.Node.graph/2, " <>
            "or supply a `read:` fn"
  end

  def items(_read, _over, %{resource: resource, payload_key: pk, read_action: action}, scope) do
    query =
      case action do
        nil -> Ash.Query.new(resource)
        name -> Ash.Query.for_read(resource, name)
      end

    query
    |> scope_query(pk, scope)
    |> Ash.read!()
  end

  defp scope_query(query, _pk, nil), do: query
  defp scope_query(query, pk, keys), do: Ash.Query.do_filter(query, [{pk, [in: keys]}])
end

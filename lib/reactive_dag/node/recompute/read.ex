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
  `query_fn` the combinator's `query:` (or nil), `claimed` the claimed dirty
  keys or nil for whole-cell (what `query:` receives), and `auto_scope` the
  keys the LIBRARY filters the over's payload key to — the claimed keys under
  `key_rule :identity` (this cell's keys ARE the over's), nil otherwise (a
  grain-changing rule's parent-grain keys must not filter the child-grain
  read; the host scopes at its own grain through `query:`).
  """
  @spec items(
          map() | nil,
          atom(),
          (Ash.Query.t(), [String.t()] | nil -> Ash.Query.t()) | nil,
          [String.t()] | nil,
          [String.t()] | nil
        ) ::
          [term()]
  def items(source, over, query_fn, claimed, auto_scope, opts \\ [])

  def items(nil, over, _query_fn, _claimed, _auto_scope, _opts) do
    raise ArgumentError,
          "reactive_dag: the read over #{inspect(over)} is resolved at graph assembly — " <>
            "build the plan with ReactiveDag.Node.graph/2 (a to_cell/1-only cell cannot " <>
            "know the over node's resource)"
  end

  def items(
        %{resource: resource, payload_key: pk, read_action: action} = source,
        _over,
        query_fn,
        claimed,
        auto_scope,
        opts
      ) do
    base =
      case action do
        nil -> Ash.Query.new(resource)
        name -> Ash.Query.for_read(resource, name)
      end

    base
    |> load_calcs(Map.get(source, :load, []))
    |> apply_query(query_fn, claimed)
    |> scope_query(pk, auto_scope)
    |> tenant_scoped(opts)
    |> Ash.read!()
  end

  # The over node's read is scoped to the PLAN's tenant. Reading a tenanted
  # upstream without one raises ("Queries against … require a tenant"), and
  # reading it under `global?` would fold another tenant's rows into this one's
  # result — the wrong answer rather than an error.
  defp tenant_scoped(query, opts) do
    case Keyword.get(opts, :tenant) do
      nil -> query
      "*" -> query
      tenant -> Ash.Query.set_tenant(query, tenant)
    end
  end

  defp load_calcs(query, []), do: query
  defp load_calcs(query, loads), do: Ash.Query.load(query, loads)

  defp apply_query(query, nil, _claimed), do: query
  defp apply_query(query, fun, claimed) when is_function(fun, 2), do: fun.(query, claimed)

  defp scope_query(query, _pk, nil), do: query

  # an IDENTITY-KEYED over (composite PK) has no key column to filter — the
  # keyed scope stands down and the read stays whole (or `query:`-scoped).
  defp scope_query(query, nil, {:keys, _keys}), do: query

  defp scope_query(query, pk, {:keys, keys}),
    do: Ash.Query.do_filter(query, [{pk, [in: keys]}])

  defp scope_query(query, _pk, {:range, attr, from, to}),
    do: Ash.Query.do_filter(query, [{attr, [greater_than_or_equal: from, less_than: to]}])

  defp scope_query(query, _pk, {:attr, attr, values}),
    do: Ash.Query.do_filter(query, [{attr, [in: values]}])

  # a COMPOSITE unit's scope: every column filtered by the values seen at its
  # position, ANDed. For several claims this admits a cross-product superset of
  # the claimed units — sound (still closed over unit boundaries) and far
  # tighter than reading whole.
  defp scope_query(query, pk, {:all_of, clauses}),
    do: Enum.reduce(clauses, query, &scope_query(&2, pk, &1))
end

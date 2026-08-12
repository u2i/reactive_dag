defmodule ReactiveDag.Node.Keys do
  @moduledoc """
  **Which keys does a cell currently have?** — one question, answered from
  whichever place that cell's rows actually live.

  Several parts of the library need a cell's key set and have no business
  knowing how it is stored: retirement subtracts it from what a pass produced
  (`current - want = vanished`), and attestations use it three times over — the
  raw rows a requirement is about, the eligibility set of who may sign, and the
  subset a set-level scope selects. None of them care about status or freshness;
  they care about *which keys exist*.

  Where that lives depends on the node:

    * a **payload node** — its own rows. The resource is the truth about which
      units it holds; the coordination tuple belongs to the host, may be written
      by a different writer, and is not what a consumer queries.
    * a **tableless verdict node** — the coordination tuple, because that is the
      only place its result exists.
    * a **leaf** — the tuple, because its keys come from outside the graph and a
      source wrote them there.

  Asking here rather than reaching for `ReactiveDag.Tuple` directly is what lets
  a node's storage change without every consumer changing with it.
  """

  alias ReactiveDag.{Cell, Tuple}
  alias ReactiveDag.Node.Recompute.Declarative

  @doc """
  The keys `cell` currently holds, or `nil` when they cannot be enumerated (an
  unconfigured coordination table, a resource that will not read).

  `nil` is deliberately distinct from `[]`: "no keys" and "I cannot tell you"
  lead to opposite decisions in a caller that retires what it does not find.
  """
  @spec current(Cell.t()) :: [String.t()] | nil
  def current(%Cell{meta: %{verdict: true}, id: id}), do: tuple_keys(id)

  def current(%Cell{meta: meta, id: id}) do
    case meta[:resource] do
      nil -> tuple_keys(id)
      resource -> resource_keys(resource, meta)
    end
  end

  def current(%Cell{id: id}), do: tuple_keys(id)

  @doc """
  The keys of the cell `cell_id` names in `plan`, resolved as `current/1` —
  for callers that hold an id rather than a cell (attestations name their
  raw and eligibility cells by id).

  Falls back to the coordination tuple for an id the plan does not know, which
  is what a host direct-writing a cell outside its plan relies on.
  """
  @spec current(ReactiveDag.Plan.t() | map(), String.t()) :: [String.t()] | nil
  def current(%{cells: cells}, cell_id) do
    case Map.get(cells, cell_id) do
      nil -> tuple_keys(cell_id)
      cell -> current(cell)
    end
  end

  @doc """
  `current/1`, narrowed to the keys a `t:ReactiveDag.Tuple.key_scope/0` selects.

  The filtering happens **in memory**, against the key list, using the same
  predicate `ReactiveDag.Attestation.Scope` applies to rows it already holds.
  That is the point: the SQL and in-memory paths had to agree exactly ("MUST
  select the same keys the SQL predicate would"), and two implementations of one
  predicate is a correctness hazard whichever way it drifts. Now there is one.
  """
  @spec scoped(Cell.t() | nil, String.t(), term()) :: [String.t()] | nil
  def scoped(cell_or_nil, cell_id, key_scope)

  def scoped(nil, cell_id, key_scope), do: filter(tuple_keys(cell_id), key_scope)
  def scoped(%Cell{} = cell, _cell_id, key_scope), do: filter(current(cell), key_scope)

  defp filter(nil, _key_scope), do: nil
  defp filter(keys, nil), do: keys

  defp filter(keys, key_scope),
    do: Enum.filter(keys, &ReactiveDag.Attestation.Scope.matches?(key_scope, &1))

  # a payload node's OWN rows, keyed the way the payload loop keys them: the
  # identity serialized in primary-key order for a composite PK, else the
  # payload key attribute.
  defp resource_keys(resource, meta) do
    key_of =
      case meta[:identity_fields] do
        fields when is_list(fields) -> Declarative.identity_key_fn(fields, nil)
        _ -> &(&1 |> Map.fetch!(meta[:payload_key] || :key) |> to_string())
      end

    resource |> Ash.read!() |> Enum.map(key_of)
  rescue
    _ -> nil
  end

  defp tuple_keys(id) do
    Tuple.all_keys(id)
  rescue
    _ -> nil
  end
end

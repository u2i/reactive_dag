defmodule ReactiveDag.Node.Recompute.Declarative do
  @moduledoc """
  The pure builders behind the DECLARATIVE combinator slots. Each turns a
  declarative spec — attribute atoms, fold keywords, side picks — into the
  same fn shape the per-slot escape hatches supply, so the recompute pipeline
  runs one code path however declarative the author went.

  Fold semantics follow SQL aggregates: `nil` source values are excluded, and
  a fold over no non-nil values yields `nil` (`count` counts rows and is never
  nil). `first` is the first non-nil value in the group's read order.
  """

  @fold_kinds [:count, :sum, :avg, :min, :max, :first]

  @doc "The fold kinds `into:`'s declarative form accepts."
  @spec fold_kinds() :: [atom()]
  def fold_kinds, do: @fold_kinds

  @doc """
  group_by spec → `(item -> group_term)`. A list groups by the TUPLE of entry
  values. An entry names an attribute OR a CALCULATION on the over resource —
  derived grouping values (a calendar bucket, a normalized code) are Ash
  calculations, declared where the data lives; the library loads them in the
  read (`ReactiveDag.Calendar` ships the calendar ones).
  """
  def group_fn(fun) when is_function(fun, 1), do: fun
  def group_fn(attr) when is_atom(attr), do: fn item -> Map.get(item, attr) end

  def group_fn(entries) when is_list(entries) do
    fn item -> entries |> Enum.map(&Map.get(item, &1)) |> List.to_tuple() end
  end

  @doc """
  key spec (+ `key_prefix`) → `(group_term_or_join_key -> key string)`. The
  default joins a tuple's values with `"|"` (a single value stringifies), and
  prepends `"<prefix>|"` when a prefix is given.
  """
  def key_fn(fun, _prefix) when is_function(fun, 1), do: fun

  def key_fn(nil, prefix) do
    fn
      tuple when is_tuple(tuple) ->
        tuple |> Tuple.to_list() |> Enum.map_join("|", &to_string/1) |> prefixed(prefix)

      value ->
        value |> to_string() |> prefixed(prefix)
    end
  end

  defp prefixed(key, nil), do: key
  defp prefixed(key, prefix), do: "#{prefix}|#{key}"

  @doc """
  reduce `into:` spec → `(group_term, items -> row)`. The declarative form
  produces the group's attribute columns (requires a declarative `group_by`,
  enforced at compile time) merged with the fold results, keyed by dest names.
  """
  def into_fn(fun, _group_by) when is_function(fun, 2), do: fun

  def into_fn(folds, group_by) when is_list(folds) do
    dests = List.wrap(group_by)

    fn group_term, items ->
      dests
      |> Enum.zip(group_values(group_term))
      |> Map.new()
      |> Map.merge(fold_row(folds, items))
    end
  end

  # a list-form group_by always yields a tuple (even single-entry); a bare-atom
  # group_by yields the bare value.
  defp group_values(term) when is_tuple(term), do: Tuple.to_list(term)
  defp group_values(term), do: [term]

  defp fold_row(folds, items) do
    folds
    |> Enum.flat_map(fn {kind, spec} -> fold_cols(kind, spec, items) end)
    |> Map.new()
  end

  # count counts the group's rows; its spec is the bare dest atom.
  defp fold_cols(:count, dest, items) when is_atom(dest), do: [{dest, length(items)}]

  defp fold_cols(kind, spec, items) when kind in @fold_kinds do
    spec
    |> normalize_srcs()
    |> Enum.map(fn {src, dest} -> {dest, fold_value(kind, items, src)} end)
  end

  defp normalize_srcs(src) when is_atom(src), do: [{src, src}]
  defp normalize_srcs(kw) when is_list(kw), do: kw

  defp fold_value(kind, items, src) do
    values = for item <- items, v = Map.get(item, src), not is_nil(v), do: v

    case {kind, values} do
      {_kind, []} -> nil
      {:sum, vs} -> Enum.sum(vs)
      {:avg, vs} -> Enum.sum(vs) / length(vs)
      {:min, vs} -> Enum.min(vs)
      {:max, vs} -> Enum.max(vs)
      {:first, [v | _]} -> v
    end
  end

  @doc """
  join side spec → `(item -> join_key | nil)`. An attribute projects it (nil
  value = not on this side); `[key: :acct, where: [kind: :budget]]` puts the
  item on the side iff every `where` pair matches, keyed by the `key:` attr.
  """
  def side_fn(fun) when is_function(fun, 1), do: fun
  def side_fn(attr) when is_atom(attr), do: fn item -> Map.get(item, attr) end

  def side_fn(spec) when is_list(spec) do
    key_attr = Keyword.fetch!(spec, :key)
    where = Keyword.get(spec, :where, [])

    fn item ->
      if Enum.all?(where, fn {a, v} -> Map.get(item, a) == v end),
        do: Map.get(item, key_attr),
        else: nil
    end
  end

  @doc """
  join `into:` spec → `(join_key, left, right -> row)`. The declarative form
  picks columns per side (`[left: [amount: :budget], right: [amount:
  :actual]]`; bare atom = same-named dest); an absent side yields nils, so
  left-join/outer gap semantics fall out.
  """
  def join_into_fn(fun) when is_function(fun, 3), do: fun

  def join_into_fn(picks) when is_list(picks) do
    lpicks = picks |> Keyword.get(:left, []) |> normalize_srcs()
    rpicks = picks |> Keyword.get(:right, []) |> normalize_srcs()

    fn _jk, left, right ->
      Map.new(
        Enum.map(lpicks, fn {s, d} -> {d, left && Map.get(left, s)} end) ++
          Enum.map(rpicks, fn {s, d} -> {d, right && Map.get(right, s)} end)
      )
    end
  end
end

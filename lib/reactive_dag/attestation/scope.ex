defmodule ReactiveDag.Attestation.Scope do
  @moduledoc """
  WHAT an attestation is about — one row, or the set a filter selects.

      {:key, key}          an assertion about one row of a cell (per-grain)
      {:filter, key_scope} an assertion about the SET a filter currently
                           selects (a `t:ReactiveDag.Tuple.key_scope/0`)

  The filter form is what carries set-level claims — "these are ALL of my
  machines" is about the subset an `owner == me` filter selects *and its
  boundary*, which no per-member mechanism can express (a member nobody entered
  has no row to be missing from).

  ## Canonical serialization

  A scope is stored on the record as text, and that text is load-bearing twice:
  it is the IDENTITY of what was signed (stances group by it), and for a filter
  it is re-parsed at evaluation time to re-select the current set. So the
  serialization must be canonical — same scope, same text, always — and carries
  a version segment so a format change is DETECTABLE. This build accepts only
  v1: `parse/1` raises on any other version (deliberately — a scope that cannot
  be re-selected must not silently mis-group). Cross-version *tolerance* lives
  one level down, in `ReactiveDag.Attestation.Basis`, where an unknown digest
  version degrades to re-ask rather than an error.

  Fields are joined with the unit separator (`0x1F`), which the spine's key
  grammar has no business containing — unlike `|` or `:`, which hosts routinely
  use inside keys.
  """

  alias ReactiveDag.Tuple

  @type t :: {:key, String.t()} | {:filter, Tuple.key_scope()}

  @unit "\x1F"
  @version "1"

  @doc "Serialize a scope to its canonical, versioned text form."
  @spec serialize(t()) :: String.t()
  def serialize({:key, key}) when is_binary(key), do: join(["key", key])

  def serialize({:filter, {:prefix, p}}), do: join(["filter", "prefix", p])

  def serialize({:filter, {:exact_or_prefix, k, p}}),
    do: join(["filter", "exact_or_prefix", k, p])

  def serialize({:filter, {:segment, i, sep, v}}),
    do: join(["filter", "segment", Integer.to_string(i), sep, v])

  @doc "Parse canonical text back to a scope. Raises on an unknown form."
  @spec parse(String.t()) :: t()
  def parse(@version <> @unit <> rest) do
    case String.split(rest, @unit) do
      ["key", key] -> {:key, key}
      ["filter", "prefix", p] -> {:filter, {:prefix, p}}
      ["filter", "exact_or_prefix", k, p] -> {:filter, {:exact_or_prefix, k, p}}
      ["filter", "segment", i, sep, v] -> {:filter, {:segment, String.to_integer(i), sep, v}}
      other -> raise ArgumentError, "unknown attestation scope form: #{inspect(other)}"
    end
  end

  def parse(other) do
    raise ArgumentError, "unknown attestation scope version/format: #{inspect(other)}"
  end

  @doc """
  The rows a scope selects from `rows` (spine-row maps with `:key`). This is the
  in-memory selection the evaluation uses; `select_db/2` is its SQL twin.
  """
  @spec select(t(), [map()]) :: [map()]
  def select({:key, key}, rows), do: Enum.filter(rows, &(&1.key == key))

  def select({:filter, key_scope}, rows) do
    Enum.filter(rows, &matches?(key_scope, &1.key))
  end

  @doc "Read the rows a scope selects for `cell_id` straight from the spine."
  @spec select_db(t(), String.t()) :: [map()]
  def select_db(scope, cell_id), do: select_db(scope, cell_id, nil)

  @doc """
  `select_db/2` against a KNOWN cell — so a raw cell that materialises rows is
  read from its own table rather than the coordination tuple. Pass `nil` when
  the cell is not in hand and the tuple is the only source.
  """
  @spec select_db(t(), String.t(), ReactiveDag.Cell.t() | nil) :: [map()]
  def select_db({:key, key}, cell_id, cell) do
    cell |> keys(cell_id, nil) |> Enum.filter(&(&1.key == key))
  end

  def select_db({:filter, key_scope}, cell_id, cell) do
    keys(cell, cell_id, key_scope)
  end

  defp keys(cell, cell_id, key_scope) do
    case ReactiveDag.Node.Keys.scoped(cell, cell_id, key_scope) do
      nil -> []
      keys -> Enum.map(keys, &%{key: &1})
    end
  end

  @doc """
  Does `key` fall in `key_scope`? THE key-scope predicate — `ReactiveDag.Node.Keys`
  filters key sets with it too, so there is one implementation rather than an
  in-memory one and a SQL one obliged to agree.
  """
  @spec matches?(term(), String.t()) :: boolean()
  def matches?({:prefix, pat}, key), do: like?(pat, key)
  def matches?({:exact_or_prefix, k, pat}, key), do: key == k or like?(pat, key)

  def matches?({:segment, i, sep, v}, key) do
    # split_part semantics: 1-indexed, "" when out of range.
    key |> String.split(sep) |> Enum.at(i - 1, "") == v
  end

  # a LIKE pattern with only trailing/inline % wildcards (the shapes hosts use);
  # `_` is intentionally NOT treated as a wildcard — spine keys use it literally.
  defp like?(pat, key) do
    regex =
      pat
      |> String.split("%", trim: false)
      |> Enum.map_join(".*", &Regex.escape/1)

    Regex.match?(~r/\A#{regex}\z/, key)
  end

  defp join(fields), do: Enum.join([@version | fields], @unit)
end

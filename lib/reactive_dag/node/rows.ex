defmodule ReactiveDag.Node.Rows do
  @moduledoc """
  Reads a cell's own rows, keyed the way the DAG keys them.

  A node's rows live in its resource, but the DAG addresses them by **cell key**
  — a `"|"`-joined identity for a composite-PK node, a single column's value
  otherwise. Anything that wants to ask a question *about a cell* rather than
  *about a table* (`ReactiveDag.Insights`, `ReactiveDag.Verdict`, a `union`
  reading its inputs) needs the rows under those keys, not under the resource's
  own primary key.

  This is the read side of what `ReactiveDag.Node.Payload` writes, and it
  deliberately mirrors that module's key derivation: `identity_fields` when the
  node is identity-keyed, `payload_key` (defaulting to `:key`) otherwise.

  ## Why it is not the coordination tuple

  These reads used to go to `reactive_dag_tuple`, which carried a `status` per
  `(cell_id, key)`. That made the tuple a second home for derived results — one
  with a fixed two-column schema, updated by a writer the host had to configure,
  and queryable only through this library. Reading the resource instead means a
  status is an ordinary column with ordinary Ash semantics: policies apply,
  loads work, and a host can add a second column without asking us.

  ## Missing columns are not errors

  A node need not have a `:status` column — most don't; a rollup has sums. Rows
  from such a node come back with `status: nil`, and callers that count statuses
  simply find nothing to count. A node with no attributes at all keeps its rows
  somewhere else entirely and reads as empty. Asking is always safe.
  """

  alias ReactiveDag.Cell
  alias ReactiveDag.Node.Recompute.Declarative

  @typedoc """
  Where a node's rows live and how they are keyed — a cell's `meta`, or the
  same three fields lifted out of it (which is what a `union` carries for each
  of its inputs, since it reads rows it does not own).
  """
  @type source :: %{
          optional(:resource) => module() | nil,
          optional(:payload_key) => atom() | nil,
          optional(:identity_fields) => [atom()] | nil
        }

  @typedoc "one of a cell's rows, addressed by cell key"
  @type row :: %{key: String.t(), status: String.t() | nil, record: struct()}

  @doc """
  Every row the cell currently holds, as `%{key:, status:, record:}`.

  Returns `[]` for a node that keeps no rows here — no resource at all, or a
  resource with no attributes (the shape a `compute`/custom-`upsert:` node has,
  where the real writes land somewhere this library never sees). That is
  different from "holds nothing", so a caller that must tell the two apart
  should check `meta[:resource]` itself.

  Raises whatever the underlying `Ash.read!/1` raises. Callers on a display path
  (`Insights`) already run these behind their own rescue; a caller on a compute
  path wants the failure.
  """
  @spec all(Cell.t() | source()) :: [row()]
  def all(%Cell{meta: meta}), do: all(meta)

  def all(%{} = source) do
    resource = source[:resource]

    if is_nil(resource) or Ash.Resource.Info.attributes(resource) == [] do
      []
    else
      resource |> Ash.read!() |> Enum.map(&to_row(&1, keyer(source)))
    end
  end

  @doc """
  `%{status => count}` over the cell's rows — the histogram `Insights` shows and
  `Verdict` folds into one answer.

  Rows with no status are counted under `nil`, so the counts always sum to the
  cell's key count and a node without a `:status` column reports
  `%{nil => n}` rather than lying with `%{}`.
  """
  @spec status_histogram(Cell.t() | source()) :: %{(String.t() | nil) => non_neg_integer()}
  def status_histogram(cell) do
    cell |> all() |> Enum.frequencies_by(& &1.status)
  end

  @doc """
  The keys whose status is in `statuses`, at most `limit` of them (sorted, so a
  sample is stable between calls rather than reshuffling on every render).
  """
  @spec keys_by_status(Cell.t() | source(), [String.t() | nil], keyword()) :: [String.t()]
  def keys_by_status(cell, statuses, opts \\ []) do
    want = MapSet.new(statuses)

    cell
    |> all()
    |> Enum.filter(&MapSet.member?(want, &1.status))
    |> Enum.map(& &1.key)
    |> Enum.sort()
    |> take(opts[:limit])
  end

  defp take(keys, nil), do: keys
  defp take(keys, limit), do: Enum.take(keys, limit)

  defp to_row(record, keyer) do
    %{key: keyer.(record), status: Map.get(record, :status), record: record}
  end

  # the same derivation `Payload` writes under: a composite-PK node serializes
  # its identity fields, everything else reads one column.
  defp keyer(source) do
    case source[:identity_fields] do
      fields when is_list(fields) -> Declarative.identity_key_fn(fields, nil)
      _ -> &(&1 |> Map.fetch!(source[:payload_key] || :key) |> to_string())
    end
  end
end

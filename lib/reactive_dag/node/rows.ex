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

  @doc """
  Reconcile a leaf's rows against the key set a scan found — the algorithm every
  leaf driver otherwise hand-rolls.

      current  = the cell's current keys (read from its resource)
      want     = `want_keys`, what the scan found
      upsert   each want key   → the host writes the row, returns true iff CHANGED
      vanished = current − want → retired
      ⇒ changed_upserts ++ vanished    (the keys to propagate)

  `:upsert` is called once per want-key, in either of two forms:

    * `(key -> row | nil)` — the common case. Return the row you observed and
      the library writes it, deciding `changed?` against the node's declared
      `fingerprint` (or every attribute, if it declares none). Return `nil` for
      a key you could not observe: nothing is written and the key is not
      reported, which is how a partial outage stays honest.
    * `(key -> boolean)` — full control. Write the row yourself and say whether
      it moved. Use this when the write is not an upsert into the node's own
      resource.
    * `:retire` — how vanished keys leave. Defaults to destroying the row via the
      node's `payload_destroy` action, or to MARKING it when the node declares
      `retain_if_vanished`. A host with a policy neither covers passes a
      `(keys -> any)` fun.
    * `:current` — the baseline `vanished` is computed against. Defaults to the
      cell's current keys, or to its LIVE keys when the node declares
      `retain_if_vanished` — so an already-retired key is neither re-retired nor
      re-reported. Pass it explicitly only for a baseline neither covers.

  Vanished keys always propagate: something disappearing is a change.

  A leaf declaring `fingerprint` needs no `:upsert` at all for the row-returning
  form to be worth using — that is the point: `poll/1` becomes fetch, build
  rows, call reconcile.

  ## Retain-if-vanish

  A node declaring `retain_if_vanished` gets all three parts consistently, from
  one fact rather than from options a host must keep in step: the baseline is
  live rows, retirement marks instead of destroying, and **a revived row counts
  as changed**. That last one is the reason it is a declaration: a row that comes
  back carries its old fingerprint — the bytes did not move, its liveness did —
  so comparing fingerprints alone reports "unchanged" when the honest answer is
  "it came back". The prior row is already in hand for the fingerprint
  comparison, so noticing costs nothing.

  > #### The honest gap {: .warning}
  >
  > Call this only with a want-set you actually observed. An upstream you could
  > not reach must write NOTHING — handing an empty `want_keys` to a scan that
  > failed retires every key the cell has, and a downstream rollup over an empty
  > set typically reads as vacuously fine. A scan that couldn't look must never
  > render as a scan that found nothing.
  """
  @spec reconcile(Cell.t(), [String.t()] | MapSet.t(), keyword()) :: {:ok, [String.t()]}
  def reconcile(%Cell{meta: meta} = cell, want_keys, opts) do
    upsert = Keyword.fetch!(opts, :upsert)
    want_set = MapSet.new(want_keys)

    changed_up =
      want_set
      |> MapSet.to_list()
      |> Enum.sort()
      |> Enum.filter(&(observe(&1, upsert, meta) == true))

    current = Keyword.get_lazy(opts, :current, fn -> baseline(cell, meta) end)
    vanished = Enum.reject(current, &MapSet.member?(want_set, &1))

    retire(vanished, Keyword.get(opts, :retire), meta)

    {:ok, changed_up ++ vanished}
  end

  # the host either wrote the row itself and told us whether it moved, or handed
  # us what it observed and let the library decide.
  defp observe(key, upsert, meta) do
    case upsert.(key) do
      changed? when is_boolean(changed?) -> changed?
      nil -> false
      row when is_map(row) -> write(key, row, meta) == :changed
    end
  end

  defp write(key, row, meta) do
    opts = [
      fingerprint: meta[:fingerprint],
      fingerprint_attribute: meta[:fingerprint_attribute],
      retain_if_vanished: meta[:retain_if_vanished]
    ]

    case meta[:identity_fields] do
      fields when is_list(fields) ->
        ReactiveDag.Node.Payload.upsert_identity(
          meta[:resource],
          fields,
          row,
          meta[:payload_action] || :upsert,
          opts
        )

      _ ->
        ReactiveDag.Node.Payload.upsert(
          meta[:resource],
          meta[:payload_key] || :key,
          key,
          row,
          meta[:payload_action] || :upsert,
          opts
        )
    end
  end

  # what `vanished` is subtracted from. A retain-if-vanish node counts only LIVE
  # rows, so a key retired on an earlier poll is not retired (or reported) again.
  defp baseline(cell, meta) do
    case meta[:retain_if_vanished] do
      nil ->
        Enum.map(all(cell), & &1.key)

      %{status: attr, live: live} ->
        cell
        |> all()
        |> Enum.filter(&(Map.get(&1.record, attr) == live))
        |> Enum.map(& &1.key)
    end
  end

  defp retire([], _how, _meta), do: :ok
  defp retire(keys, fun, _meta) when is_function(fun, 1), do: fun.(keys)

  # MARK, don't destroy: the host keeps the artifact and records that the
  # upstream withdrew it.
  defp retire(keys, nil, %{retain_if_vanished: %{} = policy} = meta) do
    ReactiveDag.Node.Payload.mark_retired(
      meta[:resource],
      meta[:payload_key] || :key,
      meta[:identity_fields],
      keys,
      policy,
      meta[:payload_action] || :upsert
    )
  end

  defp retire(keys, _nil, meta) do
    ReactiveDag.Node.Payload.retire(
      meta[:resource],
      meta[:payload_key] || :key,
      meta[:identity_fields],
      keys,
      meta[:payload_destroy] || :destroy
    )
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

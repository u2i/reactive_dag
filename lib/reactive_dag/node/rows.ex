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

  require Logger

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
    * `:retire` — how vanished keys leave. See the table below.
    * `:current` — the baseline `vanished` is computed against. Defaults to the
      cell's current keys. Pass it when your live set is narrower than all your
      rows, or when the scan asked a NARROWER QUESTION than "everything" — a
      date-scoped scan with a whole-table baseline retires everything outside
      its window.

  ## When a key stops being returned

  One decision, three answers:

  | you want | you write | the row | propagates? |
  |---|---|---|---|
  | **destroy** it | nothing — the default | destroyed | yes |
  | **keep** it | `retain_if_vanished true` on the node | untouched | no |
  | **mark** it | `retain_if_vanished mark: &tombstone/1` | yours to write | yes |

  Keep and mark are the same operation with one question between them — *do we
  write something to say it is gone?* — and propagation follows from the answer
  rather than being a separate switch:

    * **destroying** removes a unit downstream was counting, so it is a change;
    * **keeping** changes nothing — the row is still there, unmodified — so
      reporting it would be a lie, and would report it again on every poll
      forever, since nothing marks it as handled;
    * **marking** is a change you made, so downstream hears about it. It also
      means you own `:current`, and inherit one case a fingerprint cannot see:
      a marked-retired row returning with unmoved bytes. `changed?` compares
      fingerprints, so the revival is invisible — the library warns when it
      sees that shape but cannot fix it (u2i/reactive_dag#82). Report it
      yourself with the boolean `:upsert` form.

  A leaf declaring `fingerprint` needs no `:upsert` at all for the row-returning
  form to be worth using — that is the point: `poll/1` becomes fetch, build
  rows, call reconcile.

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

    want = want_set |> MapSet.to_list() |> Enum.sort()

    # `{key, changed?, library_decided?}` — the third is what tells the warning
    # below whether `changed?` came from a fingerprint comparison (which cannot
    # see a revival) or from the host (which can).
    observed = Enum.map(want, fn key -> {key, observe(key, upsert, meta)} end)
    changed_up = for {key, {true, _}} <- observed, do: key

    current = Keyword.get_lazy(opts, :current, fn -> Enum.map(all(cell), & &1.key) end)
    vanished = Enum.reject(current, &MapSet.member?(want_set, &1))

    warn_silent_revivals(observed, current, meta, opts)
    retire(vanished, Keyword.get(opts, :retire), meta)

    {:ok, changed_up ++ propagated(vanished, meta, opts)}
  end

  # A key the scan returned, that the host's OWN baseline excluded, whose
  # fingerprint has not moved.
  #
  # For a host retiring by MARKING (a custom `:retire` that tombstones rather
  # than destroys), that combination means a retired row came back carrying the
  # bytes it left with. Its liveness changed; its content did not. `changed?` is
  # fingerprint comparison, so the library reports nothing and the revival never
  # propagates — silently, with no dirty key and no drain step to notice.
  #
  # The library cannot fix this: it does not know what the host's retirement
  # marks, and `changed?` is deliberately the fingerprint's business. But it can
  # see the shape, so it says so rather than leaving a correctness gap that only
  # surfaces as stale downstream rows much later. (See u2i/reactive_dag#82.)
  #
  # Only fires with a host-supplied `:current` AND a custom `:retire` — the two
  # together are what identify a marking policy. A `retain_if_vanished` node is
  # not affected: its retained keys stay in the baseline, so they never look like
  # a revival.
  defp warn_silent_revivals(observed, current, meta, opts) do
    if is_nil(opts[:current]) or not marking?(meta, opts) do
      :ok
    else
      live = MapSet.new(current)

      # the row form only: a boolean-form host already decides `changed?` itself,
      # which is precisely how this is worked around today
      suspects =
        for {key, {false, :library}} <- observed,
            not MapSet.member?(live, key),
            do: key

      case suspects do
        [] ->
          :ok

        keys ->
          Logger.warning(
            "reactive_dag: #{length(keys)} key(s) returned by a scan were absent from the " <>
              "supplied `:current` baseline but report UNCHANGED, so they will not " <>
              "propagate: #{inspect(Enum.take(keys, 5))}. Your retirement MARKS rows " <>
              "rather than destroying them, so this is a revival the fingerprint cannot " <>
              "see — coming back is a change even when the bytes did not move. Report it " <>
              "yourself with the boolean `:upsert` form (see u2i/reactive_dag#82)."
          )
      end
    end
  end

  # Does retirement WRITE something? Only a marking policy can produce a silent
  # revival: a destroying one leaves no row to come back, and `:keep` leaves the
  # key in the baseline so it never looks absent. Declared on the node, or passed
  # per-call — both spellings, or the warning misses the case it exists for.
  defp marking?(meta, opts) do
    match?({:mark, _}, meta[:retain_if_vanished]) or is_function(opts[:retire], 1)
  end

  # the host either wrote the row itself and told us whether it moved, or handed
  # us what it observed and let the library decide.
  defp observe(key, upsert, meta) do
    case upsert.(key) do
      changed? when is_boolean(changed?) -> {changed?, :host}
      nil -> {false, :host}
      row when is_map(row) -> {write(key, row, meta) == :changed, :library}
    end
  end

  defp write(key, row, meta) do
    opts = [
      fingerprint: meta[:fingerprint],
      fingerprint_attribute: meta[:fingerprint_attribute]
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

  # Propagation FOLLOWS from whether anything was written. Nothing written means
  # nothing changed — the row is still there, unmodified — so reporting it would
  # be a lie, and would report it again on every poll forever since nothing marks
  # it handled. Anything written is a change the host made, so downstream hears.
  defp propagated(vanished, meta, opts) do
    if meta[:retain_if_vanished] == :keep and is_nil(opts[:retire]) do
      []
    else
      vanished
    end
  end

  defp retire([], _how, _meta), do: :ok

  # a per-call `:retire` still wins: the node declares the policy, a caller may
  # override it for one poll.
  defp retire(keys, fun, _meta) when is_function(fun, 1), do: fun.(keys)

  # KEEP — the row stands as it is. Nothing to write, nothing to report.
  defp retire(_keys, nil, %{retain_if_vanished: :keep}), do: :ok

  # MARK — keep the row, and record that the upstream dropped it.
  defp retire(keys, nil, %{retain_if_vanished: {:mark, fun}}), do: fun.(keys)

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

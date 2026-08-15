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
    * `:observed` — was this scan COMPLETE? `:all` (the default) means absence
      from `want_keys` is evidence a key is gone, so the vanish diff runs.
      `:partial` means the scan looked at only part of the upstream — a scoped
      `only:` poll, a windowed `recent:` one, a crawl whose index page failed —
      so absence means "not looked at" and **nothing can vanish**. See below.
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

  ## Partial observations

  Retiring a key is an inference: *the upstream no longer lists it, so it is
  gone*. That inference is only valid from a **complete** observation. A scoped
  poll, a windowed one, or a crawl whose index page failed all produce a want-set
  that is real but incomplete, where absence means "not looked at".

      Rows.reconcile(cell, observed_keys, observed: :partial, upsert: &fetch/1)

  Nothing vanishes, nothing is retired, and the keys you did see are written and
  reported exactly as usual.

  This was previously spelled `current: []` — "measure vanishing against
  nothing" — which works, and says nothing about why. The distinction is worth a
  name because the failure is silent and severe in one direction only: getting
  `:partial` wrong under-retires, leaving rows that should have gone. Getting
  `:all` wrong tombstones everything the scan did not happen to look at, which
  for an archival consumer is a mass-deletion wave from one upstream 500.

  > #### The honest gap {: .warning}
  >
  > Call this only with a want-set you actually observed. An upstream you could
  > not reach must write NOTHING — handing an empty `want_keys` to a scan that
  > failed retires every key the cell has, and a downstream rollup over an empty
  > set typically reads as vacuously fine. A scan that couldn't look must never
  > render as a scan that found nothing.
  >
  > A total outage and a partial one differ: an outage writes nothing at all, so
  > it marks nothing dirty and the drain correctly does no downstream work. A
  > partial observation writes what it saw — it just must not conclude anything
  > from what it did not.
  """
  @spec reconcile(Cell.t(), [String.t()] | MapSet.t(), keyword()) :: {:ok, [String.t()]}
  def reconcile(%Cell{meta: meta} = cell, want_keys, opts) do
    upsert = Keyword.fetch!(opts, :upsert)
    want_set = MapSet.new(want_keys)

    want = want_set |> MapSet.to_list() |> Enum.sort()

    # `{key, {changed?, who_decided}}` — the second element matters for revival:
    # only a LIBRARY verdict comes from a fingerprint comparison, which is the
    # one that cannot see a row coming back.
    observed = Enum.map(want, fn key -> {key, observe(key, upsert, meta)} end)
    changed_up = for {key, {true, _}} <- observed, do: key

    case Keyword.get(opts, :observed, :all) do
      :partial ->
        # A PARTIAL observation cannot say anything vanished — absence from
        # `want` means "not looked at", not "gone" — so the whole vanish
        # computation is skipped rather than fed an empty baseline. Revival is
        # skipped for the same reason: it is derived from absence from the
        # baseline, and here absence carries no information.
        {:ok, changed_up}

      :all ->
        current = Keyword.get_lazy(opts, :current, fn -> Enum.map(all(cell), & &1.key) end)
        vanished = Enum.reject(current, &MapSet.member?(want_set, &1))

        retire(vanished, Keyword.get(opts, :retire), meta)

        {:ok,
         changed_up ++ revived(observed, current, meta, opts) ++ propagated(vanished, meta, opts)}

      other ->
        raise ArgumentError,
              "reactive_dag: `observed:` is `:all` (the default) or `:partial`, got " <>
                "#{inspect(other)}. `:partial` says this scan looked at only part of the " <>
                "upstream, so nothing can be inferred to have vanished."
    end
  end

  # COMING BACK is a change, even when the bytes did not move.
  #
  # Under a marking policy the row survives retirement, so a key that returns
  # carries the fingerprint it left with — its content did not move, its liveness
  # did. A fingerprint comparison therefore reports "unchanged", and without this
  # the revival would never propagate: no dirty key, no drain step, nothing in
  # the trace. Silent staleness.
  #
  # Everything needed is already in hand — the baseline was computed above, and
  # `observed` records which verdicts came from the library rather than the host
  # — so this is a filter, not a query.
  #
  # Only a MARKING policy can produce it. A destroying one leaves no row to come
  # back; `retain_if_vanished true` leaves the key in its own baseline, so a
  # returning key never looks absent. And a host that decided `changed?` itself
  # (the boolean form) has already said what it thinks.
  defp revived(observed, current, meta, opts) do
    if marking?(meta, opts) do
      live = MapSet.new(current)

      for {key, {false, :library}} <- observed, not MapSet.member?(live, key), do: key
    else
      []
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

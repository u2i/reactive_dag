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

  require Ash.Query

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
  def all(source, opts \\ [])
  def all(%Cell{meta: meta}, opts), do: all(meta, opts)

  def all(%{} = source, opts) do
    resource = source[:resource]

    if is_nil(resource) or Ash.Resource.Info.attributes(resource) == [] do
      []
    else
      resource
      |> tenant_scoped(opts)
      |> Ash.read!()
      |> Enum.map(&to_row(&1, keyer(source)))
    end
  end

  # A read scoped to the plan's tenant, when there is one. Ash applies the filter
  # itself from the resource's own `multitenancy` block, so this never names the
  # column — a resource declaring no tenancy ignores it.
  defp tenant_scoped(queryable, opts) do
    case Keyword.get(opts, :tenant) do
      nil -> queryable
      tenant -> Ash.Query.set_tenant(queryable, tenant)
    end
  end

  @doc """
  How many units the cell holds, counted by the datastore.

  `Ash.count!` rather than reading the rows: an overview wants the number, and
  loading a table to reduce it to one integer decodes every payload column on the
  way — which for a node whose rows carry a JSON blob is the whole cost of the
  read for none of the value.

  Returns 0 for a node that keeps no rows here.
  """
  @spec key_count(Cell.t() | source()) :: non_neg_integer()
  def key_count(source, opts \\ [])
  def key_count(%Cell{meta: meta}, opts), do: key_count(meta, opts)

  def key_count(%{} = source, opts) do
    case queryable(source) do
      nil -> 0
      resource -> resource |> tenant_scoped(opts) |> Ash.count!()
    end
  end

  @doc """
  `%{status => count}` over the cell's rows — the histogram `Insights` shows and
  `Verdict` folds into one answer.

  Rows with no status are counted under `nil`, so the counts always sum to the
  cell's key count and a node without a `:status` column reports
  `%{nil => n}` rather than lying with `%{}`.

  ## `tenant:`

  A TENANTED resource refuses to be counted without one — Ash raises
  `"Queries against … require a tenant to be specified"`. Pass the plan's
  tenant (`opts[:tenant]`), and omit it for a host whose resources are not
  multitenant; `nil` is the untenanted read, not a wildcard.

  This is not optional polish. Every count here went through an unscoped
  `Ash.count!/1`, so once a host made its resources tenanted EVERY cell raised,
  `Insights` caught it into `rows: :unreadable`, and the dashboard rendered a
  benign-looking `?` in place of all 33 counts.
  """
  @spec status_histogram(Cell.t() | source(), keyword()) :: %{
          (String.t() | nil) => non_neg_integer()
        }
  def status_histogram(cell_or_source, opts \\ [])

  def status_histogram(%Cell{meta: meta}, opts), do: status_histogram(meta, opts)

  def status_histogram(%{} = source, opts) do
    case queryable(source) do
      nil ->
        %{}

      resource ->
        if Ash.Resource.Info.attribute(resource, :status) do
          # One narrow query for the vocabulary, then one COUNT per value. More
          # round trips than a single read, and none of them decode a row —
          # which is where the cost is for a node whose payload is a blob.
          #
          # Ash has no arbitrary `GROUP BY … -> rows` (it is why `reduce` folds
          # in the BEAM at all), so this is the closest pushdown available. The
          # vocabulary is small — a handful of statuses — so N stays tiny.
          resource
          |> distinct_statuses(opts)
          |> Map.new(&{&1, count_with_status(resource, &1, opts)})
        else
          # no status column: every row counts under `nil`, and the count is one
          # query rather than a table read
          case Ash.count!(resource, query_opts(opts)) do
            0 -> %{}
            n -> %{nil => n}
          end
        end
    end
  end

  defp distinct_statuses(resource, opts) do
    resource
    |> Ash.Query.select([:status])
    |> Ash.Query.distinct([:status])
    |> Ash.read!(query_opts(opts))
    |> Enum.map(& &1.status)
    |> Enum.uniq()
  end

  defp count_with_status(resource, nil, opts),
    do: resource |> Ash.Query.filter(is_nil(status)) |> Ash.count!(query_opts(opts))

  defp count_with_status(resource, status, opts),
    do: resource |> Ash.Query.filter(status == ^status) |> Ash.count!(query_opts(opts))

  # Only the keys Ash accepts on a read/count, and only when set: passing
  # `tenant: nil` to an untenanted resource is fine, but passing unknown keys
  # is not, and the caller's opts also carry `:limit` and friends.
  defp query_opts(opts) do
    case Keyword.get(opts, :tenant) do
      nil -> []
      tenant -> [tenant: tenant]
    end
  end

  @doc """
  The keys whose status is in `statuses`, at most `limit` of them (sorted, so a
  sample is stable between calls rather than reshuffling on every render).
  """
  @spec keys_by_status(Cell.t() | source(), [String.t() | nil], keyword()) :: [String.t()]
  def keys_by_status(cell_or_source, statuses, opts \\ [])

  def keys_by_status(%Cell{meta: meta}, statuses, opts), do: keys_by_status(meta, statuses, opts)

  def keys_by_status(%{} = source, statuses, opts) do
    case queryable(source) do
      nil ->
        []

      resource ->
        # The filter goes to the datastore; the KEY is still built here, because
        # an identity-keyed node's key is a "|"-join of its identity fields and
        # no datastore knows that. So a sample still decodes rows — but only the
        # ones that matched, and only up to the limit, rather than the table.
        resource
        |> filter_statuses(statuses)
        |> Ash.read!(query_opts(opts))
        |> Enum.map(keyer(source))
        |> Enum.sort()
        |> take(opts[:limit])
    end
  end

  # `nil` cannot go through `in`, so it is a separate predicate.
  defp filter_statuses(resource, statuses) do
    {nils, values} = Enum.split_with(statuses, &is_nil/1)

    case {nils, values} do
      {[], []} -> Ash.Query.filter(resource, false)
      {[], values} -> Ash.Query.filter(resource, status in ^values)
      {_, []} -> Ash.Query.filter(resource, is_nil(status))
      {_, values} -> Ash.Query.filter(resource, is_nil(status) or status in ^values)
    end
  end

  @doc """
  The keys whose rows match `filter` — the selection behind "reprocess just this
  year".

      Rows.keys_where(cell, fiscal_year: "FY25")
      #=> ["FY25|gf", "FY25|water"]

  The filter goes to the datastore; keys are built here, because an
  identity-keyed node's key is a `"|"`-join no datastore knows. So this decodes
  what matched rather than the table.

  A node declares which columns are meant for this with `slice`, and
  `slices/1` reports them — but nothing stops a caller filtering on any column
  it knows about. The declaration is what makes a UI possible, not what makes
  the filter legal.

  `opts` takes `:tenant`, like `all/2` and `key_count/2` — pass
  `Plan.frontier_opts/1` and the count is that tenant's. This was the one
  row-reading helper that could not be scoped, so a host asking "how many keys
  would this reprocess claim?" against a tenanted resource got a raise, or a zero
  if it guarded — a button offering to reprocess nothing.
  """
  @spec keys_where(Cell.t() | source(), keyword(), keyword()) :: [String.t()]
  def keys_where(source, filter, opts \\ [])
  def keys_where(%Cell{meta: meta}, filter, opts), do: keys_where(meta, filter, opts)

  def keys_where(%{} = source, filter, opts) do
    case queryable(source) do
      nil ->
        []

      resource ->
        resource
        |> Ash.Query.do_filter(filter)
        |> tenant_scoped(opts)
        |> Ash.read!()
        |> Enum.map(keyer(source))
        |> Enum.sort()
    end
  end

  @doc """
  The dimensions this node declared a human may select it by, with their options
  resolved.

      Rows.slices(cell)
      #=> [%{column: :fiscal_year, label: "fiscal_year",
      #      values: ["FY24", "FY25"], poll_as: :fiscal}]

  `values` is `nil` when the node named no options — a UI then takes free text,
  or offers nothing.

  `poll_as` is what to call this dimension when asking the SCANNER for it, which
  defaults to the column. See `poll_opts/2`.
  """
  @spec slices(Cell.t() | source()) :: [map()]
  def slices(%Cell{meta: meta}), do: slices(meta)

  def slices(%{} = source) do
    for slice <- source[:slices] || [] do
      %{slice | values: resolve_values(slice.values)}
    end
  end

  defp resolve_values({m, f, a}), do: apply(m, f, a)
  defp resolve_values(values), do: values

  @doc """
  Turn a selected slice into the options a POLL is asked with.

      Rows.poll_opts(cell, %{"fiscal_year" => "FY25/26"})
      #=> [fiscal: "FY25/26"]

  A slice narrows two different things. `keys_where/2` filters rows this node
  already HOLDS — "re-derive FY25 from documents I have". This narrows the
  FETCH: a source whose upstream is addressable by the same dimension can be
  asked for just that part, and a crawler that takes `fiscal:` walks twelve
  months instead of the whole corpus.

  The selection is keyed by COLUMN, because that is what a UI has — it rendered
  a button per value under the slice's own name. The result is keyed by
  `poll_as`, because that is the scanner's vocabulary. Translating here is the
  point: neither side has to learn the other's spelling.

  Accepts string or atom column names, since a selection usually arrives from a
  form or a job argument. A column this node never declared as a slice is
  IGNORED rather than passed through — an unrecognised option would otherwise
  reach `poll/1` as if the node had offered it, and a scanner that pattern
  matches its arguments would crash on a typo the DSL could not vouch for.
  """
  @spec poll_opts(Cell.t() | source(), map() | keyword()) :: keyword()
  def poll_opts(cell_or_source, selection) do
    declared = slices(cell_or_source)

    for {key, value} <- selection,
        slice = find_slice(declared, key),
        not is_nil(slice),
        do: {slice.poll_as || slice.column, value}
  end

  defp find_slice(declared, key) when is_atom(key),
    do: Enum.find(declared, &(&1.column == key))

  defp find_slice(declared, key) when is_binary(key),
    do: Enum.find(declared, &(to_string(&1.column) == key))

  defp find_slice(_declared, _key), do: nil

  @doc """
  Clear the stored fingerprint on `keys`, so the next recompute treats those rows
  as needing work.

  A fingerprint answers *"did the input move?"*. After a prompt change or a fixed
  fold the input has NOT moved and the answer is still valid — it is simply the
  wrong question, because what changed was the function. A `per_key` node would
  therefore skip exactly the rows you asked it to redo.

  Clearing the stored value makes the comparison fail honestly rather than
  bypassing it: a null fingerprint means *"no valid prior result"*, which is
  precisely true once the code that produced it has changed. Nothing needs a
  force flag threaded through the recompute, and the next run stores a fresh
  fingerprint as it always would.

  A node with no fingerprint column has nothing to clear and recomputes
  regardless, so this is a no-op there. Returns the keys it actually cleared.
  """
  @spec invalidate(Cell.t() | source(), [String.t()] | :all) :: [String.t()]
  def invalidate(source, keys, opts \\ [])
  def invalidate(%Cell{meta: meta}, keys, opts), do: invalidate(meta, keys, opts)

  def invalidate(%{} = source, keys, opts) do
    attr = source[:fingerprint_attribute] || ReactiveDag.Node.Fingerprint.default_attribute()

    with resource when not is_nil(resource) <- queryable(source),
         attribute when not is_nil(attribute) <- Ash.Resource.Info.attribute(resource, attr) do
      source
      |> rows_to_clear(keys, opts)
      |> Enum.map(fn row ->
        row.record
        |> Ash.Changeset.for_update(update_action(resource), %{attr => nil})
        |> Ash.update!()

        row.key
      end)
    else
      _ -> []
    end
  end

  defp rows_to_clear(source, :all, opts), do: all(source, opts)

  defp rows_to_clear(source, keys, opts) do
    want = MapSet.new(keys)
    source |> all(opts) |> Enum.filter(&MapSet.member?(want, &1.key))
  end

  # a node's `:upsert` is a create action, so clearing goes through an UPDATE
  defp update_action(resource) do
    case Ash.Resource.Info.primary_action(resource, :update) do
      %{name: name} -> name
      _ -> :update
    end
  end

  @doc """
  Reconcile a leaf's rows against the key set a scan found — the algorithm every
  leaf driver otherwise hand-rolls.

  Returns `{:ok, changed, detail}` — `changed` is the flat list that propagates,
  and `detail` is `%{created:, updated:, revived:, retired:}` saying WHY each key
  is in it.

  The detail is free: `reconcile/3` computes those four sets to build `changed`
  and used to flatten them away, leaving a host to rebuild the classification a
  scan report shows. It cannot be reconstructed afterwards — by the time you
  look, the rows are already written.

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
    * `(key -> boolean | :created | :changed | :unchanged)` — full control.
      Write the row yourself and say what happened. Use this when the write is
      not an upsert into the node's own resource.

      `true`/`false` mean `:changed`/`:unchanged`. Prefer the atoms when you
      know which: **only you can tell an insert from an update**, since the
      library never saw the row, and `true` reports a brand-new key as
      `updated` — which it cannot be, having had no prior row.
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
  @spec reconcile(Cell.t(), [String.t()] | MapSet.t(), keyword()) ::
          {:ok, [String.t()],
           %{
             created: [String.t()],
             updated: [String.t()],
             revived: [String.t()],
             retired: [String.t()]
           }}
  def reconcile(%Cell{meta: meta} = cell, want_keys, opts) do
    upsert = Keyword.fetch!(opts, :upsert)
    want_set = MapSet.new(want_keys)

    want = want_set |> MapSet.to_list() |> Enum.sort()

    # `{key, {changed?, who_decided}}` — the second element matters for revival:
    # only a LIBRARY verdict comes from a fingerprint comparison, which is the
    # one that cannot see a row coming back.
    observed = Enum.map(want, fn key -> {key, observe(key, upsert, meta)} end)

    case Keyword.get(opts, :observed, :all) do
      :partial ->
        # A PARTIAL observation cannot say anything vanished — absence from
        # `want` means "not looked at", not "gone" — so the whole vanish
        # computation is skipped rather than fed an empty baseline. Revival is
        # skipped for the same reason: it is derived from absence from the
        # baseline, and here absence carries no information.
        {changed, detail} = classify(observed, [], [])
        {:ok, changed, detail}

      :all ->
        current = Keyword.get_lazy(opts, :current, fn -> Enum.map(all(cell), & &1.key) end)
        vanished = Enum.reject(current, &MapSet.member?(want_set, &1))

        retire(vanished, Keyword.get(opts, :retire), meta)

        {changed, detail} =
          classify(
            observed,
            revived(observed, current, meta, opts),
            propagated(vanished, meta, opts)
          )

        {:ok, changed, detail}

      other ->
        raise ArgumentError,
              "reactive_dag: `observed:` is `:all` (the default) or `:partial`, got " <>
                "#{inspect(other)}. `:partial` says this scan looked at only part of the " <>
                "upstream, so nothing can be inferred to have vanished."
    end
  end

  # The four sets `reconcile/3` already computes, kept apart instead of flattened.
  #
  # It knew all of this: which keys were written for the first time, which moved,
  # which came back, which went away. Returning one list threw the distinction
  # away and left a host to rebuild it — which is what a "new / superseded /
  # vanished" report is, and it cannot be reconstructed afterwards because the
  # rows have already been written.
  #
  # `changed` is every key that propagates, and stays the flat list a caller
  # already destructures. The rest is why.
  defp classify(observed, revived, retired) do
    created = for {key, {:created, _}} <- observed, do: key
    updated = for {key, {:changed, _}} <- observed, do: key

    {created ++ updated ++ revived ++ retired, %{created: created, updated: updated, revived: revived, retired: retired}}
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

      for {key, {:unchanged, :library}} <- observed, not MapSet.member?(live, key), do: key
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
      changed? when is_boolean(changed?) ->
        {if(changed?, do: :changed, else: :unchanged), :host}

      # A host that wrote the row itself is the only thing that knows whether it
      # INSERTED. `true` cannot say so, and every new key was landing in
      # `updated` — a key with no prior row cannot have been updated
      # (u2i/reactive_dag#107).
      verdict when verdict in [:created, :changed, :unchanged] ->
        {verdict, :host}

      nil ->
        {:unchanged, :host}

      row when is_map(row) ->
        {write(key, row, meta), :library}

      other ->
        raise ArgumentError, """
        reactive_dag: the `:upsert` function returned #{inspect(other)} for key \
        #{inspect(key)}.

        Return one of:

            row (a map)   the library writes it and decides `changed?` for you
            nil           you could not observe this key; nothing is written
            true | false  you wrote it yourself; did it move?
            :created | :changed | :unchanged
                          you wrote it yourself, and know which — `:created`
                          is the one `true` cannot express
        """
    end
  end

  defp write(key, row, meta) do
    opts = [
      fingerprint: meta[:fingerprint],
      fingerprint_attribute: meta[:fingerprint_attribute],
      lapse: meta[:lapse],
      compare: meta[:compare]
    ]

    case ReactiveDag.Node.Payload.lookup(meta) do
      {:identity, fields} ->
        ReactiveDag.Node.Payload.upsert_identity(
          meta[:resource],
          fields,
          row,
          meta[:payload_action] || :upsert,
          opts
        )

      {:key, key_attr} ->
        ReactiveDag.Node.Payload.upsert(
          meta[:resource],
          key_attr,
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
    {key_attr, fields} =
      case ReactiveDag.Node.Payload.lookup(meta) do
        {:identity, fields} -> {meta[:payload_key] || :key, fields}
        {:key, attr} -> {attr, meta[:identity_fields]}
      end

    ReactiveDag.Node.Payload.retire(
      meta[:resource],
      key_attr,
      meta[:identity_fields],
      keys,
      meta[:payload_destroy] || :destroy,
      # The columns to find the row by, for a node with no key column. Without
      # this the lookup fell through to filtering the whole composite key against
      # the surrogate primary key — `id == "|FY25/26||"`.
      row_key_fields: fields && Enum.reject(fields, &(&1 == tenant_attribute(meta)))
    )
  end

  # A node with no resource, or one with no attributes, keeps its rows somewhere
  # this library never sees — the same guard `all/1` applies, factored out so
  # every read path agrees on what "has no rows here" means.
  defp queryable(source) do
    resource = source[:resource]

    if is_nil(resource) or Ash.Resource.Info.attributes(resource) == [],
      do: nil,
      else: resource
  end

  defp take(keys, nil), do: keys
  defp take(keys, limit), do: Enum.take(keys, limit)

  defp to_row(record, keyer) do
    %{key: keyer.(record), status: Map.get(record, :status), record: record}
  end

  # the same derivation `Payload` writes under: a composite-PK node serializes
  # its identity fields, everything else reads one column.
  # How a stored row reports its CELL KEY — the inverse of the write's `row_key`,
  # and it has to agree with it or a read-back names keys the frontier never saw.
  #
  # `row_key` first, because `payload_key` derives from the primary key and a UUID
  # primary key derives to `:id` — so a node whose identity moved off its primary
  # key would report UUIDs where cell keys belong. `Nodes.live_keys/1` in a host
  # returned exactly that.
  # The resource's own multitenancy attribute, or nil. Read from Ash rather than
  # declared again here — the resource says it once.
  defp tenant_attribute(source) do
    case source[:resource] do
      nil ->
        nil

      resource ->
        if Ash.Resource.Info.multitenancy_strategy(resource) == :attribute,
          do: Ash.Resource.Info.multitenancy_attribute(resource)
    end
  end

  defp keyer(source) do
    case source[:row_key] do
      :uuid ->
        &(&1 |> Map.fetch!(source[:payload_key] || :id) |> to_string())

      fields when is_list(fields) and fields != [] ->
        # WITHOUT the tenant. `row_key` names the fields that identify the ROW,
        # and for a tenanted resource that includes the multitenancy attribute —
        # but the tenant is not part of the cell key. It is a COLUMN, twice: on
        # the row, and on the frontier. The plan already carries it.
        #
        # Joining it in invents a key form that exists nowhere: the op writes
        # `"osc:FY24"` and a read-back saying `"org_a|osc:FY24"` names a key the
        # frontier never saw. Two tenants share a cell key precisely because it is
        # the same unit of work — the frontier's tenant column tells them apart.
        fields
        |> Enum.reject(&(&1 == tenant_attribute(source)))
        |> Declarative.identity_key_fn(nil)

      fun when is_function(fun) ->
        # A resolver decides which ROW a key writes to; it cannot be run backwards
        # to recover the key. A node declaring one must keep the key in a column
        # if it wants its keys enumerable — so fall through to `payload_key` and
        # let the resource say where.
        &(&1 |> Map.fetch!(source[:payload_key] || :key) |> to_string())

      _ ->
        case source[:identity_fields] do
          fields when is_list(fields) -> Declarative.identity_key_fn(fields, nil)
          _ -> &(&1 |> Map.fetch!(source[:payload_key] || :key) |> to_string())
        end
    end
  end
end

defmodule ReactiveDag.Node.Recompute.PerKey do
  @moduledoc """
  Runs a `per_key` node: for each claimed input row, call a generic action with
  that row and write its structured output into this node's attributes.

  The library drives the loop — scope, read, call, write — which is what makes
  **fingerprinting** possible. A `run` action is opaque by design (the library
  passes keys and gets keys back), so nothing outside it can know whether the
  work is worth doing. Here the library sees the input rows, so it can hash the
  fields the result depends on and skip the call when they are unchanged.

  For an expensive or non-deterministic action — an LLM call above all — that is
  the difference between a whole-cell claim costing one call and costing all of
  them. Skips are reported on the drain's `%Report{}` step
  (`%{called: n, skipped: n}`), so the saving is visible rather than assumed.
  """


  alias ReactiveDag.Node.Fingerprint
  alias ReactiveDag.Node.Recompute.Read

  @doc "Recompute a per_key node; returns `{changed_keys, meta}`."
  @spec recompute(ReactiveDag.Cell.t(), map(), [String.t()] | nil) :: {[String.t()], map()}
  def recompute(cell, spec, claimed, opts \\ []) do
    source = cell.meta[:over_source]

    rows =
      Read.items(source, source_over(cell), nil, claimed, keyed_scope(source, claimed), opts)

    fp_attr = spec.fingerprint_attribute || Fingerprint.default_attribute()
    existing = existing_fingerprints(cell, fp_attr, rows, source, opts)

    # PARTITION FIRST, then call. Skips are decided before anything runs, so a
    # bounded stream spends its slots only on rows that genuinely need the
    # action — a whole-cell claim over mostly-unchanged rows costs almost
    # nothing regardless of the concurrency setting.
    {to_call, skipped} =
      Enum.split_with(rows, fn row ->
        want = Fingerprint.of(spec.fingerprint, row)
        want == nil or want != Map.get(existing, row_key(row, source))
      end)

    changed = call_rows(cell, spec, to_call, source, fp_attr, opts)

    {changed, %{called: length(to_call), skipped: length(skipped)}}
  end

  # one at a time (the default) — no process overhead, no supervision, and the
  # shape every node had before concurrency was an option.
  defp call_rows(cell, %{max_concurrency: n} = spec, rows, source, fp_attr, opts)
       when is_nil(n) or n == 1 do
    Enum.map(rows, fn row ->
      key = row_key(row, source)
      write_row(cell, spec, key, row, Fingerprint.of(spec.fingerprint, row), fp_attr, opts)
      key
    end)
  end

  # bounded concurrency. `ordered: true` (the default) is deliberate: results
  # are applied in ROW order, so the changed-key list is deterministic — which
  # matters for tests, diffs, and reading a Report.
  defp call_rows(cell, spec, rows, source, fp_attr, opts) do
    rows
    |> Task.async_stream(
      fn row ->
        key = row_key(row, source)
        write_row(cell, spec, key, row, Fingerprint.of(spec.fingerprint, row), fp_attr, opts)
        key
      end,
      max_concurrency: spec.max_concurrency,
      timeout: spec.timeout || :infinity,
      # a row that dies must FAIL the recompute, not vanish from it: a
      # half-written cell that reports success is worse than a loud crash.
      on_timeout: :exit
    )
    |> Enum.map(fn {:ok, key} -> key end)
  end

  # ── the call + write ────────────────────────────────────────────────────────

  defp write_row(cell, spec, key, row, fingerprint, fp_attr, opts) do
    resource = cell.meta[:resource]

    result =
      resource
      |> Ash.ActionInput.for_action(spec.action, action_args(resource, spec, row))
      |> Ash.run_action()
      |> case do
        {:ok, result} ->
          result

        {:error, error} ->
          raise "reactive_dag: per_key action #{inspect(spec.action)} on " <>
                  "#{inspect(resource)} failed for key #{inspect(key)}: " <>
                  Exception.message(error)
      end

    attrs =
      result
      |> project(spec.into, resource)
      |> Map.put(payload_key(cell), key)
      |> Fingerprint.put(fp_attr, fingerprint, resource, "per_key … fingerprint:")

    resource
    |> Ash.Changeset.for_create(
      cell.meta[:payload_action] || :upsert,
      attrs,
      tenant_opts(opts)
    )
    |> Ash.create!()

    :ok
  end

  # The plan's tenant, handed to Ash. Ash resolves the column from the
  # resource's own `multitenancy` block, so this never names it; a resource
  # declaring none ignores it.
  defp tenant_opts(opts) do
    case Keyword.get(opts, :tenant) do
      nil -> []
      tenant -> [tenant: tenant]
    end
  end

  defp scoped(query, opts) do
    case Keyword.get(opts, :tenant) do
      nil -> query
      tenant -> Ash.Query.set_tenant(query, tenant)
    end
  end

  # the row's fields → the action's arguments. An explicit `args:` maps them
  # (`[text: :body]` = pass the row's :body as the `text` argument); otherwise
  # every argument whose name matches a field on the row is passed through.
  defp action_args(resource, spec, row) do
    case spec.args do
      args when is_list(args) and args != [] ->
        Map.new(args, fn {arg, field} -> {arg, Map.get(row, field)} end)

      _ ->
        case Ash.Resource.Info.action(resource, spec.action) do
          %{arguments: arguments} ->
            for %{name: name} <- arguments,
                value = Map.get(row, name),
                into: %{},
                do: {name, value}

          _ ->
            %{}
        end
    end
  end

  # the action's result → this node's attributes.
  defp project(result, into, _resource) when is_list(into) and into != [] do
    Map.new(into, fn {dest, src} -> {dest, fetch_result(result, src)} end)
  end

  defp project(result, _into, resource) do
    attrs = resource |> Ash.Resource.Info.attributes() |> Enum.map(& &1.name)

    for name <- attrs, value = fetch_result(result, name), into: %{}, do: {name, value}
  end

  # results come back from generic actions as maps that may be string- or
  # atom-keyed (an LLM's structured output is string-keyed JSON)
  defp fetch_result(result, name) when is_map(result) do
    Map.get(result, name) || Map.get(result, to_string(name))
  end

  defp fetch_result(_result, _name), do: nil

  # the stored fingerprint per key, for exactly the rows this pass will consider
  defp existing_fingerprints(_cell, _attr, [], _source, _opts), do: %{}

  defp existing_fingerprints(cell, attr, rows, source, opts) do
    resource = cell.meta[:resource]

    if Ash.Resource.Info.attribute(resource, attr) do
      key_attr = payload_key(cell)
      keys = Enum.map(rows, &row_key(&1, source))

      resource
      |> Ash.Query.do_filter([{key_attr, [in: keys]}])
      |> scoped(opts)
      |> Ash.read!()
      |> Map.new(&{&1 |> Map.fetch!(key_attr) |> to_string(), Map.get(&1, attr)})
    else
      %{}
    end
  rescue
    _ -> %{}
  end

  # ── keys ────────────────────────────────────────────────────────────────────

  # a per_key node maps one input row to one output row, so the input's key IS
  # the output's key (the `:identity` grain).
  defp row_key(row, %{payload_key: pk}) when not is_nil(pk),
    do: row |> Map.fetch!(pk) |> to_string()

  defp row_key(row, _source), do: row |> Map.fetch!(:key) |> to_string()

  defp payload_key(cell), do: cell.meta[:payload_key] || :key

  defp source_over(%{inputs: [over | _]}), do: String.to_atom(over)
  defp source_over(_cell), do: nil

  defp keyed_scope(_source, nil), do: nil
  defp keyed_scope(_source, keys), do: {:keys, keys}
end

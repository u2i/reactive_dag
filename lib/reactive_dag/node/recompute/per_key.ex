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

  require Ash.Query

  alias ReactiveDag.Node.Recompute.Read

  @default_fingerprint_attr :fingerprint

  @doc "Recompute a per_key node; returns `{changed_keys, meta}`."
  @spec recompute(ReactiveDag.Cell.t(), map(), [String.t()] | nil) :: {[String.t()], map()}
  def recompute(cell, spec, claimed) do
    source = cell.meta[:over_source]
    rows = Read.items(source, source_over(cell), nil, claimed, keyed_scope(source, claimed))

    fp_attr = spec.fingerprint_attribute || @default_fingerprint_attr
    existing = existing_fingerprints(cell, fp_attr, rows, source)

    {changed, called, skipped} =
      Enum.reduce(rows, {[], 0, 0}, fn row, {changed, called, skipped} ->
        key = row_key(row, source)
        want = fingerprint(spec.fingerprint, row)

        if want != nil and want == Map.get(existing, key) do
          # nothing the result depends on moved — do not pay for the call
          {changed, called, skipped + 1}
        else
          write_row(cell, spec, key, row, want, fp_attr)
          {[key | changed], called + 1, skipped}
        end
      end)

    {Enum.reverse(changed), %{called: called, skipped: skipped}}
  end

  # ── the call + write ────────────────────────────────────────────────────────

  defp write_row(cell, spec, key, row, fingerprint, fp_attr) do
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
      |> maybe_put_fingerprint(fp_attr, fingerprint, resource)

    resource
    |> Ash.Changeset.for_create(cell.meta[:payload_action] || :upsert, attrs)
    |> Ash.create!()

    ReactiveDag.Op.put(cell, key)
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

  # ── fingerprints ────────────────────────────────────────────────────────────

  # nil when the node declares none — every recompute then calls the action.
  defp fingerprint(nil, _row), do: nil
  defp fingerprint([], _row), do: nil

  defp fingerprint(fields, row) do
    fields
    |> Enum.map(&Map.get(row, &1))
    |> :erlang.term_to_binary()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  defp maybe_put_fingerprint(attrs, _attr, nil, _resource), do: attrs

  defp maybe_put_fingerprint(attrs, attr, value, resource) do
    if Ash.Resource.Info.attribute(resource, attr) do
      Map.put(attrs, attr, value)
    else
      raise ArgumentError,
            "reactive_dag: `per_key … fingerprint:` needs somewhere to store the hash, but " <>
              "#{inspect(resource)} has no #{inspect(attr)} attribute. Add " <>
              "`attribute #{inspect(attr)}, :string`, or name another with " <>
              "`fingerprint_attribute`."
    end
  end

  # the stored fingerprint per key, for exactly the rows this pass will consider
  defp existing_fingerprints(_cell, _attr, [], _source), do: %{}

  defp existing_fingerprints(cell, attr, rows, source) do
    resource = cell.meta[:resource]

    if Ash.Resource.Info.attribute(resource, attr) do
      key_attr = payload_key(cell)
      keys = Enum.map(rows, &row_key(&1, source))

      resource
      |> Ash.Query.do_filter([{key_attr, [in: keys]}])
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

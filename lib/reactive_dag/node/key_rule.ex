defmodule ReactiveDag.Node.KeyRule do
  @moduledoc """
  A GENERIC `ReactiveDag.KeyRule` for graphs declared with `ReactiveDag.Node`.
  `Node` records each node's `key_rule` (`:identity | :all`) in `cell.meta`;
  this reads it:

    * `:all`      — any input change escalates to a whole-cell recompute (the
      drain turns this into `"*"`). For aggregate / cross-range (reduce) cells.
    * `:identity` — a changed input key maps to the same output key (pass through).
      For key-local (map) cells. The default.

  This is the uniform per-cell rule (cascade's shape). A host needing per-op,
  per-input-leg rules (the portal's product/relation escalation) still writes its
  own KeyRule; this generic one covers the `:identity | :all` case.
  """
  @behaviour ReactiveDag.KeyRule

  alias ReactiveDag.Cell

  @impl true
  def rule(%Cell{meta: %{key_rule: :all}}, _child, _changed), do: :all

  # `{:bucket, kind}` — the calendar ladder: each changed child key claims its
  # bucket by PURE string work (the key's leading segment parses as a date or a
  # finer bucket label; see ReactiveDag.Calendar.bucket_of_key/2). Deletion-safe
  # (a vanished key still names the bucket it left); a key that doesn't parse
  # degrades the whole propagation to :all — correctness over precision.
  def rule(%Cell{meta: %{key_rule: {:bucket, kind}}}, _child, changed) do
    labels = Enum.map(changed, &ReactiveDag.Calendar.bucket_of_key(kind, &1))

    if :error in labels, do: :all, else: {:keys, Enum.uniq(labels)}
  end

  # `:group` — a changed child ROW claims its group: the mapping is the very
  # `group_by`/side fields the combinator already declares, evaluated by
  # reading the changed rows (one scoped query per propagation). A changed key
  # the lookup can't find — a deleted row — degrades the propagation to :all:
  # vanish must reprice everything it might have left.
  def rule(%Cell{meta: %{key_rule: :group} = meta}, _child, changed) do
    group_claims(meta[:reduce] || meta[:join], meta[:over_source], changed)
  end

  def rule(_parent, _child, changed), do: {:keys, changed}

  defp group_claims(nil, _source, _changed), do: :all
  defp group_claims(_spec, nil, _changed), do: :all

  defp group_claims(spec, source, changed) do
    alias ReactiveDag.Node.Recompute.Declarative

    rows =
      source.resource
      |> Ash.Query.do_filter([{source.payload_key, [in: changed]}])
      |> load_calcs(Map.get(source, :load, []))
      |> Ash.read!()

    if length(rows) < length(changed) do
      :all
    else
      key_fn = Declarative.key_fn(Map.get(spec, :key), Map.get(spec, :key_prefix))

      keys =
        case spec do
          %ReactiveDag.Node.Reduce{} = r ->
            group_fn = Declarative.group_fn(r.group_by)
            Enum.map(rows, &key_fn.(group_fn.(&1)))

          %ReactiveDag.Node.Join{} = j ->
            left = Declarative.side_fn(j.left)
            right = Declarative.side_fn(j.right)

            rows
            |> Enum.flat_map(&[left.(&1), right.(&1)])
            |> Enum.reject(&(&1 in [nil, false]))
            |> Enum.map(key_fn)
        end

      {:keys, Enum.uniq(keys)}
    end
  end

  defp load_calcs(query, []), do: query
  defp load_calcs(query, loads), do: Ash.Query.load(query, loads)
end

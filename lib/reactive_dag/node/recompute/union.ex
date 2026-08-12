defmodule ReactiveDag.Node.Recompute.Union do
  @moduledoc """
  Runs a `union` node: one row per `(input cell, key)` across several inputs,
  written into this node's own table.

  ## Scoping

  A claim on a union node carries its own provenance — the cell key is
  `"<input>|<key>"` — so a claim names exactly which input moved, and only that
  input is read. That is what makes N inputs safe here: nothing correlates
  across them, so a partial claim is not a partial answer (contrast a cross-node
  join, where a claim naming one side leaves the other unread and the fold
  writes nulls over good data).

  A whole-cell claim reads every input, which is the only time it does.

  ## What it reads

  Each input's own rows, through `ReactiveDag.Node.Rows` — the resource and key
  derivation are stamped into `meta.union_sources` at graph assembly, since a
  recompute receives only its own cell and an input's resource is a cross-node
  fact.

  This used to read the coordination tuple, back when an input might be a
  tableless verdict node with nowhere else to put its answer. Reading the
  resource means a union can project any column its inputs have, not just the
  tuple's fixed `status`.
  """

  alias ReactiveDag.Node.Rows

  @doc """
  The `[{key, row}]` pairs a union pass produces — the caller materialises them
  through the ordinary payload loop, so a union writes its rows exactly as any
  other node does.
  """
  @spec pairs(map(), map(), [String.t()] | nil) :: {[{String.t(), map()}], map()}
  def pairs(spec, sources, claimed) do
    inputs = inputs_for(spec, claimed)

    pairs =
      inputs
      |> Enum.flat_map(fn input ->
        sources
        |> Map.fetch!(input)
        |> Rows.all()
        |> Enum.map(fn row -> {key_for(input, row.key), project(spec, source_row(input, row))} end)
      end)
      |> filter_to_claim(claimed)

    {pairs, %{inputs_read: length(inputs)}}
  end

  # an explicit `into:` maps source fields onto this resource's attributes;
  # omitted, the source row passes through and the payload loop drops what the
  # resource has no column for.
  defp project(%{into: into}, row) when is_list(into) and into != [],
    do: Map.new(into, fn {dest, src} -> {dest, Map.get(row, src)} end)

  defp project(_spec, row), do: row

  # WHICH inputs this pass must read. A scoped claim names them in its keys, so
  # an unrelated input is never touched; a whole-cell claim reads them all.
  defp inputs_for(%{from: from}, nil), do: Enum.map(from, &to_string/1)

  defp inputs_for(%{from: from}, claimed) do
    named = claimed |> Enum.map(&(&1 |> String.split("|") |> hd())) |> MapSet.new()

    from
    |> Enum.map(&to_string/1)
    |> Enum.filter(&MapSet.member?(named, &1))
  end

  # a scoped pass writes only the keys it claimed — reading an input gives every
  # key it has, and the unclaimed ones are not this pass's business (writing them
  # would be harmless but would report them as changed).
  defp filter_to_claim(pairs, nil), do: pairs

  defp filter_to_claim(pairs, claimed) do
    want = MapSet.new(claimed)
    Enum.filter(pairs, fn {key, _row} -> MapSet.member?(want, key) end)
  end

  # the union's key IS its provenance: which input, and that input's key.
  defp key_for(input, key), do: "#{input}|#{key}"

  # the source fields a mapping may draw on: the row's own columns, plus the two
  # facts only the union knows — which input it came from, and its cell key.
  # `:cell` and `:key` win, so an input with a `:key` column of its own cannot
  # shadow the provenance the union is built on.
  defp source_row(input, row) do
    Map.merge(Map.from_struct(row.record), %{cell: input, key: row.key})
  end
end

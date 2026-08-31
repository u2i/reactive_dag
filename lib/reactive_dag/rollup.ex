defmodule ReactiveDag.Rollup do
  @moduledoc """
  Summing one key across many meta maps — the arithmetic behind every "what did
  this cost" line, wherever the numbers came from.

  Both phases report what their work cost, and they report it the same way. A
  drain step carries `meta` (`ReactiveDag.Report`); a poll carries
  `detail:` (`ReactiveDag.Source`). The containers differ because the phases do,
  but the arithmetic over them does not, and it used to be written twice — two
  implementations of one fold, which have to agree and have no mechanism forcing
  them to.

  `ReactiveDag.Report.total/2` and `ReactiveDag.Source.detail_total/2` both
  delegate here. A host calls whichever fits what it has in hand and gets the
  same answer.

  ## The two shapes

  A count may be reported flat, or broken down per bucket:

      %{tokens_in: 1600}
      %{tokens_in: %{"claude-haiku-4-5" => 1200, "openai/gpt-5.6-luna" => 400}}

  `total/2` sums either to one number, so a cost line does not have to know
  which shape a node chose. `by/2` returns the breakdown.

  The library does not interpret the buckets — a bucket is a model name only
  because a host chose to key by one. Mixing shapes is fine: a graph where one
  node reports per-model tokens and another reports a bare count totals
  correctly rather than refusing to show a number.
  """

  @typedoc "A bucket a count can be attributed to — a model name, typically."
  @type bucket :: String.t() | atom()

  @doc """
  Sum `key` across every meta map in `metas`.

  Maps lacking the key contribute nothing, so a mixed set — some nodes reporting
  tokens, most not — totals rather than raising. A key nothing reported is `0`.
  """
  @spec total(Enumerable.t(), atom()) :: number()
  def total(metas, key) do
    metas
    |> values(key)
    |> Enum.map(fn
      n when is_number(n) -> n
      m when is_map(m) -> m |> Map.values() |> Enum.filter(&is_number/1) |> Enum.sum()
      _ -> 0
    end)
    |> Enum.sum()
  end

  @doc """
  Sum `key` across `metas`, **per bucket** — the breakdown behind `total/2`.

      Rollup.by(metas, :tokens_in)
      #=> %{"claude-haiku-4-5" => 1200, "openai/gpt-5.6-luna" => 400}

  This is what a cost line needs that a single number cannot give: models differ
  in price by an order of magnitude, so one summed token count cannot be turned
  into a cost, nor say which model is driving spend.

  A flat number lands under `:unattributed` rather than being dropped — a node
  reporting tokens without saying which model produced them is a gap worth
  SEEING, and omitting it would make the breakdown disagree with `total/2` for
  no visible reason:

      Rollup.by(metas, :tokens_in)
      #=> %{"claude-haiku-4-5" => 1200, unattributed: 90}

  The returned values always sum to `total/2` for the same key. `%{}` when
  nothing reported it.
  """
  @spec by(Enumerable.t(), atom()) :: %{optional(bucket()) => number()}
  def by(metas, key) do
    metas
    |> values(key)
    |> Enum.reduce(%{}, fn
      n, acc when is_number(n) ->
        Map.update(acc, :unattributed, n, &(&1 + n))

      m, acc when is_map(m) ->
        for {bucket, n} <- m, is_number(n), reduce: acc do
          inner -> Map.update(inner, bucket, n, &(&1 + n))
        end

      _, acc ->
        acc
    end)
  end

  # `nil` for anything that is not a map, rather than raising: the callers hand
  # this whatever their container held, and a step with no meta at all is an
  # ordinary case rather than a error.
  defp values(metas, key) do
    Enum.map(metas, fn
      %{} = meta -> Map.get(meta, key)
      _ -> nil
    end)
  end
end

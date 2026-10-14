defmodule ReactiveDag.Stale do
  @moduledoc """
  WHICH ROWS ARE OUT OF DATE — asked, not pushed.

  A cascade propagates: something writes, the graph is walked, and every
  consequence is computed before the transaction closes. That is how work gets
  done, and it is not how you find out what was MISSED. A dropped propagation —
  an enqueue deduped against a running job, a crash mid-walk, a gate that was
  off — leaves a row stale with nothing recording the fact.

  This asks the opposite question, of the data rather than of the engine:

      stale(child) <=> exists parent : parent.recorded_at > child.recorded_at

  A child derived before its input last moved is out of date. Nothing has to
  have remembered; the comparison is true or it is not.

  ## Why `recorded_at` and not `valid_at`

  The two clocks answer different questions, and only one of them works here.

  `valid_at` is a property of the data — "the minutes said X from June 2019".
  `recorded_at` is a property of our knowledge — "we learned it in October
  2026". A backdated correction has a `valid_at` in 2019 and would never appear
  newer than a child derived last week, so staleness measured in valid time
  SILENTLY UNDER-CLAIMS. That is the failure mode the whole key-rule ladder is
  built to avoid: over-claiming is wasteful, under-claiming is wrong.

  Ash stamps `recorded_at` from the wall clock precisely because `now()` would
  resolve to the write's `as_of`.

  ## This does not replace the cascade

  Propagation is still how work happens — immediately, in order, with
  suspensions where it must stop. This is the audit: a sweep that finds what
  propagation missed, and a page that can say "these 40 rows are behind"
  without walking anything.
  """

  alias ReactiveDag.{Cell, Plan}

  @doc """
  The edges worth checking, as `{parent_id, child_id, rule}`.

  Derived from the plan: every declared input, paired with how a change on that
  edge reaches the consumer. Mechanical — there is no list to keep in step.

  Edges whose consumer is not temporal are omitted: without a period there is
  no `recorded_at` to compare, so the question cannot be asked of them. That is
  silence rather than a false "fresh", and `unaskable/1` names them.
  """
  @spec edges(Plan.t()) :: [{Cell.id(), Cell.id(), atom() | tuple()}]
  def edges(%Plan{cells: cells}) do
    for {child_id, child} <- cells,
        temporal_cell?(child),
        input_id <- child.inputs,
        parent = Map.get(cells, input_id),
        parent != nil,
        temporal_cell?(parent) do
      {input_id, child_id, child.meta[:key_rule] || :identity}
    end
  end

  @doc """
  Edges the question CANNOT be asked of, and why.

  A staleness sweep that quietly skipped these would report a clean graph while
  part of it was unmeasurable — the same shape of lie as a crawl that could not
  look reporting that it found nothing.
  """
  @spec unaskable(Plan.t()) :: [{Cell.id(), String.t()}]
  def unaskable(%Plan{cells: cells}) do
    for {id, cell} <- cells,
        not temporal_cell?(cell),
        cell.inputs != [] or Enum.any?(cells, fn {_, c} -> id in c.inputs end) do
      {id, reason_not_temporal(cell)}
    end
  end

  @doc """
  The stale keys on one edge.

  Returns `{:ok, keys}`, `:all` where the rule escalates, or
  `{:error, reason}` where the question cannot be answered — never a bare list,
  so a caller cannot mistake "I could not look" for "nothing is stale".

  ## What each rule becomes

    * `:identity` — a join on the key. A child is stale when the parent row of
      the same key recorded later. One query, no per-key work.

    * `:all` — one comparison for the whole cell: the newest parent against the
      oldest child. An aggregate has no per-key correspondence to join on, so
      the honest answer is the whole cell or nothing.

    * `{:group, …}` / `:group` — NOT YET. A grain join is the right shape and a
      better one than today's key-string parsing (typed columns, indexable),
      but it needs the grain's columns on both sides and this does not read
      them yet. Returns `{:error, :unsupported_rule}` rather than falling back
      to `:identity`, which would silently under-claim.
  """
  @spec on_edge(Plan.t(), Cell.id(), Cell.id(), keyword()) ::
          {:ok, [String.t()]} | :all | {:error, term()}
  def on_edge(%Plan{cells: cells} = plan, parent_id, child_id, opts \\ []) do
    with %Cell{} = parent <- Map.get(cells, parent_id),
         %Cell{} = child <- Map.get(cells, child_id) do
      compare(parent, child, child.meta[:key_rule] || :identity, plan, opts)
    else
      _ -> {:error, :no_such_cell}
    end
  end

  defp compare(parent, child, :identity, plan, opts) do
    with {:ok, parents} <- stamps(parent, plan, opts),
         {:ok, children} <- stamps(child, plan, opts) do
      stale =
        for {key, child_at} <- children,
            parent_at = Map.get(parents, key),
            parent_at != nil,
            DateTime.compare(parent_at, child_at) == :gt,
            do: key

      {:ok, Enum.sort(stale)}
    end
  end

  defp compare(parent, child, :all, plan, opts) do
    # The NEWEST parent against the OLDEST child: an aggregate reads every
    # parent row, so one row moving after the earliest-derived child makes the
    # cell stale. Comparing newest-to-newest would miss a child derived before
    # a change that a LATER child already saw.
    with {:ok, parents} <- stamps(parent, plan, opts),
         {:ok, children} <- stamps(child, plan, opts) do
      newest = parents |> Map.values() |> max_at()
      oldest = children |> Map.values() |> min_at()

      cond do
        is_nil(newest) or is_nil(oldest) -> {:ok, []}
        DateTime.compare(newest, oldest) == :gt -> :all
        true -> {:ok, []}
      end
    end
  end

  defp compare(_parent, _child, rule, _plan, _opts) when rule in [:group] or is_tuple(rule),
    do: {:error, {:unsupported_rule, rule}}

  defp compare(_parent, _child, rule, _plan, _opts), do: {:error, {:unsupported_rule, rule}}

  # `%{key => recorded_at}` for a cell's rows, as of now.
  defp stamps(%Cell{meta: meta} = cell, plan, opts) do
    resource = meta[:resource]
    key_attr = meta[:payload_key] || :key

    cond do
      is_nil(resource) ->
        {:error, {:no_rows, cell.id}}

      not temporal?(resource) ->
        {:error, {:not_temporal, cell.id}}

      true ->
        stamp_attr = Ash.Resource.Info.temporal_recorded_at(resource)

        if is_nil(stamp_attr) do
          {:error, {:no_recorded_at, cell.id}}
        else
          rows =
            resource
            |> Ash.read!(Keyword.take(Plan.frontier_opts(plan) ++ opts, [:tenant]))
            |> Map.new(&{Map.get(&1, key_attr), Map.get(&1, stamp_attr)})

          {:ok, rows}
        end
    end
  rescue
    e -> {:error, {:unreadable, Exception.message(e)}}
  end

  defp max_at([]), do: nil
  defp max_at(list), do: list |> Enum.reject(&is_nil/1) |> Enum.max(DateTime, fn -> nil end)

  defp min_at([]), do: nil
  defp min_at(list), do: list |> Enum.reject(&is_nil/1) |> Enum.min(DateTime, fn -> nil end)

  defp reason_not_temporal(%Cell{meta: meta}) do
    case meta[:resource] do
      nil -> "keeps no rows of its own"
      resource -> "#{inspect(resource)} is not temporal"
    end
  end

  defp temporal_cell?(%Cell{meta: meta}) do
    case meta[:resource] do
      nil -> false
      resource -> temporal?(resource)
    end
  end

  defp temporal?(resource) do
    function_exported?(Ash.Resource.Info, :temporal?, 1) and
      Ash.Resource.Info.temporal?(resource)
  end
end

defmodule ReactiveDag.Calendar do
  @moduledoc """
  Calendar bucketing as an ASH CALCULATION — the Ash-native answer to the
  classic "date-marked records → time-bucketed aggregate". The bucket is
  declared on the resource that OWNS the date (where derived values live in
  Ash), and a rollup node just groups by the calculation:

      # on the data's resource
      calculations do
        calculate :month, :string, {ReactiveDag.Calendar, bucket: :month, of: :observed_on}
      end

      # on the rollup node
      reduce over: :readings,
             group_by: [:month],
             into: [sum: [value: :total], count: :n]

  Buckets: `:day` (`"2026-08-11"`), `:week` (ISO — `"2026-W33"`), `:month`
  (`"2026-08"`), `:quarter` (`"2026-Q3"`), `:year` (`"2026"`). Labels sort
  lexicographically in chronological order, and the derived cell key IS the
  label. Accepts `Date`, `DateTime`, `NaiveDateTime`; nil stays nil.

  This module computes in the BEAM after the read, so it works on every data
  layer (Ets included). A Postgres host wanting datastore pushdown declares an
  expr calculation instead — the rollup neither knows nor cares:

      calculate :month, :string, expr(fragment("to_char(?, 'YYYY-MM')", observed_on))
  """
  use Ash.Resource.Calculation

  @buckets [:day, :week, :month, :quarter, :year]

  @doc "The supported bucket kinds."
  @spec buckets() :: [atom()]
  def buckets, do: @buckets

  @impl true
  def init(opts) do
    cond do
      opts[:bucket] not in @buckets ->
        {:error, "bucket: must be one of #{inspect(@buckets)}, got #{inspect(opts[:bucket])}"}

      not is_atom(opts[:of]) or is_nil(opts[:of]) ->
        {:error, "of: must name the date attribute, got #{inspect(opts[:of])}"}

      true ->
        {:ok, opts}
    end
  end

  @impl true
  def load(_query, opts, _context), do: [opts[:of]]

  @impl true
  def calculate(records, opts, _context) do
    {:ok, Enum.map(records, &label(opts[:bucket], Map.get(&1, opts[:of])))}
  end

  @doc "The bucket label for a date-ish value (nil-safe)."
  def label(_kind, nil), do: nil
  def label(kind, %DateTime{} = dt), do: label(kind, DateTime.to_date(dt))
  def label(kind, %NaiveDateTime{} = dt), do: label(kind, NaiveDateTime.to_date(dt))
  def label(:day, %Date{} = d), do: Date.to_iso8601(d)
  def label(:month, %Date{} = d), do: "#{d.year}-#{pad2(d.month)}"
  def label(:year, %Date{} = d), do: "#{d.year}"
  def label(:quarter, %Date{} = d), do: "#{d.year}-Q#{div(d.month - 1, 3) + 1}"

  def label(:week, %Date{} = d) do
    {y, w} = :calendar.iso_week_number({d.year, d.month, d.day})
    "#{y}-W#{pad2(w)}"
  end

  defp pad2(n), do: String.pad_leading("#{n}", 2, "0")
end

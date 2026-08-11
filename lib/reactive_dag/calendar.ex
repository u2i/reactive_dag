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

  @doc """
  Parse a bucket LABEL back to `{kind, first_date}` — the pure inverse of
  `label/2` (`"2026-08"` → `{:month, ~D[2026-08-01]}`). `:error` for anything
  that isn't a bucket label. This is what makes bucket keys self-describing:
  a key rule can relabel a child's key without consulting any data.
  """
  @spec parse(String.t()) :: {atom(), Date.t()} | :error
  def parse(label) when is_binary(label) do
    cond do
      match?({:ok, _}, Date.from_iso8601(label)) ->
        {:ok, d} = Date.from_iso8601(label)
        {:day, d}

      Regex.match?(~r/\A\d{4}-\d{2}\z/, label) ->
        [y, m] = label |> String.split("-") |> Enum.map(&String.to_integer/1)

        case Date.new(y, m, 1) do
          {:ok, d} -> {:month, d}
          _ -> :error
        end

      Regex.match?(~r/\A\d{4}-Q[1-4]\z/, label) ->
        {y, q} = {String.to_integer(binary_part(label, 0, 4)), String.to_integer(binary_part(label, 6, 1))}
        {:quarter, Date.new!(y, (q - 1) * 3 + 1, 1)}

      Regex.match?(~r/\A\d{4}-W\d{2}\z/, label) ->
        y = String.to_integer(binary_part(label, 0, 4))
        w = String.to_integer(binary_part(label, 6, 2))
        {:week, iso_week_first(y, w)}

      Regex.match?(~r/\A\d{4}\z/, label) ->
        {:year, Date.new!(String.to_integer(label), 1, 1)}

      true ->
        :error
    end
  end

  def parse(_), do: :error

  @doc """
  The half-open date range a bucket label covers: `{first, next_first}` —
  `range(:month, "2026-08")` → `{~D[2026-08-01], ~D[2026-09-01]}`. `:error`
  when the label isn't a `kind` label. What a host (or the library's automatic
  bucket scoping) filters the date attribute by.
  """
  @spec range(atom(), String.t()) :: {Date.t(), Date.t()} | :error
  def range(kind, label) do
    case parse(label) do
      {^kind, first} -> {first, next(kind, first)}
      _ -> :error
    end
  end

  @doc """
  The `kind` bucket a child KEY belongs to, by PURE string work: the key's
  leading `|`-segment is parsed as a date or a finer bucket label, and
  relabeled — `bucket_of_key(:month, "2026-08-11|r4")` → `"2026-08"`;
  `bucket_of_key(:month, "2026-08-11")` → `"2026-08"`. `:error` when the
  leading segment isn't date-shaped (the key rule then degrades to `:all`).

  Deletion-safe: a vanished entry's key still parses to the bucket it left.
  Nesting is "the bucket containing the child bucket's START date" — exact
  for day→month/quarter/year and month→quarter/year; a `:week` child only
  nests deterministically into `:year` (weeks straddle months).
  """
  @spec bucket_of_key(atom(), String.t()) :: String.t() | :error
  def bucket_of_key(kind, key) when is_binary(key) do
    case key |> String.split("|") |> hd() |> parse() do
      {_child_kind, first} -> label(kind, first)
      :error -> :error
    end
  end

  defp next(:day, d), do: Date.add(d, 1)
  defp next(:week, d), do: Date.add(d, 7)
  defp next(:month, d), do: d |> Date.beginning_of_month() |> Date.shift(month: 1)
  defp next(:quarter, d), do: d |> Date.beginning_of_month() |> Date.shift(month: 3)
  defp next(:year, d), do: Date.new!(d.year + 1, 1, 1)

  defp iso_week_first(year, week) do
    # ISO 8601: week 1 contains Jan 4th; weeks start Monday.
    jan4 = Date.new!(year, 1, 4)
    week1_monday = Date.add(jan4, -(Date.day_of_week(jan4) - 1))
    Date.add(week1_monday, (week - 1) * 7)
  end

  defp pad2(n), do: String.pad_leading("#{n}", 2, "0")
end

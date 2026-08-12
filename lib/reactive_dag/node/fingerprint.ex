defmodule ReactiveDag.Node.Fingerprint do
  @moduledoc """
  The one value that decides whether an observation MOVED.

  Two rungs of the ladder need the same question answered, for the same reason:
  work is expensive, and repeating it over unchanged input is the cost the
  engine exists to avoid.

    * `per_key` skips its action when the input fields it depends on are
      unchanged — the point of the rung, since the action may be an LLM call.
    * a **source-fed leaf** reports a key as changed only when what it observed
      moved. A re-crawl that finds identical bytes must not fire the cascade.

  Both compare ONE value, not the whole row, and for the same reason: a row
  carries fields that move on every observation without the observation having
  changed anything. A `last_seen_at` changes by definition. An `etag` can be
  re-issued for identical bytes. Comparing every attribute reports those as
  changes and re-runs everything downstream.

  ## The two forms

  A field list hashes those fields:

      fingerprint [:content_md5, :title]

  A function computes the value itself, for when "the same observation" is not
  a plain field comparison — a normalized URL, a hash of a hash, a version
  folded into a digest:

      fingerprint fn row -> "\#{row.content_md5}|\#{:erlang.phash2(row.title)}" end

  Either way the result is stored on the row (`fingerprint_attribute`, default
  `:fingerprint`) so the next pass has something to compare against.

  ## Why the host decides

  What counts as "the same observation" is domain knowledge the library cannot
  infer. Usually it is the content digest. Deliberately not always: a crawler
  may fold a listing title into it, so a re-titled document re-fires downstream
  work even though its bytes are identical. That is a correct domain judgement
  and the library has no business overriding it — it only needs somewhere to
  put the answer.
  """

  @default_attribute :fingerprint

  @typedoc "How a node computes its fingerprint: field list, function, or none."
  @type spec :: [atom()] | (map() -> term()) | nil

  @doc "The attribute a fingerprint is stored in when the node names none."
  @spec default_attribute() :: atom()
  def default_attribute, do: @default_attribute

  @doc """
  The fingerprint of `row` under `spec`.

  `nil` means the node declares none — every pass then treats the row as moved,
  which is the correct default: a node that has not said what makes it stale
  must not be assumed fresh.

  A function form returning `nil` means the same thing, which is how a source
  says "I could not determine this" without inventing a value that would read
  as unchanged.
  """
  @spec of(spec(), map()) :: String.t() | nil
  def of(nil, _row), do: nil
  def of([], _row), do: nil

  def of(fun, row) when is_function(fun, 1) do
    case fun.(row) do
      nil -> nil
      value -> to_string(value)
    end
  end

  def of(fields, row) when is_list(fields) do
    fields
    |> Enum.map(&Map.get(row, &1))
    |> :erlang.term_to_binary()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  @doc """
  Put `value` into `attrs` under `attr`, raising with the fix when the resource
  has nowhere to store it.

  A missing column is a configuration mistake that would otherwise be silent
  and expensive: the value is dropped on write, so the next pass reads `nil`,
  compares unequal, and re-runs the work the fingerprint existed to skip. It
  would look exactly like a fingerprint that never matches.
  """
  @spec put(map(), atom(), term(), module(), String.t()) :: map()
  def put(attrs, _attr, nil, _resource, _declared_by), do: attrs

  def put(attrs, attr, value, resource, declared_by) do
    if Ash.Resource.Info.attribute(resource, attr) do
      Map.put(attrs, attr, value)
    else
      raise ArgumentError,
            "reactive_dag: `#{declared_by}` needs somewhere to store the fingerprint, but " <>
              "#{inspect(resource)} has no #{inspect(attr)} attribute. Add " <>
              "`attribute #{inspect(attr)}, :string`, or name another with " <>
              "`fingerprint_attribute:`."
    end
  end
end

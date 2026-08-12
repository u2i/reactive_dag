defmodule ReactiveDag.Basis do
  @moduledoc """
  A **content-addressed digest of a row set** — what those rows looked like at a
  moment, so a later comparison can tell whether they have moved.

  The motivating use is human sign-off: a signature binds to *what was there*,
  not to a key. Store the digest with the signature, recompute it from current
  rows at read time, and the signature applies only while they match — so a
  correction, an addition or a removal lapses it automatically, with no
  revocation bookkeeping and nothing stored that can drift.

  Nothing here knows about signatures, though. It digests rows.

  ## Versioning is the point

  Every digest carries the scheme version it was produced under, and is
  compared under **that** version. Changing the canonicalization therefore
  cannot invalidate every stored digest on deploy — old records keep evaluating
  under the old scheme while new ones use the new.

  An unknown version never matches (it does not raise): digests from a future
  build degrade to "stale, re-check", not to a crash on the read path. That
  asymmetry is deliberate and is the part worth having in a library rather than
  re-deriving per host — the failure mode of getting it wrong is a silent mass
  re-ask, which looks like a data problem rather than a deploy problem.

  ## Choosing fields

  `fields:` names what the digest is *of*, in order, and is part of the
  contract: two callers digesting the same rows with different fields get
  different digests, correctly. Widen a digest by introducing a new **version**,
  never by changing what an existing one covers.

      Basis.digest(rows, fields: [:key, :status])
      Basis.matches?(stored, rows, fields: [:key, :status])
  """

  @current 1
  @unit "\\x1F"

  @doc "The current digest-scheme version."
  @spec current_version() :: pos_integer()
  def current_version, do: @current

  @doc """
  Digest `rows` — a list of maps — over `fields`, in field order, sorted so the
  row order of the input does not matter.

  Options:

    * `:fields` — the keys to digest (default `[:key]`)
    * `:version` — the scheme (default the current one)

  Returns the digest, or `:unknown_version` for a scheme this build does not
  know. A row missing one of `fields` raises: an absent value must never digest
  the same as a present one, which is exactly the confusion a content digest
  exists to prevent.
  """
  @spec digest([map()], keyword()) :: String.t() | :unknown_version
  def digest(rows, opts \\ []) do
    fields = Keyword.get(opts, :fields, [:key])
    do_digest(rows, fields, Keyword.get(opts, :version, @current))
  end

  defp do_digest(rows, fields, 1) do
    canonical =
      rows
      |> Enum.map(fn row -> Enum.map(fields, &fetch!(row, &1)) end)
      |> Enum.sort()
      |> Enum.map_join("\n", &Enum.join(&1, @unit))

    "1:" <> Base.encode16(:crypto.hash(:sha256, canonical), case: :lower)
  end

  defp do_digest(_rows, _fields, _unknown), do: :unknown_version

  @doc """
  Does `stored` still describe `rows`? Compares under the version `stored`
  carries, not the current one — see the versioning note above.

  `false` for an unknown version, a nil `stored`, or any mismatch.
  """
  @spec matches?(String.t() | nil, [map()], keyword()) :: boolean()
  def matches?(nil, _rows, _opts), do: false

  def matches?(stored, rows, opts) when is_binary(stored) do
    opts = Keyword.put_new(opts, :version, version_of(stored))

    case digest(rows, opts) do
      :unknown_version -> false
      current -> stored == current
    end
  end

  @doc """
  The scheme version a stored digest was produced under, or `0` for anything
  unparseable — which no scheme claims, so it never matches.
  """
  @spec version_of(String.t()) :: non_neg_integer()
  def version_of(stored) when is_binary(stored) do
    case Integer.parse(stored) do
      {version, ":" <> _} -> version
      _ -> 0
    end
  end

  # a row that cannot supply a digested field is a programming error, not a
  # value to hash around: digesting `nil` for an absent column would make
  # "column missing" and "column is null" indistinguishable.
  defp fetch!(row, field) do
    case Map.fetch(row, field) do
      {:ok, value} ->
        to_string(value)

      :error ->
        raise ArgumentError,
              "reactive_dag: cannot digest #{inspect(field)} — the row has no such key. " <>
                "Got: #{inspect(Map.keys(row))}"
    end
  end
end

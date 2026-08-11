defmodule ReactiveDag.Attestation.Basis do
  @moduledoc """
  The content-addressed BASIS of an attestation — a digest of what the scope
  selected at signing time (ADR-002 decision 4, in the host's docs).

  A signature binds to *what was there*, not to the key: at evaluation the same
  digest is recomputed from current rows, and the record applies only while
  they match. That is what makes lapse-on-world-change automatic — the data a
  rejection objected to is corrected → the basis moves → the rejection lapses,
  with no revocation bookkeeping and nothing stored that can drift.

  ## Versioning

  Every record stores the `basis_version` it was signed under, and is evaluated
  under THAT version — so changing the canonicalization (a new version) cannot
  lapse every attestation in the estate on deploy. An unknown version never
  matches (evaluates as a basis mismatch, never as a crash): records from a
  future scheme degrade to "re-ask", not to an error.

  ## What v1 digests

  The SPINE's view of the selected rows: `(key, status)` pairs, sorted by key.
  Extension columns are deliberately excluded — the lib neither reads nor
  writes them (`ReactiveDag.Tuple`'s contract), and what a signer affirms is
  the presence and verdict of the data as presented. A host that wants more
  fields in the basis proposes a v2, it does not widen v1.
  """

  @current 1

  @doc "The current digest-scheme version."
  @spec current_version() :: pos_integer()
  def current_version, do: @current

  @doc """
  Digest `rows` (spine-row maps with `:key` and `:status`) under `version`.
  Returns the digest string, or `:unknown_version` for a version this build
  does not know — which the evaluation treats as a non-matching basis.

  Every row MUST carry both fields (`ReactiveDag.Tuple.rows/2` always does);
  a row without a `:status` raises rather than digesting a status nobody
  observed — the basis is the load-bearing content identity for lapse, so
  absent must never hash like present.
  """
  @spec digest([map()], pos_integer()) :: String.t() | :unknown_version
  def digest(rows, version \\ @current)

  def digest(rows, 1) do
    canonical =
      rows
      |> Enum.map(&{Map.fetch!(&1, :key), Map.fetch!(&1, :status)})
      |> Enum.sort()
      |> Enum.map_join("\n", fn {k, s} -> "#{k}\x1F#{s}" end)

    "1:" <> Base.encode16(:crypto.hash(:sha256, canonical), case: :lower)
  end

  def digest(_rows, _unknown), do: :unknown_version

  @doc "Does `stored` (a record's basis) match `rows` under `version`?"
  @spec matches?(String.t(), [map()], pos_integer()) :: boolean()
  def matches?(stored, rows, version) do
    case digest(rows, version) do
      :unknown_version -> false
      current -> stored == current
    end
  end
end

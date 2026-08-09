defmodule ReactiveDag.Attestation.Requirement do
  @moduledoc """
  A named attestation REQUIREMENT — the policy for one kind of sign-off,
  declared once on the node that owns the raw data (`attestation :name do … end`
  in the `reactive` block) and consumed by name from an `attested` combinator or
  a `gate:` on an edge.

  Fields:

    * `name` — the requirement's name; what `requirement:`/`gate:` reference.
    * `on` — the cell id the assertions are about (filled at graph assembly
      from the declaring node; records store this as their `cell_id`).
    * `scope` — `:key` (per-grain: one assertion per row of `on`). Filter-scoped
      requirements evaluate through the same store/evaluation machinery
      (`ReactiveDag.Attestation.Evaluation.evaluate_scope/6`) but are not yet
      wired through the DSL — the completeness slice.
    * `signers` — the ELIGIBILITY CELL's id (an atom, like any `ref`). Who may
      sign is data: this cell's keys, filtered per-scope through `join`. It is a
      real input edge of every attested cell, so authority changes propagate and
      appear in lineage.
    * `join` — `(scope, eligibility_key) -> who | nil`: does this eligibility
      row license signing this scope, and as whom? The eligibility cell's key
      grammar is the host's; this is where it is interpreted.
    * `quorum` — `:any | :all | {:n_of, k}` over the eligible set.
    * `tolerance` — how long an assertion holds: `nil` (no time bound — the
      host applies its strength-derived default elsewhere), an integer of
      seconds, or `[days: n]` / `[hours: n]`.
    * `statuses` — optional override of the admission-state → spine-status
      vocabulary (default `%{affirmed: "covered", pending: "pending",
      refused: "refused"}`); status vocabulary is the host's.
  """

  defstruct name: nil,
            on: nil,
            scope: :key,
            signers: nil,
            join: nil,
            quorum: :any,
            tolerance: nil,
            statuses: nil,
            __identifier__: nil,
            __spark_metadata__: nil

  @type quorum :: :any | :all | {:n_of, pos_integer()}

  @type t :: %__MODULE__{
          name: atom(),
          on: String.t() | nil,
          scope: :key,
          signers: atom(),
          join: (ReactiveDag.Attestation.Scope.t(), String.t() -> String.t() | nil),
          quorum: quorum(),
          tolerance: nil | non_neg_integer() | keyword(),
          statuses: %{atom() => String.t()} | nil
        }

  @default_statuses %{affirmed: "covered", pending: "pending", refused: "refused"}

  @doc "The admission-state → spine-status map for this requirement."
  @spec statuses(t()) :: %{atom() => String.t()}
  def statuses(%__MODULE__{statuses: nil}), do: @default_statuses
  def statuses(%__MODULE__{statuses: s}), do: Map.merge(@default_statuses, Map.new(s))

  @doc "Tolerance normalized to seconds, or nil for no time bound."
  @spec tolerance_seconds(t()) :: non_neg_integer() | nil
  def tolerance_seconds(%__MODULE__{tolerance: nil}), do: nil
  def tolerance_seconds(%__MODULE__{tolerance: s}) when is_integer(s), do: s

  def tolerance_seconds(%__MODULE__{tolerance: kw}) when is_list(kw) do
    Keyword.get(kw, :days, 0) * 86_400 + Keyword.get(kw, :hours, 0) * 3_600 +
      Keyword.get(kw, :seconds, 0)
  end
end

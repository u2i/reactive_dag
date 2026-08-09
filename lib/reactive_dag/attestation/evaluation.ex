defmodule ReactiveDag.Attestation.Evaluation do
  @moduledoc """
  The read-time force of attestation records: stance ⨝ basis ⨝ eligibility ⨝
  tolerance → an ADMISSION per scope. Pure — takes everything as data (raw
  rows, stances, eligibility keys, a requirement, `now`), so the whole
  semantics is testable without a database; `ReactiveDag.Attestation.Op` is the
  thin DB glue around it.

  ## A stance's force

  A record is immutable history; whether it COUNTS is computed here, and it can
  fail three independent ways, each meaning something different:

    * `:basis` — the WORLD moved: what was signed is not what is there;
    * `:tolerance` — TIME passed: the assertion has aged out;
    * `:eligibility` — AUTHORITY moved: the licence to sign was withdrawn.

  All three read as not-in-force — never as green, never silently as rejected —
  but the failed predicate is reported (`lapses`), because the remedies differ
  and a UI must say which is being asked for. The predicates apply to BOTH
  polarities: a rejection decays and loses authority exactly as an affirmation
  does (everything decays; only the timescale is policy).

  ## Admission

    * `:refused` — an in-force rejection exists. Conservative and sticky by
      design: it is not out-voted by affirmations. What clears it is the world
      changing (the data is corrected → its basis moves → the rejection lapses
      like anything else) or the rejector's own later affirmation (stance =
      latest record per signer). A system that re-asks until it gets a yes is
      laundering attestations, not collecting them.
    * `:affirmed` — no in-force rejection, and in-force affirmations satisfy
      the quorum over the CURRENTLY-eligible set. An empty eligible set can
      never affirm — nobody-may-sign must not read as signed.
    * `:pending` — neither: awaiting a signer (or every past signature has
      lapsed — see `lapses` for why).
  """

  alias ReactiveDag.Attestation.{Basis, Requirement, Scope}

  @type admission :: %{
          scope: Scope.t(),
          state: :affirmed | :pending | :refused,
          signers: [String.t()],
          reasons: [%{who: String.t(), reason: String.t()}],
          lapses: [%{who: String.t(), lapse: :basis | :tolerance | :eligibility}]
        }

  @doc """
  Evaluate a `:key`-scoped requirement over a raw cell's rows: one admission
  per row, in row order. `eligibility_keys` are the signers cell's current
  keys; `stances` are `ReactiveDag.Attestation.stances/1` for the raw cell.
  """
  @spec evaluate([map()], [map()], [String.t()], Requirement.t(), DateTime.t()) :: [admission()]
  def evaluate(raw_rows, stances, eligibility_keys, %Requirement{scope: :key} = req, now) do
    by_scope = Enum.group_by(stances, & &1.scope)

    for row <- raw_rows do
      scope = {:key, row.key}
      evaluate_scope(scope, [row], Map.get(by_scope, scope, []), eligibility_keys, req, now)
    end
  end

  @doc """
  Evaluate ONE scope against the rows it currently selects. The general entry —
  `evaluate/5` maps it over a cell's rows; a filter-scoped (set-level,
  completeness) claim calls it directly with `Scope.select/2`'d rows.
  """
  @spec evaluate_scope(Scope.t(), [map()], [map()], [String.t()], Requirement.t(), DateTime.t()) ::
          admission()
  def evaluate_scope(scope, selected_rows, scope_stances, eligibility_keys, req, now) do
    eligible =
      eligibility_keys
      |> Enum.map(&req.join.(scope, &1))
      |> Enum.reject(&is_nil/1)
      |> Enum.uniq()

    tol = Requirement.tolerance_seconds(req)

    # latest record per signer IS the stance — enforced here, not just at the
    # store read, because supersede is semantics: a rejector's own later
    # affirmation must replace their rejection, whatever the caller passed.
    # A WITHDRAWAL supersedes like any record but then carries no force in
    # either direction — the signer simply has no stance, and the scope
    # returns to unaffirmed (never to refused).
    scope_stances =
      scope_stances
      |> Enum.group_by(& &1.who)
      |> Enum.map(fn {_who, records} -> Enum.max_by(records, & &1.signed_at, DateTime) end)
      |> Enum.reject(&(&1.polarity == :withdraw))

    {in_force, lapsed} =
      scope_stances
      |> Enum.map(&{&1, force(&1, selected_rows, eligible, tol, now)})
      |> Enum.split_with(fn {_s, f} -> f == :ok end)

    in_force = Enum.map(in_force, &elem(&1, 0))
    rejects = Enum.filter(in_force, &(&1.polarity == :reject))
    affirms = Enum.filter(in_force, &(&1.polarity == :affirm))

    state =
      cond do
        rejects != [] -> :refused
        quorum_met?(affirms, eligible, req.quorum) -> :affirmed
        true -> :pending
      end

    %{
      scope: scope,
      state: state,
      signers: Enum.map(if(state == :refused, do: rejects, else: affirms), & &1.who),
      reasons: for(r <- rejects, do: %{who: r.who, reason: r.reason}),
      lapses: for({s, l} <- lapsed, do: %{who: s.who, lapse: l})
    }
  end

  # the three force predicates, reported in the order of the lapse table:
  # world, then time, then authority.
  defp force(stance, rows, eligible, tolerance_seconds, now) do
    cond do
      not Basis.matches?(stance.basis, rows, stance.basis_version) -> :basis
      expired?(stance.signed_at, tolerance_seconds, now) -> :tolerance
      stance.who not in eligible -> :eligibility
      true -> :ok
    end
  end

  defp expired?(_signed_at, nil, _now), do: false

  defp expired?(signed_at, tolerance_seconds, now),
    do: DateTime.diff(now, signed_at, :second) > tolerance_seconds

  # quorum over the CURRENTLY-eligible set. An empty eligible set never affirms:
  # nobody-may-sign is a gap in the eligibility data, not a satisfied quorum.
  defp quorum_met?([], _eligible, _quorum), do: false
  defp quorum_met?(_affirms, [], _quorum), do: false

  defp quorum_met?(affirms, _eligible, :any), do: affirms != []

  defp quorum_met?(affirms, eligible, :all) do
    signed = MapSet.new(affirms, & &1.who)
    Enum.all?(eligible, &MapSet.member?(signed, &1))
  end

  defp quorum_met?(affirms, _eligible, {:n_of, k}) do
    affirms |> Enum.map(& &1.who) |> Enum.uniq() |> length() >= k
  end
end

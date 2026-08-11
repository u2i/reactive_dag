defmodule ReactiveDag.AttestationEvaluationTest do
  @moduledoc """
  The read-time FORCE semantics — the part of the attestation design where a
  wrong answer silently licenses a false green. Pure: rows, stances,
  eligibility, and `now` all come in as data.

  The load-bearing cases:

    * three lapse modes (world / time / authority), each named;
    * refused is sticky — not out-voted, cleared only by the world moving or
      the rejector's own later word;
    * an empty eligible set can never affirm (nobody-may-sign ≠ signed).
  """
  use ExUnit.Case, async: true

  alias ReactiveDag.Attestation.{Basis, Evaluation, Requirement}

  @now ~U[2026-08-09 12:00:00.000000Z]
  @row %{key: "AAA111", status: "present"}

  # eligibility keys are "serial|email"; the join licenses a matching serial.
  defp req(overrides \\ []) do
    struct!(
      %Requirement{
        name: :ownership,
        on: "machines",
        scope: :key,
        signers: :holders,
        join: fn {:key, serial}, elig_key ->
          case String.split(elig_key, "|") do
            [^serial, who] -> who
            _ -> nil
          end
        end,
        quorum: :any
      },
      overrides
    )
  end

  defp stance(overrides) do
    Map.merge(
      %{
        scope: {:key, "AAA111"},
        who: "alice@u2i.com",
        polarity: :affirm,
        reason: nil,
        basis: Basis.digest([@row]),
        basis_version: 1,
        signed_at: @now
      },
      Map.new(overrides)
    )
  end

  defp evaluate(stances, opts \\ []) do
    rows = Keyword.get(opts, :rows, [@row])
    elig = Keyword.get(opts, :eligibility, ["AAA111|alice@u2i.com"])
    [admission] = Evaluation.evaluate(rows, stances, elig, Keyword.get(opts, :req, req()), @now)
    admission
  end

  describe "admission" do
    test "no stance → pending (never-asked is unknown, not green)" do
      assert %{state: :pending, signers: [], lapses: []} = evaluate([])
    end

    test "an in-force affirmation → affirmed, naming its signer" do
      assert %{state: :affirmed, signers: ["alice@u2i.com"]} = evaluate([stance([])])
    end

    test "a rejection → refused, carrying who and why" do
      admission = evaluate([stance(polarity: :reject, reason: "not my machine")])
      assert admission.state == :refused
      assert admission.reasons == [%{who: "alice@u2i.com", reason: "not my machine"}]
    end

    test "refused is NOT out-voted by other signers' affirmations" do
      bob = stance(who: "bob@u2i.com", polarity: :reject, reason: "this serial is a rebuild")

      admission =
        evaluate([stance([]), bob],
          eligibility: ["AAA111|alice@u2i.com", "AAA111|bob@u2i.com"]
        )

      assert admission.state == :refused
    end

    test "a WITHDRAWAL clears the signer's word — back to pending, not refused" do
      # "I no longer vouch" is not "the data is wrong": the withdrawal
      # supersedes the affirmation but carries no force of its own, so the
      # scope returns to unaffirmed — re-askable, and never contradicted.
      earlier = stance(signed_at: ~U[2026-08-01 00:00:00.000000Z])
      withdrawal = stance(polarity: :withdraw, signed_at: ~U[2026-08-05 00:00:00.000000Z])

      admission = evaluate([earlier, withdrawal])
      assert admission.state == :pending
      assert admission.signers == []
      # no stance at all — a withdrawal is absence, not a lapse to report.
      assert admission.lapses == []
    end

    test "a withdrawal does not out-vote OTHER signers' affirmations" do
      bob_affirms = stance(who: "bob@u2i.com")
      alice_withdraws = stance(polarity: :withdraw)

      admission =
        evaluate([bob_affirms, alice_withdraws],
          eligibility: ["AAA111|alice@u2i.com", "AAA111|bob@u2i.com"]
        )

      assert admission.state == :affirmed
      assert admission.signers == ["bob@u2i.com"]
    end

    test "the rejector's own LATER affirmation supersedes their rejection" do
      earlier =
        stance(
          polarity: :reject,
          reason: "wrong list",
          signed_at: ~U[2026-08-01 00:00:00.000000Z]
        )

      later = stance(signed_at: ~U[2026-08-05 00:00:00.000000Z])
      assert %{state: :affirmed} = evaluate([earlier, later])
    end
  end

  describe "the three lapse modes" do
    test ":basis — the world moved: what was signed is not what is there" do
      old = stance(basis: Basis.digest([%{key: "AAA111", status: "failing"}]))
      admission = evaluate([old])
      assert admission.state == :pending
      assert admission.lapses == [%{who: "alice@u2i.com", lapse: :basis}]
    end

    test ":tolerance — time passed beyond the declared bound" do
      old = stance(signed_at: ~U[2026-01-01 00:00:00.000000Z])
      admission = evaluate([old], req: req(tolerance: [days: 30]))
      assert admission.state == :pending
      assert admission.lapses == [%{who: "alice@u2i.com", lapse: :tolerance}]
    end

    test "no tolerance declared → no time bound (the host's default applies elsewhere)" do
      old = stance(signed_at: ~U[2020-01-01 00:00:00.000000Z])
      assert %{state: :affirmed} = evaluate([old])
    end

    test ":eligibility — authority moved: the licence to sign was withdrawn" do
      admission = evaluate([stance([])], eligibility: ["AAA111|carol@u2i.com"])
      assert admission.state == :pending
      assert admission.lapses == [%{who: "alice@u2i.com", lapse: :eligibility}]
    end

    test "a REJECTION lapses by the same predicates — everything decays" do
      # the machine's row was corrected since the rejection: the objection no
      # longer applies, and the scope reverts to pending (re-ask), not refused.
      old_reject =
        stance(
          polarity: :reject,
          reason: "wrong owner shown",
          basis: Basis.digest([%{key: "AAA111", status: "failing"}])
        )

      admission = evaluate([old_reject])
      assert admission.state == :pending
      assert admission.lapses == [%{who: "alice@u2i.com", lapse: :basis}]
    end
  end

  describe "quorum over the currently-eligible set" do
    @two ["AAA111|alice@u2i.com", "AAA111|bob@u2i.com"]

    test ":any — one in-force affirmation suffices" do
      assert %{state: :affirmed} = evaluate([stance([])], eligibility: @two)
    end

    test ":all — every eligible signer must have signed (dual control)" do
      assert %{state: :pending} =
               evaluate([stance([])], eligibility: @two, req: req(quorum: :all))

      assert %{state: :affirmed} =
               evaluate([stance([]), stance(who: "bob@u2i.com")],
                 eligibility: @two,
                 req: req(quorum: :all)
               )
    end

    test "{:n_of, k} — four-eyes counting" do
      r = req(quorum: {:n_of, 2})
      assert %{state: :pending} = evaluate([stance([])], eligibility: @two, req: r)

      assert %{state: :affirmed} =
               evaluate([stance([]), stance(who: "bob@u2i.com")], eligibility: @two, req: r)
    end

    test "an EMPTY eligible set never affirms — nobody-may-sign is not signed" do
      # even a stance whose basis matches: with no eligibility row licensing it,
      # it has no force, and the quorum has no set to be met over.
      admission = evaluate([stance([])], eligibility: [])
      assert admission.state == :pending
      assert admission.lapses == [%{who: "alice@u2i.com", lapse: :eligibility}]
    end
  end

  describe "per-row evaluation over a cell" do
    test "each raw row is its own scope; only the signed one moves" do
      rows = [@row, %{key: "BBB222", status: "present"}]

      admissions =
        Evaluation.evaluate(
          rows,
          [stance([])],
          ["AAA111|alice@u2i.com", "BBB222|alice@u2i.com"],
          req(),
          @now
        )

      assert Enum.map(admissions, &{elem(&1.scope, 1), &1.state}) ==
               [{"AAA111", :affirmed}, {"BBB222", :pending}]
    end

    test "the basis binds to the ROW signed, not the whole cell" do
      # another machine appearing must not lapse alice's per-row signature —
      # that is exactly the difference between a :key scope and a :filter scope.
      rows = [@row, %{key: "NEW999", status: "present"}]

      [a, _new] =
        Evaluation.evaluate(rows, [stance([])], ["AAA111|alice@u2i.com"], req(), @now)

      assert a.state == :affirmed
    end
  end

  describe "scope instances (the filter-shaped view's rows)" do
    alias ReactiveDag.Attestation.Op

    test "{:filter, ks} is ONE instance, keyed by the requirement's instance_key" do
      r = req(scope: {:filter, {:prefix, "%"}}, instance_key: "estate")
      assert Op.instances(r, ["anything"]) == [{"estate", {:prefix, "%"}}]
    end

    test "{:filter_by, fun} derives one instance per eligibility row, deduped" do
      r =
        req(
          scope:
            {:filter_by,
             fn elig_key ->
               case String.split(elig_key, "|", parts: 2) do
                 [_serial, email] -> {email, {:segment, 2, "|", email}}
                 _ -> nil
               end
             end}
        )

      elig = ["M1|alice@u2i.com", "M2|alice@u2i.com", "M3|bob@u2i.com", "garbage"]

      assert Op.instances(r, elig) == [
               {"alice@u2i.com", {:segment, 2, "|", "alice@u2i.com"}},
               {"bob@u2i.com", {:segment, 2, "|", "bob@u2i.com"}}
             ]
    end

    test "a clause miss on an unexpected eligibility key SKIPS, like nil" do
      # regression: a single-clause fn (the shape the guide showed) used to
      # raise FunctionClauseError mid-recompute on the first unexpected row.
      r =
        req(
          scope:
            {:filter_by,
             fn "SER" <> _ = pair ->
               [_serial, email] = String.split(pair, "|", parts: 2)
               {email, {:segment, 2, "|", email}}
             end}
        )

      assert Op.instances(r, ["SER1|a@x", "garbage"]) == [{"a@x", {:segment, 2, "|", "a@x"}}]
    end

    test "a FunctionClauseError from DEEPER code still raises — only the fn's own miss skips" do
      r =
        req(
          scope:
            {:filter_by,
             fn _key ->
               # a genuine bug inside the host fn: a clause miss in code it CALLS
               String.split(:not_a_string, "|")
             end}
        )

      assert_raise FunctionClauseError, fn -> Op.instances(r, ["any"]) end
    end
  end

  describe "filter scope (set-level: the completeness carrier)" do
    test "signing THE SET lapses when the set gains a member" do
      mine = {:filter, {:prefix, "acme|%"}}
      rows = [%{key: "acme|a", status: "present"}, %{key: "acme|b", status: "present"}]

      signed =
        stance(scope: mine, basis: Basis.digest(rows))

      # unchanged set → affirmed
      assert %{state: :affirmed} =
               Evaluation.evaluate_scope(
                 mine,
                 rows,
                 [signed],
                 ["*|alice@u2i.com"],
                 req_filter(),
                 @now
               )

      # a member appears inside the filter → the completeness claim lapses
      grown = rows ++ [%{key: "acme|c", status: "present"}]

      assert %{state: :pending, lapses: [%{lapse: :basis}]} =
               Evaluation.evaluate_scope(
                 mine,
                 grown,
                 [signed],
                 ["*|alice@u2i.com"],
                 req_filter(),
                 @now
               )
    end

    defp req_filter do
      req(
        join: fn {:filter, _}, elig_key ->
          case String.split(elig_key, "|") do
            ["*", who] -> who
            _ -> nil
          end
        end
      )
    end
  end
end

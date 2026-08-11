defmodule ReactiveDag.AttestationRecordTest do
  @moduledoc """
  The Ash-resource storage pattern: the host defines the record resource, the
  `ReactiveDag.Attestation.Record` extension stamps the shape, and the store
  module reaches it via config. Ets-backed here (`private?: true` = per-process
  data), which is what makes the store testable IN the lib — the raw-SQL
  predecessor could only be proven by host suites.

  NOT async: the store finds its resource through global config.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.Attestation
  alias ReactiveDag.Attestation.{Basis, Scope}

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      allow_unregistered?(true)
    end
  end

  defmodule Record do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Attestation.Record]

    ets do
      private?(true)
    end

    attestation_record do
      who_from_actor(fn actor -> actor[:email] end)
    end
  end

  setup do
    prior = Application.get_env(:reactive_dag, :attestation_resource)
    Application.put_env(:reactive_dag, :attestation_resource, Record)
    on_exit(fn -> Application.put_env(:reactive_dag, :attestation_resource, prior) end)
    :ok
  end

  @rows [%{key: "AAA111", status: "present"}]

  describe "the extension stamps the shape" do
    test "attributes, the :sign create, and a primary read exist" do
      names = Ash.Resource.Info.attributes(Record) |> Enum.map(& &1.name)

      for a <- [
            :id,
            :cell_id,
            :scope_kind,
            :scope,
            :who,
            :polarity,
            :reason,
            :basis,
            :basis_version,
            :signed_at,
            :meta
          ] do
        assert a in names
      end

      assert %{type: :create} = Ash.Resource.Info.action(Record, :sign)
      assert %{type: :read, primary?: true} = Ash.Resource.Info.action(Record, :read)
    end

    test "APPEND-ONLY is enforced by the verifier, not by convention" do
      # Defined at runtime, the module slips past assert_raise: Elixir's
      # parallel checker runs @after_verify hooks (where Spark raises verifier
      # errors) asynchronously for runtime-defined modules and reports the
      # raise as a diagnostic. In a real `mix compile` the same resource FAILS
      # compilation — so assert the verifier's verdict directly.
      defmodule Mutable do
        use Ash.Resource,
          domain: Domain,
          data_layer: Ash.DataLayer.Ets,
          extensions: [ReactiveDag.Attestation.Record]

        actions do
          update(:amend)
        end
      end

      assert {:error, %Spark.Error.DslError{message: msg}} =
               ReactiveDag.Attestation.Record.VerifyAppendOnly.verify(Mutable.spark_dsl_config())

      assert msg =~ "APPEND-ONLY"
      assert msg =~ ":amend"
    end
  end

  describe "signing through the store" do
    test "affirm creates a record with the basis of what was presented" do
      {:ok, record, changed} =
        Attestation.affirm("machines", {:key, "AAA111"}, "alice@u2i.com", rows: @rows)

      assert record.polarity == :affirm
      assert record.basis == Basis.digest(@rows)
      assert record.basis_version == Basis.current_version()
      assert changed == [Scope.serialize({:key, "AAA111"})]
    end

    test "meta rides the record and comes back on every read (not write-only)" do
      # regression: the :sign action accepted :meta but to_record/1 dropped it —
      # host context written through meta was invisible to stances/history.
      {:ok, record, _} =
        Attestation.affirm("machines", {:key, "AAA111"}, "alice@u2i.com",
          rows: @rows,
          meta: %{"ticket" => "SEC-421"}
        )

      assert record.meta == %{"ticket" => "SEC-421"}
      assert [%{meta: %{"ticket" => "SEC-421"}}] = Attestation.stances("machines")
      assert [%{meta: %{"ticket" => "SEC-421"}}] = Attestation.history("machines")
    end

    test "an unknown option raises — a typo'd :actor must not silently disable who_from_actor" do
      # regression: opts were allowlisted with Keyword.take, so `acto:` was
      # dropped without a word and the write succeeded with the caller's who.
      assert_raise ArgumentError, ~r/unknown option.*:acto\b/s, fn ->
        Attestation.affirm("machines", {:key, "AAA111"}, "alice@u2i.com",
          rows: @rows,
          acto: %{email: "alice@u2i.com"}
        )
      end

      assert Attestation.history("machines") == []
    end

    test "reject without a reason raises before anything is written" do
      assert_raise ArgumentError, ~r/reason/, fn ->
        Attestation.reject("machines", {:key, "AAA111"}, "bob@u2i.com", "  ", rows: @rows)
      end

      assert Attestation.history("machines") == []
    end

    test "the resource's own :sign action enforces the reason rule too" do
      # a write that bypasses the store module hits the SAME rule at the action.
      assert_raise Ash.Error.Invalid, fn ->
        Record
        |> Ash.Changeset.for_create(:sign, %{
          cell_id: "machines",
          scope_kind: "key",
          scope: Scope.serialize({:key, "AAA111"}),
          who: "bob@u2i.com",
          polarity: "reject",
          basis: Basis.digest(@rows),
          basis_version: 1
        })
        |> Ash.create!()
      end
    end

    test "who_from_actor FORCES the signer from the actor — impersonation prevented at the write" do
      {:ok, record, _} =
        Attestation.affirm("machines", {:key, "AAA111"}, "mallory@evil.example",
          rows: @rows,
          actor: %{email: "alice@u2i.com"}
        )

      assert record.who == "alice@u2i.com"
    end

    test "no actor → the passed who stands (system-initiated writes still work)" do
      {:ok, record, _} =
        Attestation.affirm("machines", {:key, "AAA111"}, "alice@u2i.com", rows: @rows)

      assert record.who == "alice@u2i.com"
    end
  end

  describe "reading" do
    test "stances is the latest record per (scope, who); history keeps everything" do
      s = {:key, "AAA111"}

      {:ok, _, _} =
        Attestation.reject("machines", s, "alice@u2i.com", "not mine",
          rows: @rows,
          signed_at: ~U[2026-08-01 00:00:00.000000Z]
        )

      {:ok, _, _} =
        Attestation.affirm("machines", s, "alice@u2i.com",
          rows: @rows,
          signed_at: ~U[2026-08-05 00:00:00.000000Z]
        )

      {:ok, _, _} =
        Attestation.affirm("machines", {:key, "BBB222"}, "bob@u2i.com", rows: [])

      # stance = alice's LATER affirm supersedes her reject; bob's separate scope stands.
      assert [%{who: "alice@u2i.com", polarity: :affirm}, %{who: "bob@u2i.com"}] =
               Attestation.stances("machines")

      # history keeps all three, newest first, and narrows by scope.
      assert length(Attestation.history("machines")) == 3

      assert [%{polarity: :affirm}, %{polarity: :reject}] =
               Attestation.history("machines", scope: s)
    end

    test "records round-trip the scope and polarity types" do
      mine = {:filter, {:segment, 2, "|", "alice@u2i.com"}}
      {:ok, _, _} = Attestation.affirm("machines", mine, "alice@u2i.com", rows: [])

      assert [%{scope: ^mine, polarity: :affirm}] = Attestation.stances("machines")
    end
  end
end

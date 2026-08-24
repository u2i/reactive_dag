defmodule ReactiveDag.BasisTest do
  @moduledoc """
  `ReactiveDag.Basis` — a content digest of a row set, so a later comparison can
  tell whether those rows have moved.

  The motivating use is human sign-off: store the digest with the signature and
  the signature applies only while the rows still match, so a correction lapses
  it automatically with no revocation bookkeeping. But nothing here knows about
  signatures — it digests rows, and any "is this still what I saw?" question can
  use it.

  The versioning is the part worth having in a library rather than per host: get
  it wrong and changing the canonicalization silently invalidates every stored
  digest on deploy, which presents as a data problem rather than a deploy one.
  """
  # NOT async: these tests define modules at RUNTIME, and module definition is not
  # safely concurrent. Elixir serialises compilation behind a lock, and a Spark
  # verifier building its error reads the CALLING process's stacktrace
  # (`Spark.Error.DslError.exception/1` → `Process.info/2`) — which returns nil for a
  # process that has already moved on, failing a test whose assertion never ran.
  #
  # Observed once in a full async run and not reproducible in ~10 attempts since,
  # including at `--max-cases 64`. Left non-async rather than chased: these are a
  # handful of fast tests, and the concurrency bought nothing.
  use ExUnit.Case, async: false

  alias ReactiveDag.Basis

  describe "digest/2" do
    test "row ORDER does not matter — the set is what is digested" do
      a = [%{key: "a"}, %{key: "b"}]
      b = [%{key: "b"}, %{key: "a"}]

      assert Basis.digest(a) == Basis.digest(b)
    end

    test "a changed VALUE moves the digest — the point of the whole thing" do
      before = Basis.digest([%{key: "a", status: "present"}], fields: [:key, :status])
      later = Basis.digest([%{key: "a", status: "failing"}], fields: [:key, :status])

      refute before == later
    end

    test "an ADDED row moves it, and a REMOVED one" do
      one = Basis.digest([%{key: "a"}])
      two = Basis.digest([%{key: "a"}, %{key: "b"}])

      refute one == two
    end

    test "`fields:` is part of the contract — different fields, different digest" do
      rows = [%{key: "a", status: "present"}]

      refute Basis.digest(rows, fields: [:key]) ==
               Basis.digest(rows, fields: [:key, :status])
    end

    test "field ORDER matters, so a digest cannot be reproduced by accident" do
      rows = [%{a: "1", b: "2"}]

      refute Basis.digest(rows, fields: [:a, :b]) == Basis.digest(rows, fields: [:b, :a])
    end

    test "values are separated, so concatenation cannot collide" do
      # without a separator, {"ab", "c"} and {"a", "bc"} would digest alike
      refute Basis.digest([%{a: "ab", b: "c"}], fields: [:a, :b]) ==
               Basis.digest([%{a: "a", b: "bc"}], fields: [:a, :b])
    end

    test "an empty row set has a stable digest" do
      assert Basis.digest([]) == Basis.digest([])
      refute Basis.digest([]) == Basis.digest([%{key: "a"}])
    end

    test "a missing field RAISES — absent must never hash like present" do
      err = assert_raise ArgumentError, fn -> Basis.digest([%{key: "a"}], fields: [:key, :status]) end

      assert Exception.message(err) =~ ":status"
      assert Exception.message(err) =~ "[:key]"
    end

    test "an unknown version returns :unknown_version rather than raising" do
      assert Basis.digest([%{key: "a"}], version: 99) == :unknown_version
    end
  end

  describe "matches?/3" do
    test "true while the rows are unchanged" do
      rows = [%{key: "a", status: "present"}]
      stored = Basis.digest(rows, fields: [:key, :status])

      assert Basis.matches?(stored, rows, fields: [:key, :status])
    end

    test "false once they move — this is what lapses a signature" do
      stored = Basis.digest([%{key: "a", status: "present"}], fields: [:key, :status])

      refute Basis.matches?(stored, [%{key: "a", status: "failing"}], fields: [:key, :status])
    end

    test "a nil digest never matches" do
      refute Basis.matches?(nil, [%{key: "a"}], fields: [:key])
    end

    test "compares under the version the STORED digest carries, not the current one" do
      # a digest from a future build degrades to "stale", never to a crash —
      # so a deploy that introduces v2 does not blow up reading v99 records
      refute Basis.matches?("99:deadbeef", [%{key: "a"}], fields: [:key])
    end

    test "an unparseable digest never matches" do
      refute Basis.matches?("not-a-digest", [%{key: "a"}], fields: [:key])
    end
  end

  describe "version_of/1" do
    test "reads the version a digest was produced under" do
      assert Basis.version_of(Basis.digest([%{key: "a"}])) == 1
      assert Basis.version_of("2:abc") == 2
    end

    test "an unparseable digest is version 0, which no scheme claims" do
      assert Basis.version_of("garbage") == 0
      assert Basis.version_of("") == 0
    end
  end

  describe "how an attestable row emerges" do
    # The row and its basis come from the SAME `into:`. That matters: a basis
    # computed anywhere else could describe a different moment than the row it
    # sits beside, and the whole mechanism rests on them agreeing.
    defmodule Domain do
      use Ash.Domain, validate_config_inclusion?: false

      resources do
        allow_unregistered?(true)
      end
    end

    defmodule Machines do
      use Ash.Resource,
        domain: Domain,
        data_layer: Ash.DataLayer.Ets,
        extensions: [ReactiveDag.Node]

      ets do
      end

      attributes do
        attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
        attribute :owner, :string, public?: true
        attribute :serial, :string, public?: true
      end

      actions do
        defaults [:read, :destroy]

        create :create do
          accept([:key, :owner, :serial])
        end

        update :revise do
          accept([:serial])
        end
      end

      reactive do
        id(:machines)
        leaf?(true)
      end
    end

    # "these are ALL my machines" — a per-owner claim that needs signing
    defmodule Holdings do
      use Ash.Resource,
        domain: Domain,
        data_layer: Ash.DataLayer.Ets,
        extensions: [ReactiveDag.Node]

      ets do
      end

      attributes do
        attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
        attribute :owner, :string, public?: true
        attribute :count, :integer, public?: true
        # emerges WITH the row, from the same rows the row summarises
        attribute :basis, :string, public?: true
        # a host would carry these too; a signature writes them
        attribute :signed_by, :string, public?: true
        attribute :signed_basis, :string, public?: true
      end

      actions do
        defaults [:read, :destroy]

        create :upsert do
          upsert?(true)
          accept([:key, :owner, :count, :basis])
        end

        update :sign do
          accept([:signed_by, :signed_basis])
        end
      end

      reactive do
        id(:holdings)
        recompute_by :owner, to: :machines, from: :owner

        reduce into: fn {owner}, machines ->
                 %{
                   owner: owner,
                   count: length(machines),
                   basis: ReactiveDag.Basis.digest(machines, fields: [:key, :serial])
                 }
               end
      end
    end

    setup do
      Machines |> Ash.read!() |> Enum.each(&Ash.destroy!/1)
      Holdings |> Ash.read!() |> Enum.each(&Ash.destroy!/1)

      for {k, o, ser} <- [{"m1", "ada", "AAA"}, {"m2", "ada", "BBB"}, {"m3", "bob", "CCC"}] do
        Machines
        |> Ash.Changeset.for_create(:create, %{key: k, owner: o, serial: ser})
        |> Ash.create!()
      end

      :ok
    end

    defp recompute do
      plan = ReactiveDag.Node.graph([Machines, Holdings])
      ReactiveDag.Node.Recompute.recompute(plan.cells["holdings"], ["*"])
    end

    test "the row and its basis emerge from one `into:`" do
      {:ok, _} = recompute()

      ada = Holdings |> Ash.get!("ada")

      assert ada.count == 2
      # the digest of exactly the rows the count summarises
      assert ada.basis ==
               ReactiveDag.Basis.digest(
                 [%{key: "m1", serial: "AAA"}, %{key: "m2", serial: "BBB"}],
                 fields: [:key, :serial]
               )
    end

    test "a change to the underlying rows MOVES the basis — so a signature lapses" do
      {:ok, _} = recompute()
      signed = Holdings |> Ash.get!("ada")

      # ada signs: "these two machines are all of them"
      signed
      |> Ash.Changeset.for_update(:sign, %{signed_by: "ada@example.com", signed_basis: signed.basis})
      |> Ash.update!()

      assert (Holdings |> Ash.get!("ada")).signed_basis == signed.basis

      # a machine's serial is corrected — ada never touched the signature
      Machines
      |> Ash.get!("m1")
      |> Ash.Changeset.for_update(:revise, %{serial: "ZZZ"})
      |> Ash.update!()

      {:ok, changed} = recompute()
      assert changed == ["ada"]

      after_change = Holdings |> Ash.get!("ada")

      # the signature is now stale, and nothing had to revoke it
      refute after_change.basis == after_change.signed_basis
    end

    test "a change to ANOTHER owner's rows leaves this signature standing" do
      {:ok, _} = recompute()
      signed = Holdings |> Ash.get!("ada")

      signed
      |> Ash.Changeset.for_update(:sign, %{signed_by: "ada@example.com", signed_basis: signed.basis})
      |> Ash.update!()

      Machines
      |> Ash.get!("m3")
      |> Ash.Changeset.for_update(:revise, %{serial: "QQQ"})
      |> Ash.update!()

      {:ok, changed} = recompute()
      assert changed == ["bob"]

      after_change = Holdings |> Ash.get!("ada")
      assert after_change.basis == after_change.signed_basis
    end
  end
end

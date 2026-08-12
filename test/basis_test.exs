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
  use ExUnit.Case, async: true

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

  test "the attestation Basis is this one with its fields fixed" do
    rows = [%{key: "a", status: "present"}]

    assert ReactiveDag.Attestation.Basis.digest(rows) ==
             Basis.digest(rows, fields: [:key, :status])
  end
end

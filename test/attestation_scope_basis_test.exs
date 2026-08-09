defmodule ReactiveDag.AttestationScopeBasisTest do
  @moduledoc """
  The two identity mechanisms of an attestation record — WHAT was signed (scope)
  and WHAT IT LOOKED LIKE (basis). Both are load-bearing text: scopes group
  stances and re-select rows at evaluation; bases decide whether a signature
  still applies. Pure — the DB-backed store is proven by the host suites, as
  with Frontier/Tuple.
  """
  use ExUnit.Case, async: true

  alias ReactiveDag.Attestation.{Basis, Scope}

  describe "scope serialization" do
    test "round-trips every form" do
      for scope <- [
            {:key, "QRW0TX9H2D"},
            {:filter, {:prefix, "acme|%"}},
            {:filter, {:exact_or_prefix, "acme", "acme|%"}},
            {:filter, {:segment, 2, "|", "przemek@u2i.com"}}
          ] do
        assert scope |> Scope.serialize() |> Scope.parse() == scope
      end
    end

    test "keys containing the separators hosts actually use survive" do
      # host key grammar uses | and : freely — the serialization must not split on them.
      scope = {:key, "app|risk:r1"}
      assert scope |> Scope.serialize() |> Scope.parse() == scope
    end

    test "canonical: same scope, same text (it is the stance-grouping identity)" do
      assert Scope.serialize({:key, "a"}) == Scope.serialize({:key, "a"})
      refute Scope.serialize({:key, "a"}) == Scope.serialize({:key, "b"})
    end

    test "an unknown version refuses to parse" do
      assert_raise ArgumentError, ~r/version/, fn -> Scope.parse("9\x1Fkey\x1Fx") end
    end
  end

  describe "scope selection (the in-memory mirror of the SQL key_scope)" do
    setup do
      rows =
        for k <- ["acme|a", "acme|b", "beta|a", "acme"], do: %{key: k, status: "present"}

      {:ok, rows: rows}
    end

    test "{:key, k} selects exactly that row", %{rows: rows} do
      assert Scope.select({:key, "beta|a"}, rows) |> Enum.map(& &1.key) == ["beta|a"]
    end

    test "{:prefix, p} matches LIKE semantics", %{rows: rows} do
      assert Scope.select({:filter, {:prefix, "acme|%"}}, rows) |> Enum.map(& &1.key) ==
               ["acme|a", "acme|b"]
    end

    test "{:exact_or_prefix, k, p} takes the bare id OR its children", %{rows: rows} do
      assert Scope.select({:filter, {:exact_or_prefix, "acme", "acme|%"}}, rows)
             |> Enum.map(& &1.key) == ["acme|a", "acme|b", "acme"]
    end

    test "{:segment, i, sep, v} matches split_part semantics incl. out-of-range", %{rows: rows} do
      assert Scope.select({:filter, {:segment, 2, "|", "a"}}, rows) |> Enum.map(& &1.key) ==
               ["acme|a", "beta|a"]

      # "acme" has no 2nd segment — split_part yields "", which must not match.
      assert Scope.select({:filter, {:segment, 2, "|", ""}}, rows) |> Enum.map(& &1.key) ==
               ["acme"]
    end
  end

  describe "basis digest (v1)" do
    test "deterministic and row-order-insensitive" do
      a = [%{key: "x", status: "present"}, %{key: "y", status: "present"}]
      b = Enum.reverse(a)
      assert Basis.digest(a) == Basis.digest(b)
    end

    test "moves when a row's STATUS moves — the world changed, signatures lapse" do
      before = [%{key: "x", status: "present"}]
      after_ = [%{key: "x", status: "failing"}]
      refute Basis.digest(before) == Basis.digest(after_)
    end

    test "moves when the selected SET moves — a member appears or vanishes" do
      before = [%{key: "x", status: "present"}]
      after_ = before ++ [%{key: "z", status: "present"}]
      refute Basis.digest(before) == Basis.digest(after_)
    end

    test "an empty selection digests (an empty set is signable state, not an error)" do
      assert is_binary(Basis.digest([]))
    end

    test "a record from an unknown (future) scheme degrades to re-ask, not to a crash" do
      assert Basis.digest([%{key: "x", status: "present"}], 99) == :unknown_version
      refute Basis.matches?("whatever", [%{key: "x", status: "present"}], 99)
    end

    test "matches?/3 is digest equality under the record's own version" do
      rows = [%{key: "x", status: "present"}]
      assert Basis.matches?(Basis.digest(rows, 1), rows, 1)
      refute Basis.matches?(Basis.digest(rows, 1), [%{key: "x", status: "gone"}], 1)
    end
  end
end

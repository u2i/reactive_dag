defmodule ReactiveDag.Node.PayloadDiffTest do
  @moduledoc """
  The payload write hands its diff forward, in the same vocabulary a version row
  uses.

  A propagating consumer needs the unit a changed row belonged to BEFORE the
  write as well as after: a row that moved between units affects both, and a
  deleted row affects the one it left. Today the library answers that by reading
  the row back, which cannot see either case and degrades to `"*"` — repricing a
  whole cell.

  The payload write is the one place both sides are in hand. So it records them,
  and `ReactiveDag.Node.VersionDiff` reads the result — the same module that
  reads an `ash_paper_trail` version, because it is the same shape. That
  equivalence is what these tests are really about: a diff must not read
  differently depending on where it was born.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.Node.{Payload, VersionDiff}

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      allow_unregistered?(true)
    end
  end

  defmodule Line do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Ets

    ets do
      private?(true)
    end

    attributes do
      attribute :line_key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :fund, :string, public?: true
      attribute :value, :integer, public?: true
    end

    actions do
      defaults [:read, :destroy]
      create :upsert, upsert?: true, accept: [:line_key, :fund, :value]
    end
  end

  defp put(key, attrs), do: Payload.upsert(Line, :line_key, key, attrs, :upsert)

  describe "collecting_diffs/1" do
    test "a create records every attribute as `to` — the version shape" do
      {verdict, diffs} =
        Payload.collecting_diffs(fn -> put("l1", %{line_key: "l1", fund: "A", value: 10}) end)

      assert verdict == :created
      assert diffs["l1"]["fund"] == %{"to" => "A"}
      assert diffs["l1"]["value"] == %{"to" => 10}
    end

    test "a change records BOTH sides for what moved, `unchanged` for the rest" do
      put("l2", %{line_key: "l2", fund: "A", value: 10})

      {verdict, diffs} =
        Payload.collecting_diffs(fn -> put("l2", %{line_key: "l2", fund: "ES", value: 10}) end)

      assert verdict == :changed
      assert diffs["l2"]["fund"] == %{"from" => "A", "to" => "ES"}
      assert diffs["l2"]["value"] == %{"unchanged" => 10}
    end

    test "an unchanged write records nothing" do
      put("l3", %{line_key: "l3", fund: "A", value: 10})

      {verdict, diffs} =
        Payload.collecting_diffs(fn -> put("l3", %{line_key: "l3", fund: "A", value: 10}) end)

      assert verdict == :unchanged
      assert diffs == %{}, "nothing moved, so there is no unit to reprice"
    end

    test "several writes collect under their own keys" do
      {_, diffs} =
        Payload.collecting_diffs(fn ->
          put("m1", %{line_key: "m1", fund: "A", value: 1})
          put("m2", %{line_key: "m2", fund: "ES", value: 2})
        end)

      assert Map.keys(diffs) |> Enum.sort() == ["m1", "m2"]
    end
  end

  describe "the diff is the SAME shape a version row carries" do
    test "so VersionDiff derives the moved row's two units from it" do
      # THE equivalence. `VersionDiff` was written against
      # `AshPaperTrail.ChangeBuilders.FullDiff`; feeding it a payload diff must
      # give the same answer, or a host would get different propagation depending
      # on which writer produced the change.
      put("n1", %{line_key: "n1", fund: "A", value: 10})

      {_, diffs} =
        Payload.collecting_diffs(fn -> put("n1", %{line_key: "n1", fund: "ES", value: 10}) end)

      assert VersionDiff.units(diffs["n1"], :fund) == ["A", "ES"],
             "the group it left AND the one it landed in — which a live read cannot see"
    end

    test "and a create's single unit" do
      {_, diffs} =
        Payload.collecting_diffs(fn -> put("n2", %{line_key: "n2", fund: "GF", value: 1}) end)

      assert VersionDiff.units(diffs["n2"], :fund) == ["GF"]
      assert VersionDiff.before(diffs["n2"]) == nil, "nothing existed before"
    end

    test "keys are STRINGS on both sides" do
      # A version's `changes` is a jsonb column, so it round-trips to string
      # keys. A payload diff must match, or `VersionDiff` would need to know
      # which source it was reading.
      {_, diffs} =
        Payload.collecting_diffs(fn -> put("n3", %{line_key: "n3", fund: "A", value: 1}) end)

      assert Map.keys(diffs["n3"]) |> Enum.all?(&is_binary/1)
    end
  end

  describe "outside a collecting block" do
    test "a write costs nothing and behaves exactly as before" do
      # Every existing caller — a host op writing its own rows, a test driving
      # `upsert/6` — is unchanged. The channel is opt-in at the reader's end.
      assert put("o1", %{line_key: "o1", fund: "A", value: 1}) == :created
      assert Process.get(Payload.Diffs) == nil
    end

    test "a nested block does not leak into the outer one" do
      {_, outer} =
        Payload.collecting_diffs(fn ->
          put("p1", %{line_key: "p1", fund: "A", value: 1})

          {_, inner} =
            Payload.collecting_diffs(fn -> put("p2", %{line_key: "p2", fund: "B", value: 2}) end)

          assert Map.keys(inner) == ["p2"]
        end)

      assert Map.keys(outer) == ["p1"],
             "the inner block's writes belong to the inner block"
    end
  end
end

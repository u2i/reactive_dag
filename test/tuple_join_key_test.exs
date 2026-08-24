defmodule ReactiveDag.TupleJoinKeyTest do
  @moduledoc """
  A join pairs on SEVERAL columns, with no joined column stored beside them.

  A join key was a single attribute, so pairing on a pair of columns meant
  materializing `code <> "|" <> year` as its own column — a host's `match_key`. But
  a join indexes both sides into maps and matches keys in the BEAM
  (`Recompute.recompute/3`), and a tuple is a map key exactly as well as a string.

  So the column was never needed; the DSL just could not say "these two".
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.Node.Recompute.Declarative

  describe "side_fn/1 with a list key" do
    test "builds the tuple of the named columns" do
      f = Declarative.side_fn(key: [:code, :year])

      assert f.(%{code: "A1", year: "FY25"}) == {"A1", "FY25"}
    end

    test "a nil in ANY position means the row is not on this side" do
      f = Declarative.side_fn(key: [:code, :year])

      assert f.(%{code: "A1", year: nil}) == nil
      assert f.(%{code: nil, year: "FY25"}) == nil
    end

    test "`where` still gates the side" do
      f = Declarative.side_fn(key: [:code, :year], where: [side: "budget"])

      assert f.(%{code: "A1", year: "FY25", side: "budget"}) == {"A1", "FY25"}
      assert f.(%{code: "A1", year: "FY25", side: "actual"}) == nil
    end

    test "a single attribute is unchanged — this is additive" do
      f = Declarative.side_fn(key: :code)

      assert f.(%{code: "A1"}) == "A1"
    end
  end

  test "the two sides pair on the tuple, and DISTINGUISH what a join would conflate" do
    left = Declarative.side_fn(key: [:code, :year], where: [side: "budget"])
    right = Declarative.side_fn(key: [:code, :year], where: [side: "actual"])

    budget = [
      %{side: "budget", code: "A1", year: "FY24", amount: 10},
      %{side: "budget", code: "A1", year: "FY25", amount: 20}
    ]

    actual = [
      %{side: "actual", code: "A1", year: "FY25", amount: 19}
    ]

    index = fn items, f -> Map.new(items, &{f.(&1), &1}) end
    l = index.(budget, left)
    r = index.(actual, right)

    # FY25 pairs; FY24 has no actual. This is the case the joined column existed
    # for — matching on the code alone would pair FY24's budget with FY25's actuals.
    assert Map.keys(l) |> Enum.sort() == [{"A1", "FY24"}, {"A1", "FY25"}]
    assert Map.get(r, {"A1", "FY25"}).amount == 19
    assert Map.get(r, {"A1", "FY24"}) == nil
  end
end

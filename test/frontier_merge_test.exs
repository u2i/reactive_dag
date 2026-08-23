defmodule ReactiveDag.FrontierMergeTest do
  @moduledoc """
  Two changes to one row before a drain merge into one claim naming both ends.

  A row that moves meals → travel → lodging must reprice `meals` (where the last
  settled state had it) and `lodging` (where it is now) — and NOT `travel`, an
  intermediate no settled state ever saw.

  `ON CONFLICT DO NOTHING` keeps `meals → travel` and strands lodging.
  Overwriting keeps `travel → lodging` and strands meals. Both lose a unit that
  needs repricing, which is why the frontier merges.

  `merge_diffs/2` is the rule; the `ON CONFLICT` clause is the same rule in SQL,
  and a fake repo modelling the frontier calls this so there is one definition to
  agree with.
  """
  use ExUnit.Case, async: true

  import ReactiveDag.Frontier, only: [merge_diffs: 2]

  test "the earliest `from` and the latest `to`" do
    a = %{"category" => %{"from" => "meals", "to" => "travel"}}
    b = %{"category" => %{"from" => "travel", "to" => "lodging"}}

    assert merge_diffs(a, b) == %{"category" => %{"from" => "meals", "to" => "lodging"}}
  end

  test "an `unchanged` stored side becomes the `from` when the next write moves it" do
    # The first write did not touch `category`; the second did. The unit it was
    # in is still what needs repricing, so `unchanged` is a `from` that has not
    # moved yet.
    a = %{"category" => %{"unchanged" => "meals"}}
    b = %{"category" => %{"from" => "meals", "to" => "travel"}}

    assert merge_diffs(a, b) == %{"category" => %{"from" => "meals", "to" => "travel"}}
  end

  test "a create then a move keeps `to` only — there was nothing before" do
    a = %{"category" => %{"to" => "meals"}}
    b = %{"category" => %{"from" => "meals", "to" => "travel"}}

    assert merge_diffs(a, b) == %{"category" => %{"to" => "travel"}},
           "a row created and then moved before any drain was never in `meals` " <>
             "as far as settled state knows"
  end

  test "an attribute in only one diff survives" do
    a = %{"category" => %{"unchanged" => "meals"}}
    b = %{"amount" => %{"from" => 1, "to" => 2}}

    assert merge_diffs(a, b) == %{
             "category" => %{"unchanged" => "meals"},
             "amount" => %{"from" => 1, "to" => 2}
           }
  end

  test "a nil side is the other one — a mark with no diff erases nothing" do
    d = %{"category" => %{"to" => "meals"}}

    assert merge_diffs(nil, d) == d
    assert merge_diffs(d, nil) == d
    assert merge_diffs(nil, nil) == nil
  end

  test "three writes still name the two ends" do
    steps = [
      %{"c" => %{"from" => "a", "to" => "b"}},
      %{"c" => %{"from" => "b", "to" => "c"}},
      %{"c" => %{"from" => "c", "to" => "d"}}
    ]

    assert Enum.reduce(steps, nil, &merge_diffs(&2, &1)) ==
             %{"c" => %{"from" => "a", "to" => "d"}}
  end
end

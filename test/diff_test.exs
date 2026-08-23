defmodule ReactiveDag.Node.DiffTest do
  @moduledoc """
  Units derived from an `ash_paper_trail` `:full_diff`, both sides.

  The point of the module under test is the two cases a live lookup cannot
  answer — a **deleted** row (nothing to read) and a **moved** row (the live row
  names only where it landed). Both currently degrade the propagation to `"*"`,
  which reprices a whole cell. Every test here is ultimately about one of those.

  The diff shapes are taken from `AshPaperTrail.ChangeBuilders.FullDiff.Helpers`,
  read at its source rather than from the docs — which show only the update case:

      %{to: value}                 # create: no prior value existed
      %{from: old, to: new}        # the attribute moved
      %{unchanged: value}          # present, untouched
      (absent)                     # not accepted by the action
  """
  use ExUnit.Case, async: true

  alias ReactiveDag.Node.Diff

  describe "a moved row claims BOTH units" do
    test "the group it left and the one it landed in" do
      # THE case. `fund` moved A → ES, so the A rollup loses this row's value and
      # the ES rollup gains it. A live read names only ES, which is why today's
      # propagation has to fall back to `"*"`.
      changes = %{
        "fund" => %{"from" => "A", "to" => "ES"},
        "value" => %{"from" => 100, "to" => 100}
      }

      assert Diff.units(changes, :fund) == ["A", "ES"]
    end

    test "a composite grain moves as a whole" do
      changes = %{
        "fund" => %{"from" => "A", "to" => "ES"},
        "fiscal_year" => %{"unchanged" => "FY23/24"}
      }

      assert Diff.units(changes, [:fund, :fiscal_year]) ==
               ["A|FY23/24", "ES|FY23/24"]
    end

    test "a move in a NON-grain attribute is still one unit" do
      # `value` changed, `fund` did not. One unit — but it does need repricing,
      # which is why the entry exists at all.
      changes = %{
        "fund" => %{"unchanged" => "A"},
        "value" => %{"from" => 100, "to" => 250}
      }

      assert Diff.units(changes, :fund) == ["A"]
    end
  end

  describe "a deleted row names the unit it left" do
    test "destroy: every attribute has a prior value and none has a `to`" do
      changes = %{
        "fund" => %{"unchanged" => "A"},
        "fiscal_year" => %{"unchanged" => "FY23/24"}
      }

      # Not `"*"`. The whole point: a vanished row knows which group it left.
      assert Diff.units(changes, [:fund, :fiscal_year]) == ["A|FY23/24"]
      assert Diff.after_(changes) == nil
      refute Diff.before(changes) == nil
    end
  end

  describe "a created row names the unit it entered" do
    test "create: no attribute has a prior value" do
      changes = %{"fund" => %{"to" => "ES"}, "value" => %{"to" => 5}}

      assert Diff.units(changes, :fund) == ["ES"]
      assert Diff.before(changes) == nil
      assert Diff.after_(changes) == %{fund: "ES", value: 5}
    end
  end

  describe "before/1 and after_/1" do
    test "project the two sides as plain maps with ATOM keys" do
      # A grain declaration names attributes as atoms; a version names them as
      # strings. The translation lives here so no grain function has to know.
      changes = %{
        "fund" => %{"from" => "A", "to" => "ES"},
        "fiscal_year" => %{"unchanged" => "FY23/24"}
      }

      assert Diff.before(changes) == %{fund: "A", fiscal_year: "FY23/24"}
      assert Diff.after_(changes) == %{fund: "ES", fiscal_year: "FY23/24"}
    end

    test "an attribute absent from the diff contributes nothing" do
      # The action did not accept it. Absent is not the same as nil: a grain
      # function asking for it gets nil either way, but inventing the key would
      # claim the action touched something it did not.
      changes = %{"fund" => %{"unchanged" => "A"}}

      refute Map.has_key?(Diff.before(changes), :value)
    end

    test "an attribute the host has never named as an atom is skipped" do
      # `String.to_existing_atom` on a name from a jsonb column. A grain field is
      # validated against the resource at assembly, so it always exists; anything
      # else is not a grain field, and crashing would fail the whole propagation
      # over a column nobody groups by.
      changes = %{
        "fund" => %{"unchanged" => "A"},
        "a_column_no_atom_exists_for_xyzzy" => %{"unchanged" => 1}
      }

      assert Diff.before(changes) == %{fund: "A"}
    end
  end

  describe "edge shapes" do
    test "an empty diff is neither a create nor a destroy" do
      # An update that accepted nothing. Reading it as a destroy would claim the
      # row is gone and retire live rows — the expensive direction to be wrong in.
      assert Diff.before(%{}) == %{}
      assert Diff.after_(%{}) == %{}
      assert Diff.units(%{}, :fund) == []
    end

    test "a grain value of nil claims nothing rather than a key of `\"\"`" do
      # `to_string(nil)` is `""`, which names no unit. Claiming it would enqueue
      # work against a key that exists nowhere.
      assert Diff.units(%{"fund" => %{"unchanged" => nil}}, :fund) == []
    end

    test "an unchanged row claims its unit ONCE, not twice" do
      # Both sides project to the same map, so the union collapses. Without the
      # dedup a no-op write would claim the same unit twice and the drain would
      # recompute it twice.
      assert Diff.units(%{"fund" => %{"unchanged" => "A"}}, :fund) == ["A"]
    end
  end

  describe "the grain vocabulary is the node's own" do
    test "a function grain works, so a host escape hatch carries over" do
      changes = %{"fund" => %{"from" => "A", "to" => "ES"}}

      grain = fn row -> "fund:" <> to_string(row[:fund]) end

      assert Diff.units(changes, grain) == ["fund:A", "fund:ES"]
    end

    test "a key_fn can be supplied, for a node declaring `key_prefix`" do
      changes = %{"fund" => %{"from" => "A", "to" => "ES"}}
      key_fn = fn group -> "va|" <> to_string(group) end

      assert Diff.units(changes, :fund, key_fn) == ["va|A", "va|ES"]
    end
  end
end

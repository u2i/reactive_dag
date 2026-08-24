defmodule ReactiveDag.CalcGrainTest do
  @moduledoc """
  A fold grouping by a CALCULATION derives its units from the change.

  A calculation is an Ash `expr` the datastore evaluates, so a grain containing
  one used to fall back to reading live rows — which cannot answer for a deleted
  row and names only where a moved row landed. But a calculation over the row's
  own attributes is a function of values the change already carries, and Ash can
  evaluate it in the BEAM.

  The case that proves it is a row whose CALCULATED unit moves while the column
  behind it changes: `side` is `if kind == "osc_actual"`, so flipping `kind` moves
  the row between `"actual"` and `"budget"` without either value being stored.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.Node.{CalcGrain, Diff}

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false
    resources(do: allow_unregistered?(true))
  end

  defmodule Line do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Ets
    ets(do: private?(true))

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :kind, :string, public?: true
      attribute :code, :string, public?: true
      attribute :year, :string, public?: true
    end

    calculations do
      # The real shape from the host: one leg of a comparison, derived from `kind`
      # rather than stored, because storing it would be a second place for the
      # same fact to be wrong.
      calculate :side, :string, expr(if kind == "osc_actual", do: "actual", else: "budget")
    end

    actions do
      defaults [:read, :destroy]
      create :upsert, upsert?: true, accept: [:key, :kind, :code, :year]
    end
  end

  # The assembled plan for `group_by [side, code, year]` — one calc, two attrs.
  @plan [{:calc, :side}, {:attr, :code, true}, {:attr, :year, true}]

  describe "CalcGrain.values/3" do
    test "evaluates a calculation on a plain projected map" do
      row = %{kind: "osc_actual", code: "A1", year: "FY25"}

      assert CalcGrain.values(@plan, row, Line) == ["actual", "A1", "FY25"]
    end

    test "the OTHER branch of the same calculation" do
      row = %{kind: "budget_adopted", code: "A1", year: "FY25"}

      assert CalcGrain.values(@plan, row, Line) == ["budget", "A1", "FY25"]
    end

    test "a nil in the grain yields no values — a row missing part of its own unit" do
      assert CalcGrain.values(@plan, %{kind: "osc_actual", code: nil, year: "FY25"}, Line) == nil
    end

    test "no calculations in the plan is the plain path" do
      plan = [{:attr, :code, true}, {:attr, :year, true}]

      assert CalcGrain.values(plan, %{code: "A1", year: "FY25"}, Line) == ["A1", "FY25"]
    end
  end

  describe "Diff.groups/3 over a calc grain" do
    test "a MOVED row yields both units — the calculated one it left and the one it landed in" do
      # `kind` flips, so `side` flips with it. Neither value is stored anywhere.
      changes = %{
        "kind" => %{"from" => "budget_adopted", "to" => "osc_actual"},
        "code" => %{"unchanged" => "A1"},
        "year" => %{"unchanged" => "FY25"}
      }

      groups = Diff.groups(changes, @plan, Line)

      assert Enum.sort(groups) == [{"actual", "A1", "FY25"}, {"budget", "A1", "FY25"}],
             "the unit it left AND the one it landed in, both derived from the change"
    end

    test "an update INSIDE a unit yields that one unit" do
      changes = %{
        "kind" => %{"unchanged" => "osc_actual"},
        "code" => %{"unchanged" => "A1"},
        "year" => %{"unchanged" => "FY25"}
      }

      assert Diff.groups(changes, @plan, Line) == [{"actual", "A1", "FY25"}]
    end

    test "a CREATE yields the unit it was created in" do
      changes = %{
        "kind" => %{"to" => "osc_actual"},
        "code" => %{"to" => "A1"},
        "year" => %{"to" => "FY25"}
      }

      assert Diff.groups(changes, @plan, Line) == [{"actual", "A1", "FY25"}]
    end

    test "a DESTROY yields the unit it left — which a live read could not answer" do
      changes = %{
        "kind" => %{"from" => "osc_actual"},
        "code" => %{"from" => "A1"},
        "year" => %{"from" => "FY25"}
      }

      assert Diff.groups(changes, @plan, Line) == [{"actual", "A1", "FY25"}]
    end

    test "a plain field-list grain still works — the calc path is additive" do
      changes = %{"code" => %{"from" => "A1", "to" => "A2"}}

      assert Enum.sort(Diff.groups(changes, :code)) == ["A1", "A2"]
    end
  end
end

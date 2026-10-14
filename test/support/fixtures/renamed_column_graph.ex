defmodule ReactiveDag.Fixtures.RenamedColumnGraph do
  @moduledoc """
  The same fold, with the input's column named DIFFERENTLY from the unit.

      renamed_expenses.expense_cat ──> renamed_totals.category

  Deliberately not shareable with `ExpenseGraph`, because the mismatch IS the
  subject. `recompute_by :category, to: :expenses, from: :expense_cat` reads
  "recompute by category, from the input's `expense_cat`", and a fixture whose
  two columns happened to agree could not tell a working mapping from one that
  silently matched on name.

  Everything else is the expense fold unchanged, so a test comparing this to
  `ExpenseGraph` is comparing exactly one variable.
  """

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      resource ReactiveDag.Fixtures.RenamedColumnGraph.Expenses
      resource ReactiveDag.Fixtures.RenamedColumnGraph.CategoryTotals
    end
  end

  defmodule Expenses do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.RenamedColumnGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_renamed_expenses"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      # NOT named like the rollup's column — the `recompute_by` pair maps it.
      attribute :expense_cat, :string, public?: true
      attribute :amount, :float, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :create do
        accept [:key, :expense_cat, :amount]
      end

      update :revise do
        accept [:amount]
      end

      update :recategorise do
        accept [:expense_cat]
      end
    end

    reactive do
      id(:expenses)
      op(:source)
      leaf?(true)
      dirties_on([:create, :update, :destroy])
    end
  end

  defmodule CategoryTotals do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.RenamedColumnGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_renamed_totals"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :category, :string, public?: true
      attribute :total, :float, public?: true
      attribute :n, :integer, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert? true
        accept [:key, :category, :total, :n]
        upsert_fields [:category, :total, :n]
      end
    end

    reactive do
      id(:category_totals)
      op(:fold)
      recompute_by(:category, to: :expenses, from: :expense_cat)
      reduce(into: [sum: [amount: :total], count: :n])
    end
  end

  def plan, do: ReactiveDag.Node.graph([Expenses, CategoryTotals])

  def tables, do: ["fx_renamed_expenses", "fx_renamed_totals"]
end

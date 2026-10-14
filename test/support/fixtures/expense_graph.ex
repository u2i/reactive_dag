defmodule ReactiveDag.Fixtures.ExpenseGraph do
  @moduledoc """
  The fold graph: per-row expenses, folded into per-category totals.

      expenses ──(recompute_by :category)──> category_totals

  The most-repeated shape in the suite — `group_rule`, `augmented_by`,
  `retire_vanished` and `recompute_by` each declared their own near-identical
  copy. One declaration here, backed by real tables.

  ## What it is for

  The `:group` rung: a change to one expense invalidates its CATEGORY, not its
  key. That makes it the fixture for anything about unit derivation, fold
  reconciliation, or a row MOVING between units — `recategorise/1` exists
  precisely so a test can move a row and watch both units go stale.

  ## Actions are a union, not a minimum

  Each borrowing test needed a different subset (`revise` here, `recategorise`
  there, `dirties_on` in a third). Declaring the union costs nothing — an
  unused action is inert — and avoids the alternative, which is a fixture per
  caller and no sharing at all.

  `category` is deliberately NOT encoded in the key. A `:group` rule resolves
  units by looking rows up, and a key carrying its own category grammar would
  let a test pass by string-splitting — the exact shortcut the rule exists to
  remove.
  """

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      resource ReactiveDag.Fixtures.ExpenseGraph.Expenses
      resource ReactiveDag.Fixtures.ExpenseGraph.CategoryTotals
    end
  end

  defmodule Expenses do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.ExpenseGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_expenses"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :category, :string, public?: true
      attribute :amount, :float, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :create do
        accept [:key, :category, :amount]
      end

      create :upsert do
        upsert? true
        accept [:key, :category, :amount]
        upsert_fields [:category, :amount]
      end

      update :revise do
        accept [:amount]
      end

      # The MOVE: same row, different unit. Both the category it joined and
      # the one it left must be recomputed, and a fold that only claims the
      # new one leaves the old total counting a row that is no longer in it.
      update :recategorise do
        accept [:category]
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
      domain: ReactiveDag.Fixtures.ExpenseGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_category_totals"
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
      recompute_by(:category, to: :expenses, from: :category)
      reduce(into: [sum: [amount: :total], count: :n])
    end
  end

  @doc "The assembled plan for this graph."
  def plan, do: ReactiveDag.Node.graph([Expenses, CategoryTotals])

  @doc "The tables this fixture owns, for truncation between tests."
  def tables, do: ["fx_expenses", "fx_category_totals"]
end

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
      resource ReactiveDag.Fixtures.ExpenseGraph.CategoryHealth
      resource ReactiveDag.Fixtures.ExpenseGraph.Publish
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
      id :expenses
      op :source
      leaf? true
      dirties_on [:create, :update, :destroy]
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
      id :category_totals
      op :fold
      recompute_by :category, to: :expenses, from: :category
      reduce into: [sum: [amount: :total], count: :n]
    end
  end

  defmodule CategoryHealth do
    @moduledoc """
    A same-grain consumer of the fold.

    Present so the graph is three deep rather than two: it proves a claim
    PROPAGATES at the same grain — touch one expense, and only that category's
    health recomputes. A two-node graph cannot show that, because there is
    nothing downstream of the fold to follow.
    """
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.ExpenseGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_category_health"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :status, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert? true
        accept [:key, :status]
        upsert_fields [:status]
      end
    end

    reactive do
      id :category_health
      op :check

      reduce(
        over: :category_totals,
        group_by: :key,
        into: fn _cat, [row | _] ->
          %{status: if(row.total < 1000.0, do: "present", else: "failing")}
        end
      )
    end
  end

  defmodule NoopOp do
    @moduledoc """
    A compute that changes nothing.

    `Publish` is about STRUCTURE — a `compose` leg that owns no table — so the
    computation itself must not be a variable.
    """
    @behaviour ReactiveDag.Op
    @impl true
    def recompute(_cell, _keys), do: {:ok, []}
  end

  # A node that owns a table, over a `compose` LEG that does not. The leg is the
  # tableless cell that still exists: its work is real and its keys flow, but it
  # is an intermediate — `meta.resource` is nil, so there is no table to read and
  # "nothing lives here" is the honest answer rather than a failure to look.
  #
  # The `:elsewhere` state used to be reached through the write-elsewhere shape
  # (`Simple`, no attributes, a compute module). A derived NODE may no longer be
  # tableless, so it is reached through the one shape that legitimately has no
  # resource — which is what `Insights.rows_kind/1` has always keyed on.
  defmodule Publish do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.ExpenseGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_publish"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute(:key, :string, primary_key?: true, allow_nil?: false, public?: true)
    end

    actions do
      defaults([:read, :destroy])

      create :upsert do
        upsert?(true)
        upsert_identity(:by_key)
        accept([:key])
      end
    end

    identities do
      identity(:by_key, [:key])
    end

    reactive do
      id :publish
      compute ReactiveDag.Fixtures.ExpenseGraph.NoopOp

      compose :fold do
        as :published_rows
        compute ReactiveDag.Fixtures.ExpenseGraph.NoopOp
        ref :expenses
      end
    end
  end

  @doc """
  The assembled plan.

  Two nodes by default; pass `:with_health` for the three-deep graph when the
  test is about propagation rather than the fold itself.
  """
  def plan, do: ReactiveDag.Node.graph([Expenses, CategoryTotals])
  def plan(:with_health), do: ReactiveDag.Node.graph([Expenses, CategoryTotals, CategoryHealth])

  @doc "expenses -> publish, whose `compose` leg owns no table."
  def plan(:with_publish), do: ReactiveDag.Node.graph([Expenses, Publish])

  @doc "The tables this fixture owns, for truncation between tests."
  def tables, do: ["fx_expenses", "fx_category_totals", "fx_category_health", "fx_publish"]
end

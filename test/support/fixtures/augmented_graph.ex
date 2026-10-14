defmodule ReactiveDag.Fixtures.AugmentedGraph do
  @moduledoc """
  `augmented_by` — a node whose rows the LIBRARY writes and a HUMAN also edits.

  Five resources, each a different facet, which is why this is its own fixture
  rather than an extension of `ExpenseGraph`: `augmented_by` changes what a
  node declares, so adding it to the shared graph would silently change the
  three files already using that one.

    * `CategoryTotals` — the case the feature exists for: a computed fold the
      library upserts, that a human also corrects, approves and annotates.
    * `Expenses`       — its ordinary leaf input.
    * `Rollups`        — a COMPOSITE-PK node, so the cell key is the identity
      serialisation rather than a column.
    * `Verdicts`       — a leaf whose only human action is an override.
    * `Readings`       — a leaf that is BOTH `dirties_on`-covered and
      `augmented_by`-named on the same action. It must mark exactly ONCE, and
      a double-mark is the bug this one exists to catch.
  """

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      resource ReactiveDag.Fixtures.AugmentedGraph.CategoryTotals
      resource ReactiveDag.Fixtures.AugmentedGraph.Expenses
      resource ReactiveDag.Fixtures.AugmentedGraph.Rollups
      resource ReactiveDag.Fixtures.AugmentedGraph.Verdicts
      resource ReactiveDag.Fixtures.AugmentedGraph.Readings
    end
  end

  # THE CASE THE FEATURE EXISTS FOR: a computed node (the library writes its
  # rows through `:upsert`) that a human also corrects and approves.
  defmodule CategoryTotals do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.AugmentedGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_aug_category_totals"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :category, :string, public?: true
      attribute :total, :float, public?: true
      attribute :n, :integer, public?: true
      attribute :note, :string, public?: true
    end

    # NO `identity … pre_check_with:` here, deliberately. The single-attribute
    # primary key is what an upsert conflicts on anyway, and a pre-checked
    # identity adds a `before_action` hook that makes every UPDATE on the
    # resource non-atomic — which has nothing to do with this feature but would
    # stop the human's `:correct` from running at all.
    actions do
      defaults [:read, :destroy]

      # the PAYLOAD action — the library's own write. Never marks.
      create :upsert do
        upsert?(true)
        accept([:key, :category, :total, :n, :note])
      end

      # the HUMAN's actions. `:add` is a CREATE on purpose: it makes
      # `augmented_by` cover the same action TYPE as the payload upsert, so a
      # wiring that went by type instead of by name would sweep `:upsert` in
      # too — which is exactly the infinite loop, and is what the payload test
      # below would then catch.
      create :add do
        accept([:key, :category, :total, :n])
      end

      update :correct do
        accept([:total])
      end

      update :approve do
        accept([:note])
      end

      # a write action deliberately NOT augmented: an internal annotation that
      # should not re-run the cascade
      update :annotate do
        accept([:note])
      end
    end

    reactive do
      id :category_totals
      op :fold

      recompute_by :category, to: :expenses, from: :category
      reduce into: [sum: [amount: :total], count: :n]

      # THE FEATURE: these three actions mark; `:upsert` and `:annotate` do not
      augmented_by [:add, :correct, :approve]
    end
  end

  defmodule Expenses do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.AugmentedGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_aug_expenses"
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
        accept([:key, :category, :amount])
      end
    end

    reactive do
      id :expenses
      op :source
      leaf? true
      dirties_on [:create, :update, :destroy]
    end
  end

  # a COMPOSITE-PK computed node: the cell key is the identity serialization,
  # exactly as it is for `dirties_on` (mirrors that test's Rollups).
  defmodule Rollups do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.AugmentedGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_aug_rollups"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :fund, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :fy, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :total, :float, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        accept([:fund, :fy, :total])
      end

      update :correct do
        accept([:total])
      end
    end

    reactive do
      id :rollups
      leaf? true
      augmented_by [:correct]
    end
  end

  # An augmented write enqueues its cascade in the same
  # transaction as the mark.
  defmodule Verdicts do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.AugmentedGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_aug_verdicts"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :status, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        accept([:key, :status])
      end

      update :override do
        accept([:status])
      end
    end

    reactive do
      id :verdicts
      leaf? true
      augmented_by [:override]
    end
  end

  # a leaf that is ALSO human-augmented: `dirties_on` covers every write,
  # `augmented_by` names one of those same actions. It must mark ONCE.
  defmodule Readings do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.AugmentedGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_aug_readings"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :value, :float, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :create do
        accept([:key, :value])
      end

      update :correct do
        accept([:value])
      end
    end

    reactive do
      id :readings
      leaf? true
      # :correct is an `:update`, so BOTH cover it
      dirties_on [:create, :update]
      augmented_by [:correct]
    end
  end

  @doc "The fold graph: expenses -> category_totals."
  def plan, do: ReactiveDag.Node.graph([Expenses, CategoryTotals])

  @doc "The tables this fixture owns."
  def tables, do: ["fx_aug_category_totals", "fx_aug_expenses", "fx_aug_rollups", "fx_aug_verdicts", "fx_aug_readings"]
end

defmodule ReactiveDag.Fixtures.TwoNodeJoinGraph do
  @moduledoc """
  A JOIN of two nodes — budgets against actuals, emitting variance.
  """

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      resource ReactiveDag.Fixtures.TwoNodeJoinGraph.Budgets
      resource ReactiveDag.Fixtures.TwoNodeJoinGraph.Actuals
      resource ReactiveDag.Fixtures.TwoNodeJoinGraph.Variance
    end
  end

  defmodule Budgets do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.TwoNodeJoinGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_tnj_budgets"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      # deliberately NOT named like the right side's column
      attribute :account_code, :string, public?: true
      attribute :amount, :float, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :create do
        accept([:key, :account_code, :amount])
      end

      update :revise do
        accept([:amount])
      end
    end

    reactive do
      id :budgets
      op :source
      leaf? true
    end
  end

  defmodule Actuals do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.TwoNodeJoinGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_tnj_actuals"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :acct, :string, public?: true
      attribute :amount, :float, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :create do
        accept([:key, :acct, :amount])
      end

      update :revise do
        accept([:amount])
      end
    end

    reactive do
      id :actuals
      op :source
      leaf? true
    end
  end

  defmodule Variance do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.TwoNodeJoinGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_tnj_variance"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :budget, :float, public?: true
      attribute :actual, :float, public?: true
    end

    identities do
      identity :by_key, [:key], pre_check_with: ReactiveDag.Fixtures.TwoNodeJoinGraph.Domain
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        upsert_identity(:by_key)
        accept([:key, :budget, :actual])
      end
    end

    reactive do
      id :variance
      op :reconcile

      # NO `depends_on` — `left_over:`/`right_over:` already name both inputs,
      # and the library derives the edges from them (one per side). Restating
      # them would be a second answer to one question, and duplicates the edge.
      join(
        left_over: :budgets,
        right_over: :actuals,
        left: :account_code,
        right: :acct,
        outer: true,
        into: [left: [amount: :budget], right: [amount: :actual]]
      )
    end
  end

  def tables, do: ["fx_tnj_budgets", "fx_tnj_actuals", "fx_tnj_variance"]
end

defmodule ReactiveDag.Fixtures.DeclarativeJoinGraph do
  @moduledoc """
  Declarative `join` — two inputs matched on named columns.

  `Variance` is the joined node; its two sides come from the graph a test
  assembles around it.
  """

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      resource ReactiveDag.Fixtures.DeclarativeJoinGraph.Entries
      resource ReactiveDag.Fixtures.DeclarativeJoinGraph.Variance
      resource ReactiveDag.Fixtures.DeclarativeJoinGraph.OuterVariance
      resource ReactiveDag.Fixtures.DeclarativeJoinGraph.Linked
    end
  end

  # ONE input holding both sides, discriminated by :kind — the fiscal
  # declared-vs-observed shape.
  defmodule Entries do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.DeclarativeJoinGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_dj_entries"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :kind, :string, public?: true
      attribute :acct, :string, public?: true
      attribute :amount, :float, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :create do
        accept([:key, :kind, :acct, :amount])
      end
    end

    reactive do
      id(:entries)
      op(:source)
      leaf?(true)
    end
  end

  # the declarative variance node: [key:, where:] sides + per-side picks.
  defmodule Variance do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.DeclarativeJoinGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_dj_variance"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :budget, :float, public?: true
      attribute :actual, :float, public?: true
    end

    identities do
      identity :by_key, [:key], pre_check_with: ReactiveDag.Fixtures.DeclarativeJoinGraph.Domain
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
      id(:variance)
      op(:reconcile)
      key_rule(:all)

      join(
        over: :entries,
        left: [key: :acct, where: [kind: "budget"]],
        right: [key: :acct, where: [kind: "actual"]],
        key_prefix: "va",
        into: [left: [amount: :budget], right: [amount: :actual]]
      )
    end
  end

  defmodule OuterVariance do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.DeclarativeJoinGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_dj_outer_variance"
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
    end

    reactive do
      id(:outer_variance)
      op(:reconcile)
      key_rule(:all)

      join(
        over: :entries,
        left: [key: :acct, where: [kind: "budget"]],
        right: [key: :acct, where: [kind: "actual"]],
        outer: true,
        into: fn acct, budget, actual ->
          %{
            key: acct,
            status:
              cond do
                is_nil(budget) -> "undeclared"
                is_nil(actual) -> "unspent"
                true -> "present"
              end
          }
        end
      )
    end
  end

  defmodule Linked do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.DeclarativeJoinGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_dj_linked"
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
    end

    reactive do
      id(:linked)
      op(:reconcile)
      key_rule(:all)

      # every entry is on the left by acct; only actuals land on the right
      # via a fn (mixing forms is fine — each slot resolves independently)
      join(
        over: :entries,
        left: :acct,
        right: fn e -> e.kind == "actual" && e.acct end,
        into: fn acct, _l, r ->
          %{key: acct, status: if(r, do: "observed", else: "declared-only")}
        end
      )
    end
  end

  def tables, do: ["fx_dj_linked", "fx_dj_outer_variance", "fx_dj_entries", "fx_dj_variance"]
end

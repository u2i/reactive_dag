defmodule ReactiveDag.Fixtures.JoinExpandGraph do
  @moduledoc """
  A join whose output EXPANDS — one matched pair emitting several rows.
  """

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      resource ReactiveDag.Fixtures.JoinExpandGraph.Fiscal
      resource ReactiveDag.Fixtures.JoinExpandGraph.Variance
      resource ReactiveDag.Fixtures.JoinExpandGraph.YearBuckets
      resource ReactiveDag.Fixtures.JoinExpandGraph.KeylessExpand
    end
  end

  defmodule Fiscal do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.JoinExpandGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_je_fiscal"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :k, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :kind, :string, public?: true
      attribute :acct, :string, public?: true
      attribute :amount, :float, public?: true
      attribute :fy, :integer, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :create do
        accept([:k, :kind, :acct, :amount, :fy])
      end
    end

    reactive do
      id :fiscal
      op :source
      leaf? true
    end
  end

  # fn everything: computed side keys (the `&&` discriminator idiom), a fn key
  # with a literal prefix, a computed-columns into, an upsert capture.
  defmodule Variance do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.JoinExpandGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_je_variance"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :budget, :float, public?: true
      attribute :actual, :float, public?: true
      attribute :variance, :float, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        upsert_identity(:by_key)
        accept([:key, :budget, :actual, :variance])
      end
    end

    identities do
      identity :by_key, [:key]
    end

    reactive do
      id :variance
      op :reconcile
      key_rule :all

      join(
        over: :fiscal,
        left: fn %{kind: k, acct: a} -> k == "budget" && a end,
        right: fn %{kind: k, acct: a} -> k == "actual" && a end,
        key: fn acct -> "va|#{acct}" end,
        into: fn _acct, budget, actual ->
          %{
            budget: budget.amount,
            actual: actual && actual.amount,
            variance: budget.amount - ((actual && actual.amount) || 0.0)
          }
        end
      )
    end
  end

  # EXPAND: one group (a fiscal year) fans out to one row per bucket — each
  # row self-keyed, because one group → many keys.
  defmodule YearBuckets do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.JoinExpandGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_je_year_buckets"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :n, :integer, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        upsert_identity(:by_key)
        accept([:key, :n])
      end
    end

    identities do
      identity :by_key, [:key]
    end

    reactive do
      id :year_buckets
      op :expand
      key_rule :all

      reduce(
        over: :fiscal,
        group_by: :fy,
        expand: fn fy, lines ->
          lines
          |> Enum.group_by(& &1.kind)
          |> Enum.map(fn {kind, ls} ->
            %{key: "#{fy}|#{kind}", n: length(ls)}
          end)
        end
      )
    end
  end

  defmodule KeylessExpand do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.JoinExpandGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_je_keyless_expand"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :n, :integer, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        upsert_identity(:by_key)
        accept([:key, :n])
      end
    end

    identities do
      identity :by_key, [:key]
    end

    reactive do
      id :keyless_expand
      op :expand
      key_rule :all

      # The row has an `:n` but no `:key`, which is the error under test — the
      # table exists so the failure is the missing key, not a missing table.
      reduce(
        over: :fiscal,
        group_by: :fy,
        expand: fn _fy, lines -> [%{n: length(lines)}] end
      )
    end
  end

  def tables, do: ["fx_je_keyless_expand", "fx_je_fiscal", "fx_je_variance", "fx_je_year_buckets"]
end

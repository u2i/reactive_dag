defmodule ReactiveDag.Fixtures.BasisGraph do
  @moduledoc """
  How an ATTESTABLE row emerges — the row and its basis from the SAME `into:`.

  That is the point: a basis computed anywhere else could describe a different
  moment than the row it sits beside, and the whole mechanism rests on the two
  agreeing.
  """

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      resource ReactiveDag.Fixtures.BasisGraph.Machines
      resource ReactiveDag.Fixtures.BasisGraph.Holdings
    end
  end

  defmodule Machines do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.BasisGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_basis_machines"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :owner, :string, public?: true
      attribute :serial, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :create do
        accept([:key, :owner, :serial])
      end

      update :revise do
        accept([:serial])
      end
    end

    reactive do
      id :machines
      leaf? true
    end
  end

  defmodule Holdings do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.BasisGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_basis_holdings"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :owner, :string, public?: true
      attribute :count, :integer, public?: true
      # emerges WITH the row, from the same rows the row summarises
      attribute :basis, :string, public?: true
      # a host would carry these too; a signature writes them
      attribute :signed_by, :string, public?: true
      attribute :signed_basis, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        accept([:key, :owner, :count, :basis])
      end

      update :sign do
        accept([:signed_by, :signed_basis])
      end
    end

    reactive do
      id :holdings
      recompute_by :owner, to: :machines, from: :owner

      reduce(
        into: fn {owner}, machines ->
          %{
            owner: owner,
            count: length(machines),
            basis: ReactiveDag.Basis.digest(machines, fields: [:key, :serial])
          }
        end
      )
    end
  end

  def tables, do: ["fx_basis_machines", "fx_basis_holdings"]
end

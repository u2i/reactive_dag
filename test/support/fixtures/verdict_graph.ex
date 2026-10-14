defmodule ReactiveDag.Fixtures.VerdictGraph do
  @moduledoc """
  A VERDICT node — a judgement over another node's rows, carrying a status
  rather than a number.
  """

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      resource ReactiveDag.Fixtures.VerdictGraph.Totals
      resource ReactiveDag.Fixtures.VerdictGraph.Health
    end
  end

  defmodule Totals do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.VerdictGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_vd_totals"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :total, :float, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :create do
        accept([:key, :total])
      end
    end

    reactive do
      id(:totals)
      leaf?(true)
    end
  end

  defmodule Health do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.VerdictGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_vd_health"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :status, :string, public?: true
      # the column a tableless verdict node cannot have at all
      attribute :headroom, :float, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        accept([:key, :status, :headroom])
      end
    end

    reactive do
      id(:health)

      # a verdict WITH a table is an ordinary payload node: the status is a
      # column, written by `into:` like any other. `status:` is the tableless
      # node's slot and has no business here.
      reduce(
        over: :totals,
        group_by: :key,
        into: fn _k, [r | _] ->
          %{
            status: if(r.total < 1000.0, do: "present", else: "failing"),
            headroom: 1000.0 - r.total
          }
        end
      )
    end
  end

  def tables, do: ["fx_vd_totals", "fx_vd_health"]
end

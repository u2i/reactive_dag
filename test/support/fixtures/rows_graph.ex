defmodule ReactiveDag.Fixtures.RowsGraph do
  @moduledoc """
  Cells addressed by CELL KEY — the read side of what the payload loop writes.

  Three shapes, because the key a cell reads under is the question:

    * `Health`    — a single-column node, which reads that column.
    * `Rollup`    — a composite PK, where the key is the identity
      serialisation.
    * `Elsewhere` — a node whose rows live under a different key column
      entirely.

  The property: the key a cell reads under is the SAME key the DAG marks dirty
  and the same key `Payload` wrote.
  """

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      resource ReactiveDag.Fixtures.RowsGraph.Health
      resource ReactiveDag.Fixtures.RowsGraph.Rollup
      resource ReactiveDag.Fixtures.RowsGraph.Elsewhere
    end
  end

  # single-column key, with a status
  defmodule Health do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.RowsGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_rows_health"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute(:key, :string, primary_key?: true, allow_nil?: false, public?: true)
      attribute(:status, :string, public?: true)
    end

    actions do
      defaults([:read, :destroy])

      create :upsert do
        upsert?(true)
        accept([:key, :status])
      end
    end

    reactive do
      id :health
      leaf? true
    end
  end

  # composite PK → identity-keyed, and NO status column (a rollup, not a verdict)
  defmodule Rollup do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.RowsGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_rows_rollup"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute(:fund, :string, primary_key?: true, allow_nil?: false, public?: true)
      attribute(:fy, :string, primary_key?: true, allow_nil?: false, public?: true)
      attribute(:total, :float, public?: true)
    end

    actions do
      defaults([:read, :destroy])

      create :upsert do
        upsert?(true)
        accept([:fund, :fy, :total])
      end
    end

    reactive do
      id :rollup
      leaf? true
    end
  end

  # a node that writes elsewhere — no attributes of its own
  defmodule Elsewhere do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.RowsGraph.Domain,
      data_layer: Ash.DataLayer.Simple,
      extensions: [ReactiveDag.Node]

    reactive do
      id :elsewhere
      leaf? true
    end
  end

  @doc """
  The tables this fixture owns.

  `Elsewhere` is absent on purpose: it is on `Ash.DataLayer.Simple` and owns
  no table at all, which is what makes it the third shape here.
  """
  def tables, do: ["fx_rows_health", "fx_rows_rollup"]
end

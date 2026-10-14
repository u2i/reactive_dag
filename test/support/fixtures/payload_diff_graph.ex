defmodule ReactiveDag.Fixtures.PayloadDiffGraph do
  @moduledoc """
  The diff a payload write produces, PROPAGATED to the node's consumers.

  `Projected` maps the leaf; `Rollup` folds it. A consumer derives its
  units from the diff rather than re-reading, so the diff must survive the hop
  intact.
  """

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      resource ReactiveDag.Fixtures.PayloadDiffGraph.Lines
      resource ReactiveDag.Fixtures.PayloadDiffGraph.Projected
      resource ReactiveDag.Fixtures.PayloadDiffGraph.Rollup
    end
  end

  # A LEAF whose rows a source writes. Its `fund` is what the rollup groups by,
  # so changing it moves a row between units.
  defmodule Lines do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.PayloadDiffGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_pdp_lines"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :fund, :string, public?: true
      attribute :amount, :integer, public?: true
    end

    actions do
      defaults [:read, :destroy]
      create :upsert, upsert?: true, accept: [:key, :fund, :amount]
    end

    reactive do
      id :lines
      leaf? true
      payload_key :key
    end
  end

  # The middle cell — GRAPH-WRITTEN, through the payload loop. Its rows are what
  # the rollup above it groups, so its diffs are what must propagate.
  defmodule Projected do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.PayloadDiffGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_pdp_projected"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :fund, :string, public?: true
      attribute :amount, :integer, public?: true
    end

    actions do
      defaults [:read, :destroy]
      create :upsert, upsert?: true, accept: [:key, :fund, :amount]
    end

    reactive do
      id :projected
      op :map
      payload_key :key
      depends_on [:lines]
      compute ReactiveDag.Fixtures.PayloadDiffGraph.Project
    end
  end

  defmodule Rollup do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.PayloadDiffGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_pdp_rollup"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :fund, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :total, :integer, public?: true
    end

    actions do
      defaults [:read, :destroy]
      create :upsert, upsert?: true, accept: [:fund, :total]
    end

    reactive do
      id :rollup
      op :fold
      payload_key :fund
      recompute_by :fund, to: :projected, from: :fund
      reduce into: [sum: [amount: :total]]
    end
  end

  defmodule Project do
    @moduledoc "The map behind `Projected` — named by the resource."
    @behaviour ReactiveDag.Op

    @impl true
    def recompute(cell, keys, opts \\ []) do
      lines =
        case keys do
          ["*"] -> Ash.read!(Lines)
          ks -> Enum.filter(Ash.read!(Lines), &(&1.key in ks))
        end

      changed =
        for l <- lines,
            ReactiveDag.Node.Payload.upsert_row(
              Projected,
              cell.meta,
              l.key,
              %{key: l.key, fund: l.fund, amount: l.amount},
              opts
            ) != :unchanged do
          l.key
        end

      {:ok, changed}
    end
  end

  def tables, do: ["fx_pdp_lines", "fx_pdp_projected", "fx_pdp_rollup"]
end

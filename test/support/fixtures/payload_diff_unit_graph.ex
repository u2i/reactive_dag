defmodule ReactiveDag.Fixtures.PayloadDiffUnitGraph do
  @moduledoc """
  The diff a payload write produces, read directly.
  """

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      resource ReactiveDag.Fixtures.PayloadDiffUnitGraph.Line
    end
  end

  defmodule Line do
    use Ash.Resource, domain: ReactiveDag.Fixtures.PayloadDiffUnitGraph.Domain, data_layer: AshPostgres.DataLayer

    postgres do
      table "fx_pdu_line"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :line_key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :fund, :string, public?: true
      attribute :value, :integer, public?: true
    end

    actions do
      defaults [:read, :destroy]
      create :upsert, upsert?: true, accept: [:line_key, :fund, :value]
    end
  end

  def tables, do: ["fx_pdu_line"]
end

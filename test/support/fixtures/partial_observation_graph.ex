defmodule ReactiveDag.Fixtures.PartialObservationGraph do
  @moduledoc """
  An observation that covers only PART of what a node knows — a write that
  must not blank the columns it says nothing about.
  """

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      resource ReactiveDag.Fixtures.PartialObservationGraph.Docs
    end
  end

  defmodule Docs do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.PartialObservationGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_po_docs"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :content_md5, :string, public?: true
      attribute :fingerprint, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        accept([:key, :content_md5, :fingerprint])
      end
    end

    reactive do
      id :docs
      leaf? true
      fingerprint [:content_md5]
    end
  end

  def tables, do: ["fx_po_docs"]
end

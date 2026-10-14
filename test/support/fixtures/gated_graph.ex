defmodule ReactiveDag.Fixtures.GatedGraph do
  @moduledoc """
  A GATED node — one whose recompute is allowed to decline.

  A gate that is off makes an op report success while doing nothing, so the
  question is whether a declined recompute is distinguishable from a
  successful one that found nothing.
  """

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      resource ReactiveDag.Fixtures.GatedGraph.Versioned
      resource ReactiveDag.Fixtures.GatedGraph.Notes
    end
  end

  defmodule Versioned do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.GatedGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_gated_versioned"
      repo(ReactiveDag.TestRepo)
    end

    temporal do
      attribute :valid_at
      recorded_at :recorded_at
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :body, :string, public?: true

      attribute :recorded_at, :utc_datetime_usec,
        public?: true,
        default: &DateTime.utc_now/0,
        update_default: &DateTime.utc_now/0
    end

    actions do
      defaults [:read, :destroy]
      create :upsert, upsert?: true, accept: [:key, :body]
    end

    reactive do
      id :versioned
      leaf? true
      payload_key :key
      dirties_on [:create, :update]
      gated true
      # Nothing to declare: the resource's own period is the record. A
      # consumer that needs to narrow reads the row as of the instant the
      # enqueue carries.
    end
  end

  defmodule Notes do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.GatedGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_gated_notes"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :body, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]
      create :upsert, upsert?: true, accept: [:key, :body]
    end

    reactive do
      id :notes
      leaf? true
      payload_key :key
      dirties_on [:create, :update]
      # `gated` alone is what lowers to the :approval suspend reason —
      # `suspends` is the :expensive one, and declaring it here would make the
      # cascade stop for the wrong reason (`:expensive` wins when both are set).
      gated human?: {ReactiveDag.GatedTest, :person?, []}
      # Required of any suspending cell: a resumption has to be able to read
      # back the change that stopped it, or it recomputes the whole cell.
    end
  end

  def tables, do: ["fx_gated_notes", "fx_gated_versioned"]
end

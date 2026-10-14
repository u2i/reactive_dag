defmodule ReactiveDag.Fixtures.StaleGraph do
  @moduledoc """
  Staleness asked of the DATA — a child derived before its input last moved.

  Both resources are TEMPORAL: the comparison is `parent.recorded_at >
  child.recorded_at`, and `recorded_at` only means anything on a resource
  that keeps periods.
  """

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      resource ReactiveDag.Fixtures.StaleGraph.Source
      resource ReactiveDag.Fixtures.StaleGraph.Derived
    end
  end

  defmodule Source do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.StaleGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_st_source"
      repo(ReactiveDag.TestRepo)
    end

    temporal do
      attribute :valid_at
      recorded_at :recorded_at
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :amount, :float, public?: true

      attribute :recorded_at, :utc_datetime_usec,
        public?: true,
        default: &DateTime.utc_now/0,
        update_default: &DateTime.utc_now/0
    end

    actions do
      defaults [:read, :destroy]
      # `recorded_at` MUST be named in `upsert_fields`, or an upsert that
      # changes a value leaves the stamp at its create value — and staleness,
      # which compares those stamps, reports nothing moved. Measured; see
      # `spike/temporal_upsert_recorded_at.exs`.
      create :upsert,
        upsert?: true,
        accept: [:key, :amount],
        upsert_fields: [:amount, :recorded_at]
    end

    reactive do
      id :source
      leaf? true
    end
  end

  defmodule Derived do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.StaleGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_st_derived"
      repo(ReactiveDag.TestRepo)
    end

    temporal do
      attribute :valid_at
      recorded_at :recorded_at
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :doubled, :float, public?: true

      attribute :recorded_at, :utc_datetime_usec,
        public?: true,
        default: &DateTime.utc_now/0,
        update_default: &DateTime.utc_now/0
    end

    actions do
      defaults [:read, :destroy]

      create :upsert,
        upsert?: true,
        accept: [:key, :doubled],
        upsert_fields: [:doubled, :recorded_at]
    end

    reactive do
      id :derived
      depends_on [:source]
      compute ReactiveDag.Fixtures.StaleGraph.Double
    end
  end

  def tables, do: ["fx_st_source", "fx_st_derived"]
end

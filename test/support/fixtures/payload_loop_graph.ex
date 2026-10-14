defmodule ReactiveDag.Fixtures.PayloadLoopGraph do
  @moduledoc """
  The payload write loop: compare, write, report what moved.
  """

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      resource ReactiveDag.Fixtures.PayloadLoopGraph.DmrRow
      resource ReactiveDag.Fixtures.PayloadLoopGraph.FlowMonth
    end
  end

  # the input the fold reads (raw per-day flow rows) — a leaf NODE, so the
  # rollup's declarative read can resolve its resource at assembly.
  defmodule DmrRow do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.PayloadLoopGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_pl_dmr_row"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :id, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :plant, :string, public?: true
      attribute :month, :string, public?: true
      attribute :flow, :float, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :create do
        accept([:id, :plant, :month, :flow])
      end
    end

    reactive do
      id(:dmr_rows)
      op(:source)
      leaf?(true)
    end
  end

  # THE UNIFIED NODE: an AshPostgres-shaped (here Ets) resource that is BOTH the
  # reactive node AND its own payload table. No `upsert:` — the lib closes the loop.
  defmodule FlowMonth do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.PayloadLoopGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_pl_flow_month"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :plant, :string, public?: true
      attribute :month, :string, public?: true
      attribute :avg_flow, :float, public?: true
    end

    identities do
      identity :by_key, [:key], pre_check_with: ReactiveDag.Fixtures.PayloadLoopGraph.Domain
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        upsert_identity(:by_key)
        accept([:key, :plant, :month, :avg_flow])
      end
    end

    reactive do
      id(:flow_month)
      op(:fold)
      key_rule(:all)

      # NB: no `read:` (the library reads :dmr_rows itself) and no `upsert:` —
      # into returns the row, the library writes it into THIS resource (keyed
      # by :key) and does the Op.put. group/key/into stay fn escapes here.
      reduce(
        over: :dmr_rows,
        group_by: &ReactiveDag.Fixtures.PayloadLoopGraph.group/1,
        key: &ReactiveDag.Fixtures.PayloadLoopGraph.key/1,
        into: &ReactiveDag.Fixtures.PayloadLoopGraph.into/2
      )
    end
  end

  @doc """
  The fold's grouping, key and shape — here because the RESOURCE names them.
  """
  def group(r), do: {r.plant, r.month}
  def key({plant, month}), do: "#{plant}|#{month}"

  def into({plant, month}, rows) do
    avg = rows |> Enum.map(& &1.flow) |> then(&(Enum.sum(&1) / length(&1)))
    %{key: "#{plant}|#{month}", plant: plant, month: month, avg_flow: avg}
  end

  # a no-op coordination writer — this test exercises the PAYLOAD loop; the

  def tables, do: ["fx_pl_dmr_row", "fx_pl_flow_month"]
end

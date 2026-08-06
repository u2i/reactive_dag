defmodule ReactiveDag.FirstClassGuaranteeTest do
  @moduledoc """
  PROTOTYPE: a guarantee as a FIRST-CLASS resource that MATERIALIZES its computed
  result set as typed data (not verdict-only). The store-per-node shape (cascade's
  model) applied to a portal guarantee — the resource has real attributes, its
  recompute writes result ROWS into them, and the verdict + detail are queryable
  via Ash (no recompute for drill-down).

  Contrast the verdict-only model (model_tuple: status×strength per key, dataset
  transient): here the reconcile's FULL rows (entitled/observed/status/drift) are
  persisted as data. This is the trade — storage proportional to the population,
  in exchange for queryable/historical detail + Ash-native reads.

  Uses Ash.DataLayer.Ets (in-memory, writable) so we exercise real Ash
  create/read without a repo.
  """
  use ExUnit.Case, async: false
  require Ash.Query

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false
    resources do
      allow_unregistered?(true)
    end
  end

  defmodule Writer do
    @behaviour ReactiveDag.CoordinationWriter
    @impl true
    def put(cell_id, key, _opts), do: send(self(), {:put, cell_id, key})
    @impl true
    def delete(cell_id, keys), do: send(self(), {:delete, cell_id, keys})
  end

  # ── the FIRST-CLASS reconcile guarantee: a Node resource WITH payload data ────
  defmodule StoreEncrypted do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    # the REACTIVE facet: what the node is (op + deps + how it recomputes).
    reactive do
      id :"g:store_encrypted"
      op :reconcile
      key_rule :all
      depends_on [:entitled, :observed]
      compute ReactiveDag.FirstClassGuaranteeTest.ReconcileOp
    end

    # the DATA facet: the result set this guarantee MATERIALIZES (typed columns).
    ets do
      private? true
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :entitled, :boolean, public?: true
      attribute :observed, :boolean, public?: true
      attribute :status, :atom, public?: true          # :present | :failing
      attribute :drift, :string, public?: true          # WHY it's failing (the detail)
    end

    identities do
      identity :by_key, [:key]
    end

    actions do
      defaults [:read, :destroy, create: :*, update: :*]

      create :upsert do
        upsert? true
        upsert_identity :by_key
        accept [:key, :entitled, :observed, :status, :drift]
      end
    end
  end

  # ── the recompute: MATERIALIZE the reconcile's rows into the resource ─────────
  # reads the two input key-sets (from the test seed), computes the full-outer-join
  # detail, writes a typed row per key via Ash.create (the store-per-node write),
  # and records the coordination key for changed rows.
  defmodule ReconcileOp do
    @behaviour ReactiveDag.Op

    @impl true
    def recompute(cell, _keys) do
      entitled = Process.get(:entitled, MapSet.new())
      observed = Process.get(:observed, MapSet.new())
      all = MapSet.union(entitled, observed)

      changed =
        for k <- all do
          e = MapSet.member?(entitled, k)
          o = MapSet.member?(observed, k)
          status = if e and o, do: :present, else: :failing

          drift =
            cond do
              e and not o -> "entitled but not observed"
              o and not e -> "observed but not entitled"
              true -> nil
            end

          {:ok, _} =
            StoreEncrypted
            |> Ash.Changeset.for_create(:upsert, %{
              key: k, entitled: e, observed: o, status: status, drift: drift
            })
            |> Ash.create()

          ReactiveDag.Op.put(cell, k)
          k
        end

      {:ok, changed}
    end
  end

  setup do
    prev = Application.get_env(:reactive_dag, :coordination_writer)
    Application.put_env(:reactive_dag, :coordination_writer, Writer)
    on_exit(fn -> Application.put_env(:reactive_dag, :coordination_writer, prev) end)
    :ok
  end

  test "the resource is BOTH a reactive node AND a data table" do
    cell = ReactiveDag.Node.to_cell(StoreEncrypted)
    # reactive facet:
    assert cell.op == :reconcile
    assert cell.inputs == ["entitled", "observed"]
    assert cell.meta.compute == ReconcileOp
    # data facet: it's a real Ash resource with attributes.
    assert :key in Enum.map(Ash.Resource.Info.attributes(StoreEncrypted), & &1.name)
    assert :drift in Enum.map(Ash.Resource.Info.attributes(StoreEncrypted), & &1.name)
  end

  test "recompute MATERIALIZES result rows; detail is queryable via Ash (no recompute)" do
    Process.put(:entitled, MapSet.new(["alice", "bob"]))
    Process.put(:observed, MapSet.new(["alice"]))

    cell = ReactiveDag.Node.to_cell(StoreEncrypted)
    {:ok, changed} = ReconcileOp.recompute(cell, ["*"])
    assert Enum.sort(changed) == ["alice", "bob"]

    # the FULL result set is now stored data — read it back with Ash, no recompute.
    rows = Ash.read!(StoreEncrypted)
    assert length(rows) == 2

    # drill-down detail is THERE (the payoff): why is bob failing?
    {:ok, bob} = Ash.get(StoreEncrypted, "bob")
    assert bob.status == :failing
    assert bob.drift == "entitled but not observed"

    {:ok, alice} = Ash.get(StoreEncrypted, "alice")
    assert alice.status == :present
    assert alice.drift == nil

    # the verdict = an Ash query over stored rows (derived, not the primary datum).
    failing = StoreEncrypted |> Ash.Query.filter(status == :failing) |> Ash.read!()
    assert Enum.map(failing, & &1.key) == ["bob"]

    # coordination still recorded for the changed keys (the substrate still works).
    assert_received {:put, "g:store_encrypted", "bob"}
    assert_received {:put, "g:store_encrypted", "alice"}
  end

  test "re-running with fewer entitled updates the materialized data (drift closes)" do
    Process.put(:entitled, MapSet.new(["alice"]))
    Process.put(:observed, MapSet.new(["alice"]))
    cell = ReactiveDag.Node.to_cell(StoreEncrypted)
    {:ok, _} = ReconcileOp.recompute(cell, ["*"])

    {:ok, alice} = Ash.get(StoreEncrypted, "alice")
    assert alice.status == :present
    assert StoreEncrypted |> Ash.Query.filter(status == :failing) |> Ash.read!() == []
  end
end

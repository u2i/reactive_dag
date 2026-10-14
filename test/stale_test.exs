defmodule ReactiveDag.StaleTest do
  @moduledoc """
  Staleness, asked of the data rather than of the engine.

  A cascade propagates; this finds what propagation MISSED. The two are not
  alternatives — a dropped enqueue, a crash mid-walk, a gate that was off all
  leave a row stale with nothing recording the fact, and the engine has no way
  to know. The comparison does.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.Stale

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false
    resources do: allow_unregistered?(true)
  end

  defmodule Source do
    use Ash.Resource,
      domain: ReactiveDag.StaleTest.Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do: private?(true)

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
      domain: ReactiveDag.StaleTest.Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do: private?(true)

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
      compute ReactiveDag.StaleTest.Double
    end
  end

  defmodule Double do
    @behaviour ReactiveDag.Op
    @impl true
    def recompute(_cell, _keys, _opts), do: {:ok, []}
  end

  defp plan, do: ReactiveDag.Node.graph([Source, Derived])

  defp write(resource, attrs) do
    resource |> Ash.Changeset.for_create(:upsert, attrs) |> Ash.create!()
  end

  setup do
    for r <- [Source, Derived], row <- Ash.read!(r), do: Ash.destroy!(row)
    :ok
  end

  describe "edges come from the plan" do
    test "every declared input of a temporal cell is an edge" do
      assert [{"source", "derived", :identity}] = Stale.edges(plan())
    end
  end

  describe "a child derived BEFORE its input moved is stale" do
    test "and one derived after is not" do
      write(Source, %{key: "a", amount: 1.0})
      write(Source, %{key: "b", amount: 1.0})
      Process.sleep(5)

      # both derived now — neither is behind
      write(Derived, %{key: "a", doubled: 2.0})
      write(Derived, %{key: "b", doubled: 2.0})

      assert {:ok, []} = Stale.on_edge(plan(), "source", "derived")

      # `a`'s input moves AFTER it was derived, and nothing propagates
      Process.sleep(5)
      write(Source, %{key: "a", amount: 9.0})

      assert {:ok, ["a"]} = Stale.on_edge(plan(), "source", "derived"),
             "the comparison finds what no propagation recorded"
    end

    test "a key with no parent row is not stale — it is orphaned, which is a different question" do
      write(Derived, %{key: "ghost", doubled: 0.0})

      assert {:ok, []} = Stale.on_edge(plan(), "source", "derived")
    end
  end

  describe "the answer is never a bare list when it could not be computed" do
    test "an unknown cell says so" do
      assert {:error, :no_such_cell} = Stale.on_edge(plan(), "nope", "derived")
    end
  end
end

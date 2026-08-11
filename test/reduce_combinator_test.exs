defmodule ReactiveDag.ReduceCombinatorTest do
  @moduledoc """
  The `reduce` combinator's MECHANICS through its fn escape hatches: fn
  group/key/into slots, the `upsert:` write override (write-elsewhere shape),
  the `query:` transformer (shaping the Ash read without leaving Ash), and the
  library-owned dirty-key scoping the transformer composes with.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.Node.Recompute

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      allow_unregistered?(true)
    end
  end

  defmodule Lines do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
      private?(true)
    end

    attributes do
      attribute :k, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :fp, :string, public?: true
      attribute :flagged, :boolean, public?: true, default: false
    end

    actions do
      defaults [:read, :destroy]

      create :create do
        accept([:k, :fp, :flagged])
      end
    end

    reactive do
      id(:lines)
      op(:source)
      leaf?(true)
      payload_key(:k)
    end
  end

  # write-elsewhere shape: fn slots + an upsert: override that captures writes
  # and decides changed-ness (the host's fingerprint idiom).
  defmodule Summary do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Simple,
      extensions: [ReactiveDag.Node]

    reactive do
      id(:summary)
      op(:map)

      reduce over: :lines,
             query: fn q, dirty ->
               send(ReactiveDag.ReduceCombinatorTest, {:query, dirty})
               q
             end,
             group_by: fn line -> line.k end,
             key: fn k -> k end,
             into: fn _k, [line | _] -> %{fp: line.fp} end,
             upsert: fn key, row ->
               send(ReactiveDag.ReduceCombinatorTest, {:upsert, key, row})
               # the host's change detection: flagged lines "changed"
               String.starts_with?(row.fp, "new")
             end
    end
  end

  defmodule NullWriter do
    @behaviour ReactiveDag.CoordinationWriter
    @impl true
    def put(_cell_id, _key, _opts), do: :ok
    @impl true
    def delete(_cell_id, _keys), do: :ok
  end

  setup do
    Process.register(self(), ReactiveDag.ReduceCombinatorTest)
    prev = Application.get_env(:reactive_dag, :coordination_writer)
    Application.put_env(:reactive_dag, :coordination_writer, NullWriter)
    on_exit(fn -> Application.put_env(:reactive_dag, :coordination_writer, prev) end)

    for {k, fp} <- [{"a", "new-h1"}, {"b", "old-h2"}, {"c", "new-h3"}] do
      Lines |> Ash.Changeset.for_create(:create, %{k: k, fp: fp}) |> Ash.create!()
    end

    :ok
  end

  defp cell, do: ReactiveDag.Node.graph([Lines, Summary]).cells["summary"]

  test "fn slots + upsert override: writes captured, only upsert-true keys propagate" do
    {:ok, changed} = Recompute.recompute(cell(), ["*"])

    # every group was written…
    for k <- ["a", "b", "c"], do: assert_received({:upsert, ^k, _row})
    # …but only the ones the host reported as changed propagate
    assert Enum.sort(changed) == ["a", "c"]
  end

  test "query: receives the dirty keys, and the library STILL applies the scope" do
    {:ok, changed} = Recompute.recompute(cell(), ["a"])

    # the transformer saw the claimed keys…
    assert_received {:query, ["a"]}
    # …and the library's payload-key filter kept the read to that slice:
    # only "a" was ever grouped/written.
    assert_received {:upsert, "a", _}
    refute_received {:upsert, "b", _}
    assert changed == ["a"]
  end

  test "a whole-cell claim reaches query: as nil and reads everything" do
    {:ok, _} = Recompute.recompute(cell(), ["*"])
    assert_received {:query, nil}
    for k <- ["a", "b", "c"], do: assert_received({:upsert, ^k, _})
  end

  test "into: returning a LIST raises instructively — that shape is expand:" do
    defmodule ListInto do
      use Ash.Resource,
        domain: ReactiveDag.ReduceCombinatorTest.Domain,
        data_layer: Ash.DataLayer.Simple,
        extensions: [ReactiveDag.Node]

      reactive do
        id(:list_into)
        op(:map)

        reduce over: :lines,
               group_by: fn l -> l.k end,
               into: fn k, _ -> [%{key: k}] end,
               upsert: fn _, _ -> true end
      end
    end

    plan = ReactiveDag.Node.graph([Lines, ListInto])

    assert_raise RuntimeError, ~r/ONE row per group.*expand/s, fn ->
      Recompute.recompute(plan.cells["list_into"], ["*"])
    end
  end
end

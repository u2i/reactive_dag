defmodule ReactiveDag.ReduceCombinatorTest do
  @moduledoc """
  The `reduce` combinator's MECHANICS through its fn escape hatches: fn
  group/key/into slots, the `query:` transformer (shaping the Ash read without
  leaving Ash), and the library-owned dirty-key scoping the transformer composes
  with.

  These used to run through an `upsert:` override that captured writes and decided
  changed-ness itself — the write-elsewhere shape, now removed. The node writes its
  own rows, so what a test asserted by reading its mailbox it now asserts by
  reading the table, and the change detection under test is
  `ReactiveDag.Node.Payload`'s comparison rather than a closure's opinion.
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
    end
  end

  # fn slots over a node that owns its rows.
  defmodule Summary do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
      private?(true)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :fp, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        upsert_identity(:by_key)
        accept([:key, :fp])
      end
    end

    identities do
      identity :by_key, [:key]
    end

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
             into: fn _k, [line | _] -> %{fp: line.fp} end
    end
  end


  setup do
    Process.register(self(), ReactiveDag.ReduceCombinatorTest)

    for {k, fp} <- [{"a", "new-h1"}, {"b", "old-h2"}, {"c", "new-h3"}] do
      Lines |> Ash.Changeset.for_create(:create, %{k: k, fp: fp}) |> Ash.create!()
    end

    :ok
  end

  defp cell, do: ReactiveDag.Node.graph([Lines, Summary]).cells["summary"]

  test "fn slots write every group, and only what MOVED propagates" do
    {:ok, changed} = Recompute.recompute(cell(), ["*"])

    # every group was written…
    rows = Summary |> Ash.read!() |> Map.new(&{&1.key, &1.fp})
    assert rows == %{"a" => "new-h1", "b" => "old-h2", "c" => "new-h3"}

    # …and all three are new, so all three propagate
    assert Enum.sort(changed) == ["a", "b", "c"]

    # The change detection under test: an identical second pass writes the same
    # rows and reports NOTHING. This used to be a closure's opinion
    # (`String.starts_with?(row.fp, "new")`); it is now `Payload.upsert`'s
    # comparison against the stored row, which is the same property with nobody
    # having to implement it.
    assert {:ok, []} = Recompute.recompute(cell(), ["*"])
  end

  test "query: receives the dirty keys, and the library STILL applies the scope" do
    {:ok, changed} = Recompute.recompute(cell(), ["a"])

    # the transformer saw the claimed keys…
    assert_received {:query, ["a"]}
    # …and the library's payload-key filter kept the read to that slice:
    # only "a" was ever grouped/written.
    keys = Summary |> Ash.read!() |> Enum.map(& &1.key)
    assert keys == ["a"]
    assert changed == ["a"]
  end

  test "a whole-cell claim reaches query: as nil and reads everything" do
    {:ok, _} = Recompute.recompute(cell(), ["*"])
    assert_received {:query, nil}
    assert Summary |> Ash.read!() |> Enum.map(& &1.key) |> Enum.sort() == ["a", "b", "c"]
  end

  test "into: returning a LIST raises instructively — that shape is expand:" do
    defmodule ListInto do
      use Ash.Resource,
        domain: ReactiveDag.ReduceCombinatorTest.Domain,
        data_layer: Ash.DataLayer.Ets,
        extensions: [ReactiveDag.Node]

      ets do
        private?(true)
      end

      attributes do
        attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      end

      actions do
        defaults [:read, :destroy]

        create :upsert do
          upsert?(true)
          upsert_identity(:by_key)
          accept([:key])
        end
      end

      identities do
        identity :by_key, [:key]
      end

      reactive do
        id(:list_into)
        op(:map)

        # A table, so the failure under test is `into:` returning a LIST rather
        # than anything about where the row would go.
        reduce over: :lines,
               group_by: fn l -> l.k end,
               into: fn k, _ -> [%{key: k}] end
      end
    end

    plan = ReactiveDag.Node.graph([Lines, ListInto])

    assert_raise RuntimeError, ~r/ONE row per group.*expand/s, fn ->
      Recompute.recompute(plan.cells["list_into"], ["*"])
    end
  end
end

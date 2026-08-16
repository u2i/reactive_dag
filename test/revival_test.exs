defmodule ReactiveDag.RevivalTest do
  @moduledoc """
  Coming back is a change (#82).

  Under a MARKING policy the row survives retirement, so a key that returns
  carries the fingerprint it left with — its content did not move, its liveness
  did. A fingerprint comparison reports "unchanged", and the revival would never
  propagate: no dirty key, no drain step, nothing in the trace.

  The library reports it. Everything needed is already in hand — the baseline is
  computed for the vanish set anyway, and the observation record says which
  verdicts came from a fingerprint rather than from the host — so this costs a
  filter, not a query.

  Half of these tests assert a key is NOT revived. That matters as much as the
  other half: over-reporting would make every poll look like a change, which is
  the cost the engine exists to avoid.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.Node.Rows

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      allow_unregistered?(true)
    end
  end

  defmodule Docs do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Ets, extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :content_md5, :string, public?: true
      attribute :status, :string, public?: true
      attribute :fingerprint, :string, public?: true
    end

    actions do
      defaults [:read, :destroy, update: [:status]]

      create :upsert do
        upsert?(true)
        accept([:key, :content_md5, :status, :fingerprint])
      end
    end

    reactive do
      id(:docs)
      leaf?(true)
      fingerprint([:content_md5])
    end
  end

  setup do
    for row <- Ash.read!(Docs), do: Ash.destroy!(row)
    :ok
  end

  defp cell do
    [c] = ReactiveDag.Node.cells(Docs)
    c
  end

  @rows %{
    "a" => %{key: "a", content_md5: "aaa"},
    "b" => %{key: "b", content_md5: "bbb"}
  }

  # a marking retire, as a host with a tombstone policy writes it
  defp tombstone(keys) do
    for key <- keys, row = Enum.find(Ash.read!(Docs), &(&1.key == key)) do
      row |> Ash.Changeset.for_update(:update, %{status: "tombstoned"}) |> Ash.update!()
    end
  end

  defp live_keys do
    Docs |> Ash.read!() |> Enum.reject(&(&1.status == "tombstoned")) |> Enum.map(& &1.key)
  end

  # a node declaring the marking policy, rather than passing :retire per call
  defmodule Marked do
    use Ash.Resource,
      domain: ReactiveDag.RevivalTest.Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :content_md5, :string, public?: true
      attribute :status, :string, public?: true
      attribute :fingerprint, :string, public?: true
    end

    actions do
      defaults [:read, :destroy, update: [:status]]

      create :upsert do
        upsert?(true)
        accept([:key, :content_md5, :status, :fingerprint])
      end
    end

    reactive do
      id(:marked)
      leaf?(true)
      fingerprint([:content_md5])
      retain_if_vanished(mark: &ReactiveDag.RevivalTest.mark_gone/1)
    end
  end

  @doc false
  def mark_gone(keys) do
    for key <- keys, row = Enum.find(Ash.read!(Marked), &(&1.key == key)) do
      row |> Ash.Changeset.for_update(:update, %{status: "tombstoned"}) |> Ash.update!()
    end
  end

  describe "a returning key propagates" do
    test "under the DECLARED mark policy" do
      # this was once only a warning, and the guard keyed off `opts[:retire]` —
      # so the declared spelling silently reported nothing at all
      [cell] = ReactiveDag.Node.cells(Marked)
      for row <- Ash.read!(Marked), do: Ash.destroy!(row)

      live = fn ->
        Marked |> Ash.read!() |> Enum.reject(&(&1.status == "tombstoned")) |> Enum.map(& &1.key)
      end

      {:ok, _, _} = Rows.reconcile(cell, ["a", "b"], upsert: &Map.get(@rows, &1))
      {:ok, _, _} = Rows.reconcile(cell, ["a"], upsert: &Map.get(@rows, &1), current: live.())

      {:ok, changed, _} =
        Rows.reconcile(cell, ["a", "b"], upsert: &Map.get(@rows, &1), current: live.())

      assert changed == ["b"], "b came back; its bytes never moved"
    end

    test "under a per-call :retire fun" do
      {:ok, _, _} = Rows.reconcile(cell(), ["a", "b"], upsert: &Map.get(@rows, &1))

      {:ok, _, _} =
        Rows.reconcile(cell(), ["a"],
          upsert: &Map.get(@rows, &1),
          current: live_keys(),
          retire: &tombstone/1
        )

      # b comes back byte-identical: the fingerprint cannot see it, the baseline can
      {:ok, changed, _} =
        Rows.reconcile(cell(), ["a", "b"],
          upsert: &Map.get(@rows, &1),
          current: live_keys(),
          retire: &tombstone/1
        )

      assert changed == ["b"]
    end
  end

  describe "and nothing else is treated as one" do
    test "no marking policy — the library destroyed the row, so it is a CREATE" do
      {:ok, _, _} = Rows.reconcile(cell(), ["a", "b"], upsert: &Map.get(@rows, &1))
      {:ok, _, _} = Rows.reconcile(cell(), ["a"], upsert: &Map.get(@rows, &1))

      # b's row was destroyed, so writing it again is a new row — already
      # reported by the ordinary path, and it must not be counted twice
      {:ok, changed, _} = Rows.reconcile(cell(), ["a", "b"], upsert: &Map.get(@rows, &1))

      assert changed == ["b"]
    end

    test "no supplied :current — every row is the baseline, so nothing looks absent" do
      {:ok, _, _} = Rows.reconcile(cell(), ["a", "b"], upsert: &Map.get(@rows, &1))

      {:ok, changed, _} =
        Rows.reconcile(cell(), ["a", "b"],
          upsert: &Map.get(@rows, &1),
          retire: &tombstone/1
        )

      assert changed == []
    end

    test "the boolean form — the host decides, and is not second-guessed" do
      {:ok, _, _} = Rows.reconcile(cell(), ["a", "b"], upsert: &Map.get(@rows, &1))
      {:ok, _, _} =
        Rows.reconcile(cell(), ["a"],
          upsert: &Map.get(@rows, &1),
          current: live_keys(),
          retire: &tombstone/1
        )

      {:ok, changed, _} =
        Rows.reconcile(cell(), ["a", "b"],
          # the host says "revived" itself
          upsert: fn key -> key == "b" end,
          current: live_keys(),
          retire: &tombstone/1
        )

      assert changed == ["b"], "reported once, not twice"
    end

    test "a key whose bytes DID move is reported once, not twice" do
      {:ok, _, _} = Rows.reconcile(cell(), ["a", "b"], upsert: &Map.get(@rows, &1))
      {:ok, _, _} =
        Rows.reconcile(cell(), ["a"],
          upsert: &Map.get(@rows, &1),
          current: live_keys(),
          retire: &tombstone/1
        )

      moved = Map.put(@rows, "b", %{key: "b", content_md5: "MOVED"})

      {:ok, changed, _} =
        Rows.reconcile(cell(), ["a", "b"],
          upsert: &Map.get(moved, &1),
          current: live_keys(),
          retire: &tombstone/1
        )

      assert changed == ["b"], "reported once — revived AND moved is still one change"
    end

    test "an ordinary unchanged poll reports nothing" do
      {:ok, _, _} = Rows.reconcile(cell(), ["a", "b"], upsert: &Map.get(@rows, &1))

      {:ok, changed, _} =
        Rows.reconcile(cell(), ["a", "b"],
          upsert: &Map.get(@rows, &1),
          current: live_keys(),
          retire: &tombstone/1
        )

      assert changed == [], "over-reporting here would recompute the graph every poll"
    end

    test "a key the host could not observe (nil) is not a revival" do
      {:ok, _, _} = Rows.reconcile(cell(), ["a", "b"], upsert: &Map.get(@rows, &1))
      {:ok, _, _} =
        Rows.reconcile(cell(), ["a"],
          upsert: &Map.get(@rows, &1),
          current: live_keys(),
          retire: &tombstone/1
        )

      {:ok, changed, _} =
        Rows.reconcile(cell(), ["a", "b"],
          upsert: fn key -> if key == "a", do: Map.get(@rows, key) end,
          current: live_keys(),
          retire: &tombstone/1
        )

      assert changed == [], "nil means 'could not look', not 'it came back'"
    end
  end
end

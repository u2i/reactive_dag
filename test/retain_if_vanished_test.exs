defmodule ReactiveDag.RetainIfVanishedTest do
  @moduledoc """
  `retain_if_vanished` (#81) — retirement MARKS a row instead of destroying it.

  The default is destruction, and for a derived node that is right: a unit whose
  inputs have gone produces nothing, and a stale derived row is indistinguishable
  from a live one. But a source-fed leaf over an upstream that can *withdraw*
  items often wants the opposite — a document mirror keeps what it fetched after
  the listing drops it.

  Supporting that through `:retire` + `:current` callbacks worked, but left the
  library ignorant of the fact underneath them, and one consequence was severe:
  **a revived row is invisible to a fingerprint**. It comes back carrying the
  same bytes it left with, so the comparison says "unchanged" when the honest
  answer is "it came back" — which forced a retain-if-vanish host onto the
  boolean `:upsert` form and made it re-implement the whole write path.

  These tests pin the three behaviours that follow from the declaration, and
  most of all that one.
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
      attribute :body, :string, public?: true
      attribute :content_md5, :string, public?: true
      attribute :status, :string, public?: true
      attribute :tombstoned_at, :utc_datetime_usec, public?: true
      attribute :fingerprint, :string, public?: true
    end

    actions do
      defaults [:read, :destroy, update: [:body, :content_md5, :status, :tombstoned_at, :fingerprint]]

      create :upsert do
        upsert?(true)
        accept([:key, :body, :content_md5, :status, :tombstoned_at, :fingerprint])
      end
    end

    reactive do
      id(:docs)
      leaf?(true)
      fingerprint([:content_md5])

      retain_if_vanished status: :status,
                         at: :tombstoned_at,
                         live: "present",
                         retired: "tombstoned"
    end
  end

  # the default: retirement destroys
  defmodule Plain do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Ets, extensions: [ReactiveDag.Node]

    ets do
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
      id(:plain)
      leaf?(true)
      fingerprint([:content_md5])
    end
  end

  setup do
    for r <- [Docs, Plain], row <- Ash.read!(r), do: Ash.destroy!(row)
    :ok
  end

  defp cell(mod) do
    [c] = ReactiveDag.Node.cells(mod)
    c
  end

  defp doc(key, md5), do: %{key: key, body: "body-#{key}", content_md5: md5}

  defp scan(mod, docs) do
    by_key = Map.new(docs, &{&1.key, &1})
    {:ok, changed} = Rows.reconcile(cell(mod), Map.keys(by_key), upsert: &Map.get(by_key, &1))
    Enum.sort(changed)
  end

  defp row(key), do: Docs |> Ash.read!() |> Enum.find(&(&1.key == key))

  describe "retirement marks rather than destroys" do
    test "a vanished key keeps its row, with the retired status and a timestamp" do
      scan(Docs, [doc("a", "aaa"), doc("b", "bbb")])

      assert scan(Docs, [doc("a", "aaa")]) == ["b"]

      # the row SURVIVES — that is the whole point
      b = row("b")
      assert b.status == "tombstoned"
      assert %DateTime{} = b.tombstoned_at
      assert b.body == "body-b", "the artifact we fetched is still ours"
    end

    test "without the declaration, retirement still destroys" do
      by_key = %{"a" => %{key: "a", content_md5: "aaa"}, "b" => %{key: "b", content_md5: "bbb"}}
      {:ok, _} = Rows.reconcile(cell(Plain), Map.keys(by_key), upsert: &Map.get(by_key, &1))

      {:ok, changed} = Rows.reconcile(cell(Plain), ["a"], upsert: fn k -> Map.get(by_key, k) end)

      assert changed == ["b"]
      assert Enum.map(Ash.read!(Plain), & &1.key) == ["a"]
    end

    test "an explicit :retire fun still wins — a host policy neither default covers" do
      test_pid = self()
      scan(Docs, [doc("a", "aaa"), doc("b", "bbb")])

      {:ok, changed} =
        Rows.reconcile(cell(Docs), ["a"],
          upsert: fn _ -> nil end,
          retire: fn keys -> send(test_pid, {:custom, keys}) end
        )

      assert changed == ["b"]
      assert_received {:custom, ["b"]}
      # neither marked nor destroyed — the host took it
      assert row("b").status == "present"
    end
  end

  describe "the baseline is live rows" do
    test "an already-retired key is not retired or reported a second time" do
      scan(Docs, [doc("a", "aaa"), doc("b", "bbb")])
      assert scan(Docs, [doc("a", "aaa")]) == ["b"]

      # b is still gone, but it was already retired — this must be quiet
      assert scan(Docs, [doc("a", "aaa")]) == []
    end

    test "the retirement timestamp is not rewritten on a later poll" do
      scan(Docs, [doc("a", "aaa"), doc("b", "bbb")])
      scan(Docs, [doc("a", "aaa")])
      first = row("b").tombstoned_at

      scan(Docs, [doc("a", "aaa")])

      assert row("b").tombstoned_at == first,
             "re-stamping would make 'when did we lose this?' unanswerable"
    end
  end

  describe "revival" do
    test "a returning row is CHANGED even though its fingerprint has not moved" do
      # the case that forced hosts onto the boolean form: same bytes, but its
      # liveness moved, and a fingerprint cannot see that
      scan(Docs, [doc("a", "aaa"), doc("b", "bbb")])
      scan(Docs, [doc("a", "aaa")])
      assert row("b").status == "tombstoned"

      assert scan(Docs, [doc("a", "aaa"), doc("b", "bbb")]) == ["b"]
    end

    test "a revived row is marked live again, not left tombstoned" do
      scan(Docs, [doc("a", "aaa"), doc("b", "bbb")])
      scan(Docs, [doc("a", "aaa")])
      scan(Docs, [doc("a", "aaa"), doc("b", "bbb")])

      b = row("b")

      assert b.status == "present",
             "left tombstoned, it would be excluded from its own baseline forever"
    end

    test "a live row whose bytes did not move is still unchanged" do
      # the guard against over-reporting: revival must not mean "always changed"
      scan(Docs, [doc("a", "aaa")])
      assert scan(Docs, [doc("a", "aaa")]) == []
    end

    test "a row's own status wins over the stamped default" do
      # the host may have a vocabulary beyond live/retired
      by_key = %{"a" => %{key: "a", content_md5: "aaa", status: "quarantined"}}
      {:ok, _} = Rows.reconcile(cell(Docs), ["a"], upsert: &Map.get(by_key, &1))

      assert row("a").status == "quarantined"
    end
  end

  describe "the declaration" do
    test "reaches the cell's meta" do
      assert cell(Docs).meta[:retain_if_vanished] == %{
               status: :status,
               at: :tombstoned_at,
               live: "present",
               retired: "tombstoned"
             }

      refute cell(Plain).meta[:retain_if_vanished]
    end

    test "naming a column the resource lacks raises, rather than silently never retiring" do
      err =
        assert_raise ArgumentError, fn ->
          defmodule NoColumn do
            use Ash.Resource,
              domain: ReactiveDag.RetainIfVanishedTest.Domain,
              data_layer: Ash.DataLayer.Ets,
              extensions: [ReactiveDag.Node]

            ets do
            end

            attributes do
              attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
            end

            actions do
              defaults [:read, :destroy]
              create :upsert, upsert?: true, accept: [:key]
            end

            reactive do
              id(:no_column)
              leaf?(true)
              retain_if_vanished status: :nope
            end
          end

          ReactiveDag.Node.cells(NoColumn)
        end

      msg = Exception.message(err)
      assert msg =~ ":nope"
      assert msg =~ "no such attribute"
    end

    test "`at:` is optional — status alone is a complete policy" do
      defmodule StatusOnly do
        use Ash.Resource,
          domain: ReactiveDag.RetainIfVanishedTest.Domain,
          data_layer: Ash.DataLayer.Ets,
          extensions: [ReactiveDag.Node]

        ets do
        end

        attributes do
          attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
          attribute :state, :string, public?: true
        end

        actions do
          defaults [:read, :destroy, update: [:state]]
          create :upsert, upsert?: true, accept: [:key, :state]
        end

        reactive do
          id(:status_only)
          leaf?(true)
          retain_if_vanished status: :state, live: "up", retired: "gone"
        end
      end

      [c] = ReactiveDag.Node.cells(StatusOnly)
      assert c.meta[:retain_if_vanished].at == nil

      {:ok, _} = Rows.reconcile(c, ["x"], upsert: fn k -> %{key: k} end)
      {:ok, changed} = Rows.reconcile(c, [], upsert: fn _ -> nil end)

      assert changed == ["x"]
      assert [%{state: "gone"}] = Ash.read!(StatusOnly)
    end
  end
end

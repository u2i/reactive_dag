defmodule ReactiveDag.RetainIfVanishedTest do
  @moduledoc """
  `retain_if_vanished true` (#85) — keep the row when a scan stops returning its
  key.

  Destroying is right for a DERIVED node: a row whose inputs are gone is stale,
  and a stale derived row is indistinguishable from a live one. It is often wrong
  for a source-fed LEAF, where the upstream withdrew the listing but the artifact
  you fetched is still yours — and may not be re-fetchable.

  The consequential decision is that a retained key is **not reported as
  changed**. Two reasons, and the second is the one that would bite:

    * nothing changed. The row is still there, unmodified — from a consumer's
      side the disappearance is invisible, because the thing did not disappear.
    * reporting it would report it *again* on every subsequent poll, forever.
      Nothing marks the key as already handled, so the subtraction keeps finding
      it. That is the re-reporting trap that sank the richer design in #81.
  """
  use ReactiveDag.PostgresCase

  alias ReactiveDag.Fixtures.RetainGraph
  alias ReactiveDag.Fixtures.RetainGraph.{Docs, Marked, Plain}

  alias ReactiveDag.Node.Rows

  setup_all do
    migrate!(RetainGraph.Domain)
    :ok
  end

  setup do
    pid = Ecto.Adapters.SQL.Sandbox.start_owner!(ReactiveDag.TestRepo, shared: true)
    on_exit(fn -> Ecto.Adapters.SQL.Sandbox.stop_owner(pid) end)

    truncate(RetainGraph.tables())
    for r <- [Docs, Plain], row <- Ash.read!(r), do: Ash.destroy!(row)
    :ok
  end

  @doc false
  # a host's marking policy: keep the row, record that the upstream dropped it

  defp cell(mod) do
    [c] = ReactiveDag.Node.cells(mod)
    c
  end

  @docs %{
    "a" => %{key: "a", body: "body-a", content_md5: "aaa"},
    "b" => %{key: "b", body: "body-b", content_md5: "bbb"}
  }

  defp scan(mod, keys, opts \\ []) do
    {:ok, changed, _} =
      Rows.reconcile(cell(mod), keys, [upsert: &Map.get(@docs, &1)] ++ opts)

    Enum.sort(changed)
  end

  defp keys(mod), do: mod |> Ash.read!() |> Enum.map(& &1.key) |> Enum.sort()

  describe "the row survives" do
    test "a key the scan stops returning keeps its row, with everything on it" do
      scan(Docs, ["a", "b"])

      assert scan(Docs, ["a"]) == []
      assert keys(Docs) == ["a", "b"]

      b = Docs |> Ash.read!() |> Enum.find(&(&1.key == "b"))
      assert b.body == "body-b", "the artifact we fetched is still ours"
      assert b.content_md5 == "bbb"
    end

    test "without the declaration, the row is destroyed — the default is unchanged" do
      scan(Plain, ["a", "b"])

      assert scan(Plain, ["a"]) == ["b"]
      assert keys(Plain) == ["a"]
    end
  end

  describe "a retained key is not a change" do
    test "it is not reported, because nothing about the row moved" do
      scan(Docs, ["a", "b"])
      assert scan(Docs, ["a"]) == []
    end

    test "re-polling stays quiet — no re-reporting on every poll, forever" do
      # the trap: with the row still present and nothing marking it handled, a
      # reporting implementation would find and report it again every time
      scan(Docs, ["a", "b"])

      assert scan(Docs, ["a"]) == []
      assert scan(Docs, ["a"]) == []
      assert scan(Docs, ["a"]) == []
    end

    test "a key that comes back is quiet too — it never left" do
      scan(Docs, ["a", "b"])
      scan(Docs, ["a"])

      # b's row was never touched, and its fingerprint has not moved, so there
      # is genuinely nothing to report
      assert scan(Docs, ["a", "b"]) == []
    end

    test "a key that comes back CHANGED is still reported" do
      scan(Docs, ["a", "b"])
      scan(Docs, ["a"])

      moved = Map.put(@docs, "b", %{key: "b", body: "new", content_md5: "MOVED"})

      {:ok, changed, _} = Rows.reconcile(cell(Docs), ["a", "b"], upsert: &Map.get(moved, &1))

      assert changed == ["b"], "retention must not mask a real content change"
    end

    test "ordinary changes are unaffected" do
      scan(Docs, ["a", "b"])

      moved = Map.put(@docs, "a", %{key: "a", body: "edited", content_md5: "CHANGED"})
      {:ok, changed, _} = Rows.reconcile(cell(Docs), ["a", "b"], upsert: &Map.get(moved, &1))

      assert changed == ["a"]
    end
  end

  describe "an explicit :retire fun" do
    test "still runs, and its keys still propagate — the host did something" do
      test_pid = self()
      scan(Docs, ["a", "b"])

      changed = scan(Docs, ["a"], retire: fn keys -> send(test_pid, {:tombstoned, keys}) end)

      assert changed == ["b"], "the host acted, so downstream should hear about it"
      assert_received {:tombstoned, ["b"]}
      assert keys(Docs) == ["a", "b"], "and it still did not destroy"
    end
  end

  test "the declaration normalises to a policy in the cell's meta" do
    assert cell(Docs).meta[:retain_if_vanished] == :keep
    refute cell(Plain).meta[:retain_if_vanished]
  end

  describe "mark: — keep the row AND say the upstream dropped it" do
    setup do
      for row <- Ash.read!(Marked), do: Ash.destroy!(row)
      :ok
    end

    defp marked_cell do
      [c] = ReactiveDag.Node.cells(Marked)
      c
    end

    defp scan_marked(keys) do
      {:ok, changed, _} = Rows.reconcile(marked_cell(), keys, upsert: &Map.get(@docs, &1))
      Enum.sort(changed)
    end

    test "the row survives, and the mark fun records the drop" do
      scan_marked(["a", "b"])
      scan_marked(["a"])

      b = Marked |> Ash.read!() |> Enum.find(&(&1.key == "b"))
      assert b.status == "tombstoned"
      assert b.content_md5 == "bbb", "the artifact is still ours"
    end

    test "the key DOES propagate — something was written, so downstream hears" do
      scan_marked(["a", "b"])

      assert scan_marked(["a"]) == ["b"]
    end

    test "which is the whole difference from `true`" do
      # same operation, one question: did we write something to say it is gone?
      scan(Docs, ["a", "b"])
      scan_marked(["a", "b"])

      assert scan(Docs, ["a"]) == [], "keep: nothing written, nothing reported"
      assert scan_marked(["a"]) == ["b"], "mark: written, so reported"
    end

    test "it normalises to {:mark, fun} in meta" do
      assert {:mark, fun} = marked_cell().meta[:retain_if_vanished]
      assert is_function(fun, 1)
    end
  end

  test "a malformed declaration raises, naming the two forms" do
    err =
      assert_raise ArgumentError, fn ->
        defmodule BadRetain do
          use Ash.Resource,
            domain: ReactiveDag.Fixtures.RetainGraph.Domain,
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
            id :bad_retain
            leaf? true
            retain_if_vanished status: :status
          end
        end

        ReactiveDag.Node.cells(BadRetain)
      end

    assert Exception.message(err) =~ "`true` or `mark: fun/1`"
  end
end

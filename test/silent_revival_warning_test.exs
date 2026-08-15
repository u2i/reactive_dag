defmodule ReactiveDag.SilentRevivalWarningTest do
  @moduledoc """
  The silent-revival warning (#82).

  `changed?` for the row form is a fingerprint comparison. A host that retires by
  MARKING — a custom `:retire` that tombstones rather than destroys — has one
  case the fingerprint cannot see: a retired row comes back carrying the bytes it
  left with. Its liveness moved; its content did not. The library reports
  nothing, the revival never propagates, and there is no dirty key or drain step
  to notice.

  The library cannot fix it — it does not know what the host's retirement marks,
  and `changed?` is deliberately the fingerprint's business. But the *shape* is
  visible: a key the scan returned, which the host's own baseline excluded, whose
  fingerprint has not moved. So it warns rather than leaving a correctness gap
  that only surfaces as stale downstream rows much later.

  Most of these tests assert the warning does NOT fire. That is the point: a
  warning that cries wolf on the ordinary path would be turned off, and then it
  would not be there for the case it exists for.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

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
      domain: ReactiveDag.SilentRevivalWarningTest.Domain,
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
      retain_if_vanished(mark: &ReactiveDag.SilentRevivalWarningTest.mark_gone/1)
    end
  end

  @doc false
  def mark_gone(keys) do
    for key <- keys, row = Enum.find(Ash.read!(Marked), &(&1.key == key)) do
      row |> Ash.Changeset.for_update(:update, %{status: "tombstoned"}) |> Ash.update!()
    end
  end

  describe "it fires on the shape it exists for" do
    test "the DECLARED mark policy, not just a per-call :retire" do
      # regression: the guard originally keyed off `opts[:retire]`, so unifying
      # keep/mark into a node declaration silently stopped it firing for the very
      # case it was written for.
      [cell] = ReactiveDag.Node.cells(Marked)
      for row <- Ash.read!(Marked), do: Ash.destroy!(row)

      live = fn ->
        Marked |> Ash.read!() |> Enum.reject(&(&1.status == "tombstoned")) |> Enum.map(& &1.key)
      end

      {:ok, _} = Rows.reconcile(cell, ["a", "b"], upsert: &Map.get(@rows, &1))
      {:ok, _} = Rows.reconcile(cell, ["a"], upsert: &Map.get(@rows, &1), current: live.())

      log =
        capture_log(fn ->
          {:ok, changed} =
            Rows.reconcile(cell, ["a", "b"], upsert: &Map.get(@rows, &1), current: live.())

          assert changed == []
        end)

      assert log =~ "report UNCHANGED"
      assert log =~ "MARKS rows"
    end

    test "a marked-retired key returns with unmoved bytes" do
      # seed both, then withdraw b and mark it
      {:ok, _} = Rows.reconcile(cell(), ["a", "b"], upsert: &Map.get(@rows, &1))

      {:ok, _} =
        Rows.reconcile(cell(), ["a"],
          upsert: &Map.get(@rows, &1),
          current: live_keys(),
          retire: &tombstone/1
        )

      # b comes back, byte-identical. Its fingerprint has not moved, so the
      # library reports nothing — and says so.
      log =
        capture_log(fn ->
          {:ok, changed} =
            Rows.reconcile(cell(), ["a", "b"],
              upsert: &Map.get(@rows, &1),
              current: live_keys(),
              retire: &tombstone/1
            )

          assert changed == [], "the gap itself: the revival does not propagate"
        end)

      assert log =~ "report UNCHANGED"
      assert log =~ ~s(["b"])
      assert log =~ "reactive_dag#82"
    end
  end

  describe "it stays quiet everywhere else" do
    test "no custom :retire — the library destroys, so nothing can come back" do
      {:ok, _} = Rows.reconcile(cell(), ["a", "b"], upsert: &Map.get(@rows, &1))
      {:ok, _} = Rows.reconcile(cell(), ["a"], upsert: &Map.get(@rows, &1))

      log =
        capture_log(fn ->
          {:ok, _} = Rows.reconcile(cell(), ["a", "b"], upsert: &Map.get(@rows, &1))
        end)

      refute log =~ "report UNCHANGED"
    end

    test "no supplied :current — the baseline is every row, so nothing is 'absent'" do
      {:ok, _} = Rows.reconcile(cell(), ["a", "b"], upsert: &Map.get(@rows, &1))

      log =
        capture_log(fn ->
          {:ok, _} =
            Rows.reconcile(cell(), ["a", "b"],
              upsert: &Map.get(@rows, &1),
              retire: &tombstone/1
            )
        end)

      refute log =~ "report UNCHANGED"
    end

    test "the boolean form — the host already decides, which is the workaround" do
      {:ok, _} = Rows.reconcile(cell(), ["a", "b"], upsert: &Map.get(@rows, &1))
      {:ok, _} =
        Rows.reconcile(cell(), ["a"],
          upsert: &Map.get(@rows, &1),
          current: live_keys(),
          retire: &tombstone/1
        )

      log =
        capture_log(fn ->
          {:ok, changed} =
            Rows.reconcile(cell(), ["a", "b"],
              # the host says "revived" itself — exactly what #82 is a workaround for
              upsert: fn key -> key == "b" end,
              current: live_keys(),
              retire: &tombstone/1
            )

          assert changed == ["b"]
        end)

      refute log =~ "report UNCHANGED"
    end

    test "a key whose bytes DID move is reported, so there is nothing to warn about" do
      {:ok, _} = Rows.reconcile(cell(), ["a", "b"], upsert: &Map.get(@rows, &1))
      {:ok, _} =
        Rows.reconcile(cell(), ["a"],
          upsert: &Map.get(@rows, &1),
          current: live_keys(),
          retire: &tombstone/1
        )

      moved = Map.put(@rows, "b", %{key: "b", content_md5: "MOVED"})

      log =
        capture_log(fn ->
          {:ok, changed} =
            Rows.reconcile(cell(), ["a", "b"],
              upsert: &Map.get(moved, &1),
              current: live_keys(),
              retire: &tombstone/1
            )

          assert changed == ["b"]
        end)

      refute log =~ "report UNCHANGED"
    end

    test "an ordinary unchanged poll — every key is live, nothing is absent" do
      {:ok, _} = Rows.reconcile(cell(), ["a", "b"], upsert: &Map.get(@rows, &1))

      log =
        capture_log(fn ->
          {:ok, changed} =
            Rows.reconcile(cell(), ["a", "b"],
              upsert: &Map.get(@rows, &1),
              current: live_keys(),
              retire: &tombstone/1
            )

          assert changed == []
        end)

      refute log =~ "report UNCHANGED",
             "the routine path must stay silent or the warning gets turned off"
    end

    test "a key the host could not observe (nil) is not a revival" do
      {:ok, _} = Rows.reconcile(cell(), ["a", "b"], upsert: &Map.get(@rows, &1))
      {:ok, _} =
        Rows.reconcile(cell(), ["a"],
          upsert: &Map.get(@rows, &1),
          current: live_keys(),
          retire: &tombstone/1
        )

      log =
        capture_log(fn ->
          {:ok, _} =
            Rows.reconcile(cell(), ["a", "b"],
              upsert: fn key -> if key == "a", do: Map.get(@rows, key) end,
              current: live_keys(),
              retire: &tombstone/1
            )
        end)

      refute log =~ "report UNCHANGED", "nil means 'could not look', not 'unchanged'"
    end
  end
end

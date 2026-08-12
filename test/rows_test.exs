defmodule ReactiveDag.RowsTest do
  @moduledoc """
  `ReactiveDag.Node.Rows` — a cell's own rows, addressed by CELL KEY.

  This is the read side of what the payload loop writes, and the thing that
  replaced reading the coordination tuple for results. The property that
  matters: the key it reads under is the same key the DAG marks dirty and the
  same key `Payload` wrote — a single-column node reads that column, an
  identity-keyed node re-serializes its identity fields with `"|"`. If those two
  derivations ever drifted, a status histogram would silently be keyed by
  something no claim could ever name.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.Node.Rows

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      allow_unregistered?(true)
    end
  end

  # single-column key, with a status
  defmodule Health do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Ets, extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :status, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        accept([:key, :status])
      end
    end

    reactive do
      id(:health)
      leaf?(true)
    end
  end

  # composite PK → identity-keyed, and NO status column (a rollup, not a verdict)
  defmodule Rollup do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Ets, extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute :fund, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :fy, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :total, :float, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        accept([:fund, :fy, :total])
      end
    end

    reactive do
      id(:rollup)
      leaf?(true)
    end
  end

  # a node that writes elsewhere — no attributes of its own
  defmodule Elsewhere do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Simple,
      extensions: [ReactiveDag.Node]

    reactive do
      id(:elsewhere)
      leaf?(true)
    end
  end

  setup do
    for r <- [Health, Rollup], row <- Ash.read!(r), do: Ash.destroy!(row)

    seed(Health, %{key: "travel", status: "failing"})
    seed(Health, %{key: "meals", status: "present"})
    seed(Health, %{key: "lodging", status: "present"})
    seed(Rollup, %{fund: "gf", fy: "2025", total: 10.0})
    seed(Rollup, %{fund: "water", fy: "2025", total: 20.0})

    :ok
  end

  defp seed(resource, attrs),
    do: resource |> Ash.Changeset.for_create(:upsert, attrs) |> Ash.create!()

  defp cell(mod) do
    [c] = ReactiveDag.Node.cells(mod)
    c
  end

  describe "keying" do
    test "a single-column node reads its payload key" do
      keys = Health |> cell() |> Rows.all() |> Enum.map(& &1.key) |> Enum.sort()
      assert keys == ["lodging", "meals", "travel"]
    end

    test "an identity-keyed node re-serializes its identity fields — the DAG's own key" do
      keys = Rollup |> cell() |> Rows.all() |> Enum.map(& &1.key) |> Enum.sort()

      # exactly the "gf|2025" shape a claim on this cell would carry
      assert keys == ["gf|2025", "water|2025"]
    end

    test "the record rides along, so a caller can read any column" do
      [row] = Rollup |> cell() |> Rows.all() |> Enum.filter(&(&1.key == "gf|2025"))
      assert row.record.total == 10.0
    end
  end

  describe "status" do
    test "status_histogram/1 counts a node's statuses" do
      assert Rows.status_histogram(cell(Health)) == %{"failing" => 1, "present" => 2}
    end

    test "a node with NO status column reports nil, not an empty map" do
      # the count is still the truth about how many units the cell holds —
      # reporting %{} would say "no keys", which is a different claim
      assert Rows.status_histogram(cell(Rollup)) == %{nil => 2}
    end

    test "keys_by_status/3 samples, sorted and capped" do
      assert Rows.keys_by_status(cell(Health), ["present"]) == ["lodging", "meals"]
      assert Rows.keys_by_status(cell(Health), ["present"], limit: 1) == ["lodging"]
      assert Rows.keys_by_status(cell(Health), ["failing"]) == ["travel"]
    end

    test "asking for a status nothing has is empty, not an error" do
      assert Rows.keys_by_status(cell(Health), ["exploded"]) == []
    end
  end

  test "a node with no rows of its own reads as empty rather than raising" do
    assert Rows.all(cell(Elsewhere)) == []
    assert Rows.status_histogram(cell(Elsewhere)) == %{}
  end

  test "Verdict.for_cell/2 rolls the same read into one answer" do
    verdict = ReactiveDag.Verdict.for_cell(cell(Health))

    assert verdict.status == :findings
    assert verdict.failing == 1
    assert verdict.sample == ["travel"]
  end

  test "Verdict.for_cell/2 on an all-present cell is green" do
    Health |> Ash.read!() |> Enum.filter(&(&1.status == "failing")) |> Enum.each(&Ash.destroy!/1)

    assert ReactiveDag.Verdict.for_cell(cell(Health)).status == :green
  end

  test "Verdict.for_cell/2 on an empty cell is :unknown, not green" do
    Health |> Ash.read!() |> Enum.each(&Ash.destroy!/1)

    assert ReactiveDag.Verdict.for_cell(cell(Health)).status == :unknown
  end

  describe "reconcile/3 — the leaf-write skeleton" do
    test "retires what vanished, and propagates both sides" do
      # the scan found travel + a new one; meals and lodging are gone
      {:ok, changed} =
        Rows.reconcile(cell(Health), ["travel", "parking"],
          upsert: fn key -> key == "parking" end
        )

      # parking was written; meals + lodging vanished. travel was re-seen unchanged.
      assert Enum.sort(changed) == ["lodging", "meals", "parking"]

      # ...and the vanished rows are actually gone from the resource
      assert Rows.keys_by_status(cell(Health), ["failing", "present"]) == ["travel"]
    end

    test "a retire fun replaces destruction — the retain-if-vanish policy" do
      test_pid = self()

      {:ok, changed} =
        Rows.reconcile(cell(Health), ["travel"],
          upsert: fn _ -> false end,
          retire: fn keys -> send(test_pid, {:tombstoned, Enum.sort(keys)}) end
        )

      assert Enum.sort(changed) == ["lodging", "meals"]
      assert_received {:tombstoned, ["lodging", "meals"]}

      # ...and nothing was destroyed
      assert length(Ash.read!(Health)) == 3
    end

    test "an explicit :current narrows the baseline — already-retired keys stay put" do
      {:ok, changed} =
        Rows.reconcile(cell(Health), ["travel"],
          upsert: fn _ -> false end,
          current: ["travel", "meals"]
        )

      # lodging was not in the baseline, so it is neither retired nor reported
      assert changed == ["meals"]
      assert "lodging" in Enum.map(Rows.all(cell(Health)), & &1.key)
    end

    test "nothing vanished → nothing retired, only real upserts propagate" do
      {:ok, changed} =
        Rows.reconcile(cell(Health), ["travel", "meals", "lodging"],
          upsert: fn key -> key == "meals" end
        )

      assert changed == ["meals"]
      assert length(Ash.read!(Health)) == 3
    end

    test "an identity-keyed leaf reconciles on its serialized keys" do
      {:ok, changed} =
        Rows.reconcile(cell(Rollup), ["gf|2025"], upsert: fn _ -> false end)

      assert changed == ["water|2025"]
      assert Enum.map(Rows.all(cell(Rollup)), & &1.key) == ["gf|2025"]
    end
  end
end

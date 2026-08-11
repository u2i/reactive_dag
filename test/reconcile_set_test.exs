defmodule ReactiveDag.ReconcileSetTest do
  @moduledoc """
  `ReactiveDag.Tuple.reconcile_set/3` — the bulk variant of `reconcile/3`:
  ONE `:upsert_all` call for the whole want set (a set-op's single bulk
  statement), the same current − want vanish math, host-seamed retire. Tested
  through the pure variation points (`:current` + a retire fun) — the spine
  SQL behind the defaults is `all_keys`/`delete`, covered elsewhere.
  """
  use ExUnit.Case, async: true

  alias ReactiveDag.Tuple

  test "upsert_all is called ONCE with the whole want set; changed ++ vanished returned" do
    parent = self()

    {:ok, changed} =
      Tuple.reconcile_set("cell", ["k1", "k2", "k3"],
        upsert_all: fn keys ->
          send(parent, {:upsert_all, Enum.sort(keys)})
          ["k2"]
        end,
        current: ["k1", "k2", "gone"],
        retire: fn keys -> send(parent, {:retired, keys}) end
      )

    assert_received {:upsert_all, ["k1", "k2", "k3"]}
    assert_received {:retired, ["gone"]}
    assert Enum.sort(changed) == ["gone", "k2"]
  end

  test "an empty want set skips upsert_all entirely; everything current vanishes" do
    parent = self()

    {:ok, changed} =
      Tuple.reconcile_set("cell", [],
        upsert_all: fn _ -> flunk("must not be called for an empty want set") end,
        current: ["a", "b"],
        retire: fn keys -> send(parent, {:retired, Enum.sort(keys)}) end
      )

    assert_received {:retired, ["a", "b"]}
    assert Enum.sort(changed) == ["a", "b"]
  end

  test "nothing vanished → retire not called; only upsert changes propagate" do
    {:ok, changed} =
      Tuple.reconcile_set("cell", ["k1"],
        upsert_all: fn _ -> [] end,
        current: ["k1"],
        retire: fn _ -> flunk("nothing vanished") end
      )

    assert changed == []
  end
end

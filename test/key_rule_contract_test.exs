defmodule ReactiveDag.KeyRuleContractTest do
  @moduledoc """
  A key rule that breaks its contract must not take the cascade down.

  `rule/3..5` returns `:all | {:keys, [key]}`. `Graph.claims_for/5` used to
  `case` on exactly those two and let anything else fall through unmatched,
  where it travelled on as though it were a key list and detonated several
  frames later in `Cascade.entries_for/6` on `"*" in mapped_keys`:

      ** (Protocol.UndefinedError) protocol Enumerable not implemented for Atom
      Got value: :error
          (reactive_dag) lib/reactive_dag/cascade.ex:757: entries_for/6

  Observed in production, failing resumptions. `:error` is this module's own
  internal sentinel for a key that does not fit its grain — `KeyRule` guards it
  in the paths that produce it, but a guard that has to be remembered at every
  site is one that will eventually be missed, and the stack named neither the
  rule nor the cell.

  The fallback is `:all` rather than a raise, matching every other degradation
  in `KeyRule`: a claim that cannot be narrowed is answered WIDE — correct and
  expensive, never wrong and cheap.
  """
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      allow_unregistered?(true)
    end
  end

  defmodule Lines do
    use Ash.Resource,
      domain: ReactiveDag.KeyRuleContractTest.Domain,
      data_layer: Ash.DataLayer.Simple,
      extensions: [ReactiveDag.Node]

    reactive do
      id(:lines)
      op(:leaf)
      leaf?(true)
    end
  end

  defmodule Totals do
    use Ash.Resource,
      domain: ReactiveDag.KeyRuleContractTest.Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
      private?(true)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
    end

    actions do
      defaults([:read, :destroy])
      create(:upsert, upsert?: true, accept: [:key])
    end

    reactive do
      id(:totals)
      op(:fold)
      depends_on([:lines])
      compute(ReactiveDag.KeyRuleContractTest.Noop)
    end
  end

  defmodule Noop do
    @moduledoc false
    def recompute(_cell, _keys, _opts \\ []), do: {:ok, []}
  end

  # The bug, reproduced: a rule returning this module's own internal sentinel.
  defmodule ErroringRule do
    @moduledoc false
    def rule(_parent, _child, _changed), do: :error
  end

  # The other shape worth covering — a well-formed tuple carrying a non-list.
  defmodule BadKeysRule do
    @moduledoc false
    def rule(_parent, _child, _changed), do: {:keys, :nope}
  end

  defp plan, do: ReactiveDag.Node.graph([Lines, Totals])

  test "a bare atom degrades to a whole-cell claim instead of crashing" do
    log =
      capture_log(fn ->
        assert [{"totals", ["*"]}] =
                 ReactiveDag.Graph.claims_for(plan(), "lines", ["l1"], ErroringRule)
      end)

    assert log =~ "returned :error"
    assert log =~ "lines -> totals",
           "the warning must name the edge — not naming it is what made the " <>
             "original crash hard to attribute"
  end

  test "`{:keys, not_a_list}` degrades too" do
    capture_log(fn ->
      assert [{"totals", ["*"]}] =
               ReactiveDag.Graph.claims_for(plan(), "lines", ["l1"], BadKeysRule)
    end)
  end

  test "the contract itself still passes through untouched" do
    # The fallback must not swallow correct returns.
    assert [{"totals", ["l1"]}] =
             ReactiveDag.Graph.claims_for(plan(), "lines", ["l1"], ReactiveDag.Node.KeyRule)
  end

end

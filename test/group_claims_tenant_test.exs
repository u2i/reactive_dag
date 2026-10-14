defmodule ReactiveDag.GroupClaimsTenantTest do
  @moduledoc """
  A `:group` claim resolves by READING the changed rows, and that read is scoped
  to the plan's tenant.

  ## Why this was broken and invisible

  `:identity` and `:all` never read anything: one passes keys through, the other
  discards them. `:group` is the only rule that queries — it evaluates the
  combinator's own `group_by` against the changed rows to name the units they
  belong to.

  So a host running a graph per tenant could declare `recompute_by :unit, from:
  :field`, get `key_rule :group`, and have propagation RAISE — because the
  lookup read a tenanted resource with no tenant. Every other rule worked, and
  the failure appeared only on the narrowing declaration a host reaches for to
  make a fold cheaper.

  Worse than a raise would have been the near miss: had the resource been
  untenanted, the same read would have found every tenant's rows and claimed
  units belonging to graphs the propagation was not about.

  The scope travels in the process dictionary rather than the argument list —
  `rule/5` sets it — because the two reads sit behind several private functions
  that exist to express the GROUPING, and threading a tenant through each would
  put the plan's identity into signatures that are otherwise about fields.
  """
  use ReactiveDag.PostgresCase

  alias ReactiveDag.Fixtures.GroupClaimsTenantGraph
  alias ReactiveDag.Fixtures.GroupClaimsTenantGraph.{Lines, Rollup}

  setup_all do
    migrate!(GroupClaimsTenantGraph.Domain)
    :ok
  end

  setup do
    pid = Ecto.Adapters.SQL.Sandbox.start_owner!(ReactiveDag.TestRepo, shared: true)
    on_exit(fn -> Ecto.Adapters.SQL.Sandbox.stop_owner(pid) end)

    truncate(GroupClaimsTenantGraph.tables())
    :ok
  end

  defp seed(org, line_key, fund, amount) do
    Ash.create!(Lines, %{line_key: line_key, fund: fund, amount: amount},
      action: :upsert,
      tenant: org
    )
  end

  defp plan(tenant), do: ReactiveDag.Node.graph([Lines, Rollup], tenant: tenant)

  test "the declaration lowers to `:group`" do
    # If this stops being true the rest of the file is asserting nothing.
    assert plan("org_a").cells["rollup"].meta[:key_rule] == :group
  end

  test "a changed line claims its FUND, not the whole cell" do
    seed("org_a", "l1", "A", 10)

    assert ReactiveDag.Graph.claims_for(plan("org_a"), "lines", ["l1"]) ==
             [{"rollup", ["A"]}]
  end

  test "the lookup is scoped — another tenant's identical key does not leak" do
    # The same `line_key` in both graphs, in DIFFERENT funds. An unscoped read
    # returns both rows and claims both units; a scoped one claims only this
    # graph's.
    seed("org_a", "shared", "A", 10)
    seed("org_b", "shared", "ES", 99)

    assert ReactiveDag.Graph.claims_for(plan("org_a"), "lines", ["shared"]) ==
             [{"rollup", ["A"]}]

    assert ReactiveDag.Graph.claims_for(plan("org_b"), "lines", ["shared"]) ==
             [{"rollup", ["ES"]}]
  end

  test "a key with no row in THIS tenant degrades to :all, not to another tenant's group" do
    # `org_b` holds the row; `org_a` does not. From `org_a`'s graph the key names
    # a row it cannot see, which is the deleted-row case: reprice everything it
    # might have left. Claiming `"ES"` — the unit the row belongs to in the OTHER
    # graph — would be silently wrong.
    seed("org_b", "only_b", "ES", 99)

    assert ReactiveDag.Graph.claims_for(plan("org_a"), "lines", ["only_b"]) ==
             [{"rollup", ["*"]}]
  end

  test "an untenanted plan still works — the scope is simply empty" do
    # Not a tenanted host: `Plan.frontier_opts/1` yields `tenant: "*"`, which Ash
    # ignores on an untenanted resource. Here the resources DO declare tenancy,
    # so this asserts the narrower thing that matters — no crash, and the rule
    # still resolves through the same path.
    seed("*", "star", "GF", 5)

    assert ReactiveDag.Graph.claims_for(ReactiveDag.Node.graph([Lines, Rollup]), "lines", [
             "star"
           ]) == [{"rollup", ["GF"]}]
  end
end

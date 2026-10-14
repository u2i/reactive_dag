defmodule ReactiveDag.Fixtures.DirtiesOnGraph do
  @moduledoc """
  `dirties_on` — a write to a resource marking the graph, with no cascade call.

  Six resources, because the question has six shapes worth separating:

    * `Expenses`         — TEMPORAL, and the main subject. A write opens a new
      period, and `dirties_on` must mark from the changeset rather than from a
      wall-clock read, or a backdated correction marks the wrong thing.
    * `CategoryTotals`   — the fold downstream of it.
    * `Attestations`     — a human verdict, marking on its own action.
    * `Rollups`          — COMPOSITE PK, so the cell key is the identity
      serialisation rather than a column.
    * `Quiet`            — declares no `dirties_on` at all; a write here must
      mark NOTHING, which is the negative case.
    * `TenantedExpenses` — the same marking under multitenancy, where a lost
      tenant marks `\"*\"` and the rows are not there.
  """

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      resource ReactiveDag.Fixtures.DirtiesOnGraph.Attestations
      resource ReactiveDag.Fixtures.DirtiesOnGraph.Expenses
      resource ReactiveDag.Fixtures.DirtiesOnGraph.Rollups
      resource ReactiveDag.Fixtures.DirtiesOnGraph.Quiet
      resource ReactiveDag.Fixtures.DirtiesOnGraph.CategoryTotals
      resource ReactiveDag.Fixtures.DirtiesOnGraph.TenantedExpenses
    end
  end

  # Marks AND schedules. Separate from `Expenses` so the default (mark only)
  # stays under test — an existing host must not start enqueueing.
  defmodule Attestations do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.DirtiesOnGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_do_attestations"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :verdict, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :create do
        accept([:key, :verdict])
      end
    end

    reactive do
      id(:attestations)
      leaf?(true)
      dirties_on([:create, :destroy])
      payload_key(:key)
    end
  end

  defmodule Expenses do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.DirtiesOnGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_do_expenses"
      repo(ReactiveDag.TestRepo)
    end

    # TEMPORAL. A suspension records when a change happened, and resuming reads
    # the row as of that instant — which is what lets a MOVED row claim both
    # the unit it joined and the one it left. The period holds the old
    # membership; current values alone only ever name the destination.
    temporal do
      attribute :valid_at
      recorded_at :recorded_at
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :category, :string, public?: true
      attribute :amount, :float, public?: true

      attribute :recorded_at, :utc_datetime_usec,
        public?: true,
        default: &DateTime.utc_now/0,
        update_default: &DateTime.utc_now/0
    end

    actions do
      defaults [:read, :destroy]

      create :create do
        accept([:key, :category, :amount])
      end

      update :revise do
        accept([:amount])
      end

      update :recategorise do
        accept([:category])
      end
    end

    reactive do
      id(:expenses)
      op(:source)
      leaf?(true)
      # THE FEATURE: writes here mark this cell dirty, with no host wiring
      dirties_on([:create, :update, :destroy])

      # A queue row references the version recording the change; the consumer
      # reads it back to learn WHAT moved. Both halves are the host's, because a
      # version resource is — `ash_paper_trail` is the usual supplier.
    end
  end

  # a composite-PK resource: the cell key is the identity serialization
  defmodule Rollups do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.DirtiesOnGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_do_rollups"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :fund, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :fy, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :total, :float, public?: true
    end

    actions do
      # :destroy exists for test cleanup, but is NOT in `dirties_on` below —
      # which is what the "only declared action types mark" test relies on.
      defaults [:read, :destroy]

      create :create do
        accept([:fund, :fy, :total])
      end
    end

    reactive do
      id(:rollups)
      op(:source)
      leaf?(true)
      dirties_on([:create])
    end
  end

  # opt-in: no `dirties_on`, so writes mark nothing
  defmodule Quiet do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.DirtiesOnGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_do_quiet"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
    end

    actions do
      defaults [:read]

      create :create do
        accept([:key])
      end
    end

    reactive do
      id(:quiet)
      op(:source)
      leaf?(true)
    end
  end

  defmodule CategoryTotals do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.DirtiesOnGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_do_category_totals"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :category, :string, public?: true
      attribute :total, :float, public?: true
      attribute :n, :integer, public?: true
    end

    identities do
      identity :by_key, [:key], pre_check_with: ReactiveDag.DirtiesOnTest.Domain
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        upsert_identity(:by_key)
        accept([:key, :category, :total, :n])
      end
    end

    reactive do
      id(:category_totals)
      op(:fold)

      recompute_by(:category, to: :expenses, from: :category)
      reduce(into: [sum: [amount: :total], count: :n])
    end
  end

  # A TENANTED write-fed leaf. `dirties_on` runs as an Ash change with no plan in
  # scope, so the tenant can only come off the changeset — which is where a
  # tenanted write already put it.
  defmodule TenantedExpenses do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.DirtiesOnGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_do_tenanted_expenses"
      repo(ReactiveDag.TestRepo)
    end

    multitenancy do
      strategy :attribute
      attribute :org_id
    end

    attributes do
      uuid_primary_key(:id)
      attribute :key, :string, allow_nil?: false, public?: true
      attribute :org_id, :string, public?: true
      attribute :amount, :float, public?: true
    end

    identities do
      identity :by_org_key, [:org_id, :key], pre_check_with: Domain
    end

    actions do
      defaults [:read, :destroy]

      create :create do
        accept([:key, :org_id, :amount])
      end
    end

    reactive do
      id(:tenanted_expenses)
      op(:source)
      leaf?(true)
      row_key([:org_id, :key])
      dirties_on([:create])
    end
  end

  @doc "expenses -> category_totals."
  def plan, do: ReactiveDag.Node.graph([Expenses, CategoryTotals])

  @doc "The tables this fixture owns."
  def tables,
    do: [
      "fx_do_attestations",
      "fx_do_expenses",
      "fx_do_rollups",
      "fx_do_quiet",
      "fx_do_category_totals",
      "fx_do_tenanted_expenses"
    ]
end

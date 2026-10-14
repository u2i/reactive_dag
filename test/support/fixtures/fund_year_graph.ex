defmodule ReactiveDag.Fixtures.FundYearGraph do
  @moduledoc """
  A COMPOSITE unit: `{fund, year}`, folded two different ways.

      lines ──> projected ──(recompute_by [fund: :fund, year: :year])──> rollup

  Two rollups over the same data, because the difference between them is a
  subject in its own right:

    * `KeyedRollup`   — carries a `"|"`-joined `rollup_key` column beside the
      two columns it duplicates. That column exists only as a LOOKUP handle:
      a payload write and a retire both need "the row for this unit".
    * `KeylessRollup` — no key column. The unit is named by the `row_key`
      columns the row already carries, and both the write and the retire find
      the row by those columns instead of by splitting a string.

  The retire is the half that fails silently — it returns false when it cannot
  find a row, so a stale derived row survives and reads exactly like a live
  one. That is why both shapes are kept rather than one being deleted as a
  duplicate.
  """

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      resource ReactiveDag.Fixtures.FundYearGraph.Lines
      resource ReactiveDag.Fixtures.FundYearGraph.Projected
      resource ReactiveDag.Fixtures.FundYearGraph.KeyedRollup
      resource ReactiveDag.Fixtures.FundYearGraph.KeylessRollup
    end
  end

  defmodule Lines do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.FundYearGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_fy_lines"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :fund, :string, public?: true
      attribute :year, :string, public?: true
      attribute :amount, :integer, public?: true
    end

    actions do
      defaults [:read, :destroy]
      create :upsert, upsert?: true, accept: [:key, :fund, :year, :amount]
    end

    reactive do
      id :lines
      leaf? true
      payload_key :key
    end
  end

  defmodule Projected do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.FundYearGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_fy_projected"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :fund, :string, public?: true
      attribute :year, :string, public?: true
      attribute :amount, :integer, public?: true
    end

    actions do
      defaults [:read, :destroy]
      create :upsert, upsert?: true, accept: [:key, :fund, :year, :amount]
    end

    reactive do
      id :projected
      op :map
      payload_key :key
      depends_on [:lines]
      compute ReactiveDag.Fixtures.FundYearGraph.Project
    end
  end

  defmodule KeyedRollup do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.FundYearGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_fy_keyed_rollup"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :rollup_key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :fund, :string, public?: true
      attribute :year, :string, public?: true
      attribute :total, :integer, public?: true
    end

    actions do
      defaults [:read, :destroy]
      create :upsert, upsert?: true, accept: [:rollup_key, :fund, :year, :total]
    end

    reactive do
      id :rollup
      op :fold
      payload_key :rollup_key
      recompute_by [fund: :fund, year: :year], to: :projected
      # COUNTS the rows the fold was handed, so an over-read is visible.
      reduce query: &ReactiveDag.Fixtures.FundYearGraph.count_read/2, into: [sum: [amount: :total]]
    end
  end

  # THE POINT: a UUID pk, `fund`/`year` as plain columns, and NO key column.
  defmodule KeylessRollup do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.FundYearGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_fy_keyless_rollup"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      uuid_primary_key :id
      attribute :fund, :string, public?: true
      attribute :year, :string, public?: true
      attribute :total, :integer, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        accept [:fund, :year, :total]
        upsert?(true)
        upsert_identity :by_unit
      end
    end

    identities do
      identity :by_unit, [:fund, :year]
    end

    reactive do
      id :rollup
      op :fold
      # `payload_key false` — this node HAS no key column, declared rather than
      # deduced. Its row is identified by the columns below.
      payload_key false
      row_key [:fund, :year]
      recompute_by [fund: :fund, year: :year], to: :projected
      reduce into: [sum: [amount: :total]]
    end
  end

  defmodule Project do
    @moduledoc """
    The map from `lines` to `projected` — a plain per-key copy.

    Lives in the fixture because `Projected` NAMES it, so a
    graph assembled from the fixture cannot run without it. It used to sit in
    the test file, which worked only while the resources did too.
    """
    @behaviour ReactiveDag.Op

    alias ReactiveDag.Fixtures.FundYearGraph.{Lines, Projected}

    @impl true
    def recompute(cell, keys, opts \\ []) do
      lines =
        case keys do
          ["*"] -> Ash.read!(Lines)
          ks -> Enum.filter(Ash.read!(Lines), &(&1.key in ks))
        end

      changed =
        for l <- lines,
            ReactiveDag.Node.Payload.upsert_row(
              Projected,
              cell.meta,
              l.key,
              %{key: l.key, fund: l.fund, year: l.year, amount: l.amount},
              opts
            ) != :unchanged do
          l.key
        end

      {:ok, changed}
    end
  end

  @doc """
  The fold's `query:`.

  Runs BEFORE the library's auto scope, so it is a pass-through — the scope is
  asserted directly via `auto_scope/3` rather than through what this returns.
  """
  def count_read(query, _claimed), do: query

  @doc "lines -> projected -> keyed rollup."
  def plan, do: ReactiveDag.Node.graph([Lines, Projected, KeyedRollup])

  @doc "lines -> projected -> keyless rollup."
  def plan(:keyless), do: ReactiveDag.Node.graph([Lines, Projected, KeylessRollup])

  def tables, do: ["fx_fy_lines", "fx_fy_projected", "fx_fy_keyed_rollup", "fx_fy_keyless_rollup"]
end

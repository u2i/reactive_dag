defmodule ReactiveDag.Fixtures.CompareGraph do
  @moduledoc """
  `compare` — which of a node's columns constitute its RESULT.

  Ten resources, all variants of ONE question, which is why they share a
  fixture: a derived row carries fields that are part of the RECORD but not
  part of the RESULT. `ordinal` is the sharp one — a re-parse that reorders
  rows moves every `ordinal` without anything having actually changed, and one
  spurious change re-runs every fold downstream.

  The groups:

    * `Narrowed` / `Everything` / `Both` — the core property, same columns,
      differing only in what each declares as its result.
    * `ExtractedLine` -> `AccountTotal` / `AccountTotalWide` — a document
      parse where `doc_id` and `ordinal` are record-not-result.
    * `Reading` -> `PlantMonth` / `PlantMonthWide` — the same shape with a
      fold over readings.

  The `Wide` variants compare every column, so a test can show the SAME change
  being news to one and not the other.
  """

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      resource ReactiveDag.Fixtures.CompareGraph.Narrowed
      resource ReactiveDag.Fixtures.CompareGraph.Everything
      resource ReactiveDag.Fixtures.CompareGraph.Both
      resource ReactiveDag.Fixtures.CompareGraph.Rollups
      resource ReactiveDag.Fixtures.CompareGraph.ExtractedLine
      resource ReactiveDag.Fixtures.CompareGraph.AccountTotal
      resource ReactiveDag.Fixtures.CompareGraph.AccountTotalWide
      resource ReactiveDag.Fixtures.CompareGraph.Reading
      resource ReactiveDag.Fixtures.CompareGraph.PlantMonth
      resource ReactiveDag.Fixtures.CompareGraph.PlantMonthWide
    end
  end

  # the core shape: :a and :b are the result, :c rides along on the record
  defmodule Narrowed do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.CompareGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_cmp_narrowed"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :a, :string, public?: true
      attribute :b, :string, public?: true
      attribute :c, :string, public?: true
    end

    identities do
      identity :by_key, [:key], pre_check_with: ReactiveDag.Fixtures.CompareGraph.Domain
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        upsert_identity(:by_key)
        accept([:key, :a, :b, :c])
      end
    end

    reactive do
      id(:narrowed)
      leaf?(true)
      compare([:a, :b])
    end
  end

  # the same row shape with NO `compare` — the default, which every existing
  # node relies on: every field the row carries is compared.
  defmodule Everything do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.CompareGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_cmp_everything"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :a, :string, public?: true
      attribute :b, :string, public?: true
      attribute :c, :string, public?: true
    end

    identities do
      identity :by_key, [:key], pre_check_with: ReactiveDag.Fixtures.CompareGraph.Domain
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        upsert_identity(:by_key)
        accept([:key, :a, :b, :c])
      end
    end

    reactive do
      id(:everything)
      leaf?(true)
    end
  end

  # BOTH declared. The doc says the fingerprint wins; this resource exists to
  # pin it, so `compare` cannot quietly start overriding it.
  defmodule Both do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.CompareGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_cmp_both"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :a, :string, public?: true
      attribute :b, :string, public?: true
      attribute :fingerprint, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        accept([:key, :a, :b, :fingerprint])
      end
    end

    reactive do
      id(:both)
      leaf?(true)
      fingerprint([:a])
      compare([:b])
    end
  end

  # a COMPOSITE-PK node: no key column, its cell key is the identity's "|"
  # serialization. It goes through `upsert_identity`, which shares `moved?/3`,
  # so `compare` must narrow there identically.
  defmodule Rollups do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.CompareGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_cmp_rollups"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :fund, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :fy, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :total, :float, public?: true
      attribute :ordinal, :integer, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        accept([:fund, :fy, :total, :ordinal])
      end
    end

    reactive do
      id(:rollups)
      leaf?(true)
      compare([:total])
    end
  end

  defmodule ExtractedLine do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.CompareGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_cmp_extracted_line"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :id, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :account, :string, public?: true
      attribute :amount, :float, public?: true
      attribute :doc_id, :string, public?: true
      attribute :ordinal, :integer, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        accept([:id, :account, :amount, :doc_id, :ordinal])
      end
    end

    reactive do
      id(:extracted_lines)
      op(:source)
      leaf?(true)
    end
  end

  # the derived node: one row per account, folded from the extracted lines.
  # Its `into` carries the provenance and position of the FIRST line it saw —
  # part of the record, useless as a change signal.
  defmodule AccountTotal do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.CompareGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_cmp_account_total"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :account, :string, public?: true
      attribute :total, :float, public?: true
      attribute :doc_id, :string, public?: true
      attribute :ordinal, :integer, public?: true
    end

    identities do
      identity :by_key, [:key], pre_check_with: ReactiveDag.Fixtures.CompareGraph.Domain
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        upsert_identity(:by_key)
        accept([:key, :account, :total, :doc_id, :ordinal])
      end
    end

    reactive do
      id(:account_totals)
      op(:fold)
      key_rule(:all)

      # `account` and `total` ARE the answer; doc_id/ordinal are the record.
      compare([:account, :total])

      reduce(
        over: :extracted_lines,
        group_by: &ReactiveDag.Fixtures.CompareGraph.group/1,
        key: &ReactiveDag.Fixtures.CompareGraph.key/1,
        into: &ReactiveDag.Fixtures.CompareGraph.into/2
      )
    end
  end

  # the same fold with no `compare` — the regression guard's DSL twin: a
  # reorder DOES propagate here, which is the behaviour being fixed.
  defmodule AccountTotalWide do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.CompareGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_cmp_account_total_wide"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :account, :string, public?: true
      attribute :total, :float, public?: true
      attribute :doc_id, :string, public?: true
      attribute :ordinal, :integer, public?: true
    end

    identities do
      identity :by_key, [:key], pre_check_with: ReactiveDag.Fixtures.CompareGraph.Domain
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        upsert_identity(:by_key)
        accept([:key, :account, :total, :doc_id, :ordinal])
      end
    end

    reactive do
      id(:account_totals_wide)
      op(:fold)
      key_rule(:all)

      reduce(
        over: :extracted_lines,
        group_by: &ReactiveDag.Fixtures.CompareGraph.group/1,
        key: &ReactiveDag.Fixtures.CompareGraph.key/1,
        into: &ReactiveDag.Fixtures.CompareGraph.into/2
      )
    end
  end

  # ── an AGGREGATE node: the third threading site ──────────────────────────
  #
  # `Recompute.Aggregate` writes through its own `lapse_opts/1`, a different
  # line from the reduce/join path's.
  #
  # Note what it takes to make `compare` load-bearing here. The aggregate's
  # `project/3` builds the payload from the key column plus each aggregate's
  # `dest` and NOTHING else — a column merely sitting on the resource never
  # reaches `attrs`, so narrowing past it would be a no-op. The narrowing only
  # bites when the node declares TWO aggregates and only one is the result:
  # `avg_flow` is the answer, `reading_count` is bookkeeping about the source
  # (how many rows the parse happened to emit), and a re-parse that splits one
  # reading into two without moving the average must not propagate.
  defmodule Reading do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.CompareGraph.Domain,
      data_layer: AshPostgres.DataLayer

    postgres do
      table "fx_cmp_reading"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :id, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :pm_key, :string, public?: true
      attribute :flow, :float, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :create do
        accept([:id, :pm_key, :flow])
      end
    end
  end

  defmodule PlantMonth do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.CompareGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_cmp_plant_month"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :avg_flow, :float, public?: true
      # bookkeeping: how many rows the source happened to emit
      attribute :reading_count, :integer, public?: true
    end

    identities do
      identity :by_key, [:key], pre_check_with: ReactiveDag.Fixtures.CompareGraph.Domain
    end

    relationships do
      has_many :readings, ReactiveDag.Fixtures.CompareGraph.Reading do
        source_attribute :key
        destination_attribute :pm_key
      end
    end

    actions do
      defaults [:read, :destroy]

      create :seed do
        accept([:key])
      end

      create :upsert do
        upsert?(true)
        upsert_identity(:by_key)
        accept([:key, :avg_flow, :reading_count])
      end
    end

    reactive do
      id(:plant_months)
      op(:fold)
      key_rule(:all)
      compare([:avg_flow])

      aggregate(over: :readings, avg: [flow: :avg_flow], count: :reading_count)
    end
  end

  # the same aggregate with NO `compare` — the count moving DOES propagate here
  defmodule PlantMonthWide do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.CompareGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_cmp_plant_month_wide"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :avg_flow, :float, public?: true
      attribute :reading_count, :integer, public?: true
    end

    identities do
      identity :by_key, [:key], pre_check_with: ReactiveDag.Fixtures.CompareGraph.Domain
    end

    relationships do
      has_many :readings, ReactiveDag.Fixtures.CompareGraph.Reading do
        source_attribute :key
        destination_attribute :pm_key
      end
    end

    actions do
      defaults [:read, :destroy]

      create :seed do
        accept([:key])
      end

      create :upsert do
        upsert?(true)
        upsert_identity(:by_key)
        accept([:key, :avg_flow, :reading_count])
      end
    end

    reactive do
      id(:plant_months_wide)
      op(:fold)
      key_rule(:all)

      aggregate(over: :readings, avg: [flow: :avg_flow], count: :reading_count)
    end
  end

  @doc """
  The fold's grouping, key and shape.

  Here rather than in the test because the RESOURCES name them — a graph
  assembled from this fixture cannot run without them.

  `into/2` takes `doc_id` and `ordinal` from the FIRST line by ordinal, which
  is the whole point: those two travel with the result without being part of
  it, so a re-parse that reorders lines moves them and must still not count as
  a change.
  """

  def group(line), do: line.account
  def key(account), do: account

  def into(account, lines) do
    first = Enum.min_by(lines, & &1.ordinal)

    %{
      key: account,
      account: account,
      total: lines |> Enum.map(& &1.amount) |> Enum.sum(),
      doc_id: first.doc_id,
      ordinal: first.ordinal
    }
  end

  def tables,
    do: [
      "fx_cmp_narrowed",
      "fx_cmp_everything",
      "fx_cmp_both",
      "fx_cmp_rollups",
      "fx_cmp_extracted_line",
      "fx_cmp_account_total",
      "fx_cmp_account_total_wide",
      "fx_cmp_reading",
      "fx_cmp_plant_month",
      "fx_cmp_plant_month_wide"
    ]
end

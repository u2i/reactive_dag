defmodule ReactiveDag.Fixtures.LapseGraph do
  @moduledoc """
  `lapse` — a derived row that EXPIRES rather than being recomputed away.

  Seven resources, because lapsing has that many distinct answers: a row that
  survives, one that lapses on any change, one that lapses only on a narrow
  set, a correction that revives it, a fund/fiscal-year rollup pair, and a
  sign-off whose approval lapses when what it approved moves.
  """

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      resource ReactiveDag.Fixtures.LapseGraph.Lines
      resource ReactiveDag.Fixtures.LapseGraph.Survives
      resource ReactiveDag.Fixtures.LapseGraph.LapsesAny
      resource ReactiveDag.Fixtures.LapseGraph.LapsesNarrow
      resource ReactiveDag.Fixtures.LapseGraph.Correction
      resource ReactiveDag.Fixtures.LapseGraph.LapsesChildren
      resource ReactiveDag.Fixtures.LapseGraph.FundLines
      resource ReactiveDag.Fixtures.LapseGraph.FundRollups
      resource ReactiveDag.Fixtures.LapseGraph.FySignOff
    end
  end

  defmodule Lines do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.LapseGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_lp_lines"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :fy, :string, public?: true
      attribute :amount, :float, public?: true
      attribute :label, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :create do
        accept([:key, :fy, :amount, :label])
      end

      update :revise do
        accept([:amount])
      end

      update :relabel do
        accept([:label])
      end
    end

    reactive do
      id :lines
      op :source
      leaf? true
      dirties_on [:create, :update, :destroy]
    end
  end

  # The default. `:approved_at` is absent from `:upsert`'s accept list, so the
  # payload write cannot touch it — and no declaration protects it.
  defmodule Survives do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.LapseGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_lp_survives"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :fy, :string, public?: true
      attribute :total, :float, public?: true
      attribute :approved_at, :string, public?: true
    end

    identities do
      identity :by_key, [:key], pre_check_with: ReactiveDag.Fixtures.LapseGraph.Domain
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        upsert_identity(:by_key)
        accept([:key, :fy, :total])
      end

      update :approve do
        require_atomic? false
        accept([:approved_at])
      end
    end

    reactive do
      id :survives
      recompute_by :fy, to: :lines, from: :fy
      reduce into: [sum: [amount: :total]]
    end
  end

  defmodule LapsesAny do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.LapseGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_lp_lapses_any"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :fy, :string, public?: true
      attribute :total, :float, public?: true
      attribute :approved_at, :string, public?: true
    end

    identities do
      identity :by_key, [:key], pre_check_with: ReactiveDag.Fixtures.LapseGraph.Domain
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        upsert_identity(:by_key)
        accept([:key, :fy, :total])
      end

      update :approve do
        require_atomic? false
        accept([:approved_at])
      end

      # the lapse's own write — deliberately NOT the payload action
      update :lapse do
        require_atomic? false
        accept([:approved_at])
      end
    end

    reactive do
      id :lapses_any
      recompute_by :fy, to: :lines, from: :fy
      reduce into: [sum: [amount: :total]]

      lapse :approved_at, when_changed: :any
    end
  end

  # `:total` is watched, `:label` is not. A relabel moves `:label` (so the
  # recompute is `:changed` and propagates) while `:total` sits still — and the
  # sign-off must survive that.
  defmodule LapsesNarrow do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.LapseGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_lp_lapses_narrow"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :fy, :string, public?: true
      attribute :total, :float, public?: true
      attribute :label, :string, public?: true
      attribute :signed_off_by, :string, public?: true
    end

    identities do
      identity :by_key, [:key], pre_check_with: ReactiveDag.Fixtures.LapseGraph.Domain
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        upsert_identity(:by_key)
        accept([:key, :fy, :total, :label])
      end

      update :sign_off do
        require_atomic? false
        accept([:signed_off_by])
      end

      update :lapse do
        require_atomic? false
        accept([:signed_off_by])
      end
    end

    reactive do
      id :lapses_narrow
      recompute_by :fy, to: :lines, from: :fy

      reduce(
        group_by: [fy: :fy],
        into: fn {fy}, items ->
          %{
            fy: fy,
            total: items |> Enum.map(& &1.amount) |> Enum.sum(),
            # the cosmetic column: moves on a relabel, and is NOT watched
            label: items |> Enum.map(& &1.label) |> Enum.sort() |> Enum.join(",")
          }
        end
      )

      lapse :signed_off_by, when_changed: [:total]
    end
  end

  defmodule Correction do
    use Ash.Resource, domain: ReactiveDag.Fixtures.LapseGraph.Domain, data_layer: AshPostgres.DataLayer

    postgres do
      table "fx_lp_correction"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      uuid_primary_key :id
      attribute :fy, :string, public?: true
      attribute :note, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :create do
        accept([:fy, :note])
      end
    end
  end

  defmodule LapsesChildren do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.LapseGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_lp_lapses_children"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :fy, :string, public?: true
      attribute :total, :float, public?: true
    end

    identities do
      identity :by_key, [:key], pre_check_with: ReactiveDag.Fixtures.LapseGraph.Domain
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        upsert_identity(:by_key)
        accept([:key, :fy, :total])
      end
    end

    reactive do
      id :lapses_children
      recompute_by :fy, to: :lines, from: :fy
      reduce into: [sum: [amount: :total]]

      lapse ReactiveDag.Fixtures.LapseGraph.Correction, key: :fy, when_changed: [:total]
    end
  end

  defmodule FundLines do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.LapseGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_lp_fund_lines"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :fund_code, :string, public?: true
      attribute :fy, :string, public?: true
      attribute :amount, :float, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :create do
        accept([:key, :fund_code, :fy, :amount])
      end

      update :revise do
        accept([:amount])
      end
    end

    reactive do
      id :fund_lines
      op :source
      leaf? true
      dirties_on [:create, :update, :destroy]
    end
  end

  # composite PK: the cell key is the identity serialization ("gf|2026")
  defmodule FundRollups do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.LapseGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_lp_fund_rollups"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :fund, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :fy, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :total, :float, public?: true
      attribute :approved_at, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        accept([:fund, :fy, :total])
      end

      update :approve do
        require_atomic? false
        accept([:approved_at])
      end

      update :lapse do
        require_atomic? false
        accept([:approved_at])
      end
    end

    reactive do
      id :fund_rollups
      recompute_by [fund: :fund_code, fy: :fy], to: :fund_lines
      reduce into: [sum: [amount: :total]]

      lapse :approved_at, when_changed: [:total]
    end
  end

  # `expand:` fans one unit out to many rows, so the sign-off lives on each of
  # them and every one must be cleared when any member moves.
  defmodule FySignOff do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.LapseGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_lp_fy_sign_off"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :fy, :string, public?: true
      attribute :bucket, :string, public?: true
      attribute :total, :float, public?: true
      attribute :fy_approved_at, :string, public?: true
    end

    identities do
      identity :by_key, [:key], pre_check_with: ReactiveDag.Fixtures.LapseGraph.Domain
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        upsert_identity(:by_key)
        accept([:key, :fy, :bucket, :total])
      end

      update :approve do
        require_atomic? false
        accept([:fy_approved_at])
      end

      update :lapse do
        require_atomic? false
        accept([:fy_approved_at])
      end
    end

    reactive do
      id :fy_sign_off
      recompute_by :fy, to: :lines, from: :fy

      # one unit -> many rows, each self-keyed "<fy>|<bucket>"
      reduce(
        group_by: [fy: :fy],
        expand: fn {fy}, items ->
          items
          |> Enum.group_by(& &1.label)
          |> Enum.map(fn {bucket, rows} ->
            %{
              key: "#{fy}|#{bucket}",
              fy: fy,
              bucket: bucket,
              total: rows |> Enum.map(& &1.amount) |> Enum.sum()
            }
          end)
        end
      )

      lapse :fy_approved_at, when_changed: :any, over: :fy
    end
  end

  def tables,
    do: [
      "fx_lp_lines",
      "fx_lp_survives",
      "fx_lp_lapses_any",
      "fx_lp_lapses_narrow",
      "fx_lp_correction",
      "fx_lp_lapses_children",
      "fx_lp_fund_lines",
      "fx_lp_fund_rollups",
      "fx_lp_fy_sign_off"
    ]
end

defmodule ReactiveDag.Fixtures.RevivalGraph do
  @moduledoc """
  A retired row coming BACK — the unit vanished, then reappeared.

  A fold that retires on absence must be able to revive, or a row that
  returns stays dead and reads exactly like one that never existed.
  """

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      resource ReactiveDag.Fixtures.RevivalGraph.Docs
      resource ReactiveDag.Fixtures.RevivalGraph.Marked
    end
  end

  defmodule Docs do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.RevivalGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_rev_docs"
      repo(ReactiveDag.TestRepo)
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

  # a node declaring the marking policy, rather than passing :retire per call
  defmodule Marked do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.RevivalGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_rev_marked"
      repo(ReactiveDag.TestRepo)
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
      retain_if_vanished(mark: &ReactiveDag.Fixtures.RevivalGraph.mark_gone/1)
    end
  end

  @doc """
  The declared mark policy: tombstone the rows that vanished.

  In the fixture because `Marked` NAMES it (`retain_if_vanished mark:`), so a
  graph assembled from here cannot reconcile without it.
  """
  def mark_gone(keys) do
    for key <- keys, row = Enum.find(Ash.read!(Marked), &(&1.key == key)) do
      row |> Ash.Changeset.for_update(:update, %{status: "tombstoned"}) |> Ash.update!()
    end
  end

  def tables, do: ["fx_rev_docs", "fx_rev_marked"]
end

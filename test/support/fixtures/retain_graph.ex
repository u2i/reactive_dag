defmodule ReactiveDag.Fixtures.RetainGraph do
  @moduledoc """
  `retain_if_vanished` — what happens to a derived row when its input goes.

  `Plain` retires; `Marked` keeps the row and marks it. The difference
  matters because a retire that cannot find its row returns false and the
  stale row survives looking exactly like a live one.
  """

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      resource ReactiveDag.Fixtures.RetainGraph.Docs
      resource ReactiveDag.Fixtures.RetainGraph.Plain
      resource ReactiveDag.Fixtures.RetainGraph.Marked
    end
  end

  defmodule Docs do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.RetainGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_riv_docs"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :body, :string, public?: true
      attribute :content_md5, :string, public?: true
      attribute :fingerprint, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        accept([:key, :body, :content_md5, :fingerprint])
      end
    end

    reactive do
      id(:docs)
      leaf?(true)
      fingerprint([:content_md5])
      retain_if_vanished(true)
    end
  end

  # the default: a vanished key's row is destroyed
  defmodule Plain do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.RetainGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_riv_plain"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :body, :string, public?: true
      attribute :content_md5, :string, public?: true
      attribute :fingerprint, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        accept([:key, :body, :content_md5, :fingerprint])
      end
    end

    reactive do
      id(:plain)
      leaf?(true)
      fingerprint([:content_md5])
    end
  end

  defmodule Marked do
    use Ash.Resource,
      domain: ReactiveDag.Fixtures.RetainGraph.Domain,
      data_layer: AshPostgres.DataLayer,
      extensions: [ReactiveDag.Node]

    postgres do
      table "fx_riv_marked"
      repo(ReactiveDag.TestRepo)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :body, :string, public?: true
      attribute :content_md5, :string, public?: true
      attribute :status, :string, public?: true
      attribute :fingerprint, :string, public?: true
    end

    actions do
      defaults [:read, :destroy, update: [:status]]

      create :upsert do
        upsert?(true)
        accept([:key, :body, :content_md5, :status, :fingerprint])
      end
    end

    reactive do
      id(:marked)
      leaf?(true)
      fingerprint([:content_md5])
      retain_if_vanished(mark: &ReactiveDag.Fixtures.RetainGraph.tombstone/1)
    end
  end

  @doc """
  The mark policy `Marked` declares: tombstone what vanished.

  In the fixture because the resource NAMES it — a graph assembled from here
  cannot reconcile without it.
  """
  def tombstone(keys) do
    for key <- keys,
        row = Enum.find(Ash.read!(__MODULE__.Marked), &(&1.key == key)) do
      row |> Ash.Changeset.for_update(:update, %{status: "tombstoned"}) |> Ash.update!()
    end
  end

  def tables, do: ["fx_riv_marked", "fx_riv_docs", "fx_riv_plain"]
end

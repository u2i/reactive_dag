Application.put_env(:ash, :default_string_length_count, :codepoints)

# Does an UNCHANGED write split the validity period?
# The assumption everything else rests on, measured rather than inferred.

defmodule Spike.Repo do
  use AshPostgres.Repo, otp_app: :reactive_dag
  def installed_extensions, do: ["ash-functions"]
  def min_pg_version, do: %Version{major: 18, minor: 0, patch: 0}
end

defmodule Spike.Domain do
  use Ash.Domain, validate_config_inclusion?: false
  resources do
    allow_unregistered? true
  end
end

defmodule Spike.Doc do
  use Ash.Resource, domain: Spike.Domain, data_layer: AshPostgres.DataLayer

  postgres do
    table "spike_doc"
    repo Spike.Repo
  end

  temporal do
    attribute :valid_at
    recorded_at :recorded_at
  end

  attributes do
    attribute :id, :string, primary_key?: true, allow_nil?: false
    attribute :title, :string
    attribute :md5, :string
    attribute :recorded_at, :utc_datetime_usec, default: &DateTime.utc_now/0, update_default: &DateTime.utc_now/0
  end

  actions do
    defaults [:read, :destroy]
    create :upsert do
      accept [:id, :title, :md5]
      upsert? true
    end
  end
end
Application.put_env(:reactive_dag, Spike.Repo,
  username: "cascade", password: "cascade", hostname: "localhost",
  port: 5545, database: "reactive_dag_temporal", pool_size: 2)
Application.put_env(:reactive_dag, :ash_domains, [Spike.Domain])

{:ok, _} = Spike.Repo.start_link()

Spike.Repo.query!("DROP TABLE IF EXISTS spike_doc", [])
Spike.Repo.query!("""
CREATE TABLE spike_doc (
  id text NOT NULL,
  title text,
  md5 text,
  recorded_at timestamptz,
  valid_at tstzrange NOT NULL,
  PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)
)
""", [])

periods = fn label ->
  %{rows: rows} = Spike.Repo.query!(
    "SELECT title, md5, lower(valid_at)::text, upper(valid_at)::text, recorded_at::text FROM spike_doc ORDER BY lower(valid_at)", [])
  IO.puts("#{label}: #{length(rows)} period(s)")
  Enum.each(rows, fn [t, m, lo, hi, r] ->
    IO.puts("   #{t}/#{m}  [#{lo} .. #{hi || "∞"})  recorded=#{r}")
  end)
end

w = fn title, md5, as_of ->
  Spike.Doc
  |> Ash.Changeset.for_create(:upsert, %{id: "d1", title: title, md5: md5})
  |> Ash.Changeset.as_of(as_of)
  |> Ash.create!()
end

w.("Budget", "abc", DateTime.utc_now())

# Can an ORDINARY Ash read see the period, the way `Payload.existing/4` does?
prior = Spike.Doc |> Ash.Query.do_filter(id: "d1") |> Ash.read_one!()
IO.puts("prior struct keys include valid_at? #{Map.has_key?(prior, :valid_at)}")
IO.puts("valid_at = #{inspect(Map.get(prior, :valid_at))}")

# And is its lower bound reachable without SQL?
case Map.get(prior, :valid_at) do
  %Ash.Range{lower: lo} -> IO.puts("lower bound = #{inspect(lo)}")
  other -> IO.puts("period shape = #{inspect(other)}")
end

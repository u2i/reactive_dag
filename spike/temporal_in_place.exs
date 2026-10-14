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

# 1. establish a period
w.("Budget", "abc", DateTime.utc_now())
periods.("baseline")
Process.sleep(1100)

# 2. write a BOOKKEEPING change pinned to the CURRENT period's start.
#    If `as_of` equals the period's lower bound, is it updated in place?
%{rows: [[lo]]} = Spike.Repo.query!("SELECT lower(valid_at) FROM spike_doc ORDER BY lower(valid_at) DESC LIMIT 1", [])
IO.puts("pinning as_of to current lower bound: #{inspect(lo)}")
w.("Budget", "xyz-bookkeeping", lo)
periods.("after write pinned to period start")
Process.sleep(1100)

# 3. a RESULT change, pinned to now — must split
w.("Budget (revised)", "def", DateTime.utc_now())
periods.("after a REAL change at now")
Process.sleep(1100)

# 4. bookkeeping again, pinned to the NEW period's start
%{rows: [[lo2]]} = Spike.Repo.query!("SELECT lower(valid_at) FROM spike_doc ORDER BY lower(valid_at) DESC LIMIT 1", [])
w.("Budget (revised)", "def-seen-again", lo2)
periods.("after bookkeeping on the latest period")

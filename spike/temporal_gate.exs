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

# THE GATE, through Payload — not a hand-written as_of.
alias ReactiveDag.Node.Payload

w = fn title, md5 ->
  Payload.upsert(Spike.Doc, :id, "d1", %{title: title, md5: md5}, :upsert, [])
end

IO.puts("verdict: #{inspect(w.("Budget", "abc"))}")
periods.("after first write")
Process.sleep(1100)

IO.puts("verdict: #{inspect(w.("Budget", "abc"))}")
periods.("after UNCHANGED write")
Process.sleep(1100)

IO.puts("verdict: #{inspect(w.("Budget (revised)", "def"))}")
periods.("after CHANGED write")

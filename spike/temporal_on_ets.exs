Application.put_env(:ash, :default_string_length_count, :codepoints)

defmodule T.Domain do
  use Ash.Domain, validate_config_inclusion?: false
  resources do allow_unregistered? true end
end

defmodule T.Doc do
  use Ash.Resource, domain: T.Domain, data_layer: Ash.DataLayer.Ets
  ets do private? true end

  temporal do
    attribute :valid_at
    recorded_at :recorded_at
  end

  attributes do
    attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
    attribute :title, :string, public?: true
    attribute :recorded_at, :utc_datetime_usec, public?: true,
      default: &DateTime.utc_now/0, update_default: &DateTime.utc_now/0
  end

  actions do
    defaults [:read]
    create :upsert, accept: [:key, :title], upsert?: true
  end
end

w = fn title, as_of ->
  T.Doc
  |> Ash.Changeset.for_create(:upsert, %{key: "k1", title: title})
  |> Ash.Changeset.as_of(as_of)
  |> Ash.create!()
end

show = fn label ->
  rows = Ash.read!(T.Doc)
  IO.puts("#{label}: #{length(rows)} row(s)")
  Enum.each(rows, fn r -> IO.puts("   #{r.title}  #{inspect(Map.get(r, :valid_at))}") end)
end

t0 = ~U[2026-01-01 00:00:00.000000Z]
w.("Budget", t0)
show.("first write")

w.("Budget (revised)", ~U[2026-01-01 00:00:10.000000Z])
show.("after change")

# THE READ THE RESUMPTION NEEDS: the row as it was at t0
old = T.Doc |> Ash.Query.do_filter(key: "k1") |> Ash.Query.as_of(t0) |> Ash.read_one!()
now = T.Doc |> Ash.Query.do_filter(key: "k1") |> Ash.read_one!()
IO.puts("as_of(t0).title = #{inspect(old && old.title)}")
IO.puts("now.title       = #{inspect(now && now.title)}")
IO.puts("DIFF: #{inspect(ReactiveDag.Node.Diff.between(old, now) |> Map.take(["title"]))}")

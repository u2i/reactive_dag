Application.put_env(:ash, :default_string_length_count, :codepoints)
defmodule O.Domain do
  use Ash.Domain, validate_config_inclusion?: false
  resources do allow_unregistered? true end
end
defmodule O.Line do
  use Ash.Resource, domain: O.Domain, data_layer: Ash.DataLayer.Ets
  require Ash.Query
  ets do private? true end
  temporal do
    attribute :valid_at
    recorded_at :recorded_at
  end
  attributes do
    attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
    attribute :fy, :string, public?: true
    attribute :recorded_at, :utc_datetime_usec, public?: true,
      default: &DateTime.utc_now/0, update_default: &DateTime.utc_now/0
  end
  actions do
    defaults [:read, :destroy]
    create :upsert, upsert?: true, accept: [:key, :fy], upsert_fields: [:fy, :recorded_at]
  end
end

require Ash.Query
w = fn fy -> O.Line |> Ash.Changeset.for_create(:upsert, %{key: "l1", fy: fy}) |> Ash.create!() end

r1 = w.("FY24")
Process.sleep(20)
t_mid = DateTime.utc_now()
Process.sleep(20)
r2 = w.("FY25")
IO.puts("moved FY24 -> FY25 at #{inspect(r2.recorded_at)}")

# EQUALITY on current values: only FY25
cur = O.Line |> Ash.Query.filter(fy == "FY24") |> Ash.read!()
IO.puts("current fy==FY24: #{inspect(Enum.map(cur, & &1.fy))}")

# OVERLAP: was it EVER in FY24 since t_mid?
window = %Ash.Range{lower: t_mid, upper: nil, bounds: :"[)"}
ever =
  O.Line
  |> Ash.Query.filter(fy == "FY24" and range_overlaps(valid_at, ^window))
  |> Ash.read!()
IO.puts("ever in FY24 since mid: #{inspect(Enum.map(ever, &{&1.fy, &1.valid_at.lower}))}")

# Is the old period reachable AT ALL without as_of?
IO.puts("plain read: #{inspect(Ash.read!(O.Line) |> Enum.map(&{&1.fy, &1.valid_at.lower}))}")
old = O.Line |> Ash.Query.as_of(r1.recorded_at) |> Ash.read!()
IO.puts("as_of(create): #{inspect(Enum.map(old, &{&1.fy, &1.valid_at.lower}))}")
# overlap WITHOUT the fy filter
any = O.Line |> Ash.Query.filter(range_overlaps(valid_at, ^window)) |> Ash.read!()
IO.puts("overlap only: #{inspect(Enum.map(any, &{&1.fy, &1.valid_at.lower}))}")

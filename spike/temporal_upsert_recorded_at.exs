Application.put_env(:ash, :default_string_length_count, :codepoints)
defmodule D.Domain do
  use Ash.Domain, validate_config_inclusion?: false
  resources do allow_unregistered? true end
end
defmodule D.Row do
  use Ash.Resource, domain: D.Domain, data_layer: Ash.DataLayer.Ets
  ets do private? true end
  temporal do
    attribute :valid_at
    recorded_at :recorded_at
  end
  attributes do
    attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
    attribute :amount, :float, public?: true
    attribute :recorded_at, :utc_datetime_usec, public?: true,
      default: &DateTime.utc_now/0, update_default: &DateTime.utc_now/0
  end
  actions do
    defaults [:read, :destroy]
    create :upsert, upsert?: true, accept: [:key, :amount]
    create :upsert_touch, upsert?: true, accept: [:key, :amount], upsert_fields: [:amount, :recorded_at]
  end
end

w = fn amt -> D.Row |> Ash.Changeset.for_create(:upsert, %{key: "a", amount: amt}) |> Ash.create!() end
r1 = w.(1.0)
IO.puts("first:  amount=#{r1.amount} recorded_at=#{inspect(r1.recorded_at)}")
Process.sleep(20)
r2 = w.(9.0)
IO.puts("second: amount=#{r2.amount} recorded_at=#{inspect(r2.recorded_at)}")
IO.puts("moved? #{DateTime.compare(r2.recorded_at, r1.recorded_at)}")
IO.puts("read back: #{inspect(Ash.read!(D.Row) |> Enum.map(&{&1.amount, &1.recorded_at}))}")

Process.sleep(20)
r3 = D.Row |> Ash.Changeset.for_create(:upsert_touch, %{key: "a", amount: 7.0}) |> Ash.create!()
IO.puts("with upsert_fields: amount=#{r3.amount} recorded_at=#{inspect(r3.recorded_at)}")
IO.puts("moved now? #{DateTime.compare(r3.recorded_at, r1.recorded_at)}")

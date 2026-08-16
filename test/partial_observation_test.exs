defmodule ReactiveDag.PartialObservationTest do
  @moduledoc """
  `observed: :partial` (#83) — a scan that looked at only part of the upstream.

  Retiring a key is an inference: *the upstream no longer lists it, so it is
  gone*. That is only valid from a COMPLETE observation. A scoped poll, a
  windowed one, or a crawl whose index page failed all produce a want-set that is
  real but incomplete, where absence means "not looked at".

  It was previously spelled `current: []` — measure vanishing against nothing —
  which works and says nothing about why. The asymmetry is what makes the name
  worth having: getting `:partial` wrong under-retires, leaving rows that should
  have gone. Getting `:all` wrong tombstones everything the scan did not happen
  to look at, which for an archival consumer is a mass-deletion wave from one
  upstream 500.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.Node.Rows

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      allow_unregistered?(true)
    end
  end

  defmodule Docs do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Ets, extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :content_md5, :string, public?: true
      attribute :fingerprint, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        accept([:key, :content_md5, :fingerprint])
      end
    end

    reactive do
      id(:docs)
      leaf?(true)
      fingerprint([:content_md5])
    end
  end

  setup do
    for row <- Ash.read!(Docs), do: Ash.destroy!(row)
    :ok
  end

  defp cell do
    [c] = ReactiveDag.Node.cells(Docs)
    c
  end

  @rows %{
    "a" => %{key: "a", content_md5: "aaa"},
    "b" => %{key: "b", content_md5: "bbb"},
    "c" => %{key: "c", content_md5: "ccc"}
  }

  defp scan(keys, opts \\ []) do
    {:ok, changed, _} = Rows.reconcile(cell(), keys, [upsert: &Map.get(@rows, &1)] ++ opts)
    Enum.sort(changed)
  end

  defp keys, do: Docs |> Ash.read!() |> Enum.map(& &1.key) |> Enum.sort()

  describe "nothing can vanish from a partial observation" do
    test "unobserved keys survive, and are not reported" do
      scan(["a", "b", "c"])

      # a scoped poll that only looked at "a"
      assert scan(["a"], observed: :partial) == []
      assert keys() == ["a", "b", "c"], "b and c were not looked at, not gone"
    end

    test "the same scan WITHOUT the flag destroys them — the failure being prevented" do
      scan(["a", "b", "c"])

      assert scan(["a"]) == ["b", "c"]
      assert keys() == ["a"], "two rows deleted by a scan that never looked at them"
    end

    test "an empty partial observation is a no-op, not a mass deletion" do
      # a crawl whose index page failed entirely: it observed nothing, and must
      # conclude nothing
      scan(["a", "b", "c"])

      assert scan([], observed: :partial) == []
      assert keys() == ["a", "b", "c"]
    end
  end

  describe "what a partial observation still does" do
    test "writes the rows it saw" do
      assert scan(["a", "b"], observed: :partial) == ["a", "b"]
      assert keys() == ["a", "b"]
    end

    test "reports a genuine change in the slice it looked at" do
      scan(["a", "b", "c"])

      moved = Map.put(@rows, "a", %{key: "a", content_md5: "MOVED"})

      {:ok, changed, _} =
        Rows.reconcile(cell(), ["a"], upsert: &Map.get(moved, &1), observed: :partial)

      assert changed == ["a"]
    end

    test "stays quiet when the slice it looked at did not move" do
      scan(["a", "b", "c"])

      assert scan(["a"], observed: :partial) == []
    end
  end

  describe "the default is unchanged" do
    test "`:all` is the default, and absence means gone" do
      scan(["a", "b"])

      assert scan(["a"], observed: :all) == ["b"]
      assert keys() == ["a"]
    end

    test "omitting the option behaves identically to `:all`" do
      scan(["a", "b"])

      assert scan(["a"]) == ["b"]
      assert keys() == ["a"]
    end
  end

  describe "the option is checked" do
    test "a typo raises rather than silently reconciling" do
      # the dangerous direction: a misspelt `:partial` must not quietly fall
      # through to the retiring path
      err =
        assert_raise ArgumentError, fn ->
          Rows.reconcile(cell(), ["a"], upsert: &Map.get(@rows, &1), observed: :partail)
        end

      msg = Exception.message(err)
      assert msg =~ ":all"
      assert msg =~ ":partial"
    end
  end
end

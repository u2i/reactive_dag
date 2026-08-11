defmodule ReactiveDag.VerdictNodeTest do
  @moduledoc """
  A VERDICT-only node (`verdict? true`): its computed result lives in the
  coordination tuple, declared through the first-class `status:` slot —
  `(group, items -> status | {status, strength})` — with the key deriving
  exactly as a payload row's would. No payload table, no `into:` row, no
  `upsert:`. This is the "purely calculated, no separate persistence" node.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.Node.Recompute

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      allow_unregistered?(true)
    end
  end

  # the observed stores — a leaf node the verdict reads declaratively.
  defmodule Stores do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
      private?(true)
    end

    attributes do
      attribute :store, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :enc, :boolean, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :create do
        accept([:store, :enc])
      end
    end

    reactive do
      id(:stores)
      op(:source)
      leaf?(true)
      payload_key(:store)
    end
  end

  # captures every Op.put so we can assert what landed in the tuple.
  defmodule CapturingWriter do
    @behaviour ReactiveDag.CoordinationWriter
    @impl true
    def put(cell_id, key, opts),
      do: send(ReactiveDag.VerdictNodeTest, {:put, cell_id, key, opts}) && :ok

    @impl true
    def tombstone(_cell_id, _keys), do: :ok
    @impl true
    def delete(_cell_id, _keys), do: :ok
  end

  # THE VERDICT NODE: no data_layer table, no attributes, no upsert, no into —
  # `status:` IS the result. Declarative read + group; the key derives.
  defmodule StoreEncrypted do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Simple,
      extensions: [ReactiveDag.Node]

    reactive do
      id(:store_encrypted)
      op(:reconcile)
      key_rule(:all)
      verdict?(true)

      reduce over: :stores,
             group_by: :store,
             status: fn _store, [r | _] -> if(r.enc, do: "present", else: "failing") end
    end
  end

  setup do
    Process.register(self(), ReactiveDag.VerdictNodeTest)
    prev = Application.get_env(:reactive_dag, :coordination_writer)
    Application.put_env(:reactive_dag, :coordination_writer, CapturingWriter)
    on_exit(fn -> Application.put_env(:reactive_dag, :coordination_writer, prev) end)

    # declared: s1,s2,s3 ; observed-encrypted: s1,s3 → s2 is failing.
    for {s, enc} <- [{"s1", true}, {"s2", false}, {"s3", true}] do
      Stores |> Ash.Changeset.for_create(:create, %{store: s, enc: enc}) |> Ash.create!()
    end

    :ok
  end

  defp cell, do: ReactiveDag.Node.graph([Stores, StoreEncrypted]).cells["store_encrypted"]

  test "a verdict node writes status into the TUPLE, not a payload table" do
    cell = cell()
    assert cell.meta.verdict == true

    {:ok, changed} = Recompute.recompute(cell, ["*"])
    assert Enum.sort(changed) == ["s1", "s2", "s3"]

    # collect the Op.puts — each carries the computed status IN the tuple opts
    puts = for _ <- 1..3, do: receive(do: ({:put, cid, k, opts} -> {cid, k, opts[:status]}))
    puts = Map.new(puts, fn {_cid, k, status} -> {k, status} end)

    assert puts["s1"] == "present"
    assert puts["s2"] == "failing"
    assert puts["s3"] == "present"
  end

  test "status: may return {status, strength} — the host-extended tuple shape" do
    defmodule StoreStrength do
      use Ash.Resource,
        domain: ReactiveDag.VerdictNodeTest.Domain,
        data_layer: Ash.DataLayer.Simple,
        extensions: [ReactiveDag.Node]

      reactive do
        id(:store_strength)
        op(:reconcile)
        key_rule(:all)
        verdict?(true)

        reduce over: :stores,
               group_by: :store,
               status: fn _store, [r | _] ->
                 if r.enc, do: {"present", "observed"}, else: {"failing", "observed"}
               end
      end
    end

    plan = ReactiveDag.Node.graph([Stores, StoreStrength])
    {:ok, _} = Recompute.recompute(plan.cells["store_strength"], ["*"])

    assert_received {:put, "store_strength", _k, opts}
    assert opts[:strength] == "observed"
  end

  # a writer that reports the boolean CHANGED signal: s2's verdict flipped,
  # s1/s3 re-put the same status.
  defmodule FlipReportingWriter do
    @behaviour ReactiveDag.CoordinationWriter
    @impl true
    def put(_cell_id, key, _opts), do: key == "s2"
    @impl true
    def delete(_cell_id, _keys), do: :ok
  end

  test "a change-reporting writer scopes verdict propagation to real flips" do
    Application.put_env(:reactive_dag, :coordination_writer, FlipReportingWriter)

    {:ok, changed} = Recompute.recompute(cell(), ["*"])
    assert changed == ["s2"]
  end

  test "a verdict node needs NO resource table, NO upsert, NO into — it doesn't raise" do
    cell = cell()
    assert cell.meta.reduce.upsert == nil
    assert cell.meta.reduce.into == nil
    assert {:ok, _} = Recompute.recompute(cell, ["*"])
  end

  test "a verdict? node that ALSO declares payload attributes raises (the half-state)" do
    # verdict? nodes store nothing but the tuple — payload attributes would be
    # silently dropped, so the library refuses at lowering time.
    assert_raise RuntimeError, ~r/declares payload attribute/, fn ->
      defmodule BadVerdict do
        use Ash.Resource,
          domain: Domain,
          data_layer: Ash.DataLayer.Ets,
          extensions: [ReactiveDag.Node]

        ets do
          private?(true)
        end

        attributes do
          attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
          # ← payload on a verdict node
          attribute :avg, :float, public?: true
        end

        reactive do
          op(:fold)
          verdict?(true)

          reduce over: :x,
                 group_by: :key,
                 status: fn _, _ -> "present" end
        end
      end

      ReactiveDag.Node.to_cell(BadVerdict)
    end
  end
end

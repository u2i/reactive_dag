defmodule ReactiveDag.VerdictNodeTest do
  @moduledoc """
  A VERDICT-only node (`verdict true`): its computed result lives in the
  coordination tuple (status/strength), with NO payload table of its own. A
  `reduce`/`join` on it writes each row's status/strength straight into the tuple
  via `Op.put` — no resource, no `upsert:`, no attributes. This is the "purely
  calculated, no separate persistence" node.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.Node.Recompute

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false
    resources do
      allow_unregistered? true
    end
  end

  # captures every Op.put so we can assert what landed in the tuple.
  defmodule CapturingWriter do
    @behaviour ReactiveDag.CoordinationWriter
    @impl true
    def put(cell_id, key, opts), do: send(ReactiveDag.VerdictNodeTest, {:put, cell_id, key, opts}) && :ok
    @impl true
    def tombstone(_cell_id, _keys), do: :ok
    @impl true
    def delete(_cell_id, _keys), do: :ok
  end

  # THE VERDICT NODE: no data_layer table, no attributes, no upsert. A reconcile
  # that emits a per-key status. `verdict true` = the result IS the tuple row.
  defmodule StoreEncrypted do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Simple, extensions: [ReactiveDag.Node]

    reactive do
      id :store_encrypted
      op :reconcile
      key_rule :all
      verdict true

      # a toy "reconcile": declared stores vs observed-encrypted — a store missing
      # from the observed set is `failing`, else `present`. Emits status per key.
      reduce over: :stores,
             read: &ReactiveDag.VerdictNodeTest.read/1,
             group_by: &ReactiveDag.VerdictNodeTest.grp/1,
             key: &ReactiveDag.VerdictNodeTest.key/1,
             into: &ReactiveDag.VerdictNodeTest.into/2
    end
  end

  # declared: s1,s2,s3 ; observed-encrypted: s1,s3  → s2 is failing.
  def read(:stores), do: [%{store: "s1", enc: true}, %{store: "s2", enc: false}, %{store: "s3", enc: true}]
  def grp(r), do: r.store
  def key(store), do: store
  def into(store, [r | _]) do
    %{key: store, status: if(r.enc, do: "present", else: "failing")}
  end

  setup do
    Process.register(self(), ReactiveDag.VerdictNodeTest)
    prev = Application.get_env(:reactive_dag, :coordination_writer)
    Application.put_env(:reactive_dag, :coordination_writer, CapturingWriter)
    on_exit(fn -> Application.put_env(:reactive_dag, :coordination_writer, prev) end)
    :ok
  end

  test "a verdict node writes status into the TUPLE, not a payload table" do
    cell = ReactiveDag.Node.to_cell(StoreEncrypted)
    assert cell.meta.verdict == true
    # it IS a resource (everything is, post-collapse), but a TABLELESS one — the
    # verdict flag is what routes around the payload write, not resource == nil.

    {:ok, changed} = Recompute.recompute(cell, ["*"])
    assert Enum.sort(changed) == ["s1", "s2", "s3"]

    # collect the Op.puts — each carries the computed status IN the tuple opts
    puts = for _ <- 1..3, do: (receive do {:put, cid, k, opts} -> {cid, k, opts[:status]} end)
    puts = Map.new(puts, fn {_cid, k, status} -> {k, status} end)

    assert puts["s1"] == "present"
    assert puts["s2"] == "failing"    # the computed verdict, in the tuple
    assert puts["s3"] == "present"
  end

  test "a verdict node needs NO resource, NO upsert, NO attributes — it doesn't raise" do
    cell = ReactiveDag.Node.to_cell(StoreEncrypted)
    # the reduce has no upsert: and the node has no backing resource; without
    # `verdict true` this would raise. It doesn't, because verdict is intentional.
    assert cell.meta.reduce.upsert == nil
    assert {:ok, _} = Recompute.recompute(cell, ["*"])
  end
end

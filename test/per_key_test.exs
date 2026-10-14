defmodule ReactiveDag.PerKeyTest do
  @moduledoc """
  The `per_key` rung (#30 items 1 + 2): for each claimed input row, call a
  generic action with that row and write its structured output here.

  The point is not ergonomics. Because the LIBRARY drives the loop, it sees the
  input rows — so it can hash the fields the result depends on and **skip the
  call** when nothing has moved. A `run` action is opaque by design (the library
  passes keys and gets keys back), so no declaration outside it could do that.

  For an expensive or non-deterministic action — an LLM call above all — this is
  the difference between a whole-cell claim costing one call and costing all of
  them, which is why it earns a rung rather than a guide section.
  """
  use ReactiveDag.PostgresCase

  alias ReactiveDag.Fixtures.PerKeyGraph

  alias ReactiveDag.Fixtures.PerKeyGraph.{
    Always,
    Concurrent,
    Embeddings,
    Summaries,
    Transcripts
  }

  defmodule Calls do
    def start_link, do: Agent.start_link(fn -> [] end, name: __MODULE__)
    def record(key), do: Agent.update(__MODULE__, &[key | &1])
    def all, do: Agent.get(__MODULE__, &Enum.reverse/1)
    def count, do: length(all())
  end

  alias ReactiveDag.{Cascade}
  alias ReactiveDag.Node.Recompute

  # counts calls so the tests can assert on what was actually PAID FOR
  setup_all do
    migrate!(PerKeyGraph.Domain)
    :ok
  end

  setup do
    pid = Ecto.Adapters.SQL.Sandbox.start_owner!(ReactiveDag.TestRepo, shared: true)
    on_exit(fn -> Ecto.Adapters.SQL.Sandbox.stop_owner(pid) end)

    migrate_library_tables!()
    truncate(PerKeyGraph.tables())
    start_supervised!(%{id: Calls, start: {Calls, :start_link, []}})

    for {k, body} <- [{"t1", "first call"}, {"t2", "second call"}] do
      Transcripts
      |> Ash.Changeset.for_create(:create, %{key: k, body: body, note: "n"})
      |> Ash.create!()
    end

    :ok
  end

  defp plan, do: ReactiveDag.Node.graph([Transcripts, Summaries])
  defp cell, do: plan().cells["summaries"]

  defp drain(plan),
    do: ReactiveDag.Test.Pending.cascade(plan)

  test "one call per claimed row; the structured output lands in this node's attributes" do
    {:ok, changed} = Recompute.recompute(cell(), ["*"]) |> normalise()

    assert Enum.sort(changed) == ["t1", "t2"]
    assert Calls.count() == 2

    rows = Summaries |> Ash.read!() |> Map.new(&{&1.key, &1.summary})
    assert rows == %{"t1" => "summary of first call", "t2" => "summary of second call"}
  end

  test "FINGERPRINTING: a re-run of unchanged rows calls nothing" do
    {:ok, _} = Recompute.recompute(cell(), ["*"]) |> normalise()
    assert Calls.count() == 2

    # a WHOLE-CELL claim, which without fingerprinting re-bills everything
    {:ok, changed, meta} = Recompute.recompute(cell(), ["*"])

    assert changed == []
    assert Calls.count() == 2
    assert meta == %{called: 0, skipped: 2}
  end

  test "a change to a FINGERPRINTED field re-bills only that row" do
    {:ok, _} = Recompute.recompute(cell(), ["*"]) |> normalise()
    before = Calls.count()

    Transcripts
    |> Ash.get!("t2")
    |> Ash.Changeset.for_update(:revise, %{body: "revised call"})
    |> Ash.update!()

    {:ok, changed, meta} = Recompute.recompute(cell(), ["*"])

    assert changed == ["t2"]
    assert Calls.count() - before == 1
    assert meta == %{called: 1, skipped: 1}
    assert (Summaries |> Ash.get!("t2")).summary == "summary of revised call"
  end

  test "a change to a field NOT in the fingerprint costs nothing" do
    {:ok, _} = Recompute.recompute(cell(), ["*"]) |> normalise()
    before = Calls.count()

    # :note is not in `fingerprint: [:body]` — the result does not depend on it
    Transcripts
    |> Ash.get!("t1")
    |> Ash.Changeset.for_update(:revise, %{note: "edited"})
    |> Ash.update!()

    {:ok, changed, meta} = Recompute.recompute(cell(), ["*"])

    assert changed == []
    assert Calls.count() == before
    assert meta.skipped == 2
  end

  test "a scoped claim reads and calls only its keys" do
    {:ok, _} = Recompute.recompute(cell(), ["*"]) |> normalise()

    Transcripts
    |> Ash.get!("t1")
    |> Ash.Changeset.for_update(:revise, %{body: "changed"})
    |> Ash.update!()

    before = Calls.count()
    {:ok, changed, meta} = Recompute.recompute(cell(), ["t1"])

    assert changed == ["t1"]
    assert Calls.count() - before == 1
    # t2 was never even read, so it is neither called nor skipped
    assert meta == %{called: 1, skipped: 0}
  end

  test "the skip count rides on the drain's Report step" do
    p = plan()
    ReactiveDag.Test.Pending.add("transcripts", ["*"])
    {:ok, _} = drain(p)

    ReactiveDag.Test.Pending.add("transcripts", ["*"])
    {:ok, report} = drain(p)

    step = Enum.find(report.steps, &(&1.cell == "summaries"))
    assert step.meta == %{called: 0, skipped: 2}

    # ...and rolls up, so a cost line can show what was avoided
    assert ReactiveDag.Report.total(report, :skipped) == 2
  end

  test "without `fingerprint:` every recompute calls — the opt-in is explicit" do
    c = ReactiveDag.Node.graph([Transcripts, Always]).cells["always"]

    {:ok, _, m1} = Recompute.recompute(c, ["*"])
    {:ok, _, m2} = Recompute.recompute(c, ["*"])

    assert m1 == %{called: 2, skipped: 0}
    assert m2 == %{called: 2, skipped: 0}
  end

  # the 3-tuple is the per_key contract; normalise where a test only wants keys
  defp normalise({:ok, changed, _meta}), do: {:ok, changed}
  defp normalise(other), do: other

  describe "compile-time errors" do
    alias ReactiveDag.Node.Verifiers.VerifyReactive

    test "a fingerprint with nowhere to live — the skip could never fire" do
      defmodule NoHome do
        use Ash.Resource,
          domain: ReactiveDag.Fixtures.PerKeyGraph.Domain,
          data_layer: Ash.DataLayer.Simple,
          extensions: [ReactiveDag.Node]

        attributes do
          attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
          attribute :summary, :string, public?: true
        end

        actions do
          defaults [:read]
          action :summarise, :map, do: run(fn _i, _c -> {:ok, %{}} end)
        end

        reactive do
          id(:no_home)
          recompute_by(:key, to: :transcripts, from: :key)
          # no :fingerprint attribute on this resource
          per_key(:summarise, fingerprint: [:body], into: [summary: :summary])
        end
      end

      assert {:error, %Spark.Error.DslError{message: msg}} =
               VerifyReactive.verify(NoHome.spark_dsl_config())

      assert msg =~ "somewhere to store the hash"
      assert msg =~ "could never fire"
    end

    test "an `into:` destination that isn't an attribute" do
      defmodule BadInto do
        use Ash.Resource,
          domain: ReactiveDag.Fixtures.PerKeyGraph.Domain,
          data_layer: Ash.DataLayer.Simple,
          extensions: [ReactiveDag.Node]

        attributes do
          attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
        end

        actions do
          defaults [:read]
          action :summarise, :map, do: run(fn _i, _c -> {:ok, %{}} end)
        end

        reactive do
          id(:bad_into)
          recompute_by(:key, to: :transcripts, from: :key)
          per_key(:summarise, into: [no_such_column: :summary])
        end
      end

      assert {:error, %Spark.Error.DslError{message: msg}} =
               VerifyReactive.verify(BadInto.spark_dsl_config())

      assert msg =~ ":no_such_column"
    end

    test "naming a non-generic action" do
      defmodule NotGeneric do
        use Ash.Resource,
          domain: ReactiveDag.Fixtures.PerKeyGraph.Domain,
          data_layer: Ash.DataLayer.Simple,
          extensions: [ReactiveDag.Node]

        attributes do
          attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
        end

        actions do
          defaults [:read]
        end

        reactive do
          id(:not_generic)
          recompute_by(:key, to: :transcripts, from: :key)
          per_key(:read)
        end
      end

      assert {:error, %Spark.Error.DslError{message: msg}} =
               VerifyReactive.verify(NotGeneric.spark_dsl_config())

      assert msg =~ ":read"
      assert msg =~ "GENERIC"
    end
  end

  describe "bounded concurrency" do
    # records max simultaneous in-flight calls, so the tests assert on actual
    # parallelism rather than on the option merely being accepted
    defmodule Gauge do
      def start_link, do: Agent.start_link(fn -> %{now: 0, peak: 0} end, name: __MODULE__)

      def enter do
        Agent.update(__MODULE__, fn s ->
          now = s.now + 1
          %{now: now, peak: max(s.peak, now)}
        end)
      end

      def leave, do: Agent.update(__MODULE__, &%{&1 | now: &1.now - 1})
      def peak, do: Agent.get(__MODULE__, & &1.peak)
    end

    setup do
      start_supervised!(%{id: Gauge, start: {Gauge, :start_link, []}})

      # the shared (non-private) ETS table outlives a test, so start each one
      # from a known-empty state
      Concurrent |> Ash.read!() |> Enum.each(&Ash.destroy!/1)

      # enough rows that 4-way concurrency is observable
      for i <- 3..8 do
        Transcripts
        |> Ash.Changeset.for_create(:create, %{key: "t#{i}", body: "body #{i}", note: "n"})
        |> Ash.create!()
      end

      :ok
    end

    defp concurrent_cell,
      do: ReactiveDag.Node.graph([Transcripts, Concurrent]).cells["concurrent"]

    test "rows run in parallel, bounded by max_concurrency" do
      {:ok, changed, meta} = Recompute.recompute(concurrent_cell(), ["*"])

      assert length(changed) == 8
      assert meta == %{called: 8, skipped: 0}

      # actually concurrent...
      assert Gauge.peak() > 1
      # ...and never beyond the bound
      assert Gauge.peak() <= 4
    end

    test "results are applied in ROW order — the changed list stays deterministic" do
      {:ok, first, _} = Recompute.recompute(concurrent_cell(), ["*"])

      # wipe the fingerprints so the second run calls again
      Concurrent |> Ash.read!() |> Enum.each(&Ash.destroy!/1)
      {:ok, second, _} = Recompute.recompute(concurrent_cell(), ["*"])

      assert first == second
      assert first == Enum.sort(first)
    end

    test "SKIPPED rows never enter the stream — slots go only to real calls" do
      {:ok, _, _} = Recompute.recompute(concurrent_cell(), ["*"])
      before = Calls.count()

      # change exactly one row; the other 7 are fingerprint-identical
      Transcripts
      |> Ash.get!("t5")
      |> Ash.Changeset.for_update(:revise, %{body: "changed"})
      |> Ash.update!()

      {:ok, changed, meta} = Recompute.recompute(concurrent_cell(), ["*"])

      assert changed == ["t5"]
      assert meta == %{called: 1, skipped: 7}
      assert Calls.count() - before == 1
    end
  end

  describe "embeddings (#30 item 5)" do
    # An embeddings NODE is just `per_key` with an embedding action — the
    # result is a list of floats instead of a string, and nothing else changes.
    # No new rung is needed. (For embeddings on the SAME resource as the text,
    # ash_ai's own `vectorize` is the better tool — it hooks the changeset, so
    # it detects change without a fingerprint round-trip.)

    setup do
      Embeddings |> Ash.read!() |> Enum.each(&Ash.destroy!/1)
      :ok
    end

    test "an embeddings node is per_key with an embedding action — vectors and all" do
      cell = ReactiveDag.Node.graph([Transcripts, Embeddings]).cells["embeddings"]

      {:ok, changed, meta} = Recompute.recompute(cell, ["*"])

      assert Enum.sort(changed) == ["t1", "t2"]
      assert meta == %{called: 2, skipped: 0}

      rows = Embeddings |> Ash.read!() |> Map.new(&{&1.key, &1.vector})
      assert [1.0, 10.0, 0.5] = rows["t1"]
      assert length(rows["t2"]) == 3
    end

    test "fingerprinting works the same — re-embedding is not paid for twice" do
      cell = ReactiveDag.Node.graph([Transcripts, Embeddings]).cells["embeddings"]
      {:ok, _, _} = Recompute.recompute(cell, ["*"])
      before = Calls.count()

      {:ok, changed, meta} = Recompute.recompute(cell, ["*"])

      assert changed == []
      assert meta == %{called: 0, skipped: 2}
      assert Calls.count() == before
    end
  end
end

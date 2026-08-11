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
  use ExUnit.Case, async: false

  alias ReactiveDag.{Drain, Frontier}
  alias ReactiveDag.Node.Recompute

  # counts calls so the tests can assert on what was actually PAID FOR
  defmodule Calls do
    def start_link, do: Agent.start_link(fn -> [] end, name: __MODULE__)
    def record(key), do: Agent.update(__MODULE__, &[key | &1])
    def all, do: Agent.get(__MODULE__, &Enum.reverse/1)
    def count, do: length(all())
  end

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      allow_unregistered?(true)
    end
  end

  defmodule Transcripts do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
      private?(true)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :body, :string, public?: true
      # deliberately NOT in the fingerprint: changing it must not re-bill
      attribute :note, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :create do
        accept([:key, :body, :note])
      end

      update :revise do
        accept([:body, :note])
      end
    end

    reactive do
      id(:transcripts)
      op(:source)
      leaf?(true)
    end
  end

  defmodule Summaries do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
      private?(true)
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :summary, :string, public?: true
      # where the library stores the input hash
      attribute :fingerprint, :string, public?: true
    end

    identities do
      identity :by_key, [:key], pre_check_with: ReactiveDag.PerKeyTest.Domain
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        upsert_identity(:by_key)
        accept([:key, :summary, :fingerprint])
      end

      # AN ORDINARY GENERIC ACTION taking ONE row's fields. That this could be
      # `AshAi.Actions.prompt/2` is the library's business not at all.
      action :summarise, :map do
        argument :text, :string, allow_nil?: false

        run fn input, _ctx ->
          ReactiveDag.PerKeyTest.Calls.record(input.arguments.text)
          {:ok, %{"summary" => "summary of #{input.arguments.text}"}}
        end
      end
    end

    reactive do
      id(:summaries)
      op(:summarise)

      recompute_by :key, to: :transcripts, from: :key

      per_key :summarise,
        args: [text: :body],
        fingerprint: [:body],
        into: [summary: :summary]
    end
  end

  defmodule FakeRepo do
    def start_link, do: Agent.start_link(fn -> MapSet.new() end, name: __MODULE__)

    def query!("INSERT INTO " <> _, params) do
      params
      |> Enum.chunk_every(4)
      |> Enum.each(fn [cell, key, _r, _t] -> Agent.update(__MODULE__, &MapSet.put(&1, {cell, key})) end)

      %{rows: []}
    end

    def query!("SELECT DISTINCT cell_id" <> _, _params) do
      ids = Agent.get(__MODULE__, & &1) |> Enum.map(&elem(&1, 0)) |> Enum.uniq()
      %{rows: Enum.map(ids, &[&1])}
    end

    def query!("DELETE FROM " <> _, [cell]) do
      keys =
        Agent.get_and_update(__MODULE__, fn set ->
          {mine, rest} = Enum.split_with(set, fn {c, _} -> c == cell end)
          {Enum.map(mine, &elem(&1, 1)), MapSet.new(rest)}
        end)

      %{rows: Enum.map(keys, &[&1])}
    end

    def query!("SELECT COUNT" <> _, _params), do: %{rows: [[Agent.get(__MODULE__, &MapSet.size/1)]]}
  end

  defmodule NullWriter do
    @behaviour ReactiveDag.CoordinationWriter
    @impl true
    def put(_cell_id, _key, _opts), do: :ok
    @impl true
    def delete(_cell_id, _keys), do: :ok
  end

  setup do
    start_supervised!(%{id: FakeRepo, start: {FakeRepo, :start_link, []}})
    start_supervised!(%{id: Calls, start: {Calls, :start_link, []}})
    prev_repo = Application.get_env(:reactive_dag, :repo)
    prev_writer = Application.get_env(:reactive_dag, :coordination_writer)
    Application.put_env(:reactive_dag, :repo, FakeRepo)
    Application.put_env(:reactive_dag, :coordination_writer, NullWriter)

    on_exit(fn ->
      Application.put_env(:reactive_dag, :repo, prev_repo)
      Application.put_env(:reactive_dag, :coordination_writer, prev_writer)
    end)

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
    do: Drain.run(plan, recompute: ReactiveDag.Node.Recompute, key_rule: ReactiveDag.Node.KeyRule)

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
    Frontier.mark_dirty("transcripts", ["*"], "seed")
    {:ok, _} = drain(p)

    Frontier.mark_dirty("transcripts", ["*"], "again")
    {:ok, report} = drain(p)

    step = Enum.find(report.steps, &(&1.cell == "summaries"))
    assert step.meta == %{called: 0, skipped: 2}

    # ...and rolls up, so a cost line can show what was avoided
    assert Drain.Report.total(report, :skipped) == 2
  end

  test "without `fingerprint:` every recompute calls — the opt-in is explicit" do
    defmodule Always do
      use Ash.Resource,
        domain: ReactiveDag.PerKeyTest.Domain,
        data_layer: Ash.DataLayer.Ets,
        extensions: [ReactiveDag.Node]

      ets do
        private?(true)
      end

      attributes do
        attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
        attribute :summary, :string, public?: true
      end

      actions do
        defaults [:read]

        create :upsert do
          upsert?(true)
          accept([:key, :summary])
        end

        action :summarise, :map do
          argument :text, :string, allow_nil?: false

          run fn input, _ctx ->
            ReactiveDag.PerKeyTest.Calls.record(input.arguments.text)
            {:ok, %{"summary" => "s"}}
          end
        end
      end

      reactive do
        id(:always)
        recompute_by :key, to: :transcripts, from: :key
        per_key :summarise, args: [text: :body], into: [summary: :summary]
      end
    end

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
          domain: ReactiveDag.PerKeyTest.Domain,
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
          recompute_by :key, to: :transcripts, from: :key
          # no :fingerprint attribute on this resource
          per_key :summarise, fingerprint: [:body], into: [summary: :summary]
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
          domain: ReactiveDag.PerKeyTest.Domain,
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
          recompute_by :key, to: :transcripts, from: :key
          per_key :summarise, into: [no_such_column: :summary]
        end
      end

      assert {:error, %Spark.Error.DslError{message: msg}} =
               VerifyReactive.verify(BadInto.spark_dsl_config())

      assert msg =~ ":no_such_column"
    end

    test "naming a non-generic action" do
      defmodule NotGeneric do
        use Ash.Resource,
          domain: ReactiveDag.PerKeyTest.Domain,
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
          recompute_by :key, to: :transcripts, from: :key
          per_key :read
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

    defmodule Concurrent do
      use Ash.Resource,
        domain: ReactiveDag.PerKeyTest.Domain,
        data_layer: Ash.DataLayer.Ets,
        extensions: [ReactiveDag.Node]

      # NB: NOT `private?(true)`. A private ETS table is owned by the process
      # that created it, and `max_concurrency:` writes each row from a task
      # process — so a private table would silently drop the writes. Real hosts
      # on AshPostgres are unaffected; this is the fixture matching reality.
      ets do
      end

      attributes do
        attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
        attribute :summary, :string, public?: true
        attribute :fingerprint, :string, public?: true
      end

      identities do
        identity :by_key, [:key], pre_check_with: ReactiveDag.PerKeyTest.Domain
      end

      actions do
        defaults [:read, :destroy]

        create :upsert do
          upsert?(true)
          upsert_identity(:by_key)
          accept([:key, :summary, :fingerprint])
        end

        action :summarise, :map do
          argument :text, :string, allow_nil?: false

          run fn input, _ctx ->
            ReactiveDag.PerKeyTest.Gauge.enter()
            # long enough that serial execution could never overlap
            Process.sleep(40)
            ReactiveDag.PerKeyTest.Gauge.leave()
            ReactiveDag.PerKeyTest.Calls.record(input.arguments.text)
            {:ok, %{"summary" => "s:#{input.arguments.text}"}}
          end
        end
      end

      reactive do
        id(:concurrent)
        recompute_by :key, to: :transcripts, from: :key

        per_key :summarise,
          args: [text: :body],
          fingerprint: [:body],
          max_concurrency: 4,
          into: [summary: :summary]
      end
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
end

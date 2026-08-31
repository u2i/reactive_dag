defmodule ReactiveDag.AugmentedByTest do
  @moduledoc """
  `augmented_by`: a HUMAN EDIT on a COMPUTED node triggers the cascade.

  `dirties_on` already makes ordinary Ash writes mark the written record's key,
  and for a source-fed LEAF that is exactly right: every write is an
  observation, and being GLOBAL is the point — no write site can be forgotten.

  On a COMPUTED node it is unusable. The library writes that node's rows itself,
  through `ReactiveDag.Node.Payload.upsert`, and a payload upsert is an ordinary
  Ash write — so a global change would make every recompute re-dirty the cell it
  just computed. Compute, mark, compute, mark: the drain never settles.

  But a computed node is precisely where human augmentation lands. Somebody
  corrects a figure the pipeline got wrong, or approves a finding, and that must
  re-run everything downstream of the correction. So `augmented_by` names
  ACTIONS rather than action types, and wires the SAME
  `ReactiveDag.Node.Changes.MarkDirty` change into each named action's own
  `changes` list. The payload upsert is excluded by construction rather than by
  a filter someone must remember to maintain — and naming it is a compile error.

  The human edit attaches to the node's key by construction too: the write goes
  through the node's own action, on the node's own row.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.{Cascade, Node}
  alias ReactiveDag.Node.Verifiers.VerifyReactive

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      allow_unregistered?(true)
    end
  end

  # THE CASE THE FEATURE EXISTS FOR: a computed node (the library writes its
  # rows through `:upsert`) that a human also corrects and approves.
  defmodule CategoryTotals do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :category, :string, public?: true
      attribute :total, :float, public?: true
      attribute :n, :integer, public?: true
      attribute :note, :string, public?: true
    end

    # NO `identity … pre_check_with:` here, deliberately. The single-attribute
    # primary key is what an upsert conflicts on anyway, and a pre-checked
    # identity adds a `before_action` hook that makes every UPDATE on the
    # resource non-atomic — which has nothing to do with this feature but would
    # stop the human's `:correct` from running at all.
    actions do
      defaults [:read, :destroy]

      # the PAYLOAD action — the library's own write. Never marks.
      create :upsert do
        upsert?(true)
        accept([:key, :category, :total, :n, :note])
      end

      # the HUMAN's actions. `:add` is a CREATE on purpose: it makes
      # `augmented_by` cover the same action TYPE as the payload upsert, so a
      # wiring that went by type instead of by name would sweep `:upsert` in
      # too — which is exactly the infinite loop, and is what the payload test
      # below would then catch.
      create :add do
        accept([:key, :category, :total, :n])
      end

      update :correct do
        accept([:total])
      end

      update :approve do
        accept([:note])
      end

      # a write action deliberately NOT augmented: an internal annotation that
      # should not re-run the cascade
      update :annotate do
        accept([:note])
      end
    end

    reactive do
      id(:category_totals)
      op(:fold)

      recompute_by :category, to: :expenses, from: :category
      reduce into: [sum: [amount: :total], count: :n]

      # THE FEATURE: these three actions mark; `:upsert` and `:annotate` do not
      augmented_by([:add, :correct, :approve])
    end
  end

  defmodule Expenses do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :category, :string, public?: true
      attribute :amount, :float, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :create do
        accept([:key, :category, :amount])
      end
    end

    reactive do
      id(:expenses)
      op(:source)
      leaf?(true)
      dirties_on([:create, :update, :destroy])
    end
  end

  # a COMPOSITE-PK computed node: the cell key is the identity serialization,
  # exactly as it is for `dirties_on` (mirrors that test's Rollups).
  defmodule Rollups do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute :fund, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :fy, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :total, :float, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        accept([:fund, :fy, :total])
      end

      update :correct do
        accept([:total])
      end
    end

    reactive do
      id(:rollups)
      leaf?(true)
      augmented_by([:correct])
    end
  end

  # An augmented write enqueues its cascade in the same
  # transaction as the mark.
  defmodule Verdicts do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :status, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        accept([:key, :status])
      end

      update :override do
        accept([:status])
      end
    end

    reactive do
      id(:verdicts)
      leaf?(true)
      augmented_by([:override])
    end
  end

  # a leaf that is ALSO human-augmented: `dirties_on` covers every write,
  # `augmented_by` names one of those same actions. It must mark ONCE.
  defmodule Readings do
    use Ash.Resource,
      domain: Domain,
      data_layer: Ash.DataLayer.Ets,
      extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :value, :float, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :create do
        accept([:key, :value])
      end

      update :correct do
        accept([:value])
      end
    end

    reactive do
      id(:readings)
      leaf?(true)
      # :correct is an `:update`, so BOTH cover it
      dirties_on([:create, :update])
      augmented_by([:correct])
    end
  end

  defmodule FakeRepo do
    def start_link, do: Agent.start_link(fn -> %{} end, name: __MODULE__)

    def query!("INSERT INTO " <> _, params) do
      params
      |> Enum.chunk_every(7)
      |> Enum.each(fn [cell, _tenant, key, _r, _t, _held, vid] ->
        Agent.update(__MODULE__, fn m -> Map.put_new(m, {cell, key}, vid) end)
      end)

      %{rows: []}
    end

    def query!("SELECT DISTINCT cell_id" <> _, _params) do
      ids = Agent.get(__MODULE__, & &1) |> Map.keys() |> Enum.map(&elem(&1, 0)) |> Enum.uniq()
      %{rows: Enum.map(ids, &[&1])}
    end

    def query!("DELETE FROM " <> _, [cell | _tenant]) do
      rows =
        Agent.get_and_update(__MODULE__, fn m ->
          {mine, rest} = Enum.split_with(m, fn {{c, _}, _} -> c == cell end)
          {Enum.map(mine, fn {{_c, k}, vid} -> [k, vid] end), Map.new(rest)}
        end)

      %{rows: rows}
    end

    def query!("SELECT COUNT" <> _, _params), do: %{rows: [[Agent.get(__MODULE__, &map_size/1)]]}
    def query!("SELECT pg_try_advisory_lock" <> _, _params), do: %{rows: [[true]]}
    def query!("SELECT pg_advisory_unlock" <> _, _params), do: %{rows: [[true]]}

    def dirty, do: Agent.get(__MODULE__, &Map.keys/1) |> Enum.sort()
  end

  setup do
    start_supervised!(%{id: FakeRepo, start: {FakeRepo, :start_link, []}})
    prev_repo = Application.get_env(:reactive_dag, :repo)
    Application.put_env(:reactive_dag, :repo, FakeRepo)

    on_exit(fn -> Application.put_env(:reactive_dag, :repo, prev_repo) end)

    # An augmented write no longer marks a table a test can read back — it
    # enqueues a cascade. This is the seam that catches those enqueues, so the
    # assertions below can ask what a write ORIGINATED without an Oban running.
    ReactiveDag.Test.Pending.capture_enqueues()

    # the ETS tables are shared, so start each test from a known-empty state —
    # including the frontier, which the destroys above would otherwise re-dirty.
    for res <- [Expenses, CategoryTotals, Rollups, Verdicts, Readings] do
      res |> Ash.read!() |> Enum.each(&Ash.destroy!/1)
    end

    # AFTER the cleanup destroys, which enqueue too.
    ReactiveDag.Test.Pending.reset_enqueued()
    ReactiveDag.Test.Pending.reset()

    :ok
  end

  # rows written the way the library writes them, so the tests start from a
  # computed cell rather than a hand-made one
  defp computed_row(attrs) do
    Node.Payload.upsert(CategoryTotals, :key, attrs.key, attrs)
    Ash.get!(CategoryTotals, attrs.key)
  end

  test "a write through an AUGMENTED action marks the node's own key" do
    row = computed_row(%{key: "travel", category: "travel", total: 100.0, n: 2})

    row |> Ash.Changeset.for_update(:correct, %{total: 120.0}) |> Ash.update!()

    assert ReactiveDag.Test.Pending.enqueued_work() == [{"category_totals", ["travel"]}]
  end

  test "every action in the list marks — not just the first" do
    row = computed_row(%{key: "meals", category: "meals", total: 10.0, n: 1})

    row |> Ash.Changeset.for_update(:approve, %{note: "checked"}) |> Ash.update!()

    assert ReactiveDag.Test.Pending.enqueued_work() == [{"category_totals", ["meals"]}]
  end

  test "an augmented CREATE marks too — even sharing its type with the payload upsert" do
    # `:add` and `:upsert` are both `create` actions on this resource. Only the
    # one NAMED in `augmented_by` marks; that they cannot be told apart by TYPE
    # is the whole reason this option names actions.
    CategoryTotals
    |> Ash.Changeset.for_create(:add, %{key: "fuel", category: "fuel", total: 5.0, n: 1})
    |> Ash.create!()

    assert ReactiveDag.Test.Pending.enqueued_work() == [{"category_totals", ["fuel"]}]
  end

  test "a write through a NON-augmented action on the same resource marks nothing" do
    row = computed_row(%{key: "lodging", category: "lodging", total: 50.0, n: 1})

    # `:annotate` writes the same column `:approve` does, and is not in the list
    row |> Ash.Changeset.for_update(:annotate, %{note: "internal"}) |> Ash.update!()

    assert ReactiveDag.Test.Pending.enqueued_work() == []
  end

  test "THE POINT: the library's own payload upsert does NOT mark" do
    # This is the reason `augmented_by` names actions rather than action types.
    # Wired globally the way `dirties_on` is, this upsert would re-dirty the
    # cell the drain just computed, and the next drain would do it again.
    Node.Payload.upsert(CategoryTotals, :key, "travel", %{
      key: "travel",
      category: "travel",
      total: 100.0,
      n: 2
    })

    assert ReactiveDag.Test.Pending.enqueued_work() == [],
           "the recompute's own write must not re-originate its cell"

    # and again, as a repeated drain would: still nothing, so it settles
    Node.Payload.upsert(CategoryTotals, :key, "travel", %{
      key: "travel",
      category: "travel",
      total: 100.0,
      n: 2
    })

    assert ReactiveDag.Test.Pending.enqueued_work() == []
  end

  test "a drain over an augmented node settles — it does not re-dirty itself" do
    plan = Node.graph([Expenses, CategoryTotals])

    Expenses
    |> Ash.Changeset.for_create(:create, %{key: "e1", category: "travel", amount: 100.0})
    |> Ash.create!()

    # The write enqueued its own cascade; run it the way `CascadeWorker` would.
    for {cell, keys, opts} <- ReactiveDag.Test.Pending.enqueued() do
      ReactiveDag.Test.Pending.add(cell, keys, versions: Keyword.get(opts, :versions, %{}))
    end

    ReactiveDag.Test.Pending.reset_enqueued()
    {:ok, _} = ReactiveDag.Test.Pending.cascade(plan)

    assert (CategoryTotals |> Ash.get!("travel")).total == 100.0

    # THE POINT: the fold's own payload write into `category_totals` must not
    # enqueue a further cascade, or the graph never settles.
    assert ReactiveDag.Test.Pending.enqueued_work() == [],
           "the cascade left nothing behind — an augmented node does not re-originate itself"
  end

  test "the key is the IDENTITY serialization for a composite primary key" do
    Node.Payload.upsert_identity(Rollups, [:fund, :fy], %{fund: "gf", fy: "2025", total: 1.0})
    assert ReactiveDag.Test.Pending.enqueued_work() == [],
           "and the payload write itself still originates nothing"

    Rollups
    |> Ash.get!(%{fund: "gf", fy: "2025"})
    |> Ash.Changeset.for_update(:correct, %{total: 2.0})
    |> Ash.update!()

    assert ReactiveDag.Test.Pending.enqueued_work() == [{"rollups", ["gf|2025"]}]
  end

  describe "originating composes with dirties_on" do
    # This block counted calls through a `:drain_enqueuer` seam, because under
    # the queue a write MARKED and then — separately, only if the node opted in
    # — scheduled a drain to consume that mark. `MarkDirty` now enqueues the
    # cascade directly and nothing in `lib/` reads `:drain_enqueuer`, so those
    # counts would all be zero and the tests would pass while asserting nothing.
    #
    # The property that matters survives intact, and is now easier to state:
    # every enqueue IS the schedule, so counting enqueues is counting schedules.

    test "an augmented write enqueues its own cascade, so the correction lands promptly" do
      Node.Payload.upsert(Verdicts, :key, "v1", %{key: "v1", status: "fail"})

      assert ReactiveDag.Test.Pending.enqueued_work() == [],
             "the library's own payload write neither originates nor schedules"

      Verdicts
      |> Ash.get!("v1")
      |> Ash.Changeset.for_update(:override, %{status: "accepted"})
      |> Ash.update!()

      assert ReactiveDag.Test.Pending.enqueued_work() == [{"verdicts", ["v1"]}]
    end

    test "an action covered by BOTH dirties_on and augmented_by enqueues ONCE" do
      # Ash concatenates an action's own changes with the resource's global
      # changes and dedupes neither, so a naive wiring runs MarkDirty twice.
      # The frontier insert was ON CONFLICT DO NOTHING and would have hidden
      # that; a list of enqueues cannot hide it, which is why this assertion
      # counts entries rather than asking whether one exists.
      Readings
      |> Ash.Changeset.for_create(:create, %{key: "r1", value: 1.0})
      |> Ash.create!()

      assert length(ReactiveDag.Test.Pending.enqueued()) == 1,
             "the create is covered by dirties_on only"

      ReactiveDag.Test.Pending.reset_enqueued()

      Readings
      |> Ash.get!("r1")
      |> Ash.Changeset.for_update(:correct, %{value: 2.0})
      |> Ash.update!()

      assert length(ReactiveDag.Test.Pending.enqueued()) == 1,
             ":correct is covered by BOTH, and must enqueue exactly once"

      assert ReactiveDag.Test.Pending.enqueued_work() == [{"readings", ["r1"]}]
    end
  end

  describe "compile-time checks" do
    # A name that doesn't wire is the worst outcome for a feature whose whole
    # job is that a human's correction is not silently dropped — so each of
    # these is rejected at compile time, not at the correction that never
    # propagated.
    #
    # Asserted by calling the verifier directly (the idiom in
    # `no_computation_test.exs`): Spark runs verifiers from `@after_verify`, in
    # the parallel checker, so a `defmodule` here reports the DslError as a
    # compiler warning after this file is already built — visible in the output,
    # but not something a test can catch. `verify/1` on the built DSL config is
    # the same check at a moment a test can assert on.

    test "an UNKNOWN action is rejected, and the message lists the real ones" do
      defmodule Ghost do
        use Ash.Resource,
          domain: ReactiveDag.AugmentedByTest.Domain,
          data_layer: Ash.DataLayer.Ets,
          extensions: [ReactiveDag.Node]

        ets do
        end

        attributes do
          attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
        end

        actions do
          defaults [:read]

          create :upsert do
            accept([:key])
          end

          update :correct do
            accept([])
          end
        end

        reactive do
          id(:ghost)
          leaf?(true)
          augmented_by([:aprove])
        end
      end

      assert {:error, %Spark.Error.DslError{message: msg}} =
               VerifyReactive.verify(Ghost.spark_dsl_config())

      assert msg =~ "names an action this resource doesn't have"
      assert msg =~ "never propagate"
      # the fix is in the message: here are the actions you could have meant
      assert msg =~ ":correct"
    end

    test "a READ action is rejected — a mark is a consequence of a WRITE" do
      defmodule Readable do
        use Ash.Resource,
          domain: ReactiveDag.AugmentedByTest.Domain,
          data_layer: Ash.DataLayer.Ets,
          extensions: [ReactiveDag.Node]

        ets do
        end

        attributes do
          attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
        end

        actions do
          defaults [:read]

          create :upsert do
            accept([:key])
          end
        end

        reactive do
          id(:readable)
          leaf?(true)
          augmented_by([:read])
        end
      end

      assert {:error, %Spark.Error.DslError{message: msg}} =
               VerifyReactive.verify(Readable.spark_dsl_config())

      assert msg =~ "names a :read action"
      assert msg =~ "nothing to dirty when nothing changed"
    end

    test "the PAYLOAD action is rejected, with the infinite loop spelled out" do
      defmodule SelfDirtying do
        use Ash.Resource,
          domain: ReactiveDag.AugmentedByTest.Domain,
          data_layer: Ash.DataLayer.Ets,
          extensions: [ReactiveDag.Node]

        ets do
        end

        attributes do
          attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
        end

        actions do
          defaults [:read]

          create :upsert do
            upsert?(true)
            accept([:key])
          end
        end

        reactive do
          id(:self_dirtying)
          leaf?(true)
          augmented_by([:upsert])
        end
      end

      assert {:error, %Spark.Error.DslError{message: msg}} =
               VerifyReactive.verify(SelfDirtying.spark_dsl_config())

      assert msg =~ "names the PAYLOAD action"
      assert msg =~ "the drain would never settle"
      # and the fix, for a host who really does edit through it
      assert msg =~ "payload_action"
    end

    test "a RENAMED payload action is what's rejected — the check follows the option" do
      defmodule Renamed do
        use Ash.Resource,
          domain: ReactiveDag.AugmentedByTest.Domain,
          data_layer: Ash.DataLayer.Ets,
          extensions: [ReactiveDag.Node]

        ets do
        end

        attributes do
          attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
        end

        actions do
          defaults [:read]

          create :write_row do
            upsert?(true)
            accept([:key])
          end
        end

        reactive do
          id(:renamed)
          leaf?(true)
          payload_action(:write_row)
          augmented_by([:write_row])
        end
      end

      assert {:error, %Spark.Error.DslError{message: msg}} =
               VerifyReactive.verify(Renamed.spark_dsl_config())

      assert msg =~ "names the PAYLOAD action"
    end

    test "a correctly-named action passes — the checks reject the mistake, not the feature" do
      # the control. `CategoryTotals` at the top of this file is the real one:
      # it verified at compile time, which is why the rest of this file runs.
      assert :ok = VerifyReactive.verify(CategoryTotals.spark_dsl_config())
      assert :ok = VerifyReactive.verify(Readings.spark_dsl_config())
    end
  end
end

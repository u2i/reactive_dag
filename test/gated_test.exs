defmodule ReactiveDag.GatedTest do
  @moduledoc """
  `gated` — a change waits for a human before it PROPAGATES.

  The row is written as normal. What waits is the cascade, which matters because
  a host's derived tables are often what it serves: deferring the write would put
  a review queue between a user's write and the page that shows it.

  The property this file is really about:

    * **the actor decides, not the cell alone.** A person editing a row should
      not queue for approval of their own edit; an extractor claiming what a
      meeting decided is exactly what wants review. The library cannot tell a
      person from a service account, so the host supplies the predicate.

  ## Approval assertions were removed pending the sign-off phase

  This file used to assert the reviewer's half of the gate as well: that
  `approve/1` released a held key, that `reject/1` discarded it while the row
  stood, that approving twice was a no-op, that approving one key left its
  siblings held, and that a second change to a held key merged into it keeping
  the EARLIEST version — so a reviewer saw one net change rather than a queue of
  intermediate steps.

  Every one of those went through `Frontier.approve/reject/awaiting`, and the
  frontier is gone. Approval is a deferred phase in the new engine: a gated
  change SUSPENDS, which is asserted below, but nothing yet reads those
  suspensions back and releases them. There is no replacement API to point these
  at, and rewriting them against `FakeSuspensionRepo` would assert that a row
  exists rather than that approving it does anything — so they are deleted
  rather than made vacuous, and belong with the sign-off phase when it lands.

  The merge property in particular is worth restoring deliberately: it is the
  one a reimplementation is most likely to get wrong, because append-only
  suspensions naturally produce the queue-of-steps the old design refused.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.Test.FakeSuspensionRepo

  # The host's answers, at MODULE scope — a DSL `{Mod, :fun, []}` resolves
  # against the module, not against a `describe` block.
  def person?(%{kind: :person}), do: true
  def person?(_), do: false

  # Stands in for "look up the version row just written". `boom` raises, to prove
  # a failing resolver costs the record rather than the host's write.
  def fake_version_for(%{key: "boom"}, _changeset), do: raise("no version table")
  def fake_version_for(record, _changeset), do: "version-for-" <> record.key

  # Reads a version reference back. A suspending cell must declare one, so a
  # resumption can narrow to what moved rather than recomputing everything.
  def fake_changes(_version_id), do: %{}

  setup do
    start_supervised!(FakeSuspensionRepo)
    FakeSuspensionRepo.install()

    # A gated write no longer parks a key on a hold queue — it enqueues a cascade
    # that carries the gate verdict as `skip_gate:`. Capturing the enqueue is how
    # a test reads that verdict back without standing up Oban.
    ReactiveDag.Test.Pending.capture_enqueues()
    ReactiveDag.Test.Pending.reset_enqueued()
    :ok
  end

  # Run the cascade a write enqueued, exactly as `CascadeWorker` would — the
  # `skip_gate:` the write decided is what makes the gate hold or clear.
  defp cascade_enqueued(plan) do
    [{cell, keys, opts}] = ReactiveDag.Test.Pending.enqueued()
    ReactiveDag.Test.Pending.reset_enqueued()

    run_opts =
      if Keyword.get(opts, :skip_gate, false), do: [skip_gate: cell], else: []

    ReactiveDag.Cascade.run(
      plan,
      [%{cell: cell, keys: keys, versions: Keyword.get(opts, :versions, %{})}],
      run_opts
    )
  end

  describe "the actor decides" do
    defmodule Domain do
      use Ash.Domain, validate_config_inclusion?: false

      resources do
        allow_unregistered?(true)
      end
    end

    # The host's answer to "was this a person". A real one would check a struct
    # or a role; this is the shape, which is all the library depends on.
    defmodule Notes do
      use Ash.Resource,
        domain: ReactiveDag.GatedTest.Domain,
        data_layer: Ash.DataLayer.Ets,
        extensions: [ReactiveDag.Node]

      ets do
        private?(true)
      end

      attributes do
        attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
        attribute :body, :string, public?: true
      end

      actions do
        defaults [:read, :destroy]
        create :upsert, upsert?: true, accept: [:key, :body]
      end

      reactive do
        id(:notes)
        leaf?(true)
        payload_key(:key)
        dirties_on([:create, :update])
        # `gated` alone is what lowers to the :approval suspend reason —
        # `suspends` is the :expensive one, and declaring it here would make the
        # cascade stop for the wrong reason (`:expensive` wins when both are set).
        gated(human?: {ReactiveDag.GatedTest, :person?, []})
        # Required of any suspending cell: a resumption has to be able to read
        # back the change that stopped it, or it recomputes the whole cell.
        version_id({ReactiveDag.GatedTest, :fake_version_for, []})
        version_diff({ReactiveDag.GatedTest, :fake_changes, []})
      end
    end

    test "a MACHINE change is held" do
      # No actor: the graph, a worker, an extractor. Nothing claimed to be a
      # person, so it waits. Under the queue "waits" meant unclaimable; it now
      # means the cascade suspends on the gate instead of propagating.
      Ash.create!(Notes, %{key: "m1", body: "extracted"}, action: :upsert)

      assert [{"notes", ["m1"], opts}] = ReactiveDag.Test.Pending.enqueued()
      refute Keyword.get(opts, :skip_gate, false), "a machine write does not clear the gate"

      {:ok, report} = cascade_enqueued(ReactiveDag.Node.graph([Notes]))

      assert [%{waiting: waiting, reason: :approval, row_uuid: "m1"}] = report.suspended
      assert waiting == inspect(Notes)
      assert report.steps == [], "a held change propagates nothing"
    end

    test "a PERSON's change propagates immediately" do
      # Nobody should queue for approval of their own edit.
      Ash.create!(Notes, %{key: "h1", body: "typed"},
        action: :upsert,
        actor: %{kind: :person}
      )

      assert [{"notes", ["h1"], opts}] = ReactiveDag.Test.Pending.enqueued()
      assert Keyword.get(opts, :skip_gate), "a person's own edit clears the gate"

      {:ok, report} = cascade_enqueued(ReactiveDag.Node.graph([Notes]))

      assert report.suspended == []
      assert "notes" in ReactiveDag.Report.cells(report)
    end

    test "a non-person actor is a machine — a service account is not a human" do
      # The case `nil`-means-machine would get wrong: a host whose LLM calls run
      # as their own identity.
      Ash.create!(Notes, %{key: "s1", body: "by robot"},
        action: :upsert,
        actor: %{kind: :service}
      )

      assert [{"notes", ["s1"], opts}] = ReactiveDag.Test.Pending.enqueued()
      refute Keyword.get(opts, :skip_gate, false), "a service account is not a person"

      {:ok, report} = cascade_enqueued(ReactiveDag.Node.graph([Notes]))

      assert [%{waiting: waiting, reason: :approval, row_uuid: "s1"}] = report.suspended
      assert waiting == inspect(Notes)
    end
  end

  describe "the version reference" do
    # An enqueued cascade says WHICH entity changed; the version says WHAT the
    # change was. The cascade is consumed and gone, so the version is the only
    # thing that can explain an approved or rejected change afterwards.
    defmodule Versioned do
      use Ash.Resource,
        domain: ReactiveDag.GatedTest.Domain,
        data_layer: Ash.DataLayer.Ets,
        extensions: [ReactiveDag.Node]

      ets do
        private?(true)
      end

      attributes do
        attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
        attribute :body, :string, public?: true
      end

      actions do
        defaults [:read, :destroy]
        create :upsert, upsert?: true, accept: [:key, :body]
      end

      reactive do
        id(:versioned)
        leaf?(true)
        payload_key(:key)
        dirties_on([:create, :update])
        gated(true)
        # The HOST resolves it: a version resource is the host's, with whatever
        # name and key type it chose. A real one would look up the row
        # `ash_paper_trail` just wrote in the same transaction.
        version_id({ReactiveDag.GatedTest, :fake_version_for, []})
      end
    end

    test "the change carries the version id, and a reviewer gets it back" do
      # Was read off the held frontier row via `awaiting/1`; it now rides on the
      # enqueued cascade's `versions:`. Same reference, same guarantee — the
      # change is referenced, not copied.
      Ash.create!(Versioned, %{key: "v1", body: "extracted"}, action: :upsert)

      assert [{"versioned", ["v1"], opts}] = ReactiveDag.Test.Pending.enqueued()
      assert Keyword.fetch!(opts, :versions) == %{"v1" => "version-for-v1"}
    end

    test "a resolver that raises costs the record, not the write" do
      # Failing a host's write over a bookkeeping lookup would be the wrong
      # trade. The change is still enqueued and still propagates; only the
      # durable reference is missing.
      Ash.create!(Versioned, %{key: "boom", body: "x"}, action: :upsert)

      assert [{"versioned", ["boom"], opts}] = ReactiveDag.Test.Pending.enqueued()
      assert Keyword.fetch!(opts, :versions) == %{}
    end
  end
end

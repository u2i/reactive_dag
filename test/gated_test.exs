defmodule ReactiveDag.GatedTest do
  @moduledoc """
  `gated` — a change waits for a human before it PROPAGATES.

  The row is written as normal. What waits is the cascade, which matters because
  a host's derived tables are often what it serves: deferring the write would put
  a review queue between a user's write and the page that shows it.

  Two properties this file is really about:

    * **the actor decides, not the cell alone.** A person editing a row should
      not queue for approval of their own edit; an extractor claiming what a
      meeting decided is exactly what wants review. The library cannot tell a
      person from a service account, so the host supplies the predicate.
    * **a second change MERGES into a held one.** A reviewer sees the whole state
      change since the last settled point — never a queue of intermediate steps,
      and never an intermediate unit no settled state held.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.Frontier

  # The host's answers, at MODULE scope — a DSL `{Mod, :fun, []}` resolves
  # against the module, not against a `describe` block.
  def person?(%{kind: :person}), do: true
  def person?(_), do: false

  # Stands in for "look up the version row just written". `boom` raises, to prove
  # a failing resolver costs the record rather than the host's write.
  def fake_version_for(%{key: "boom"}, _changeset), do: raise("no version table")
  def fake_version_for(record, _changeset), do: "version-for-" <> record.key

  setup do
    start_supervised!(ReactiveDag.Test.FakeFrontierRepo)
    ReactiveDag.Test.FakeFrontierRepo.install()
    :ok
  end

  describe "a held change is not claimable" do
    test "until it is approved" do
      Frontier.mark_dirty("c", ["k1"], "extraction", awaiting_approval: true)

      assert Frontier.claim("c") == [], "a held change is not work the drain may take"
      assert Frontier.awaiting("c") == [{"k1", nil}]

      assert Frontier.approve("c") == ["k1"]
      assert Frontier.claim("c") == ["k1"]
    end

    test "and an ordinary mark beside it claims freely" do
      Frontier.mark_dirty("c", ["held"], "extraction", awaiting_approval: true)
      Frontier.mark_dirty("c", ["free"], "poll")

      assert Frontier.claim("c") == ["free"]
      assert Frontier.awaiting("c") == [{"held", nil}]
    end

    test "`empty?/1` counts CLAIMABLE work, so a held change leaves it true" do
      # Otherwise a caller looping until the frontier empties never finishes:
      # there is nothing the drain can do, which is exactly the state.
      Frontier.mark_dirty("c", ["k1"], "extraction", awaiting_approval: true)

      assert Frontier.empty?()
    end

    test "a held cell is not offered for selection" do
      # `dirty_cells/1` feeds the drain's cell choice. Offering a cell it cannot
      # claim from would have it pick, claim nothing, and pick again.
      Frontier.mark_dirty("c", ["k1"], "extraction", awaiting_approval: true)

      assert Frontier.dirty_cells() == []
    end
  end

  describe "approve and reject" do
    test "reject discards the mark — the row stands, the consumers do not move" do
      # The gate holds PROPAGATION, so "no" means the derived row stays as
      # written and nothing downstream recomputes from it.
      Frontier.mark_dirty("c", ["k1"], "extraction", awaiting_approval: true)

      assert Frontier.reject("c") == ["k1"]
      assert Frontier.awaiting("c") == []
      assert Frontier.claim("c") == []
    end

    test "approving a specific key leaves the others held" do
      for k <- ["a", "b"], do: Frontier.mark_dirty("c", [k], "x", awaiting_approval: true)

      assert Frontier.approve("c", ["a"]) == ["a"]
      assert Frontier.awaiting("c") |> Enum.map(&elem(&1, 0)) == ["b"]
    end

    test "approving twice is a no-op, not an error" do
      # A double click, or two reviewers, must not fail.
      Frontier.mark_dirty("c", ["k1"], "x", awaiting_approval: true)

      assert Frontier.approve("c") == ["k1"]
      assert Frontier.approve("c") == []
    end

    test "approving nothing claims nothing" do
      assert Frontier.approve("c") == []
      assert Frontier.reject("c") == []
    end
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
        gated(human?: {ReactiveDag.GatedTest, :person?, []})
      end
    end

    test "a MACHINE change is held" do
      # No actor: the graph, a worker, an extractor. Nothing claimed to be a
      # person, so it waits.
      Ash.create!(Notes, %{key: "m1", body: "extracted"}, action: :upsert)

      assert Frontier.claim("notes") == []
      assert Frontier.awaiting("notes") |> Enum.map(&elem(&1, 0)) == ["m1"]
    end

    test "a PERSON's change propagates immediately" do
      # Nobody should queue for approval of their own edit.
      Ash.create!(Notes, %{key: "h1", body: "typed"},
        action: :upsert,
        actor: %{kind: :person}
      )

      assert Frontier.claim("notes") == ["h1"]
    end

    test "a non-person actor is a machine — a service account is not a human" do
      # The case `nil`-means-machine would get wrong: a host whose LLM calls run
      # as their own identity.
      Ash.create!(Notes, %{key: "s1", body: "by robot"},
        action: :upsert,
        actor: %{kind: :service}
      )

      assert Frontier.claim("notes") == []
      assert Frontier.awaiting("notes") |> Enum.map(&elem(&1, 0)) == ["s1"]
    end
  end

  describe "the version reference" do
    # A queue row says WHICH entity changed; the version says WHAT the change
    # was. The queue is consumed by the drain, so the version is the only thing
    # that can explain an approved or rejected change afterwards.
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

    test "the mark carries the version id, and a reviewer gets it back" do
      Ash.create!(Versioned, %{key: "v1", body: "extracted"}, action: :upsert)

      assert [{"v1", "version-for-v1"}] = Frontier.awaiting("versioned"),
             "the queue references the record of the change; it does not copy it"
    end

    test "a resolver that raises costs the record, not the write" do
      # Failing a host's write over a bookkeeping lookup would be the wrong
      # trade. The change is still marked and still propagates; only the durable
      # reference is missing.
      Ash.create!(Versioned, %{key: "boom", body: "x"}, action: :upsert)

      assert [{"boom", nil}] = Frontier.awaiting("versioned")
    end

  end

  describe "a second change to a held key" do
    test "keeps the EARLIEST version — the change the settled state was succeeded by" do
      # Two changes land before review. The reviewer must be pointed at the FIRST
      # version: it records the change that succeeded the last settled state,
      # which is what still needs repricing. The later one names where the row
      # ended up, which the live row already says.
      Frontier.mark_dirty("c", [{"k1", "version-1"}], "first", awaiting_approval: true)
      Frontier.mark_dirty("c", [{"k1", "version-2"}], "second", awaiting_approval: true)

      assert Frontier.awaiting("c") == [{"k1", "version-1"}]
    end

    test "stays held even when the second change is not itself gated" do
      # A reviewer approves a net effect, not a moving target — so an ungated
      # write landing on a held key must not release it.
      Frontier.mark_dirty("c", [{"k1", "version-1"}], "first", awaiting_approval: true)
      Frontier.mark_dirty("c", ["k1"], "second")

      assert Frontier.claim("c") == []
      assert Frontier.awaiting("c") |> Enum.map(&elem(&1, 0)) == ["k1"]
    end
  end
end

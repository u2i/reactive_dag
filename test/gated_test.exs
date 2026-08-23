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

  defmodule FakeRepo do
    def start_link, do: Agent.start_link(fn -> %{} end, name: __MODULE__)

    def query!("INSERT INTO " <> _, params) do
      params
      |> Enum.chunk_every(7)
      |> Enum.each(fn [cell, tenant, key, _r, _t, prior, held] ->
        Agent.update(__MODULE__, fn m ->
          Map.update(m, {tenant, cell, key}, {prior, held}, fn {old_prior, old_held} ->
            # The ON CONFLICT clause: merge the diffs, and a HELD key stays held.
            {Frontier.merge_diffs(old_prior, prior), old_held || nil}
          end)
        end)
      end)

      %{rows: []}
    end

    def query!("SELECT DISTINCT cell_id" <> _, _p) do
      ids =
        Agent.get(__MODULE__, & &1)
        |> Enum.reject(fn {_k, {_p, held}} -> held == true end)
        |> Enum.map(fn {{_t, c, _k}, _v} -> c end)
        |> Enum.uniq()

      %{rows: Enum.map(ids, &[&1])}
    end

    # Two DELETEs share this prefix and RETURN different shapes: a claim wants
    # `key, prior`; a reject wants `key` alone. Distinguished the way the SQL
    # does — by whether it filters on held rows.
    def query!("DELETE FROM " <> _ = q, [cell, tenant]) do
      held? = String.contains?(q, "awaiting_approval IS TRUE")

      rows =
        Agent.get_and_update(__MODULE__, fn m ->
          {mine, rest} =
            Enum.split_with(m, fn {{t, c, _}, {_p, held}} ->
              t == tenant and c == cell and if held?, do: held == true, else: held != true
            end)

          taken =
            Enum.map(mine, fn {{_t, _c, k}, {p, _h}} ->
              if held?, do: [k], else: [k, p]
            end)

          {taken, Map.new(rest)}
        end)

      %{rows: rows}
    end

    def query!("UPDATE " <> _, [cell, tenant]) do
      keys =
        Agent.get_and_update(__MODULE__, fn m ->
          {mine, rest} =
            Enum.split_with(m, fn {{t, c, _}, {_p, held}} ->
              t == tenant and c == cell and held == true
            end)

          released = Map.new(mine, fn {k, {p, _}} -> {k, {p, nil}} end)
          {Enum.map(mine, fn {{_t, _c, k}, _v} -> [k] end), Map.merge(Map.new(rest), released)}
        end)

      %{rows: keys}
    end

    # The keyed variants of approve/reject take a third param.
    def query!("UPDATE " <> _, [cell, tenant, keys]) do
      released =
        Agent.get_and_update(__MODULE__, fn m ->
          {mine, rest} =
            Enum.split_with(m, fn {{t, c, k}, {_p, held}} ->
              t == tenant and c == cell and held == true and k in keys
            end)

          {Enum.map(mine, fn {{_t, _c, k}, _v} -> [k] end),
           Map.merge(Map.new(rest), Map.new(mine, fn {k, {p, _}} -> {k, {p, nil}} end))}
        end)

      %{rows: released}
    end

    def query!("DELETE FROM " <> _, [cell, tenant, keys]) do
      rows =
        Agent.get_and_update(__MODULE__, fn m ->
          {mine, rest} =
            Enum.split_with(m, fn {{t, c, k}, {_p, held}} ->
              t == tenant and c == cell and held == true and k in keys
            end)

          {Enum.map(mine, fn {{_t, _c, k}, _v} -> [k] end), Map.new(rest)}
        end)

      %{rows: rows}
    end

    def query!("SELECT key, prior" <> _, [cell, tenant]) do
      rows =
        Agent.get(__MODULE__, & &1)
        |> Enum.filter(fn {{t, c, _}, {_p, held}} ->
          t == tenant and c == cell and held == true
        end)
        |> Enum.map(fn {{_t, _c, k}, {p, _h}} -> [k, p] end)

      %{rows: rows}
    end

    def query!("SELECT COUNT" <> _, _p) do
      n =
        Agent.get(__MODULE__, & &1)
        |> Enum.count(fn {_k, {_p, held}} -> held != true end)

      %{rows: [[n]]}
    end

    def query!("SELECT pg_" <> _, _p), do: %{rows: [[true]]}
  end

  setup do
    start_supervised!(%{id: FakeRepo, start: {FakeRepo, :start_link, []}})
    prev = Application.get_env(:reactive_dag, :repo)
    Application.put_env(:reactive_dag, :repo, FakeRepo)
    on_exit(fn -> Application.put_env(:reactive_dag, :repo, prev) end)
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
    def person?(%{kind: :person}), do: true
    def person?(_), do: false

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

  describe "a second change to a held key" do
    test "MERGES, so a reviewer sees the net effect" do
      # meals -> travel is held; travel -> lodging arrives. The reviewer must see
      # meals -> lodging: `travel` is an intermediate no settled state held, and
      # `meals` is the unit that still needs repricing.
      Frontier.mark_dirty(
        "c",
        [{"k1", %{"cat" => %{"from" => "meals", "to" => "travel"}}}],
        "first",
        awaiting_approval: true
      )

      Frontier.mark_dirty(
        "c",
        [{"k1", %{"cat" => %{"from" => "travel", "to" => "lodging"}}}],
        "second",
        awaiting_approval: true
      )

      assert Frontier.awaiting("c") ==
               [{"k1", %{"cat" => %{"from" => "meals", "to" => "lodging"}}}]
    end

    test "stays held even when the second change is not itself gated" do
      # A reviewer approves a net effect, not a moving target — so an ungated
      # write landing on a held key must not release it.
      Frontier.mark_dirty("c", ["k1"], "first", awaiting_approval: true)
      Frontier.mark_dirty("c", ["k1"], "second")

      assert Frontier.claim("c") == []
      assert Frontier.awaiting("c") |> Enum.map(&elem(&1, 0)) == ["k1"]
    end
  end
end

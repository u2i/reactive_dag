# Human augmentation

Some of what a node holds is not computed. A person corrects a
mis-transcribed name, approves a set of figures, annotates a row with something
no upstream data contains. That mark has to live somewhere, survive the right
recomputes, lapse on the wrong ones, and — the part hosts most often get wrong —
**re-run the cascade**, because a human edit is an input change like any other.

Two declarations cover it:

- `augmented_by` — which of this node's actions are human edits. They mark the
  node's key dirty; the library's own payload write does not.
- `lapse` — what a machine recompute does to the human's mark: leave it,
  clear it, or clear it only when the fields the human was actually looking at
  move.

## Scope

These two declarations are deliberately small. `augmented_by` adds an edge to
the dirty graph; `lapse` adds a rule about what survives a recompute. Between
them they cover the graph-shaped part of human input: *when does an edit
re-run things, and what does a re-run do to the edit?*

They are not an approval system. Who may approve, what constitutes a quorum,
whether an approval can be delegated, what any of it *means* — those are domain
questions, they differ between applications, and they belong in yours.

This library did once ship the larger thing: an attestation subsystem with
records, bases, gates, eligibility and quorum, ~3200 lines. It was removed and
is being rebuilt from the graph outwards, starting here, because the mechanism
below is the part every host needs and the part that has to be right. If you
need the rest today, build it on top — and if a piece of it turns out to be
genuinely graph-shaped, that is a good argument for the next instalment.

## The mark attaches to the node's key

A human edit is not an independent fact that happens to arrive near a node. A
correction is *about meeting X*; a sign-off is *on fiscal year 2026*. Its
identity is the thing it annotates.

So the write goes **through the node's own action**, and the key follows by
construction rather than by convention:

```elixir
defmodule MyApp.TranscriptRecord do
  use Ash.Resource,
    domain: MyApp.Domain,
    data_layer: AshPostgres.DataLayer,
    extensions: [ReactiveDag.Node]

  attributes do
    attribute :meeting_id, :string, primary_key?: true
    attribute :body, :string          # computed
    attribute :note, :string          # human
  end

  actions do
    create :upsert do
      upsert?(true)
      accept([:meeting_id, :body])    # NOT :note — see below
    end

    update :correct do
      accept([:note])
    end
  end

  reactive do
    id :transcript_record
    recompute_by :cell
    augmented_by [:correct]
  end
end
```

`Ash.update!(record, :correct, %{note: "the name is Okonkwo"})` marks
`transcript_record`'s key dirty inside the write's transaction, and the next
drain re-runs the extraction with the correction in hand.

The alternative — a free-floating `Correction` resource that some other node
happens to read — is the shape to avoid. It has its own key, which has to be
kept in step with the key it is really about, by hand, forever.

### Many marks per key

A one-to-many annotation (six corrections on one meeting, each with its author
and timestamp) stays a child resource. What makes it *attached* is that the
write routes through a node action:

```elixir
actions do
  update :correct do
    accept([])
    argument :note, :string, allow_nil?: false
    change fn cs, _ ->
      Ash.Changeset.after_action(cs, fn _cs, record ->
        MyApp.Correction
        |> Ash.Changeset.for_create(:create, %{
             meeting_id: record.meeting_id,
             note: Ash.Changeset.get_argument(cs, :note)
           })
        |> Ash.create!()

        {:ok, record}
      end)
    end
  end
end
```

The node's key is the child's foreign key by construction, and `augmented_by
[:correct]` marks it exactly as before.

## `augmented_by` is not `dirties_on`

Both mark a key from an ordinary Ash write, and they are for opposite
situations.

`dirties_on` wires a **global** change covering every action of the given
types, deliberately: for a source-fed leaf, every write is an observation, and
a per-action wiring would let a new write site be forgotten — silent staleness.

That is exactly wrong for a **computed** node. The library writes the node's
rows itself (`ReactiveDag.Node.Payload.upsert`), which is an Ash write. A
global change would make every recompute re-dirty the cell it just computed,
and the drain would spin forever.

`augmented_by` therefore names **specific actions**, not action types. The
payload write is excluded by construction rather than by hoping the types do
not collide.

| | `dirties_on` | `augmented_by` |
|---|---|---|
| for | source-fed leaves | computed nodes |
| names | action *types* | specific *actions* |
| wiring | global (nothing forgotten) | per-action (payload write excluded) |
| risk it removes | a forgotten write site | a self-dirtying recompute |

Both may appear on one resource; an action covered by both marks once.

`schedule_drain true` applies to either: mark *and* enqueue a drain in the same
transaction, so an edit lands in seconds rather than at the next sweep. For
someone who just typed a correction and is watching the page, that is the
point.

## `lapse` — what survives a recompute

The default is **survival**, and it needs no declaration. The payload write only
sets what the computation emits, so a column the upsert action does not
`accept` is never touched. That is why `:note` is absent from `:upsert`'s
`accept` list above.

Survival is right when the mark is about something upstream that did not
change. A transcript correction is about the *recording*; re-extracting it does
not make "you misheard that name" any less true.

It is wrong for a sign-off. "I checked this" is a claim about content, and when
the content moves the claim is stale:

```elixir
reactive do
  id :fiscal_lines
  recompute_by :fy, from_key: true

  # cleared whenever the computed content moves at all
  lapse :approved_at, when_changed: :any

  # cleared only when the fields the approval was ABOUT move;
  # a spelling fix leaves it standing
  lapse :signed_off_by, when_changed: [:total, :vote_count]
end
```

`when_changed: :any` and a field list are the same mechanism at two grains —
a content comparison the library already performs to decide whether to
propagate, run a second time over the fields you name.

Note the two grains are genuinely independent: a recompute can be `:changed`
overall (so it propagates) while the lapse fields sat still, and the mark then
survives. Lapse asks its own question rather than reusing the propagate verdict.

**A lapse is its own write**, made after the payload write and only when it
fires. That is what keeps survival free: the payload action never accepts the
human column, so the normal path cannot touch it, and no declaration is needed
to protect it. Lapse needs an action it can clear the column with — a plain
`update` accepting the lapsing attributes:

```elixir
update :lapse do
  require_atomic?(false)
  accept([:approved_at, :signed_off_by])
end
```

It must **accept** every attribute it lapses. An action that accepts nothing is
a write that succeeds and clears nothing — silent, and exactly the failure this
feature exists to prevent — so a missing `accept` raises at assembly rather
than at the first lapse.

Name it with `lapse_action:` if you call it something else. For a child
resource it is a destroy action instead, and the resource must have one — a row
that should have gone but silently stayed is indistinguishable from a live one.

Choosing the narrow form is worth the thought it takes. `:any` is safe and
will clear approvals for reasons nobody considers meaningful — a re-ordered
label, a rounding change — and an approval that lapses constantly stops being
read as information.

### Clearing child rows

`lapse` takes a resource as well as an attribute. The rows attached to the
lapsing key are destroyed:

```elixir
lapse MyApp.Correction, key: :meeting_id, when_changed: [:speaker_ids]
```

`key:` names the child's column holding the node's cell key. It is required
rather than inferred: a resource may reference a node by more than one column,
and guessing wrong here deletes the wrong rows.

The resource needs a destroy action, for the same reason `retain_if_vanished`
demands one: a row that should have gone but silently stayed is
indistinguishable from a live one. Absent one, the node raises at assembly with
the fix rather than silently keeping the rows.

### Lapse is removal

A lapse nulls the column or destroys the rows. A lapsed mark is simply gone,
and the state afterwards is the state before anyone marked anything — which is
the truth: the approval no longer holds.

## Set-grain sign-off

Some marks are coarser than a row. "I approve fiscal year 2026" covers every
line in that year, and the mark should live once, not on each of nine hundred
rows.

`over:` names the unit:

```elixir
reactive do
  id :fiscal_lines
  recompute_by :fy, from_key: true

  lapse :fy_approved_at, when_changed: :any, over: :fy
end
```

The unit must be one this node already declares with `recompute_by`. That
constraint is the whole reason set-grain works: the graph knows how to
invalidate a `recompute_by` unit, so "what exactly did I approve" has an answer
the substrate can also act on. A sign-off over a set the graph has no name for
is a promise nobody can keep, and the verifier rejects it at compile time.

The unit is read off the row being written. A `recompute_by` unit is a column
the node groups by, so a declarative `group_by` puts it on the row by
construction — the row carrying `fy: "2026"` names its own unit, with no key
parsing and no extra query.

That matters most for the shape where set-grain is actually interesting. A
plain fold emits one row per unit, so `over:` there is a no-op; the case worth
declaring is a node whose `expand:` emits many rows per unit, and those keys
are host-supplied strings the library must not assume a grammar for. Reading
the column works for both.

**A set-grain mark lapses when any member moves.** One line changing clears an
approval covering the whole year — correct, since you approved a total that no
longer holds, but broad. `when_changed:` with a narrow field list is usually
the better declaration here: a cosmetic edit to one line then leaves the
sign-off standing, while a change to the figures clears it.

## What to reach for

| you want | declare |
|---|---|
| a human edit re-runs the cascade | `augmented_by [:action]` |
| ...and applies immediately | `+ schedule_drain true` |
| the mark outlives recomputes | nothing — the default; keep the column out of the payload action's `accept` |
| the mark is a claim about content | `lapse :attr, when_changed: :any` |
| ...about *particular* content | `lapse :attr, when_changed: [:fields]` |
| several marks per key | `lapse MyApp.Child, when_changed: …` |
| one mark over a set of keys | `+ over: :unit` (a `recompute_by` unit) |

## See also

- [Authoring nodes](authoring-nodes.md) — `recompute_by`, the payload loop,
  and the combinators whose output a mark attaches to.
- [One engine, and where the domain enters](seams.md) — where domain meaning
  belongs when it outgrows these two declarations.

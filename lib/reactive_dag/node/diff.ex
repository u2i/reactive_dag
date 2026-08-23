defmodule ReactiveDag.Node.Diff do
  @moduledoc """
  Which units a CHANGE affects, from the diff of that change.

  A `:group` claim answers "which unit of mine does this changed row belong to".
  Answering it by READING the row works for an update in place and fails for the
  two cases that matter:

    * the row was **deleted** — nothing to read, so the propagation degrades to
      `"*"`: reprice the whole cell, because a vanished row might have left any
      group;
    * the row **moved** between units — the live row names where it landed, never
      where it came from, so the group it left is stranded and the safe answer is
      again `"*"`.

  A version carries both sides. So the same question becomes arithmetic on a map,
  with no query and nothing to fail:

      units(version) = grain(from) ∪ grain(to)

  See `docs/adr-004-changes-as-the-propagation-source.md`.

  ## The diff shape

  One entry per attribute, in exactly four shapes:

      %{"to" => value}                    # a create: there was no prior value
      %{"from" => old, "to" => new}       # the attribute moved
      %{"unchanged" => value}             # present, untouched
      # …and an attribute absent from the map entirely was not accepted

  Borrowed from `ash_paper_trail`'s `change_tracking_mode :full_diff`
  (`AshPaperTrail.ChangeBuilders.FullDiff.Helpers`) rather than invented. A host
  already keeping a paper trail speaks this, and a claim should not depend on
  which producer wrote the change — today that is `Payload`'s own write and the
  `dirties_on` hook.

  `before/1` and `after/1` project those into two plain maps, which is all a
  grain function needs — `Declarative.group_fn/1` reads with `Map.get/2`, so a
  projected map substitutes for a row.

  ## Why both sides, always

  A `:destroy` has no `to`; a `:create` has no `from`. An update has both, and
  they are the SAME unit unless the change touched a grain attribute — in which
  case they are two, and both need repricing. Taking the union covers all four
  without a branch per action type, and `version_action_type` is then a fact for
  a human reading the log rather than a switch this module dispatches on.
  """

  alias ReactiveDag.Node.Recompute.Declarative

  @typedoc "One attribute's entry in a `:full_diff` `changes` map."
  @type entry :: %{optional(String.t()) => term()}

  @typedoc "A `:full_diff` `changes` map: attribute name (as a string) to entry."
  @type changes :: %{optional(String.t()) => entry()}

  @doc """
  The units a version affects, given the consumer's grain.

  `grain` is whatever the node declared — an attribute, a list of them, or a
  function — in the same vocabulary `Declarative.group_fn/1` accepts, because it
  is the same declaration.

  Returns a list with one element for a create, a destroy, or an update inside a
  unit; **two** for a move. Never empty, and never `:all`.

      iex> units(%{"fund" => %{"from" => "A", "to" => "ES"}}, :fund)
      ["A", "ES"]

      iex> units(%{"fund" => %{"unchanged" => "A"}}, :fund)
      ["A"]
  """
  @spec units(changes(), term(), (term() -> String.t()) | nil) :: [String.t()]
  def units(changes, grain, key_fn \\ nil) do
    key = key_fn || Declarative.key_fn(nil, nil)

    changes |> groups(grain) |> Enum.map(key) |> Enum.uniq()
  end

  @doc """
  The units a version affects, as the GROUP'S OWN VALUES — a tuple for a
  composite grain, a bare value for a single one. `units/3` is this, serialized.

  The values are what a consumer actually wants. A fold scoping its read needs
  `fund == "gf" and fiscal_year == "2025"`, and from a joined `"gf|2025"` it can
  only split on `"|"` and scope each column independently — which admits pairs
  that never changed (`"gf|2025"` and `"water|2026"` together also admit
  `"gf|2026"`). The values name the exact pairs, so the read is exact.

      iex> groups(%{"fund" => %{"from" => "A", "to" => "ES"}}, :fund)
      ["A", "ES"]

      iex> groups(%{"f" => %{"unchanged" => "A"}, "y" => %{"to" => "25"}}, [:f, :y])
      [{"A", "25"}]
  """
  @spec groups(changes(), term()) :: [term()]
  def groups(changes, grain) do
    group = Declarative.group_fn(grain)

    [before(changes), after_(changes)]
    |> Enum.reject(&is_nil/1)
    |> Enum.map(group)
    # A nil group names no unit, and `to_string(nil)` is `""` — a key that exists
    # nowhere. Rejected BEFORE serialising, or the empty string is what survives.
    # A composite grain is a tuple, so a nil in one position is checked too: a row
    # missing part of its own grain does not belong to a unit.
    |> Enum.reject(&nil_group?/1)
    |> Enum.uniq()
  end

  @doc """
  The row as it was, as a plain map — `nil` for a create.

  Keys are ATOMS, because a grain declaration names attributes as atoms and a
  version's `changes` names them as strings. Converting here keeps that
  translation in one place instead of at every grain function.
  """
  @spec before(changes()) :: map() | nil
  def before(changes) when is_map(changes) do
    if create?(changes), do: nil, else: project(changes, ["from", "unchanged"])
  end

  @doc "The row as it is, as a plain map — `nil` for a destroy."
  @spec after_(changes()) :: map() | nil
  def after_(changes) when is_map(changes) do
    if destroy?(changes), do: nil, else: project(changes, ["to", "unchanged"])
  end

  defp nil_group?(nil), do: true
  defp nil_group?(t) when is_tuple(t), do: Enum.any?(Tuple.to_list(t), &is_nil/1)
  defp nil_group?(_), do: false

  # A create has no prior value for ANY attribute, so no entry carries `from` or
  # `unchanged`. Inferred from the diff rather than taken from
  # `version_action_type`, so this module works on a bare `changes` map — which is
  # what a test has, and what a host reading its own version table has.
  # `changes != %{}` for the same reason `destroy?/1` needs it: `Enum.all?` over
  # nothing is TRUE, so an empty diff would read as a create — and an empty diff
  # is what an update accepting no attributes produces. Reading it as a create
  # would make `before/1` nil, which claims the row did not exist.
  defp create?(changes) do
    changes != %{} and
      Enum.all?(changes, fn {_k, e} ->
        not (Map.has_key?(e, "from") or Map.has_key?(e, "unchanged"))
      end)
  end

  # A destroy is the mirror: nothing has a `to`, and at least one attribute has a
  # prior value. The second half matters — without it an EMPTY changes map reads
  # as a destroy, and an empty map is what an update that accepted nothing
  # produces.
  defp destroy?(changes) do
    changes != %{} and
      Enum.all?(changes, fn {_k, e} -> not Map.has_key?(e, "to") end) and
      Enum.any?(changes, fn {_k, e} ->
        Map.has_key?(e, "from") or Map.has_key?(e, "unchanged")
      end)
  end

  # Take the first key present, in preference order: an attribute that moved
  # carries `from`/`to`, one that did not carries `unchanged`, and one absent from
  # the map was not accepted by the action and contributes nothing.
  defp project(changes, keys) do
    Enum.reduce(changes, %{}, fn {name, entry}, acc ->
      with {:ok, value} <-
             Enum.find_value(keys, :none, fn k -> if Map.has_key?(entry, k), do: {:ok, entry[k]} end),
           attr when not is_nil(attr) <- atomize(name) do
        Map.put(acc, attr, value)
      else
        # `:none` — the attribute was not accepted by this action; `nil` — the
        # host has never named it as an atom, so it cannot be a grain field.
        _ -> acc
      end
    end)
  end

  # `to_existing_atom`: a grain declaration is validated against the resource's
  # attributes at graph assembly, so every name a grain can ask for already
  # exists as an atom. A version naming something else is not a grain field, and
  # crashing on it would fail a propagation over an attribute nobody grouped by.
  defp atomize(name) when is_atom(name), do: name

  defp atomize(name) when is_binary(name) do
    String.to_existing_atom(name)
  rescue
    ArgumentError -> nil
  end
end

defmodule ReactiveDag.Node.Verifiers.VerifyReactive do
  @moduledoc """
  Compile-time checks for the `reactive` block — everything verifiable against
  the node's OWN resource fails at `defmodule`, not at drain time. (Checks that
  need the OVER node's resource — named read actions, attribute existence —
  run at graph assembly instead: that is the earliest point cross-node facts
  are known.)
  """
  use Spark.Dsl.Verifier

  alias ReactiveDag.Node.Recompute.Declarative

  alias ReactiveDag.Node.{
    Compose,
    Compute,
    Context,
    Join,
    PerKey,
    RecomputeBy,
    Reduce,
    Ref,
    Run,
    Union
  }

  alias Spark.Dsl.Verifier

  @impl true
  def verify(dsl) do
    entities =
      Verifier.get_entities(dsl, [:reactive])
      |> Enum.reject(
        &(match?(%Ref{}, &1) or match?(%Context{}, &1) or match?(%Compose{}, &1) or
            match?(%RecomputeBy{}, &1))
      )

    # NOT Attested: `attested` + an explicit `compute` is a documented pair
    # (the explicit compute overrides the default attestation Op).
    computations =
      Enum.filter(
        entities,
        &(match?(%Reduce{}, &1) or match?(%Join{}, &1) or match?(%Compute{}, &1) or
            match?(%Run{}, &1) or match?(%PerKey{}, &1) or match?(%Union{}, &1) or
            match?(%ReactiveDag.Node.Aggregate{}, &1))
      )

    with :ok <- one_computation(dsl, computations),
         :ok <- some_computation(dsl, computations),
         :ok <- verify_augmented_by(dsl),
         :ok <- verify_lapse(dsl),
         :ok <- verify_fingerprint_reaches_something(dsl, computations),
         :ok <- verify_compare(dsl),
         :ok <- verify_combinators(dsl, computations),
         # LAST: a node with a broken declaration should hear about THAT. This
         # check is about where correct output goes, so it is the least specific
         # thing that can be wrong and must not mask the rest.
         :ok <- verify_owns_rows(dsl, computations) do
      :ok
    end
  end

  # THE TWIN of `some_computation/2` above: that one asks whether a node will
  # compute anything, this one asks whether it has anywhere to put the answer.
  #
  # A derived node's rows ARE its resource's rows. Until rc.39 a node could
  # declare no attributes and hand the library an `upsert:` closure writing into
  # some OTHER resource's table — the "write-elsewhere" shape. It is gone, and the
  # cost it carried is why: a node whose rows live under another node's resource
  # cannot use the payload loop, so it gets no change detection and reports a
  # change on every recompute; and the library cannot see it holds rows at all, so
  # every question about them answers empty. Cascade accumulated three such cells
  # and each one caused a bug that took reading dispatch code to find.
  #
  # The exemptions are `some_computation/2`'s, for the same reasons: a LEAF's rows
  # are written by a source outside the graph, a `compose` node's cells are its
  # nested legs (the outer module builds none), and a node with no computation at
  # all is already refused above with a better message.
  defp verify_owns_rows(dsl, computations) do
    cond do
      computations == [] ->
        :ok

      Verifier.get_option(dsl, [:reactive], :leaf?) == true ->
        :ok

      Enum.any?(Verifier.get_entities(dsl, [:reactive]), &match?(%Compose{}, &1)) ->
        :ok

      Ash.Resource.Info.attributes(dsl) == [] ->
        error(
          dsl,
          "this node computes something but declares no attributes, so its rows have " <>
            "nowhere to go. A derived node's rows are its own resource's rows.\n\n" <>
            "Give it a data layer and the columns its result carries (an " <>
            "`AshPostgres`/`Ets` resource with an `:upsert` action), or `leaf? true` if " <>
            "a `ReactiveDag.Source` writes them.\n\n" <>
            "If the rows would land in ANOTHER resource's table, this node should be " <>
            "that resource's node instead — one node, one table. Writing into a " <>
            "neighbour's table costs the change detection the payload loop provides, " <>
            "and makes this cell look empty to everything that asks."
        )

      true ->
        :ok
    end
  end

  # `lapse … over:` is SET-GRAIN: one mark covering a whole unit. The unit must
  # be one this node already declares with `recompute_by`, and that constraint is
  # the whole reason set-grain works — the graph knows how to invalidate a
  # `recompute_by` unit, so "what exactly did I approve" has an answer the
  # substrate can also act on. A sign-off over a set the graph has no name for is
  # a promise nobody can keep, so it is refused here rather than at the drain,
  # where the only symptom would be an approval that never lapses.
  #
  # The rest of the entity's checking — the target being an attribute or a
  # resource, the clearing action existing and accepting the column — happens at
  # ASSEMBLY (`ReactiveDag.Node.lapses/1`), because a CHILD lapse's checks are
  # about the OTHER resource, and cross-resource facts are not reliably known at
  # `defmodule` time. `over:` is checkable here: it names this node's own unit.
  defp verify_lapse(dsl) do
    dsl
    |> Verifier.get_entities([:reactive])
    |> Enum.filter(&match?(%ReactiveDag.Node.Lapse{}, &1))
    |> Enum.reject(&is_nil(&1.over))
    |> Enum.reduce_while(:ok, fn l, :ok ->
      case verify_lapse_over(dsl, l) do
        :ok -> {:cont, :ok}
        {:error, _} = err -> {:halt, err}
      end
    end)
  end

  defp verify_lapse_over(dsl, l) do
    units =
      case unit(dsl) do
        %RecomputeBy{unit: :cell} -> [:cell]
        %RecomputeBy{unit: pairs} when is_list(pairs) -> Keyword.keys(pairs)
        %RecomputeBy{unit: u} -> [u]
        nil -> []
      end

    cond do
      units == [] ->
        error(
          dsl,
          "`lapse #{inspect(l.target)}, over: #{inspect(l.over)}` is a SET-GRAIN mark, but " <>
            "this node declares no `recompute_by` — so the graph has no name for the set " <>
            "being signed off, and no way to say when it moved. A sign-off over a set the " <>
            "graph cannot invalidate is a promise nobody can keep. Declare " <>
            "`recompute_by #{inspect(l.over)}, …`, or drop `over:` for a row-grain mark."
        )

      l.over not in units ->
        error(
          dsl,
          "`lapse #{inspect(l.target)}, over: #{inspect(l.over)}` names a unit this node " <>
            "does not recompute by — `recompute_by` declares #{inspect(units)}. `over:` " <>
            "must name one of them: that is what makes the set one the substrate can act " <>
            "on rather than a label only the human understands."
        )

      true ->
        :ok
    end
  end

  # `augmented_by` names ACTIONS, and a name that doesn't resolve wires nothing
  # at all — the worst outcome for a feature whose whole job is that a human's
  # correction is not silently dropped. So each name is checked here, where the
  # actions are already known, rather than discovered as a correction that never
  # propagated.
  defp verify_augmented_by(dsl) do
    case Verifier.get_option(dsl, [:reactive], :augmented_by) do
      names when is_list(names) and names != [] ->
        payload_action = Verifier.get_option(dsl, [:reactive], :payload_action) || :upsert
        actions = Ash.Resource.Info.actions(dsl)

        Enum.reduce_while(names, :ok, fn name, :ok ->
          case verify_augmented_action(dsl, name, payload_action, actions) do
            :ok -> {:cont, :ok}
            {:error, _} = err -> {:halt, err}
          end
        end)

      _ ->
        :ok
    end
  end

  defp verify_augmented_action(dsl, name, payload_action, actions) do
    found = Enum.find(actions, &(&1.name == name))

    cond do
      # The infinite loop, refused at compile time. The library writes this
      # node's rows through the payload action; marking there would re-dirty
      # the cell the drain just computed, and the next drain would do it again.
      name == payload_action ->
        error(
          dsl,
          "`augmented_by #{inspect(name)}` names the PAYLOAD action — the action the " <>
            "library itself writes this node's rows with. Marking there would re-dirty " <>
            "the cell on every recompute, so the drain would never settle: compute, " <>
            "mark, compute, mark. `augmented_by` names the actions a HUMAN writes " <>
            "through (`:correct`, `:approve`), which excludes the payload upsert by " <>
            "construction — that is the whole reason it names actions rather than " <>
            "action types the way `dirties_on` does. Drop it from the list, or (if a " <>
            "human really does edit through it) give the payload loop its own action " <>
            "with `payload_action`."
        )

      is_nil(found) ->
        writes = for %{type: t, name: n} <- actions, t in [:create, :update, :destroy], do: n

        error(
          dsl,
          "`augmented_by #{inspect(name)}` names an action this resource doesn't have, " <>
            "so the human edit it stands for would mark nothing and the correction would " <>
            "never propagate. Write actions declared: #{inspect(writes)}"
        )

      found.type not in [:create, :update, :destroy] ->
        error(
          dsl,
          "`augmented_by #{inspect(name)}` names a #{inspect(found.type)} action, and a " <>
            "mark is a consequence of a WRITE — there is nothing to dirty when nothing " <>
            "changed. Name the `:create`/`:update`/`:destroy` action the human's edit " <>
            "actually goes through."
        )

      true ->
        :ok
    end
  end

  defp one_computation(dsl, computations) when length(computations) > 1 do
    names = Enum.map(computations, &(&1.__struct__ |> Module.split() |> List.last()))

    error(
      dsl,
      "a node declares ONE computation — this block has #{inspect(names)}. " <>
        "Split into separate nodes (or compose legs) instead."
    )
  end

  defp one_computation(_dsl, _), do: :ok

  # A node with no computation is not an error in itself — a LEAF is fed from
  # outside, and a `compose` node's legs do the work. Anything else, though, is
  # a cell that will pass its claimed keys through untouched, so its consumers
  # recompute against inputs that never moved. That used to surface as a
  # `Logger.warning` at drain time, which is the wrong moment and the wrong
  # channel: you learn about it from stale data, long after the deploy that
  # caused it.
  defp some_computation(dsl, []) do
    cond do
      Verifier.get_option(dsl, [:reactive], :leaf?) == true ->
        :ok

      Enum.any?(Verifier.get_entities(dsl, [:reactive]), &match?(%Compose{}, &1)) ->
        :ok

      true ->
        error(
          dsl,
          "this node declares no computation, so it would pass its dirty keys through " <>
            "unchanged and everything downstream would recompute against inputs that " <>
            "never moved.\n\nDeclare one of `aggregate` / `reduce` / `join` / `union` / " <>
            "`per_key` / `run` / `compute`, or say what this node is instead: `leaf? true` " <>
            "for a node fed from outside the graph, or a `compose` block whose legs do the " <>
            "work.\n\n(`op` is a label and does not declare a computation — recompute " <>
            "dispatches on the entity, not on `op`.)"
        )
    end
  end

  defp some_computation(_dsl, _), do: :ok

  defp verify_combinators(dsl, computations) do
    Enum.reduce_while(computations, :ok, fn entity, :ok ->
      case verify_entity(dsl, entity) do
        :ok -> {:cont, :ok}
        {:error, _} = err -> {:halt, err}
      end
    end)
  end

  defp verify_entity(dsl, %Reduce{} = r) do
    # check what LOWERING will produce: the unit's pair becomes the combinator's
    # group_by, so the row-column checks below must see it.
    effective = %{r | group_by: r.group_by || block_group_by(dsl)}

    with :ok <- verify_over(dsl, r),
         :ok <- verify_unit_vs_key_rule(dsl),
         :ok <- verify_key_rule_home(dsl, r.key_rule),
         :ok <- verify_key_prefix(dsl, r.key, r.key_prefix),
         :ok <- verify_result_slots(dsl, :reduce, into: r.into, expand: r.expand),
         :ok <- verify_reduce_into(dsl, effective) do
      :ok
    end
  end

  defp verify_entity(dsl, %Join{} = j) do
    with :ok <- verify_key_rule_home(dsl, j.key_rule),
         :ok <- verify_key_prefix(dsl, j.key, j.key_prefix),
         :ok <- verify_join_inputs(dsl, j),
         :ok <- verify_side(dsl, :left, j.left),
         :ok <- verify_side(dsl, :right, j.right),
         :ok <- verify_result_slots(dsl, :join, into: j.into),
         :ok <- verify_join_into(dsl, j.into) do
      :ok
    end
  end

  defp verify_entity(dsl, %Run{action: action}),
    do: verify_generic_action(dsl, action, "run")

  # a `per_key` node's action must exist, be GENERIC, and its result must land
  # somewhere — the same shape of check `run` gets, since both name an action.
  defp verify_entity(dsl, %PerKey{} = pk) do
    with :ok <- verify_generic_action(dsl, pk.action, "per_key"),
         :ok <- verify_per_key_dests(dsl, pk) do
      verify_fingerprint_home(dsl, pk)
    end
  end

  # `aggregate` speaks the SAME fold vocabulary as `reduce into:`, so its dest
  # attributes get the same check: every column the row would carry must exist
  # on this resource.
  defp verify_entity(dsl, %ReactiveDag.Node.Aggregate{} = a) do
    folds =
      for kind <- Declarative.fold_kinds(),
          spec = Map.get(a, kind),
          not is_nil(spec),
          do: {kind, spec}

    if folds == [] do
      error(
        dsl,
        "an `aggregate` declares no aggregates — give it at least one of " <>
          "#{inspect(Declarative.fold_kinds())} (e.g. `count: :n`, `avg: [flow: :avg_flow]`)"
      )
    else
      verify_agg_dests(dsl, fold_dests(folds))
    end
  end

  defp verify_entity(_dsl, _other), do: :ok

  # ONE input or TWO, never a mix — and a two-input join must say which columns
  # each side owns.
  #
  # The ownership declaration is not bookkeeping: a claim names one side's keys,
  # so the write must omit the other side's columns to preserve them. Without
  # `owns:` the library cannot know which columns to omit, and the shape degrades
  # to exactly the nil-over-good-data bug that got it reverted once. So it is
  # REQUIRED rather than defaulted — there is no safe default, and a silent one
  # would read as coverage.
  defp verify_join_inputs(dsl, %Join{left_over: nil, right_over: nil} = j) do
    if j.over do
      :ok
    else
      error(
        dsl,
        "declares a `join` with no input: name `over:` (ONE input, split into sides by " <>
          "`left:`/`right:`) or `left_over:` + `right_over:` (TWO inputs, each read and " <>
          "scoped independently)."
      )
    end
  end

  defp verify_join_inputs(dsl, %Join{} = j) do
    cond do
      j.over ->
        error(
          dsl,
          "declares both `over:` and `left_over:`/`right_over:` on one `join`. Those are " <>
            "the one-input and two-input forms — pick one. `over:` reads a single node " <>
            "and splits it by `left:`/`right:`; `left_over:`/`right_over:` read two nodes."
        )

      is_nil(j.left_over) or is_nil(j.right_over) ->
        missing = if is_nil(j.left_over), do: "left_over:", else: "right_over:"

        error(
          dsl,
          "declares only one half of a two-input `join` — #{missing} is missing. A join " <>
            "correlates two sides; with one node use `over:` and split it by `left:`/`right:`."
        )

      true ->
        :ok
    end
  end

  defp verify_generic_action(dsl, action, label) do
    case Ash.Resource.Info.action(dsl, action) do
      %{type: :action} ->
        :ok

      %{type: other} ->
        error(
          dsl,
          "`#{label} #{inspect(action)}` names a #{inspect(other)} action — it must be a " <>
            "GENERIC action — `run` returns the changed keys " <>
            "(`action …, {:array, :string}`), `per_key` returns one row's result " <>
            "(`action …, :map`)"
        )

      nil ->
        generic = for %{type: :action, name: n} <- Ash.Resource.Info.actions(dsl), do: n

        error(
          dsl,
          "`#{label} #{inspect(action)}` names an action this resource doesn't have. " <>
            "Generic actions declared: #{inspect(generic)}"
        )
    end
  end

  # every `into:` destination must be a real attribute — the payload loop writes
  # the row into this node's own columns.
  defp verify_per_key_dests(_dsl, %PerKey{into: nil}), do: :ok
  defp verify_per_key_dests(_dsl, %PerKey{into: []}), do: :ok

  defp verify_per_key_dests(dsl, %PerKey{into: into}) do
    attrs = dsl |> Ash.Resource.Info.attributes() |> Enum.map(& &1.name) |> MapSet.new()

    case Keyword.keys(into) |> Enum.reject(&MapSet.member?(attrs, &1)) do
      [] ->
        :ok

      missing ->
        error(
          dsl,
          "`per_key … into:` would write #{inspect(missing)}, but this resource has no " <>
            "such attribute(s). Declared: #{inspect(MapSet.to_list(attrs))}"
        )
    end
  end

  # The TOP-LEVEL `fingerprint` is read on exactly two paths: `Node.Rows.write/3`
  # (a source-fed leaf's reconcile) and `Recompute.PerKey`. A node that is neither
  # — one whose computation is a `compute` module, a combinator, a `run` action —
  # never consults it, so the declaration is inert.
  #
  # Inert is the whole problem. Cascade declared `fingerprint & &1.agenda_fingerprint`
  # on a `compute` node and wrote TWO comments, in two files, explaining the skip
  # it believed it had bought; the extraction re-ran and re-propagated on every
  # pass regardless (u2i/muni_watch#20). Nothing failed, so nothing said so.
  #
  # An error rather than a warning, for the reason `some_computation/2` gives
  # above: a warning about a silent no-op is discovered from its consequences,
  # long after the deploy. Both fixes are one line — `per_key` if the skip was
  # wanted, or delete the declaration.
  # `compare` names the columns that constitute this node's result. A name that is
  # not an attribute compares nothing — `Map.take` simply omits it — so a typo
  # silently narrows the comparison further than intended, and the narrower it
  # gets the more changes go unreported. That failure is invisible: the node keeps
  # working and quietly stops telling anyone its rows moved.
  #
  # An empty list is refused for the same reason rather than treated as "compare
  # nothing": a node that can never report a change is a node whose consumers are
  # permanently stale, and if that is genuinely wanted it should be said by not
  # declaring the node.
  defp verify_compare(dsl) do
    case Verifier.get_option(dsl, [:reactive], :compare) do
      nil ->
        :ok

      [] ->
        error(
          dsl,
          "`compare []` compares no columns, so this node could never report a change and " <>
            "everything downstream of it would stay stale forever. Name the columns that " <>
            "constitute the result, or omit `compare` to compare them all."
        )

      fields when is_list(fields) ->
        attrs = dsl |> Ash.Resource.Info.attributes() |> Enum.map(& &1.name) |> MapSet.new()

        case Enum.reject(fields, &MapSet.member?(attrs, &1)) do
          [] ->
            :ok

          missing ->
            error(
              dsl,
              "`compare` names #{inspect(missing)}, which this resource has no attribute(s) " <>
                "for — a name that is not a column compares nothing, so the comparison would " <>
                "be narrower than it reads and changes would go unreported. Declared: " <>
                "#{inspect(MapSet.to_list(attrs))}"
            )
        end
    end
  end

  defp verify_fingerprint_reaches_something(dsl, computations) do
    cond do
      is_nil(Verifier.get_option(dsl, [:reactive], :fingerprint)) ->
        :ok

      # a leaf reconciles through `Rows`, which is one of the two readers
      Verifier.get_option(dsl, [:reactive], :leaf?) == true ->
        :ok

      Enum.any?(computations, &match?(%PerKey{}, &1)) ->
        :ok

      true ->
        error(
          dsl,
          "`fingerprint` is declared here but nothing will read it. It is consulted on " <>
            "two paths only: a `leaf? true` node's reconcile, and `per_key`. This node " <>
            "is neither#{computation_kind(computations)}, so the recompute runs in full " <>
            "every time the cell is claimed and the row is written and propagated — the " <>
            "cost the declaration reads as avoiding.\n\n" <>
            "Either declare `per_key` (if the input-comparison skip is what you want), " <>
            "or remove `fingerprint` and skip inside the computation itself, which is " <>
            "where a `compute` module already decides what work to do."
        )
    end
  end

  defp computation_kind([]), do: ""

  defp computation_kind([c | _]) do
    " (it declares #{c.__struct__ |> Module.split() |> List.last() |> Macro.underscore()})"
  end

  # a fingerprint needs somewhere to live, or the skip can never fire — a
  # silently-never-skipping node is exactly the expensive mistake the rung exists
  # to prevent, so it is a compile error rather than a runtime surprise.
  defp verify_fingerprint_home(_dsl, %PerKey{fingerprint: nil}), do: :ok
  defp verify_fingerprint_home(_dsl, %PerKey{fingerprint: []}), do: :ok

  defp verify_fingerprint_home(dsl, %PerKey{} = pk) do
    attr = pk.fingerprint_attribute || :fingerprint

    if Ash.Resource.Info.attribute(dsl, attr) do
      :ok
    else
      error(
        dsl,
        "`per_key … fingerprint:` needs somewhere to store the hash, but this resource " <>
          "has no #{inspect(attr)} attribute — without it the skip could never fire. " <>
          "Add `attribute #{inspect(attr)}, :string`, or name another with " <>
          "`fingerprint_attribute`."
      )
    end
  end

  defp verify_agg_dests(dsl, dests) do
    payload_attrs =
      dsl |> Ash.Resource.Info.attributes() |> Enum.map(& &1.name) |> MapSet.new()

    if MapSet.size(payload_attrs) == 0 do
      :ok
    else
      case Enum.reject(dests, &MapSet.member?(payload_attrs, &1)) do
        [] ->
          :ok

        missing ->
          error(
            dsl,
            "the `aggregate` would write #{inspect(missing)}, but this resource has no " <>
              "such attribute(s) — each aggregate maps onto one of the node's own " <>
              "attributes. Declared: #{inspect(MapSet.to_list(payload_attrs))}"
          )
      end
    end
  end

  # the `recompute_by` unit, as the group_by it lowers to.
  # ONE derivation, shared with assembly (`RecomputeBy.group_by/1`). This used to
  # re-derive it and handled only the `unit: u, from: f` shape — so a COMPOSITE
  # unit (`recompute_by [fund: :fund_code, fy: :fy]`) read as nil, and
  # `verify_dests` then reported the row "would carry [nil]" on every build of a
  # perfectly correct node. The composite path went unverified as a result, which
  # is worse than having no check: the warning read as coverage.
  defp block_group_by(dsl), do: dsl |> unit() |> group_by_of()

  defp group_by_of(nil), do: nil
  defp group_by_of(%RecomputeBy{} = u), do: RecomputeBy.group_by(u)

  defp unit(dsl) do
    Verifier.get_entities(dsl, [:reactive]) |> Enum.find(&match?(%RecomputeBy{}, &1))
  end

  # the input is named ONCE (`recompute_by to:` or the combinator's `over:`),
  # and the grouping comes from the unit's `from:` or an explicit `group_by:`.
  defp verify_over(dsl, %Reduce{} = r) do
    u = unit(dsl)

    cond do
      is_nil(r.over) and (is_nil(u) or is_nil(u.to)) ->
        error(
          dsl,
          "a `reduce` names no input — declare the unit with its edge " <>
            "(`recompute_by :category, to: :expenses, from: :expense_cat`), or name it " <>
            "on the combinator (`over: :expenses, group_by: [...]`)."
        )

      not is_nil(r.over) and not is_nil(u) and not is_nil(u.to) ->
        error(
          dsl,
          "the input is named TWICE — `recompute_by to: #{inspect(u.to)}` and " <>
            "`reduce over: #{inspect(r.over)}`. Declare it once."
        )

      # `:cell` recomputes whole, so it names no grouping — the combinator must
      not is_nil(u) and u.unit == :cell and is_nil(r.group_by) ->
        error(
          dsl,
          "`recompute_by :cell` redoes the whole cell, so it names no grouping — the " <>
            "`reduce` still needs `group_by:` to say how rows are folded."
        )

      # the composite form carries `from:` per entry — a top-level `from:` too
      # is two answers to one question
      not is_nil(u) and is_list(u.unit) and not is_nil(u.from) ->
        error(
          dsl,
          "`recompute_by #{inspect(u.unit)}` is a COMPOSITE unit — each entry already " <>
            "carries the input field it comes from, so the top-level " <>
            "`from: #{inspect(u.from)}` is a second answer. Drop it."
        )

      # a composite unit's entries must all be `this_column: :input_field` pairs
      not is_nil(u) and is_list(u.unit) and
          not Enum.all?(u.unit, &match?({a, b} when is_atom(a) and is_atom(b), &1)) ->
        error(
          dsl,
          "a COMPOSITE `recompute_by` takes `[this_column: :input_field, …]` pairs — " <>
            "got #{inspect(u.unit)}"
        )

      not is_nil(u) and is_list(u.unit) and u.unit == [] ->
        error(dsl, "a COMPOSITE `recompute_by` names no columns — got `[]`")

      # a single-column unit with no `from:` can't supply the grouping
      not is_nil(u) and u.unit != :cell and not is_list(u.unit) and is_nil(u.from) and
          is_nil(r.group_by) ->
        error(
          dsl,
          "`recompute_by #{inspect(u.unit)}` declares no `from:` (the input field the " <>
            "unit is computed from) and the `reduce` declares no `group_by:` — one of " <>
            "them must say how the input's rows group."
        )

      is_nil(u) and is_nil(r.group_by) ->
        error(
          dsl,
          "a `reduce over:` must declare `group_by:` — the grouping is only implicit " <>
            "when `recompute_by` names the unit and the field it comes from."
        )

      # `from_key:` resolves from the key's segments, so it needs the key grammar
      not is_nil(u) and u.from_key == true and u.unit == :cell ->
        error(
          dsl,
          "`recompute_by :cell` redoes everything, so there is no unit to resolve from " <>
            "the key — drop `from_key:`."
        )

      true ->
        :ok
    end
  end

  defp verify_over(_dsl, _r), do: :ok

  # `recompute_by` sets the claim rule, so a block-level `key_rule` alongside it
  # is the same fact stated twice — and they can disagree.
  defp verify_unit_vs_key_rule(dsl) do
    case {unit(dsl), Verifier.get_option(dsl, [:reactive], :key_rule)} do
      {%RecomputeBy{} = u, rule} when rule not in [nil, :identity] ->
        error(
          dsl,
          "`recompute_by #{inspect(u.unit)}` already declares the claim unit, but this " <>
            "block also sets `key_rule #{inspect(rule)}` — the same fact twice. Drop the " <>
            "`key_rule` (`recompute_by :cell` is the whole-cell unit)."
        )

      _ ->
        :ok
    end
  end

  # the node-shape × result-slot matrix: a VERDICT node's result IS its status
  # (`status:` required, row slots forbidden); a payload node emits rows
  # (exactly one of `into:`/`expand:`, `status:` forbidden).
  # every node emits ROWS, so the matrix is only "exactly one row slot".
  defp verify_result_slots(dsl, kind, row_slots) do
    case for {name, v} <- row_slots, not is_nil(v), do: name do
      [_one] ->
        :ok

      [] ->
        error(
          dsl,
          "a #{kind} needs `into:`" <>
            if(kind == :reduce, do: " (or `expand:` for the group → many-rows shape)", else: "")
        )

      several ->
        error(dsl, "declare ONE of #{inspect(several)} — a node emits rows one way")
    end
  end

  # the combinator's key_rule: is the preferred home (the claim grain and the
  # computation it must agree with, in one unit) — a NON-DEFAULT block-level
  # key_rule alongside it is two contradictory declarations.
  defp verify_key_rule_home(_dsl, nil), do: :ok

  defp verify_key_rule_home(dsl, _entity_rule) do
    case Verifier.get_option(dsl, [:reactive], :key_rule) do
      rule when rule in [nil, :identity] ->
        :ok

      block_rule ->
        error(
          dsl,
          "key_rule is declared BOTH on the combinator and at block level " <>
            "(#{inspect(block_rule)}) — declare it once, on the combinator " <>
            "(the claim grain and the computation it must agree with belong together)"
        )
    end
  end

  defp verify_key_prefix(dsl, key_fn, prefix) do
    if is_function(key_fn) and not is_nil(prefix) do
      error(
        dsl,
        "`key_prefix:` shapes the DEFAULT key and is dead alongside an explicit `key:` fn — " <>
          "drop one (fold the prefix into your fn, or drop the fn for the default)"
      )
    else
      :ok
    end
  end

  defp verify_reduce_into(_dsl, %Reduce{into: into}) when is_function(into) or is_nil(into),
    do: :ok

  defp verify_reduce_into(dsl, %Reduce{into: folds} = r) when is_list(folds) do
    with :ok <- verify_fold_shapes(dsl, folds),
         :ok <- require_declarative_group(dsl, r.group_by) do
      verify_dests(dsl, r, folds)
    end
  end

  defp require_declarative_group(dsl, group_by) do
    if declarative_group?(group_by) do
      :ok
    else
      error(
        dsl,
        "a declarative `into:` fold needs a declarative `group_by:` (an attribute or list " <>
          "of attributes) — the group's attributes become the row's columns. With a " <>
          "`group_by:` fn, write the `into:` fn too."
      )
    end
  end

  defp declarative_group?(g) do
    is_atom(g) or
      (is_list(g) and
         Enum.all?(g, fn
           a when is_atom(a) -> true
           {parent, child} when is_atom(parent) and is_atom(child) -> true
           _ -> false
         end))
  end

  defp verify_fold_shapes(dsl, folds) do
    Enum.reduce_while(folds, :ok, fn
      {:count, dest}, :ok when is_atom(dest) ->
        {:cont, :ok}

      {:count, bad}, :ok ->
        {:halt, error(dsl, "`count:` takes the destination attribute (an atom), got #{inspect(bad)}")}

      {kind, spec}, :ok ->
        cond do
          kind not in Declarative.fold_kinds() ->
            {:halt,
             error(
               dsl,
               "unknown fold kind #{inspect(kind)} in `into:` — supported: " <>
                 "#{inspect(Declarative.fold_kinds())}"
             )}

          is_atom(spec) or (is_list(spec) and Enum.all?(spec, &match?({s, d} when is_atom(s) and is_atom(d), &1))) ->
            {:cont, :ok}

          true ->
            {:halt,
             error(
               dsl,
               "#{inspect(kind)}: takes an attribute or `[source: dest]` pairs, got #{inspect(spec)}"
             )}
        end
    end)
  end

  # Every column the declarative row produces must be an attribute of this node —
  # the payload loop writes them there, and there is nowhere else for them to go.
  #
  # This used to fork twice: a clause matching `upsert: nil` (with a fallthrough
  # skipping any node that supplied one) and, inside it, a skip for a resource with
  # NO attributes. Both existed because a node could write into another resource's
  # table. It cannot; `verify_owns_rows/2` refuses a derived node with no
  # attributes, so an empty set is now unreachable here and every node reaching
  # this check writes its own rows.
  defp verify_dests(dsl, %Reduce{group_by: group_by}, folds) do
    payload_attrs =
      dsl |> Ash.Resource.Info.attributes() |> Enum.map(& &1.name) |> MapSet.new()

    dests = Declarative.group_dests(group_by) ++ fold_dests(folds)

    cond do
      (missing = Enum.reject(dests, &MapSet.member?(payload_attrs, &1))) != [] ->
        error(
          dsl,
          "the declarative `into:` row would carry #{inspect(missing)}, but this " <>
            "resource has no such attribute(s) — the payload loop writes row columns " <>
            "into the node's own attributes. Declared: #{inspect(MapSet.to_list(payload_attrs))}"
        )

      (uncovered = identity_uncovered(dsl, dests)) != [] ->
        error(
          dsl,
          "an IDENTITY-KEYED node's row must produce every primary-key field — " <>
            "#{inspect(uncovered)} never appear(s) in the group columns or fold " <>
            "destinations, so the upsert could not identify its row"
        )

      true ->
        :ok
    end
  end

  # for a composite primary key, the row IS its identity: every pk field must
  # be produced by the declarative row (group dests ∪ fold dests).
  defp identity_uncovered(dsl, dests) do
    case Ash.Resource.Info.primary_key(dsl) do
      pk when is_list(pk) and length(pk) > 1 -> pk -- dests
      _ -> []
    end
  end

  defp fold_dests(folds) do
    Enum.flat_map(folds, fn
      {:count, dest} -> [dest]
      {_kind, spec} when is_atom(spec) -> [spec]
      {_kind, spec} when is_list(spec) -> Keyword.values(spec)
    end)
  end

  defp verify_side(_dsl, _which, side) when is_atom(side) or is_function(side), do: :ok

  defp verify_side(dsl, which, side) when is_list(side) do
    cond do
      not Keyword.keyword?(side) or not Keyword.has_key?(side, :key) ->
        error(
          dsl,
          "`#{which}:` keyword form needs `key:` (the join-key attribute), e.g. " <>
            "`#{which}: [key: :acct, where: [kind: :budget]]` — got #{inspect(side)}"
        )

      not valid_side_key?(side[:key]) ->
        error(
          dsl,
          "`#{which}: [key: …]` takes an attribute or a non-empty list of them " <>
            "(the join key is their tuple) — got #{inspect(side[:key])}"
        )

      not (side |> Keyword.get(:where, []) |> Keyword.keyword?()) ->
        error(dsl, "`#{which}: [where: …]` takes attribute-value pairs, got #{inspect(side[:where])}")

      true ->
        :ok
    end
  end

  defp valid_side_key?(key) when is_atom(key) and not is_nil(key), do: true
  defp valid_side_key?([_ | _] = keys), do: Enum.all?(keys, &(is_atom(&1) and not is_nil(&1)))
  defp valid_side_key?(_), do: false

  defp verify_join_into(_dsl, into) when is_function(into) or is_nil(into), do: :ok

  defp verify_join_into(dsl, picks) when is_list(picks) do
    case Keyword.keys(picks) -- [:left, :right] do
      [] ->
        :ok

      extra ->
        error(
          dsl,
          "a declarative join `into:` picks columns per side — `[left: [...], right: [...]]` — " <>
            "got extra key(s) #{inspect(extra)}"
        )
    end
  end

  defp error(dsl, message) do
    {:error,
     Spark.Error.DslError.exception(
       module: Verifier.get_persisted(dsl, :module),
       path: [:reactive],
       message: "reactive_dag: " <> message
     )}
  end
end

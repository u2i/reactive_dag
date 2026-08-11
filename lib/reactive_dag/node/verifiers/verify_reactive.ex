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
  alias ReactiveDag.Node.{Compose, Compute, Context, Join, RecomputeBy, Reduce, Ref, Run}
  alias Spark.Dsl.Verifier

  @impl true
  def verify(dsl) do
    entities =
      Verifier.get_entities(dsl, [:reactive])
      |> Enum.reject(
        &(match?(%Ref{}, &1) or match?(%Context{}, &1) or match?(%Compose{}, &1) or
            match?(%RecomputeBy{}, &1) or match?(%ReactiveDag.Attestation.Requirement{}, &1))
      )

    # NOT Attested: `attested` + an explicit `compute` is a documented pair
    # (the explicit compute overrides the default attestation Op).
    computations =
      Enum.filter(
        entities,
        &(match?(%Reduce{}, &1) or match?(%Join{}, &1) or match?(%Compute{}, &1) or
            match?(%Run{}, &1) or match?(%ReactiveDag.Node.Aggregate{}, &1))
      )

    with :ok <- one_computation(dsl, computations),
         :ok <- verify_combinators(dsl, computations) do
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
         :ok <- verify_result_slots(dsl, :reduce, r.status, into: r.into, expand: r.expand),
         :ok <- verify_reduce_into(dsl, effective) do
      :ok
    end
  end

  defp verify_entity(dsl, %Join{} = j) do
    with :ok <- verify_key_rule_home(dsl, j.key_rule),
         :ok <- verify_key_prefix(dsl, j.key, j.key_prefix),
         :ok <- verify_side(dsl, :left, j.left),
         :ok <- verify_side(dsl, :right, j.right),
         :ok <- verify_result_slots(dsl, :join, j.status, into: j.into),
         :ok <- verify_join_into(dsl, j.into) do
      :ok
    end
  end


  defp verify_entity(dsl, %Run{action: action}) do
    case Ash.Resource.Info.action(dsl, action) do
      %{type: :action} ->
        :ok

      %{type: other} ->
        error(
          dsl,
          "`run #{inspect(action)}` names a #{inspect(other)} action — it must be a GENERIC " <>
            "action (`action #{inspect(action)}, {:array, :string} do run … end`) returning " <>
            "the changed keys"
        )

      nil ->
        generic = for %{type: :action, name: n} <- Ash.Resource.Info.actions(dsl), do: n

        error(
          dsl,
          "`run #{inspect(action)}` names an action this resource doesn't have. " <>
            "Generic actions declared: #{inspect(generic)}"
        )
    end
  end

  # `aggregate` speaks the SAME fold vocabulary as `reduce into:`, so its dest
  # attributes get the same check: every column the row would carry must exist
  # on this resource. (The aggregate's groups are its own rows, so there are no
  # group columns to add — the identity is already the row's.)
  defp verify_entity(dsl, %ReactiveDag.Node.Aggregate{} = a) do
    folds =
      for kind <- Declarative.fold_kinds(),
          spec = Map.get(a, kind),
          not is_nil(spec),
          do: {kind, spec}

    cond do
      folds == [] ->
        error(
          dsl,
          "an `aggregate` declares no aggregates — give it at least one of " <>
            "#{inspect(Declarative.fold_kinds())} (e.g. `count: :n`, " <>
            "`avg: [flow: :avg_flow]`)"
        )

      true ->
        verify_agg_dests(dsl, fold_dests(folds))
    end
  end

  defp verify_entity(_dsl, _other), do: :ok

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
  defp block_group_by(dsl) do
    case unit(dsl) do
      %RecomputeBy{unit: u, from: f} when not is_nil(f) -> [{u, f}]
      _ -> nil
    end
  end

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

      # a unit with no `from:` can't supply the grouping
      not is_nil(u) and u.unit != :cell and is_nil(u.from) and is_nil(r.group_by) ->
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
  defp verify_result_slots(dsl, kind, status, row_slots) do
    verdict? = Verifier.get_option(dsl, [:reactive], :verdict?)
    declared = for {name, v} <- row_slots, not is_nil(v), do: name

    cond do
      verdict? and declared != [] ->
        error(
          dsl,
          "a verdict node has no payload row — drop #{inspect(declared)}; its result is " <>
            "`status:` (`(… -> status | {status, strength})`), written straight into the " <>
            "coordination tuple"
        )

      verdict? and is_nil(status) ->
        error(
          dsl,
          "a verdict node's #{kind} declares `status:` — its result IS the coordination " <>
            "row, so there is no `into:` row to build"
        )

      not verdict? and not is_nil(status) ->
        error(
          dsl,
          "`status:` is the VERDICT node's slot — mark the node `verdict? true`, or emit " <>
            "payload rows with `into:`"
        )

      not verdict? and declared == [] ->
        error(
          dsl,
          "a payload #{kind} needs `into:`" <>
            if(kind == :reduce, do: " (or `expand:` for the group → many-rows shape)", else: "")
        )

      not verdict? and length(declared) > 1 ->
        error(dsl, "declare ONE of #{inspect(declared)} — a node emits rows one way")

      true ->
        :ok
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
        {:halt,
         error(dsl, "`count:` takes the destination attribute (an atom), got #{inspect(bad)}")}

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

  # when the node closes the payload loop (no upsert:, not verdict, has payload
  # attributes), every column the declarative row produces must be an attribute.
  defp verify_dests(dsl, %Reduce{upsert: nil, group_by: group_by}, folds) do
    payload_attrs =
      dsl |> Ash.Resource.Info.attributes() |> Enum.map(& &1.name) |> MapSet.new()

    if MapSet.size(payload_attrs) == 0 do
      :ok
    else
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
  end

  defp verify_dests(_dsl, _reduce_with_upsert, _folds), do: :ok

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

      not (side |> Keyword.get(:where, []) |> Keyword.keyword?()) ->
        error(dsl, "`#{which}: [where: …]` takes attribute-value pairs, got #{inspect(side[:where])}")

      true ->
        :ok
    end
  end

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

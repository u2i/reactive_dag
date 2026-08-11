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
  alias ReactiveDag.Node.{Compose, Compute, Join, Reduce, Reference, Ref, Run}
  alias Spark.Dsl.Verifier

  @impl true
  def verify(dsl) do
    entities =
      Verifier.get_entities(dsl, [:reactive])
      |> Enum.reject(
        &(match?(%Ref{}, &1) or match?(%Reference{}, &1) or match?(%Compose{}, &1) or
            match?(%ReactiveDag.Attestation.Requirement{}, &1))
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
    with :ok <- verify_key_prefix(dsl, r.key, r.key_prefix),
         :ok <- verify_result_slots(dsl, :reduce, r.status, into: r.into, expand: r.expand),
         :ok <- verify_reduce_into(dsl, r) do
      :ok
    end
  end

  defp verify_entity(dsl, %Join{} = j) do
    with :ok <- verify_key_prefix(dsl, j.key, j.key_prefix),
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

  defp verify_entity(_dsl, _other), do: :ok

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


  defp declarative_group?(g), do: is_atom(g) or (is_list(g) and Enum.all?(g, &is_atom/1))

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
      dests = List.wrap(group_by) ++ fold_dests(folds)

      case Enum.reject(dests, &MapSet.member?(payload_attrs, &1)) do
        [] ->
          :ok

        missing ->
          error(
            dsl,
            "the declarative `into:` row would carry #{inspect(missing)}, but this " <>
              "resource has no such attribute(s) — the payload loop writes row columns " <>
              "into the node's own attributes. Declared: #{inspect(MapSet.to_list(payload_attrs))}"
          )
      end
    end
  end

  defp verify_dests(_dsl, _reduce_with_upsert, _folds), do: :ok

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

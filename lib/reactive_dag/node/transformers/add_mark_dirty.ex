defmodule ReactiveDag.Node.Transformers.AddMarkDirty do
  @moduledoc """
  Wires `dirties_on` and `augmented_by` onto `ReactiveDag.Node.Changes.MarkDirty`,
  so ordinary Ash writes trigger the cascade with no host boilerplate.

  ## Two wirings, because they answer two different questions

  `dirties_on` names action TYPES and becomes ONE GLOBAL change. Global (rather
  than one per action) means actions declared *later* are covered too — the
  point of the feature is that no write site can be forgotten, and a per-action
  wiring would reintroduce exactly that risk. That is right for a source-fed
  LEAF, where every write is an observation.

  `augmented_by` names specific ACTIONS and becomes one change added to EACH of
  those actions' own `changes` list. Per-action is not a weaker `dirties_on`
  here — it is the only wiring that works. On a COMPUTED node the library writes
  the rows itself through `payload_action`, which is an ordinary Ash write: a
  global change would make every recompute re-dirty the cell it just computed,
  and the drain would spin forever. Naming the human-facing actions excludes the
  payload upsert BY CONSTRUCTION rather than by a filter someone must maintain.

  ## Marking exactly once

  Ash concatenates an action's own changes with the resource's global changes
  and dedupes NEITHER (`Ash.Changeset.run_action_changes/6`). So an action named
  in `augmented_by` whose type is also in `dirties_on` would run MarkDirty
  twice. The frontier insert is `ON CONFLICT DO NOTHING` and would survive that,
  but `schedule_drain` would enqueue twice per write — so such actions are
  wired ONCE, by the global change, and skipped here.

  The key derivation is resolved HERE, at compile time, and passed as options:
  the change then does no introspection per write.
  """
  use Spark.Dsl.Transformer

  alias Spark.Dsl.Transformer

  @impl true
  def transform(dsl) do
    with {:ok, dsl} <- maybe_add_global(dsl) do
      maybe_add_per_action(dsl)
    end
  end

  # after the DSL is built (we read the resource's own key shape), but before
  # anything that consumes `changes`.
  @impl true
  def after?(_), do: true

  defp maybe_add_global(dsl) do
    case Transformer.get_option(dsl, [:reactive], :dirties_on) do
      nil -> {:ok, dsl}
      [] -> {:ok, dsl}
      on -> add_change(dsl, on)
    end
  end

  # `augmented_by` names ACTIONS, so the change goes into each named action's own
  # `changes` list — the mechanism Ash's own `ResolvePipelines` transformer uses
  # to rewrite an action in place.
  defp maybe_add_per_action(dsl) do
    case Transformer.get_option(dsl, [:reactive], :augmented_by) do
      nil ->
        {:ok, dsl}

      [] ->
        {:ok, dsl}

      names ->
        # an action already covered by the global `dirties_on` change is left to
        # it: wiring both would mark twice (see the moduledoc).
        covered = Transformer.get_option(dsl, [:reactive], :dirties_on) || []
        opts = mark_opts(dsl)

        dsl
        |> Transformer.get_entities([:actions])
        # only WRITE actions are wired here. A `:read` (or generic) action has no
        # `change` entity to build, and naming one is rejected by
        # `VerifyReactive` — but a verifier runs AFTER the transformers, so
        # reaching for the entity anyway would crash with a Spark internal error
        # in place of the message that explains the mistake.
        |> Enum.filter(
          &(&1.name in names and &1.type in [:create, :update, :destroy] and
              &1.type not in covered)
        )
        |> Enum.reduce({:ok, dsl}, fn action, {:ok, dsl} ->
          {:ok, add_action_change(dsl, action, opts)}
        end)
    end
  end

  defp add_action_change(dsl, action, opts) do
    # the ACTION-level `change` entity, whose schema has no `on:` — the action
    # it lives on IS the scope.
    change =
      Transformer.build_entity!(Ash.Resource.Dsl, [:actions, action.type], :change,
        change: {ReactiveDag.Node.Changes.MarkDirty, opts}
      )

    Transformer.replace_entity(
      dsl,
      [:actions],
      %{action | changes: action.changes ++ [change]},
      &(&1.name == action.name and &1.type == action.type)
    )
  end

  defp add_change(dsl, on) do
    change =
      Transformer.build_entity!(Ash.Resource.Dsl, [:changes], :change,
        change: {ReactiveDag.Node.Changes.MarkDirty, mark_opts(dsl)},
        on: on
      )

    {:ok, Transformer.add_entity(dsl, [:changes], change)}
  end

  defp mark_opts(dsl) do
    schedule? = Transformer.get_option(dsl, [:reactive], :schedule_drain) || false

    # Raise at COMPILE time rather than at the first write. `schedule_drain: true`
    # without Oban would mark and never drain — the exact silent staleness the
    # option exists to remove, and worse for being asked for explicitly.
    if schedule? and not Code.ensure_loaded?(Oban) do
      raise Spark.Error.DslError,
        module: Transformer.get_persisted(dsl, :module),
        path: [:reactive, :schedule_drain],
        message: """
        `schedule_drain: true` needs Oban, which is an optional dependency of
        reactive_dag and is not loaded.

        Add it:

            {:oban, "~> 2.17"}

        and give it the queue the worker runs on:

            config :my_app, Oban, queues: [drain: 1]

        Or drop the option — `dirties_on`/`augmented_by` still mark correctly,
        and whatever drains next (a ScanWorker sweep) will pick the mark up.
        """
    end

    [
      cell: dsl |> cell_id() |> to_string(),
      payload_key: payload_key(dsl),
      identity_fields: identity_fields(dsl),
      schedule_drain: schedule?
    ]
  end

  # mirrors ReactiveDag.Node's own derivation: an explicit `payload_key`, else
  # a single-attribute primary key, else the conventional `:key`.
  defp payload_key(dsl) do
    Transformer.get_option(dsl, [:reactive], :payload_key) ||
      case Ash.Resource.Info.primary_key(dsl) do
        [single] -> single
        _ -> :key
      end
  end

  # a COMPOSITE primary key means the row is its own identity; the cell key is
  # that identity serialized in primary-key order.
  defp identity_fields(dsl) do
    case Ash.Resource.Info.primary_key(dsl) do
      pk when is_list(pk) and length(pk) > 1 -> pk
      _ -> nil
    end
  end

  defp cell_id(dsl) do
    Transformer.get_option(dsl, [:reactive], :id) ||
      dsl
      |> Transformer.get_persisted(:module)
      |> Module.split()
      |> List.last()
      |> Macro.underscore()
  end
end

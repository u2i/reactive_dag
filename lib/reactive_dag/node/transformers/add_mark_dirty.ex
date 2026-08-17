defmodule ReactiveDag.Node.Transformers.AddMarkDirty do
  @moduledoc """
  Wires `dirties_on` into a global `change`, so ordinary Ash writes trigger the
  cascade with no host boilerplate.

  A global change (rather than one added per action) means actions declared
  *later* are covered too — the point of the feature is that no write site can
  be forgotten, and a per-action wiring would reintroduce exactly that risk.

  The key derivation is resolved HERE, at compile time, and passed as options:
  the change then does no introspection per write.
  """
  use Spark.Dsl.Transformer

  alias Spark.Dsl.Transformer

  @impl true
  def transform(dsl) do
    case Transformer.get_option(dsl, [:reactive], :dirties_on) do
      nil -> {:ok, dsl}
      [] -> {:ok, dsl}
      on -> add_change(dsl, on)
    end
  end

  # after the DSL is built (we read the resource's own key shape), but before
  # anything that consumes `changes`.
  @impl true
  def after?(_), do: true

  defp add_change(dsl, on) do
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

        Or drop the option — `dirties_on` still marks correctly, and whatever
        drains next (a ScanWorker sweep) will pick the mark up.
        """
    end

    opts = [
      cell: dsl |> cell_id() |> to_string(),
      payload_key: payload_key(dsl),
      identity_fields: identity_fields(dsl),
      schedule_drain: schedule?
    ]

    change =
      Transformer.build_entity!(Ash.Resource.Dsl, [:changes], :change,
        change: {ReactiveDag.Node.Changes.MarkDirty, opts},
        on: on
      )

    {:ok, Transformer.add_entity(dsl, [:changes], change)}
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

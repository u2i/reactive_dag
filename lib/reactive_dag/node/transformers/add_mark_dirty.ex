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
    opts = [
      cell: dsl |> cell_id() |> to_string(),
      payload_key: payload_key(dsl),
      identity_fields: identity_fields(dsl)
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

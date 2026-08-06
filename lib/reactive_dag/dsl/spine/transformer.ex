defmodule ReactiveDag.Dsl.Spine.Transformer do
  @moduledoc """
  Compile-time validation for a `ReactiveDag.Dsl.Spine` module: the graph is
  structurally sound — every `ref`/input resolves, ids are unique, and it is
  acyclic — by running the same `Info` lowering the drain uses and letting
  `ReactiveDag.Graph.build/1` raise on a dangle/cycle. Surfaced as a
  `Spark.Error.DslError` so an authoring mistake fails the build with a located
  message rather than at first drain.

  (The scanner↔leaf binding is NOT checked here — a scanner is not authored in the
  graph. Its driver's `leaf_cells/1` names the cells it writes, verified at runtime
  against the built plan by `ReactiveDag.Source.verify!/2`.)
  """
  use Spark.Dsl.Transformer

  alias ReactiveDag.Dsl.Spine.{Node, Observed}
  alias Spark.Dsl.Transformer

  @impl true
  def transform(dsl) do
    entities = Transformer.get_entities(dsl, [:graph])
    validate_structure!(entities)
    {:ok, dsl}
  end

  # ── structural: refs resolve, ids unique, acyclic ───────────────────────────
  # Reuse the exact lowering the drain uses; Graph.build raises ArgumentError on a
  # dangling input or cycle. Re-raise as a located DslError.
  defp validate_structure!(entities) do
    entities
    |> Enum.filter(&(match?(%Observed{}, &1) or match?(%Node{}, &1)))
    |> ReactiveDag.Dsl.Spine.Info.lower_all()
    |> ReactiveDag.Graph.build()

    :ok
  rescue
    e in ArgumentError ->
      reraise %Spark.Error.DslError{
                module: __MODULE__,
                path: [:graph],
                message: "invalid reactive graph: " <> Exception.message(e)
              },
              __STACKTRACE__
  end
end

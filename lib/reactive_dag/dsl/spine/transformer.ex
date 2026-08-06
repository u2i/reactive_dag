defmodule ReactiveDag.Dsl.Spine.Transformer do
  @moduledoc """
  Compile-time validation for a `ReactiveDag.Dsl.Spine` module:

    1. every `observed.fed_by` names a declared `source` (the compile-time half of
       the leaf↔scanner binding; the runtime half — each source's `leaf_cells/1`
       resolving to a real cell — is `ReactiveDag.Source.verify!/2`, which needs
       the lowered graph);
    2. the graph is structurally sound — every `ref`/input resolves, ids are
       unique, and it is acyclic — by running the same `Info.plan/1` lowering the
       drain uses and letting `ReactiveDag.Graph.build/1` raise on a dangle/cycle.

  Both are surfaced as `Spark.Error.DslError` so authoring mistakes fail the build
  with a located message rather than at first drain.
  """
  use Spark.Dsl.Transformer

  alias ReactiveDag.Dsl.Spine.{Node, Observed, Source}
  alias Spark.Dsl.Transformer

  @impl true
  def transform(dsl) do
    entities = Transformer.get_entities(dsl, [:graph])
    validate_fed_by!(entities)
    validate_structure!(entities)
    {:ok, dsl}
  end

  # ── 1. every observed.fed_by names a declared source ────────────────────────
  defp validate_fed_by!(entities) do
    source_ids = for %Source{id: id} <- entities, into: MapSet.new(), do: id

    for %Observed{id: leaf, fed_by: fed_by} <- entities,
        not is_nil(fed_by),
        not MapSet.member?(source_ids, fed_by) do
      raise %Spark.Error.DslError{
        module: __MODULE__,
        path: [:graph, :observed, leaf, :fed_by],
        message:
          "observed #{inspect(leaf)} is fed_by #{inspect(fed_by)}, which is not a declared source"
      }
    end

    :ok
  end

  # ── 2. structural: refs resolve, ids unique, acyclic ────────────────────────
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

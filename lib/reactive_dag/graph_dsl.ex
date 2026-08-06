defmodule ReactiveDag.Graph.Dsl do
  @moduledoc """
  The base a host `use`s to author a reactive DAG in the shared grammar. It is a
  `Spark.Dsl` wired with the `ReactiveDag.Dsl.Spine` extension, so a module that
  does `use ReactiveDag.Graph.Dsl` can write a `graph do … end` block directly:

      defmodule MyApp.Pipeline do
        use ReactiveDag.Graph.Dsl

        graph do
          # a leaf a scanner writes out-of-band; `scan:` inlines the scanner
          # (the 1:1 case — see `ReactiveDag.Source`).
          observed :machines, grain: :machine, scan: MyApp.Sources.FleetScan

          node :health do
            op :reduce
            meta grain: :machine
            ref :machines
          end
        end
      end

  Introspect the result with `ReactiveDag.Dsl.Spine.Info`:

      ReactiveDag.Dsl.Spine.Info.plan(MyApp.Pipeline)     # → %ReactiveDag.Plan{}

  A host that wants its OWN typed domain entities (a `guarantee`/`dataset` beyond
  the open `node` op-kind) composes its extension alongside the spine by using
  `Spark.Dsl` directly with both extensions:

      use Spark.Dsl, default_extensions: [extensions: [ReactiveDag.Dsl.Spine, MyApp.Domain.Dsl]]

  (`MyApp.Domain.Dsl` adds its entities to the `graph` section; both lower through
  `ReactiveDag.Lowering`.)
  """
  use Spark.Dsl, default_extensions: [extensions: [ReactiveDag.Dsl.Spine]]
end

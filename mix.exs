defmodule ReactiveDag.MixProject do
  use Mix.Project

  @moduledoc false

  def project do
    [
      app: :reactive_dag,
      version: "0.11.0",
      elixir: "~> 1.18",
      description:
        "Reactive DAG engine as an Ash extension: a dirty frontier + depth-ordered " <>
          "incremental drain + change propagation + a shared coordination-tuple spine. " <>
          "Author nodes as Ash resources (the `ReactiveDag.Node` extension) with " <>
          "reduce/join/expand combinators, or bring your own cells. Domain plugs in at " <>
          "three seams: recompute strategy (per-key Elixir or set-based SQL), key rule, " <>
          "and coordination writer.",
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      docs: docs()
    ]
  end

  defp docs do
    [
      main: "getting-started",
      source_url: "https://github.com/u2i/reactive_dag",
      source_ref: "v0.11.0",
      extras: [
        "guides/getting-started.md",
        "guides/authoring-nodes.md",
        "guides/sources.md",
        "guides/attestations.md",
        "guides/seams.md",
        "README.md",
        "docs/adr-001-reactive-dag-library.md"
      ],
      groups_for_extras: [
        Guides: ~r/guides\/.*/,
        Design: ~r/docs\/.*/
      ],
      groups_for_modules: [
        Authoring: [
          ReactiveDag.Node,
          ReactiveDag.Node.Payload,
          ReactiveDag.Node.Recompute.Aggregate
        ],
        Attestation: [
          ReactiveDag.Attestation,
          ReactiveDag.Attestation.Scope,
          ReactiveDag.Attestation.Basis,
          ReactiveDag.Attestation.Requirement,
          ReactiveDag.Attestation.Evaluation,
          ReactiveDag.Attestation.Op
        ],
        Seams: [
          ReactiveDag.Source,
          ReactiveDag.RecomputeStrategy,
          ReactiveDag.KeyRule,
          ReactiveDag.CoordinationWriter
        ],
        Engine: [
          ReactiveDag.Drain,
          ReactiveDag.Graph,
          ReactiveDag.Lowering,
          ReactiveDag.Plan,
          ReactiveDag.Cell
        ]
      ]
    ]
  end

  def application, do: [extra_applications: [:logger]]

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  # Both host apps are Ash/AshPostgres/Spark, so the library IS an Ash extension:
  # it owns the frontier + substrate resources and the reactive-DAG Spark DSL.
  # Only the op algebra + recompute model stay app-side (the two real seams).
  defp deps do
    [
      {:ash, "~> 3.5"},
      {:ash_postgres, "~> 2.0"},
      {:spark, "~> 2.7"},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false}
    ]
  end
end

defmodule ReactiveDag.MixProject do
  use Mix.Project

  @moduledoc false

  # release-please manages this version (and the tag/CHANGELOG) via the
  # annotation below — bump it by merging the release PR, not by hand.
  @version "0.17.0-rc.60" # x-release-please-version

  def project do
    [
      app: :reactive_dag,
      version: @version,
      elixir: "~> 1.18",
      description:
        "Reactive DAG engine as an Ash extension: dirty frontier, depth-ordered " <>
          "incremental drain, change propagation. Author nodes as Ash resources " <>
          "with reduce/join/aggregate combinators; each node's results are its own " <>
          "rows. You declare the relationships; one engine runs them.",
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      docs: docs(),
      package: package()
    ]
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/u2i/reactive_dag"},
      # guides/ + docs/ ship in the package so hexdocs can build the extras
      files: ~w(lib guides docs mix.exs README.md LICENSE)
    ]
  end

  defp docs do
    [
      main: "getting-started",
      source_url: "https://github.com/u2i/reactive_dag",
      source_ref: "v#{@version}",
      extras: [
        "guides/getting-started.md",
        "guides/configuration.md",
        "guides/authoring-nodes.md",
        "guides/human-augmentation.md",
        "guides/llm-nodes.md",
        "guides/sources.md",
        "guides/seams.md",
        "README.md",
        "docs/adr-001-reactive-dag-library.md",
        "docs/adr-002-a-graph-per-tenant.md",
        "docs/adr-003-uuid-identity-and-uniform-keys.md"
      ],
      groups_for_extras: [
        Guides: ~r/guides\/.*/,
        Design: ~r/docs\/.*/
      ],
      groups_for_modules: [
        Authoring: [
          ReactiveDag.Node,
          ReactiveDag.Node.Payload,
          ReactiveDag.Node.Recompute.Aggregate,
          ReactiveDag.Calendar
        ],
        "Getting data in": [
          ReactiveDag.Source,
          ReactiveDag.Op
        ],
        Engine: [
          ReactiveDag.Drain,
          ReactiveDag.Node.Recompute,
          ReactiveDag.Node.KeyRule,
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
  # it owns the frontier, the substrate resources, the reactive-DAG Spark DSL AND
  # the engine that reads it. A host declares; it does not bring a strategy.
  defp deps do
    [
      {:ash, "~> 3.5"},
      {:ash_postgres, "~> 2.0"},
      {:spark, "~> 2.7"},
      # the drain's observability seam. Arrives transitively via Ash anyway, but
      # the drain calls :telemetry.execute directly, so it is a direct dependency.
      {:telemetry, "~> 1.1"},
      # the scan worker. Optional: a host that schedules its own polls, or runs
      # none, never loads `ReactiveDag.ScanWorker` and needs no Oban.
      {:oban, "~> 2.17", optional: true},
      # TEST-ONLY: proves an ash_ai prompt-backed action composes with `run`,
      # so an LLM node needs no library code (see guides/llm-nodes.md). Never a
      # runtime dependency — hosts that want LLM nodes add ash_ai themselves.
      {:ash_ai, "~> 0.8", only: :test},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false}
    ]
  end
end

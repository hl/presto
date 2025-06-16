defmodule Presto.MixProject do
  use Mix.Project

  @version "0.1.0"

  def project do
    [
      app: :presto,
      version: @version,
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      description: description(),
      package: package(),
      name: "Presto",
      source_url: "https://github.com/hl/presto",
      homepage_url: "https://github.com/hl/presto",
      docs: [
        main: "readme",
        extras: [
          "README.md",
          "CHANGELOG.md",
          "docs/OVERVIEW.md",
          "docs/PUBLISHING.md",
          "specs/presto.md",
          "specs/architecture.md",
          "specs/rete_algorithm.md",
          "specs/api_design.md",
          "specs/elixir_implementation.md",
          "specs/performance.md"
        ],
        groups_for_extras: [
          "Getting Started": ["README.md", "CHANGELOG.md", "docs/OVERVIEW.md"],
          Publishing: ["docs/PUBLISHING.md"],
          "Architecture & Design": [
            "specs/presto.md",
            "specs/architecture.md",
            "specs/rete_algorithm.md"
          ],
          Implementation: [
            "specs/api_design.md",
            "specs/elixir_implementation.md",
            "specs/performance.md"
          ]
        ],
        groups_for_modules: [
          "Core Engine": [
            Presto,
            Presto.RuleEngine,
            Presto.WorkingMemory,
            Presto.AlphaNetwork,
            Presto.BetaNetwork
          ],
          "Rules & Registry": [
            Presto.RuleRegistry,
            Presto.RuleBehaviour,
            Presto.RuleAnalyzer
          ],
          Requirements: [
            Presto.RequirementBehaviour,
            Presto.RequirementScheduler,
            Presto.Requirements.TimeBasedRequirement
          ],
          Examples: [
            Presto.Examples.PayrollRules,
            Presto.Examples.ComplianceRules,
            Presto.Examples.CaliforniaSpikeBreakRules,
            Presto.Examples.OvertimeRules,
            Presto.Examples.TroncRules
          ],
          Benchmarking: [
            Presto.Benchmarks.BenchmarkRunner,
            Presto.Benchmarks.MetricsCollector,
            Presto.Benchmarks.PerformanceMonitor,
            Presto.Benchmarks.ResultAnalyzer
          ],
          Utilities: [
            Presto.Utils,
            Presto.Application,
            Presto.FastPathExecutor
          ]
        ],
        source_ref: "v#{@version}",
        source_url_pattern: "https://github.com/hl/presto/blob/v#{@version}/%{path}#L%{line}"
      ]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {Presto.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:jason, "~> 1.4"},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false}
    ]
  end

  defp description() do
    "A high-performance rules engine implementing the RETE algorithm for Elixir. " <>
      "Presto enables configuration-driven business logic with efficient pattern matching, " <>
      "incremental processing, and concurrent execution. Perfect for payroll processing, " <>
      "compliance checking, and complex business rule validation."
  end

  defp package() do
    [
      name: "ex_presto",
      files: ~w(lib specs docs .formatter.exs mix.exs README* LICENSE* CHANGELOG*),
      licenses: ["MIT"],
      links: %{
        "GitHub" => "https://github.com/hl/presto",
        "Documentation" => "https://hexdocs.pm/ex_presto"
      }
    ]
  end
end

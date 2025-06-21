#!/usr/bin/env elixir

# Presto Debugging and Introspection Example
# 
# This example demonstrates the comprehensive debugging and introspection
# tools added to Presto for performance analysis and system understanding.

# Start dependencies
Mix.install([])

# Add lib to path for running as script
Code.append_path("lib")

defmodule Examples.DebuggingExample do
  @moduledoc """
  Demonstrates Presto's debugging and introspection capabilities.

  Shows how to inspect rules, analyze performance, trace fact execution,
  and get comprehensive engine diagnostics.
  """

  def run_demo do
    IO.puts("\n=== Presto Debugging & Introspection Demo ===")

    # Start the engine
    {:ok, engine} = Presto.start_engine()

    # Set up a realistic scenario
    setup_demo_scenario(engine)

    # Demonstrate different debugging tools
    demonstrate_rule_inspection(engine)
    demonstrate_engine_diagnostics(engine)
    demonstrate_performance_profiling(engine)
    demonstrate_fact_tracing(engine)
    demonstrate_network_visualization(engine)
    demonstrate_performance_recommendations(engine)

    Presto.stop_engine(engine)
    IO.puts("\n=== Debugging Demo Complete ===")
  end

  defp setup_demo_scenario(engine) do
    IO.puts("\n--- Setting up demo scenario ---")

    # Create a mix of simple and complex rules
    rules = [
      # Simple rule - should use fast path
      Presto.Rule.new(
        :adult_check,
        [
          {:person, :name, :age},
          {:age, :>, 18}
        ],
        fn bindings ->
          [{:adult, bindings[:name]}]
        end,
        priority: 10
      ),

      # Complex rule - should use RETE network
      Presto.Rule.new(
        :senior_high_performer,
        [
          {:person, :name, :age, :department},
          {:employment, :name, :company, :salary},
          {:performance, :name, :year, :rating},
          {:age, :>, 30},
          {:salary, :>, 70000},
          {:rating, :>, 4.0}
        ],
        fn bindings ->
          [{:senior_high_performer, bindings[:name], bindings[:company]}]
        end,
        priority: 5
      ),

      # Aggregation rule
      Presto.Rule.aggregation(
        :department_avg_salary,
        [
          {:person, :name, :age, :department},
          {:employment, :name, :company, :salary}
        ],
        [:department],
        :avg,
        :salary
      )
    ]

    # Add facts
    facts = [
      # People
      {:person, "Alice", 28, "Engineering"},
      {:person, "Bob", 35, "Marketing"},
      {:person, "Carol", 24, "Engineering"},
      {:person, "Dave", 42, "Sales"},
      {:person, "Eve", 31, "Engineering"},
      {:person, "Frank", 19, "Intern"},

      # Employment
      {:employment, "Alice", "TechCorp", 75000},
      {:employment, "Bob", "TechCorp", 65000},
      {:employment, "Carol", "StartupInc", 70000},
      {:employment, "Dave", "TechCorp", 80000},
      {:employment, "Eve", "StartupInc", 85000},
      {:employment, "Frank", "TechCorp", 35000},

      # Performance
      {:performance, "Alice", 2023, 4.5},
      {:performance, "Bob", 2023, 3.8},
      {:performance, "Carol", 2023, 4.2},
      {:performance, "Dave", 2023, 3.5},
      {:performance, "Eve", 2023, 4.8},
      {:performance, "Frank", 2023, 3.0}
    ]

    # Use batch operations
    :ok = Presto.add_rules(engine, rules)
    :ok = Presto.assert_facts(engine, facts)

    # Execute rules to generate statistics
    results = Presto.fire_rules(engine)
    IO.puts("Added #{length(rules)} rules and #{length(facts)} facts")
    IO.puts("Initial execution produced #{length(results)} results")
  end

  defp demonstrate_rule_inspection(engine) do
    IO.puts("\n--- Rule Inspection ---")

    # Inspect individual rules
    [:adult_check, :senior_high_performer, :department_avg_salary]
    |> Enum.each(fn rule_id ->
      info = Presto.inspect_rule(engine, rule_id)
      IO.puts("\nRule: #{rule_id}")
      IO.puts("  Type: #{Map.get(info, :type, "standard")}")
      IO.puts("  Strategy: #{Map.get(info, :strategy, "unknown")}")
      IO.puts("  Complexity: #{Map.get(info, :complexity, "unknown")}")
      IO.puts("  Conditions: #{Map.get(info, :condition_count, 0)}")

      if Map.has_key?(info, :execution_stats) do
        stats = info.execution_stats
        IO.puts("  Executions: #{Map.get(stats, :count, 0)}")
        IO.puts("  Avg time: #{Map.get(stats, :avg_time_us, 0)}μs")
      end

      if Map.has_key?(info, :network_info) do
        network = info.network_info
        IO.puts("  Alpha nodes: #{Map.get(network, :alpha_nodes, 0)}")
        IO.puts("  Beta nodes: #{Map.get(network, :beta_nodes, 0)}")
      end
    end)
  end

  defp demonstrate_engine_diagnostics(engine) do
    IO.puts("\n--- Engine Diagnostics ---")

    diagnostics = Presto.diagnostics(engine)

    IO.puts("Engine Health:")
    IO.puts("  Status: #{Map.get(diagnostics, :status, "unknown")}")
    IO.puts("  Uptime: #{Map.get(diagnostics, :uptime_ms, 0)}ms")

    if Map.has_key?(diagnostics, :memory) do
      memory = diagnostics.memory
      IO.puts("\nMemory Usage:")
      IO.puts("  Facts table: #{Map.get(memory, :facts_table_size, 0)} entries")
      IO.puts("  Rules table: #{Map.get(memory, :rules_table_size, 0)} entries")
      IO.puts("  Alpha memories: #{Map.get(memory, :alpha_memories_size, 0)} entries")
      IO.puts("  Memory pressure: #{Map.get(memory, :memory_pressure, "none")}")
    end

    if Map.has_key?(diagnostics, :performance) do
      perf = diagnostics.performance
      IO.puts("\nPerformance Metrics:")
      IO.puts("  Total rule firings: #{Map.get(perf, :total_rule_firings, 0)}")
      IO.puts("  Fast path executions: #{Map.get(perf, :fast_path_executions, 0)}")
      IO.puts("  RETE executions: #{Map.get(perf, :rete_executions, 0)}")
      IO.puts("  Avg execution time: #{Map.get(perf, :avg_execution_time_us, 0)}μs")
    end

    if Map.has_key?(diagnostics, :optimization) do
      opt = diagnostics.optimization
      IO.puts("\nOptimization Status:")
      IO.puts("  Alpha nodes shared: #{Map.get(opt, :alpha_nodes_shared, 0)}")
      IO.puts("  Fast path enabled: #{Map.get(opt, :fast_path_enabled, false)}")
      IO.puts("  Memory optimized: #{Map.get(opt, :memory_optimized, false)}")
    end
  end

  defp demonstrate_performance_profiling(engine) do
    IO.puts("\n--- Performance Profiling ---")

    # Profile execution with specific rules
    profile = Presto.profile_execution(engine, rules: [:adult_check, :senior_high_performer])

    IO.puts("Profiling Results:")

    if Map.has_key?(profile, :execution_breakdown) do
      breakdown = profile.execution_breakdown
      IO.puts("  Total time: #{Map.get(breakdown, :total_time_us, 0)}μs")
      IO.puts("  Pattern matching: #{Map.get(breakdown, :pattern_matching_us, 0)}μs")
      IO.puts("  Action execution: #{Map.get(breakdown, :action_execution_us, 0)}μs")
      IO.puts("  Fact assertion: #{Map.get(breakdown, :fact_assertion_us, 0)}μs")
    end

    if Map.has_key?(profile, :rule_performance) do
      rule_perf = profile.rule_performance
      IO.puts("\nPer-rule Performance:")

      Enum.each(rule_perf, fn {rule_id, stats} ->
        IO.puts("  #{rule_id}:")
        IO.puts("    Time: #{Map.get(stats, :time_us, 0)}μs")
        IO.puts("    Facts processed: #{Map.get(stats, :facts_processed, 0)}")
        IO.puts("    Strategy: #{Map.get(stats, :strategy, "unknown")}")
      end)
    end

    if Map.has_key?(profile, :bottlenecks) do
      bottlenecks = profile.bottlenecks
      IO.puts("\nBottlenecks:")

      Enum.each(bottlenecks, fn bottleneck ->
        IO.puts("  - #{bottleneck}")
      end)
    end
  end

  defp demonstrate_fact_tracing(engine) do
    IO.puts("\n--- Fact Execution Tracing ---")

    # Trace how a specific fact moves through the system
    trace = Presto.trace_fact(engine, {:person, "Eve", 31, "Engineering"})

    IO.puts("Fact Trace for Eve:")

    if Map.has_key?(trace, :path) do
      path = trace.path
      IO.puts("  Execution path:")

      Enum.with_index(path, 1)
      |> Enum.each(fn {step, index} ->
        IO.puts("    #{index}. #{Map.get(step, :description, "unknown step")}")

        if Map.has_key?(step, :time_us) do
          IO.puts("       Time: #{step.time_us}μs")
        end
      end)
    end

    if Map.has_key?(trace, :matches) do
      matches = trace.matches
      IO.puts("\n  Rule matches:")

      Enum.each(matches, fn match ->
        IO.puts(
          "    - #{Map.get(match, :rule_id, "unknown")}: #{Map.get(match, :status, "unknown")}"
        )
      end)
    end

    if Map.has_key?(trace, :derived_facts) do
      derived = trace.derived_facts
      IO.puts("\n  Derived facts:")

      Enum.each(derived, fn fact ->
        IO.puts("    - #{inspect(fact)}")
      end)
    end
  end

  defp demonstrate_network_visualization(engine) do
    IO.puts("\n--- Network Visualization ---")

    network = Presto.visualize_network(engine)

    if Map.has_key?(network, :alpha_network) do
      alpha = network.alpha_network
      IO.puts("Alpha Network:")
      IO.puts("  Nodes: #{Map.get(alpha, :node_count, 0)}")
      IO.puts("  Shared nodes: #{Map.get(alpha, :shared_nodes, 0)}")

      if Map.has_key?(alpha, :nodes) do
        IO.puts("  Node details:")

        Enum.each(Map.get(alpha, :nodes, []), fn node ->
          IO.puts(
            "    - #{Map.get(node, :id, "unknown")}: #{Map.get(node, :pattern, "unknown pattern")}"
          )
        end)
      end
    end

    if Map.has_key?(network, :beta_network) do
      beta = network.beta_network
      IO.puts("\nBeta Network:")
      IO.puts("  Join nodes: #{Map.get(beta, :join_nodes, 0)}")
      IO.puts("  Aggregation nodes: #{Map.get(beta, :aggregation_nodes, 0)}")
    end

    if Map.has_key?(network, :connections) do
      connections = network.connections
      IO.puts("\nNetwork connections: #{length(connections)} edges")
    end
  end

  defp demonstrate_performance_recommendations(engine) do
    IO.puts("\n--- Performance Recommendations ---")

    recommendations = Presto.performance_recommendations(engine)

    IO.puts("System Analysis:")

    if length(recommendations) == 0 do
      IO.puts("  No performance issues detected - system is well optimized!")
    else
      IO.puts("  Found #{length(recommendations)} recommendations:")

      Enum.with_index(recommendations, 1)
      |> Enum.each(fn {rec, index} ->
        IO.puts("  #{index}. #{Map.get(rec, :title, "Recommendation")}")
        IO.puts("     #{Map.get(rec, :description, "No description")}")
        IO.puts("     Impact: #{Map.get(rec, :impact, "unknown")}")
        IO.puts("     Effort: #{Map.get(rec, :effort, "unknown")}")
        IO.puts("")
      end)
    end
  end
end

# Run the demo
Examples.DebuggingExample.run_demo()

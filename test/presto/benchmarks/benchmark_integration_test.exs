defmodule Presto.Benchmarks.BenchmarkIntegrationTest do
  use ExUnit.Case, async: false

  alias Presto.RuleEngine

  alias Presto.Benchmarks.{
    BenchmarkRunner,
    PerformanceMonitor,
    MetricsCollector,
    ResultAnalyzer
  }

  alias Presto.Benchmarks.Strategies.{
    AlphaNetworkStrategy,
    BetaNetworkStrategy,
    RuleEngineStrategy,
    MemoryStrategy
  }

  setup do
    {:ok, metrics_collector} = MetricsCollector.start_link()
    {:ok, engine} = RuleEngine.start_link([])

    on_exit(fn ->
      if Process.alive?(metrics_collector), do: GenServer.stop(metrics_collector)
      if Process.alive?(engine), do: GenServer.stop(engine)
    end)

    %{engine: engine, metrics_collector: metrics_collector}
  end

  describe "benchmark runner integration" do
    test "can run individual benchmarks" do
      # Test alpha network throughput benchmark
      result =
        BenchmarkRunner.run_benchmark(:alpha_network_throughput, %{
          iterations: 5,
          warmup_iterations: 2,
          data_size: :small
        })

      assert result.name == "alpha_network_throughput"
      assert result.passed == true
      assert result.execution_time > 0
      assert result.memory_usage > 0
      assert result.iterations == 5
      assert is_map(result.metrics)
    end

    test "can run benchmark suite" do
      benchmark_names = [:alpha_network_throughput, :rule_engine_execution]

      results =
        BenchmarkRunner.run_benchmark_suite(benchmark_names, %{
          iterations: 3,
          data_size: :small
        })

      assert length(results) == 2
      assert Enum.all?(results, &(&1.passed == true))
      assert Enum.all?(results, &(&1.iterations == 3))
    end

    test "can run optimization comparison" do
      # Define a mock optimization function
      optimization_fn = fn ->
        # This would apply some optimization
        :ok
      end

      result =
        BenchmarkRunner.run_optimization_comparison(
          :rule_engine_execution,
          optimization_fn,
          %{iterations: 3, data_size: :small}
        )

      assert result.name == "rule_engine_execution_comparison"
      assert result.type == :comparison
      assert is_map(result.baseline_metrics)
      assert is_map(result.metrics)
      assert is_number(result.improvement_percentage)
    end
  end

  describe "performance monitoring integration" do
    @tag :slow
    test "can start and stop performance monitoring" do
      {:ok, monitor} =
        PerformanceMonitor.start_link(%{
          # 1 second for testing
          monitoring_interval: 1000,
          benchmark_suite: [:alpha_network_throughput]
        })

      # Start monitoring
      assert :ok = PerformanceMonitor.start_monitoring()

      # Check status
      status = PerformanceMonitor.get_monitoring_status()
      assert status.monitoring_active == true

      # Wait a bit for at least one check
      Process.sleep(1500)

      # Stop monitoring
      assert :ok = PerformanceMonitor.stop_monitoring()

      # Check final status
      final_status = PerformanceMonitor.get_monitoring_status()
      assert final_status.monitoring_active == false
      assert final_status.last_check_time != nil

      GenServer.stop(monitor)
    end

    test "can trigger manual performance check" do
      {:ok, monitor} =
        PerformanceMonitor.start_link(%{
          benchmark_suite: [:alpha_network_throughput]
        })

      # Trigger manual check
      assert :ok = PerformanceMonitor.trigger_manual_check()

      # Wait for check to complete
      Process.sleep(500)

      # Should have some trends and alerts
      trends = PerformanceMonitor.get_performance_trends()
      alerts = PerformanceMonitor.get_recent_alerts()

      assert is_list(trends)
      assert is_list(alerts)

      GenServer.stop(monitor)
    end
  end

  describe "metrics collector integration" do
    test "can collect metrics during benchmark execution", %{engine: engine} do
      # Start collection
      assert :ok = MetricsCollector.start_collection()

      # Simulate some work
      rule = %{
        id: :test_rule,
        conditions: [{:person, :name, :age}, {:age, :>, 18}],
        action: fn facts -> [{:adult, facts[:name]}] end
      }

      RuleEngine.add_rule(engine, rule)
      RuleEngine.assert_fact(engine, {:person, "Alice", 25})
      RuleEngine.fire_rules(engine)

      # Stop collection
      result = MetricsCollector.stop_collection()

      assert is_map(result)
      assert result.duration > 0
      assert is_integer(result.memory_usage)
      assert is_integer(result.gc_count)
      assert is_integer(result.reductions)
    end

    test "can collect RETE-specific metrics", %{engine: engine} do
      # Add some rules and facts
      rule = %{
        id: :test_rule,
        conditions: [{:person, :name, :age}, {:age, :>, 18}],
        action: fn facts -> [{:adult, facts[:name]}] end
      }

      RuleEngine.add_rule(engine, rule)
      RuleEngine.assert_fact(engine, {:person, "Alice", 25})
      RuleEngine.assert_fact(engine, {:person, "Bob", 16})
      RuleEngine.fire_rules(engine)

      # Collect RETE metrics
      rete_metrics = MetricsCollector.collect_rete_metrics(engine)

      assert is_map(rete_metrics)
      assert Map.has_key?(rete_metrics, :engine_stats)
      assert Map.has_key?(rete_metrics, :rule_stats)
      assert Map.has_key?(rete_metrics, :total_rules)
    end
  end

  describe "result analyzer integration" do
    test "can analyze benchmark results" do
      # Create a mock benchmark result
      mock_result = %{
        name: "test_benchmark",
        type: :micro,
        metrics: %{
          execution_time: %{
            mean: 1000,
            median: 950,
            min: 800,
            max: 1200,
            std_dev: 100
          },
          memory_usage: %{
            mean: 50000,
            median: 48000,
            min: 45000,
            max: 55000
          },
          iterations: 10
        },
        baseline_metrics: nil,
        improvement_percentage: nil,
        passed: true,
        execution_time: 1000,
        memory_usage: 50000,
        iterations: 10
      }

      analysis = ResultAnalyzer.analyze_benchmark_result(mock_result)

      assert is_map(analysis)
      assert Map.has_key?(analysis, :summary)
      assert Map.has_key?(analysis, :statistical_analysis)
      assert Map.has_key?(analysis, :validation_results)
      assert Map.has_key?(analysis, :recommendations)
      assert is_boolean(analysis.passed)
    end

    test "can analyze optimization comparison" do
      # Mock baseline and optimized results
      baseline_result = %{
        metrics: %{
          execution_time: %{mean: 2000},
          memory_usage: %{mean: 100_000}
        }
      }

      optimized_result = %{
        metrics: %{
          execution_time: %{mean: 1500},
          memory_usage: %{mean: 80000}
        }
      }

      analysis = ResultAnalyzer.analyze_optimization_comparison(baseline_result, optimized_result)

      assert is_map(analysis)
      assert Map.has_key?(analysis, :summary)
      assert Map.has_key?(analysis.summary, :comparison)
      assert analysis.summary.comparison.overall_improvement > 0
      assert analysis.passed == true
    end
  end

  describe "strategy-specific benchmarks" do
    test "alpha network strategy comprehensive benchmark", %{engine: engine} do
      metrics = AlphaNetworkStrategy.run_comprehensive_alpha_benchmark(engine)

      assert is_map(metrics)
      assert Map.has_key?(metrics, :throughput_metrics)
      assert Map.has_key?(metrics, :pattern_matching_metrics)
      assert Map.has_key?(metrics, :memory_efficiency_metrics)
      assert Map.has_key?(metrics, :conditional_evaluation_metrics)
      assert Map.has_key?(metrics, :compilation_metrics)
      assert Map.has_key?(metrics, :scalability_metrics)
    end

    test "beta network strategy comprehensive benchmark", %{engine: engine} do
      metrics = BetaNetworkStrategy.run_comprehensive_beta_benchmark(engine)

      assert is_map(metrics)
      assert Map.has_key?(metrics, :join_performance_metrics)
      assert Map.has_key?(metrics, :partial_match_metrics)
      assert Map.has_key?(metrics, :cross_product_metrics)
      assert Map.has_key?(metrics, :join_ordering_metrics)
      assert Map.has_key?(metrics, :memory_efficiency_metrics)
      assert Map.has_key?(metrics, :scalability_metrics)
    end

    test "rule engine strategy comprehensive benchmark", %{engine: engine} do
      metrics = RuleEngineStrategy.run_comprehensive_rule_engine_benchmark(engine)

      assert is_map(metrics)
      assert Map.has_key?(metrics, :execution_performance_metrics)
      assert Map.has_key?(metrics, :classification_metrics)
      assert Map.has_key?(metrics, :incremental_processing_metrics)
      assert Map.has_key?(metrics, :concurrent_execution_metrics)
      assert Map.has_key?(metrics, :compilation_optimization_metrics)
      assert Map.has_key?(metrics, :scalability_metrics)
    end

    test "memory strategy comprehensive benchmark", %{engine: engine} do
      metrics = MemoryStrategy.run_comprehensive_memory_benchmark(engine)

      assert is_map(metrics)
      assert Map.has_key?(metrics, :allocation_metrics)
      assert Map.has_key?(metrics, :gc_metrics)
      assert Map.has_key?(metrics, :leak_detection_metrics)
      assert Map.has_key?(metrics, :cache_effectiveness_metrics)
      assert Map.has_key?(metrics, :scalability_metrics)
    end
  end

  describe "optimization benchmarking" do
    test "can benchmark alpha network optimization", %{engine: engine} do
      optimization_fn = fn ->
        # Mock optimization
        :ok
      end

      result = AlphaNetworkStrategy.benchmark_alpha_indexing_optimization(engine, optimization_fn)

      assert is_map(result)
      assert Map.has_key?(result, :baseline)
      assert Map.has_key?(result, :optimized)
      assert Map.has_key?(result, :improvements)
      assert Map.has_key?(result, :optimization_effectiveness)
    end

    test "can benchmark beta network optimization", %{engine: engine} do
      optimization_fn = fn ->
        # Mock optimization
        :ok
      end

      result = BetaNetworkStrategy.benchmark_join_optimization(engine, optimization_fn)

      assert is_map(result)
      assert Map.has_key?(result, :baseline)
      assert Map.has_key?(result, :optimized)
      assert Map.has_key?(result, :improvements)
      assert Map.has_key?(result, :optimization_effectiveness)
    end

    test "can benchmark rule engine optimization", %{engine: engine} do
      optimization_fn = fn ->
        # Mock optimization
        :ok
      end

      result = RuleEngineStrategy.benchmark_rule_engine_optimization(engine, optimization_fn)

      assert is_map(result)
      assert Map.has_key?(result, :baseline)
      assert Map.has_key?(result, :optimized)
      assert Map.has_key?(result, :improvements)
      assert Map.has_key?(result, :optimization_effectiveness)
    end

    test "can benchmark memory optimization", %{engine: engine} do
      optimization_fn = fn ->
        # Mock optimization
        :ok
      end

      result = MemoryStrategy.benchmark_memory_optimization(engine, optimization_fn)

      assert is_map(result)
      assert Map.has_key?(result, :baseline)
      assert Map.has_key?(result, :optimized)
      assert Map.has_key?(result, :improvements)
      assert Map.has_key?(result, :optimization_effectiveness)
    end
  end

  describe "performance report generation" do
    @tag :slow
    test "can generate comprehensive performance report" do
      {:ok, monitor} =
        PerformanceMonitor.start_link(%{
          benchmark_suite: [:alpha_network_throughput]
        })

      # Trigger a check to have some data
      PerformanceMonitor.trigger_manual_check()
      Process.sleep(500)

      report = PerformanceMonitor.generate_performance_report()

      assert is_binary(report)
      assert String.contains?(report, "Performance Monitoring Report")
      assert String.contains?(report, "Generated:")
      assert String.contains?(report, "Recent Alerts")
      assert String.contains?(report, "Performance Trends")

      GenServer.stop(monitor)
    end
  end

  describe "end-to-end optimization workflow" do
    @tag :slow
    test "complete optimization measurement workflow" do
      # Step 1: Establish baseline performance
      baseline_config = %{
        iterations: 5,
        data_size: :small
      }

      baseline_result = BenchmarkRunner.run_benchmark(:rule_engine_execution, baseline_config)
      assert baseline_result.passed

      # Step 2: Apply mock optimization
      optimization_fn = fn ->
        # In a real scenario, this would apply actual optimizations
        # For testing, we just simulate the optimization
        # Simulate optimization work
        :timer.sleep(10)
      end

      # Step 3: Run optimization comparison
      comparison_result =
        BenchmarkRunner.run_optimization_comparison(
          :rule_engine_execution,
          optimization_fn,
          baseline_config
        )

      assert comparison_result.type == :comparison
      assert is_map(comparison_result.baseline_metrics)
      assert is_map(comparison_result.metrics)

      # Step 4: Analyze results
      analysis =
        ResultAnalyzer.analyze_optimization_comparison(
          %{metrics: comparison_result.baseline_metrics},
          %{metrics: comparison_result.metrics}
        )

      assert is_map(analysis)
      assert Map.has_key?(analysis, :summary)
      assert Map.has_key?(analysis, :recommendations)

      # Step 5: Generate final report (would include all analysis)
      report_data = %{
        baseline: baseline_result,
        optimization_comparison: comparison_result,
        analysis: analysis,
        timestamp: DateTime.utc_now()
      }

      assert is_map(report_data)
      assert Map.has_key?(report_data, :baseline)
      assert Map.has_key?(report_data, :optimization_comparison)
      assert Map.has_key?(report_data, :analysis)
    end
  end
end

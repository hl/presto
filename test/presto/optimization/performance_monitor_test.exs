defmodule Presto.Optimization.PerformanceMonitorTest do
  use ExUnit.Case, async: false

  alias Presto.Optimization.PerformanceMonitor

  setup do
    monitor = start_supervised!(PerformanceMonitor)
    {:ok, monitor: monitor}
  end

  describe "operation recording" do
    test "records shared memory operations", %{monitor: _monitor} do
      metrics = %{bytes_saved: 1024, total_memory_used: 10_240}

      assert :ok = PerformanceMonitor.record_shared_memory_operation(:memory_saved, metrics)
    end

    test "records join ordering operations", %{monitor: _monitor} do
      metrics = %{improvement_percentage: 25.5}

      assert :ok = PerformanceMonitor.record_join_ordering_operation(metrics)
    end

    test "records indexing operations", %{monitor: _monitor} do
      metrics = %{lookup_time_us: 150, hit: true, total_lookups: 1}

      assert :ok = PerformanceMonitor.record_indexing_operation(:index_lookup, metrics)
    end

    test "records join performance", %{monitor: _monitor} do
      assert :ok = PerformanceMonitor.record_join_performance(:hash_join, 2500, 42)
    end
  end

  describe "metrics retrieval" do
    test "gets current metrics", %{monitor: _monitor} do
      # Record some operations first
      PerformanceMonitor.record_shared_memory_operation(:cache_hit, %{})
      PerformanceMonitor.record_join_performance(:cartesian_join, 1000, 10)

      # Small delay to ensure operations are processed
      Process.sleep(10)

      metrics = PerformanceMonitor.get_current_metrics()

      assert is_map(metrics)
      assert Map.has_key?(metrics, :shared_memory)
      assert Map.has_key?(metrics, :join_ordering)
      assert Map.has_key?(metrics, :advanced_indexing)
      assert Map.has_key?(metrics, :overall)

      # Verify structure of nested metrics
      assert is_map(metrics.shared_memory)
      assert Map.has_key?(metrics.shared_memory, :total_shared_memories)
      assert Map.has_key?(metrics.shared_memory, :cache_hit_rate)

      assert is_map(metrics.overall)
      assert Map.has_key?(metrics.overall, :total_join_operations)
      assert Map.has_key?(metrics.overall, :average_join_time)
    end

    test "gets performance alerts", %{monitor: _monitor} do
      alerts = PerformanceMonitor.get_performance_alerts()

      assert is_list(alerts)
    end

    test "gets optimization recommendations", %{monitor: _monitor} do
      recommendations = PerformanceMonitor.get_optimization_recommendations()

      assert is_list(recommendations)
      assert Enum.all?(recommendations, &is_binary/1)
    end
  end

  describe "performance reporting" do
    test "generates comprehensive performance report", %{monitor: _monitor} do
      # Record various operations to populate metrics
      PerformanceMonitor.record_shared_memory_operation(:memory_saved, %{bytes_saved: 2048})
      PerformanceMonitor.record_join_ordering_operation(%{improvement_percentage: 15.0})
      PerformanceMonitor.record_indexing_operation(:index_created, %{memory_size: 1024})
      PerformanceMonitor.record_join_performance(:hash_join, 3000, 25)

      # Small delay to ensure operations are processed
      Process.sleep(10)

      report = PerformanceMonitor.generate_performance_report()

      assert is_map(report)
      assert Map.has_key?(report, :summary)
      assert Map.has_key?(report, :detailed_metrics)
      assert Map.has_key?(report, :alerts)
      assert Map.has_key?(report, :recommendations)
      assert Map.has_key?(report, :trends)
      assert Map.has_key?(report, :active_optimizations)

      # Verify summary structure
      assert is_map(report.summary)
      assert Map.has_key?(report.summary, :total_optimizations)
      assert Map.has_key?(report.summary, :overall_effectiveness)
      assert Map.has_key?(report.summary, :active_optimization_count)
      assert Map.has_key?(report.summary, :optimization_types_used)

      # Verify active optimizations are tracked
      assert is_list(report.active_optimizations)
    end
  end

  describe "metrics reset" do
    test "resets all metrics", %{monitor: _monitor} do
      # Record some operations
      PerformanceMonitor.record_shared_memory_operation(:cache_hit, %{})
      PerformanceMonitor.record_join_performance(:cartesian_join, 1000, 5)

      # Reset metrics
      assert :ok = PerformanceMonitor.reset_metrics()

      # Verify metrics are reset
      metrics = PerformanceMonitor.get_current_metrics()
      alerts = PerformanceMonitor.get_performance_alerts()

      # Should be back to initial state
      assert metrics.overall.total_join_operations == 0
      assert metrics.shared_memory.total_shared_memories == 0
      assert alerts == []
    end
  end

  describe "MapSet usage optimization" do
    test "efficiently handles multiple alert deduplication", %{monitor: _monitor} do
      # This test verifies that duplicate alerts are efficiently handled using MapSet

      # Generate conditions that would create similar alerts
      for _i <- 1..10 do
        # Above threshold
        PerformanceMonitor.record_join_performance(:slow_join, 15_000, 1)
      end

      # Small delay for processing
      Process.sleep(50)

      alerts = PerformanceMonitor.get_performance_alerts()

      # Should not have 10 identical alerts due to MapSet deduplication
      # The exact number depends on alert deduplication logic, but should be much less than 10
      assert length(alerts) < 10
    end

    test "tracks active optimization types efficiently", %{monitor: _monitor} do
      # Record operations of different types
      PerformanceMonitor.record_shared_memory_operation(:cache_hit, %{})
      PerformanceMonitor.record_join_ordering_operation(%{improvement_percentage: 10.0})
      PerformanceMonitor.record_indexing_operation(:index_lookup, %{hit: true})

      # Record more operations of same types (should not duplicate in active set)
      PerformanceMonitor.record_shared_memory_operation(:cache_miss, %{})
      PerformanceMonitor.record_join_ordering_operation(%{improvement_percentage: 20.0})

      Process.sleep(10)

      report = PerformanceMonitor.generate_performance_report()

      # Should have exactly 3 unique optimization types
      assert report.summary.active_optimization_count == 3
      assert :shared_memory in report.active_optimizations
      assert :join_ordering in report.active_optimizations
      assert :advanced_indexing in report.active_optimizations

      # Should not have duplicates
      unique_optimizations = Enum.uniq(report.active_optimizations)
      assert length(unique_optimizations) == length(report.active_optimizations)
    end
  end
end

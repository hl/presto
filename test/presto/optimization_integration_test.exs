defmodule Presto.OptimizationIntegrationTest do
  use ExUnit.Case, async: false

  alias Presto.AlphaNetwork
  alias Presto.BetaNetwork

  alias Presto.Optimization.{
    AdvancedIndexing,
    JoinOptimizer,
    PerformanceMonitor,
    SharedMemoryManager
  }

  describe "optimization integration" do
    setup do
      # Start shared memory manager
      shared_memory_pid = start_supervised!(SharedMemoryManager)

      # Start performance monitor
      monitor_pid = start_supervised!(PerformanceMonitor)

      # Create a mock working memory for alpha network
      working_memory = self()

      # Start alpha network
      {:ok, alpha_pid} = AlphaNetwork.start_link(working_memory: working_memory)

      # Start beta network with optimization enabled
      beta_opts = [
        alpha_network: alpha_pid
      ]

      {:ok, beta_pid} = BetaNetwork.start_link(beta_opts)

      {:ok,
       shared_memory: shared_memory_pid,
       monitor: monitor_pid,
       alpha_network: alpha_pid,
       beta_network: beta_pid}
    end

    test "optimizations work together without conflicts", ctx do
      # Test that all optimization modules can be used together

      # 1. Test shared memory manager
      memory_key = {[:integration_test], [:id]}
      test_data = [%{id: 1, value: "integration"}]

      {:ok, memory_ref} = SharedMemoryManager.get_or_create_shared_memory(memory_key, test_data)
      {:ok, retrieved_data} = SharedMemoryManager.get_shared_memory(memory_ref)
      assert retrieved_data == test_data

      # 2. Test join optimizer
      nodes = [
        %{id: "node_1", join_keys: [:id, :type], left_type: :alpha, right_type: :alpha},
        %{id: "node_2", join_keys: [:id], left_type: :alpha, right_type: :beta}
      ]

      optimization_plan =
        JoinOptimizer.optimize_join_order(nodes, %{enable_join_reordering: true})

      assert is_map(optimization_plan)
      assert is_boolean(optimization_plan.optimization_applied)

      # 3. Test advanced indexing
      join_keys = [:employee_id]
      index = AdvancedIndexing.create_multi_level_index(join_keys)
      indexed = AdvancedIndexing.index_data(index, test_data)

      lookup_results = AdvancedIndexing.lookup_with_advanced_index(indexed, %{id: 1})
      assert is_list(lookup_results)

      # 4. Test performance monitoring
      PerformanceMonitor.record_shared_memory_operation(:memory_saved, %{bytes_saved: 1024})
      PerformanceMonitor.record_join_ordering_operation(%{improvement_percentage: 15.0})
      PerformanceMonitor.record_indexing_operation(:index_lookup, %{hit: true})

      metrics = PerformanceMonitor.get_current_metrics()
      assert is_map(metrics)

      # 5. Test beta network with optimizations
      join_condition = {:join, "alpha_1", "alpha_2", [:employee_id]}
      {:ok, node_id} = BetaNetwork.create_beta_node(ctx.beta_network, join_condition)

      node_info = BetaNetwork.get_beta_node_info(ctx.beta_network, node_id)
      assert node_info != nil
      # Should have indexing enabled by default
      assert node_info.index_enabled == true

      # Cleanup
      SharedMemoryManager.release_shared_memory(memory_ref)
    end

    test "performance characteristics are within expected bounds", _ctx do
      # Test that optimizations actually improve performance characteristics

      # Create a larger dataset for meaningful performance testing
      large_dataset =
        for i <- 1..1000 do
          %{
            # 100 unique employees
            employee_id: rem(i, 100),
            # 30 unique dates
            date: Date.add(~D[2024-01-01], rem(i, 30)),
            # Hours between 8-11
            hours: 8 + rem(i, 4),
            department: Enum.at(["engineering", "sales", "marketing"], rem(i, 3))
          }
        end

      # Test advanced indexing performance
      join_keys = [:employee_id, :date]
      start_time = System.monotonic_time(:microsecond)

      index = AdvancedIndexing.create_multi_level_index(join_keys)
      indexed = AdvancedIndexing.index_data(index, large_dataset)

      index_creation_time = System.monotonic_time(:microsecond) - start_time

      # Perform multiple lookups to test performance
      lookup_start = System.monotonic_time(:microsecond)

      for i <- 1..100 do
        employee_id = rem(i, 100)

        _results =
          AdvancedIndexing.lookup_with_advanced_index(indexed, %{employee_id: employee_id})
      end

      lookup_time = System.monotonic_time(:microsecond) - lookup_start

      # Performance should be reasonable (these are loose bounds for testing)
      # Less than 100ms to create index
      assert index_creation_time < 100_000
      # Less than 50ms for 100 lookups
      assert lookup_time < 50_000

      # Test shared memory efficiency
      memory_key = {[:large_dataset], [:employee_id]}

      {:ok, memory_ref1} =
        SharedMemoryManager.get_or_create_shared_memory(memory_key, large_dataset)

      {:ok, memory_ref2} =
        SharedMemoryManager.get_or_create_shared_memory(memory_key, large_dataset)

      # Should reuse the same memory reference
      assert memory_ref1 == memory_ref2

      stats = SharedMemoryManager.get_memory_statistics()
      assert stats.total_shared_memories >= 1

      # Record performance metrics
      PerformanceMonitor.record_indexing_operation(:index_created, %{memory_size: 1024})
      PerformanceMonitor.record_join_performance(:hash_join, index_creation_time, 1000)

      # Cleanup
      SharedMemoryManager.release_shared_memory(memory_ref1)
    end

    test "optimizations maintain correctness under concurrent access", _ctx do
      # Test that optimizations work correctly when accessed concurrently

      test_data = [
        %{id: 1, type: "A", value: 100},
        %{id: 2, type: "B", value: 200},
        %{id: 3, type: "A", value: 300}
      ]

      # Spawn multiple processes that use shared memory concurrently
      tasks =
        for i <- 1..10 do
          Task.async(fn ->
            memory_key = {[:concurrent_test, i], [:id]}

            {:ok, memory_ref} =
              SharedMemoryManager.get_or_create_shared_memory(memory_key, test_data)

            {:ok, retrieved_data} = SharedMemoryManager.get_shared_memory(memory_ref)

            # Verify data integrity
            assert retrieved_data == test_data

            # Record some metrics
            PerformanceMonitor.record_shared_memory_operation(:cache_hit, %{})

            memory_ref
          end)
        end

      # Wait for all tasks and verify they completed successfully
      memory_refs = Enum.map(tasks, &Task.await/1)
      assert length(memory_refs) == 10
      assert Enum.all?(memory_refs, &is_binary/1)

      # Test concurrent join optimization
      nodes =
        for i <- 1..5 do
          %{
            id: "concurrent_node_#{i}",
            join_keys: [:id],
            left_type: :alpha,
            right_type: if(rem(i, 2) == 0, do: :beta, else: :alpha)
          }
        end

      optimization_tasks =
        for _i <- 1..5 do
          Task.async(fn ->
            JoinOptimizer.optimize_join_order(nodes, %{enable_join_reordering: true})
          end)
        end

      optimization_results = Enum.map(optimization_tasks, &Task.await/1)
      assert length(optimization_results) == 5
      assert Enum.all?(optimization_results, &is_map/1)

      # Cleanup shared memory
      Enum.each(memory_refs, &SharedMemoryManager.release_shared_memory/1)
    end
  end

  describe "optimization configuration" do
    test "optimizations can be enabled/disabled independently" do
      # Test with different optimization configurations

      configs = [
        %{enable_join_reordering: true, enable_advanced_indexing: true},
        %{enable_join_reordering: false, enable_advanced_indexing: true},
        %{enable_join_reordering: true, enable_advanced_indexing: false},
        %{enable_join_reordering: false, enable_advanced_indexing: false}
      ]

      nodes = [
        %{id: "test_node", join_keys: [:id], left_type: :alpha, right_type: :alpha}
      ]

      Enum.each(configs, fn config ->
        plan = JoinOptimizer.optimize_join_order(nodes, config)

        # Optimization should be applied only when enabled
        expected_optimization =
          config[:enable_join_reordering] &&
            Map.get(config, :cost_threshold, 0) < 1000

        if expected_optimization do
          # May or may not be applied depending on cost analysis
          assert is_boolean(plan.optimization_applied)
        else
          assert plan.optimization_applied == false
        end
      end)
    end
  end
end

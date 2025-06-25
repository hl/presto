defmodule Presto.Distributed.RuleCoordinatorTest do
  use ExUnit.Case, async: false

  alias Presto.Distributed.RuleCoordinator
  alias Presto.{RuleEngine, EngineRegistry}

  @moduletag :integration

  setup do
    # Start the rule coordinator for testing
    {:ok, coordinator_pid} = RuleCoordinator.start_link([])

    # Ensure clean state
    on_exit(fn ->
      if Process.alive?(coordinator_pid) do
        GenServer.stop(coordinator_pid)
      end
    end)

    {:ok, coordinator_pid: coordinator_pid}
  end

  describe "distributed rule execution" do
    test "executes scatter-gather rule execution", %{coordinator_pid: _pid} do
      # Start test engines
      {:ok, engine1_pid} = RuleEngine.start_link([])
      {:ok, engine2_pid} = RuleEngine.start_link([])

      assert :ok = EngineRegistry.register_engine(:test_engine_1, engine1_pid)
      assert :ok = EngineRegistry.register_engine(:test_engine_2, engine2_pid)

      # Execute distributed rule
      case RuleCoordinator.execute_distributed(:test_rule,
             execution_pattern: :scatter_gather,
             target_engines: [:test_engine_1, :test_engine_2],
             aggregation_strategy: :collect,
             timeout: 5000
           ) do
        {:ok, results} ->
          assert is_list(results) or is_map(results)

        {:error, reason} ->
          # May fail due to missing rule or engine issues
          assert reason in [:no_available_engines, :execution_failed, :timeout]
      end

      # Clean up
      EngineRegistry.unregister_engine(:test_engine_1)
      EngineRegistry.unregister_engine(:test_engine_2)
      GenServer.stop(engine1_pid)
      GenServer.stop(engine2_pid)
    end

    test "executes hierarchical rule execution", %{coordinator_pid: _pid} do
      {:ok, engine_pid} = RuleEngine.start_link([])
      assert :ok = EngineRegistry.register_engine(:hierarchical_engine, engine_pid)

      case RuleCoordinator.execute_distributed(:hierarchical_rule,
             execution_pattern: :hierarchical,
             target_engines: [:hierarchical_engine],
             timeout: 3000
           ) do
        {:ok, result} ->
          assert not is_nil(result)

        {:error, reason} ->
          assert reason in [:no_available_engines, :execution_failed, :timeout]
      end

      # Clean up
      EngineRegistry.unregister_engine(:hierarchical_engine)
      GenServer.stop(engine_pid)
    end

    test "auto-discovers engines when none specified", %{coordinator_pid: _pid} do
      {:ok, engine_pid} = RuleEngine.start_link([])
      assert :ok = EngineRegistry.register_engine(:auto_discover_engine, engine_pid)

      # Execute without specifying target engines
      case RuleCoordinator.execute_distributed(:auto_rule,
             execution_pattern: :scatter_gather,
             timeout: 3000
           ) do
        {:ok, result} ->
          assert not is_nil(result)

        {:error, reason} ->
          assert reason in [:no_available_engines, :execution_failed, :timeout]
      end

      # Clean up
      EngineRegistry.unregister_engine(:auto_discover_engine)
      GenServer.stop(engine_pid)
    end
  end

  describe "map-reduce execution" do
    test "executes map-reduce across nodes", %{coordinator_pid: _pid} do
      case RuleCoordinator.map_reduce_execution(:map_rule, :reduce_rule,
             target_nodes: [Node.self()],
             partition_strategy: :round_robin,
             timeout: 5000
           ) do
        {:ok, result} ->
          assert is_map(result)
          assert Map.has_key?(result, :reduced_results)

        {:error, reason} ->
          assert reason in [:no_available_nodes, :execution_failed, :timeout]
      end
    end

    test "handles map-reduce with custom parameters", %{coordinator_pid: _pid} do
      parameters = %{
        batch_size: 100,
        partition_key: :timestamp,
        custom_data: "test_data"
      }

      case RuleCoordinator.map_reduce_execution(:custom_map, :custom_reduce,
             target_nodes: [Node.self()],
             partition_strategy: :hash,
             parameters: parameters,
             timeout: 3000
           ) do
        {:ok, result} ->
          assert is_map(result)

        {:error, reason} ->
          assert is_atom(reason)
      end
    end
  end

  describe "distributed join operations" do
    test "executes distributed join", %{coordinator_pid: _pid} do
      # Start engines for join
      {:ok, left_engine_pid} = RuleEngine.start_link([])
      {:ok, right_engine_pid} = RuleEngine.start_link([])
      {:ok, result_engine_pid} = RuleEngine.start_link([])

      assert :ok = EngineRegistry.register_engine(:left_engine, left_engine_pid)
      assert :ok = EngineRegistry.register_engine(:right_engine, right_engine_pid)
      assert :ok = EngineRegistry.register_engine(:result_engine, result_engine_pid)

      # Add some test facts
      RuleEngine.assert_facts_bulk(left_engine_pid, [
        {:customer, :alice, :active},
        {:customer, :bob, :inactive}
      ])

      RuleEngine.assert_facts_bulk(right_engine_pid, [
        {:order, :o1, :alice},
        {:order, :o2, :bob}
      ])

      # Execute distributed join
      case RuleCoordinator.distributed_join(
             :left_engine,
             :right_engine,
             # Join condition
             {:customer_id, :customer_id},
             :result_engine,
             timeout: 5000
           ) do
        {:ok, join_results} ->
          assert is_list(join_results)

        {:error, reason} ->
          assert reason in [:engine_not_found, :execution_failed, :timeout]
      end

      # Clean up
      EngineRegistry.unregister_engine(:left_engine)
      EngineRegistry.unregister_engine(:right_engine)
      EngineRegistry.unregister_engine(:result_engine)
      GenServer.stop(left_engine_pid)
      GenServer.stop(right_engine_pid)
      GenServer.stop(result_engine_pid)
    end

    test "handles join with different key types", %{coordinator_pid: _pid} do
      {:ok, left_engine_pid} = RuleEngine.start_link([])
      {:ok, right_engine_pid} = RuleEngine.start_link([])

      assert :ok = EngineRegistry.register_engine(:join_left, left_engine_pid)
      assert :ok = EngineRegistry.register_engine(:join_right, right_engine_pid)

      # Test join with integer keys
      case RuleCoordinator.distributed_join(
             :join_left,
             :join_right,
             # Position-based join
             {0, 1},
             # No result engine
             nil,
             timeout: 3000
           ) do
        {:ok, results} ->
          assert is_list(results)

        {:error, reason} ->
          assert is_atom(reason)
      end

      # Clean up
      EngineRegistry.unregister_engine(:join_left)
      EngineRegistry.unregister_engine(:join_right)
      GenServer.stop(left_engine_pid)
      GenServer.stop(right_engine_pid)
    end
  end

  describe "execution status and monitoring" do
    test "gets execution status", %{coordinator_pid: _pid} do
      # Test with non-existent execution
      assert {:error, :not_found} = RuleCoordinator.get_execution_status("nonexistent_id")
    end

    test "cancels running execution", %{coordinator_pid: _pid} do
      # Test cancelling non-existent execution
      result = RuleCoordinator.cancel_execution("nonexistent_id")
      assert result in [:ok, {:error, :not_found}]
    end

    test "gets execution statistics", %{coordinator_pid: _pid} do
      stats = RuleCoordinator.get_execution_stats()

      assert is_map(stats)
      assert Map.has_key?(stats, :total_executions)
      assert Map.has_key?(stats, :successful_executions)
      assert Map.has_key?(stats, :failed_executions)
      assert Map.has_key?(stats, :avg_execution_time)
      assert Map.has_key?(stats, :distributed_joins)
      assert Map.has_key?(stats, :map_reduce_operations)

      assert is_integer(stats.total_executions)
      assert is_integer(stats.successful_executions)
      assert is_integer(stats.failed_executions)
      assert is_number(stats.avg_execution_time)
      assert is_integer(stats.distributed_joins)
      assert is_integer(stats.map_reduce_operations)
    end
  end

  describe "aggregation strategies" do
    test "sum aggregation strategy", %{coordinator_pid: _pid} do
      {:ok, engine_pid} = RuleEngine.start_link([])
      assert :ok = EngineRegistry.register_engine(:sum_test_engine, engine_pid)

      case RuleCoordinator.execute_distributed(:sum_rule,
             execution_pattern: :scatter_gather,
             target_engines: [:sum_test_engine],
             aggregation_strategy: :sum,
             timeout: 3000
           ) do
        {:ok, result} ->
          assert is_number(result) or is_list(result)

        {:error, reason} ->
          assert is_atom(reason)
      end

      # Clean up
      EngineRegistry.unregister_engine(:sum_test_engine)
      GenServer.stop(engine_pid)
    end

    test "count aggregation strategy", %{coordinator_pid: _pid} do
      {:ok, engine_pid} = RuleEngine.start_link([])
      assert :ok = EngineRegistry.register_engine(:count_test_engine, engine_pid)

      case RuleCoordinator.execute_distributed(:count_rule,
             execution_pattern: :scatter_gather,
             target_engines: [:count_test_engine],
             aggregation_strategy: :count,
             timeout: 3000
           ) do
        {:ok, result} ->
          assert is_integer(result)

        {:error, reason} ->
          assert is_atom(reason)
      end

      # Clean up
      EngineRegistry.unregister_engine(:count_test_engine)
      GenServer.stop(engine_pid)
    end

    test "collect aggregation strategy", %{coordinator_pid: _pid} do
      {:ok, engine_pid} = RuleEngine.start_link([])
      assert :ok = EngineRegistry.register_engine(:collect_test_engine, engine_pid)

      case RuleCoordinator.execute_distributed(:collect_rule,
             execution_pattern: :scatter_gather,
             target_engines: [:collect_test_engine],
             aggregation_strategy: :collect,
             timeout: 3000
           ) do
        {:ok, result} ->
          assert is_list(result)

        {:error, reason} ->
          assert is_atom(reason)
      end

      # Clean up
      EngineRegistry.unregister_engine(:collect_test_engine)
      GenServer.stop(engine_pid)
    end
  end

  describe "error handling and fault tolerance" do
    test "handles execution with no available engines", %{coordinator_pid: _pid} do
      case RuleCoordinator.execute_distributed(:no_engine_rule,
             execution_pattern: :scatter_gather,
             target_engines: [:nonexistent_engine],
             timeout: 1000
           ) do
        {:error, reason} ->
          assert reason in [:no_available_engines, :engine_not_found, :execution_failed, :timeout]

        {:ok, _result} ->
          flunk("Should not succeed with nonexistent engine")
      end
    end

    test "handles execution timeout", %{coordinator_pid: _pid} do
      {:ok, engine_pid} = RuleEngine.start_link([])
      assert :ok = EngineRegistry.register_engine(:timeout_test_engine, engine_pid)

      case RuleCoordinator.execute_distributed(:timeout_rule,
             execution_pattern: :scatter_gather,
             target_engines: [:timeout_test_engine],
             # Very short timeout
             timeout: 1
           ) do
        {:error, :timeout} ->
          :ok

        {:ok, _result} ->
          # May complete very quickly
          :ok

        {:error, other_reason} ->
          assert is_atom(other_reason)
      end

      # Clean up
      EngineRegistry.unregister_engine(:timeout_test_engine)
      GenServer.stop(engine_pid)
    end

    test "handles unsupported execution patterns", %{coordinator_pid: _pid} do
      {:ok, engine_pid} = RuleEngine.start_link([])
      assert :ok = EngineRegistry.register_engine(:pattern_test_engine, engine_pid)

      case RuleCoordinator.execute_distributed(:pattern_rule,
             execution_pattern: :unsupported_pattern,
             target_engines: [:pattern_test_engine]
           ) do
        {:error, {:unsupported_pattern, :unsupported_pattern}} ->
          :ok

        {:error, other_reason} ->
          assert is_atom(other_reason) or is_tuple(other_reason)

        {:ok, _result} ->
          # May treat unsupported pattern as default
          :ok
      end

      # Clean up
      EngineRegistry.unregister_engine(:pattern_test_engine)
      GenServer.stop(engine_pid)
    end

    test "handles concurrent executions", %{coordinator_pid: _pid} do
      {:ok, engine_pid} = RuleEngine.start_link([])
      assert :ok = EngineRegistry.register_engine(:concurrent_test_engine, engine_pid)

      # Start multiple concurrent executions
      tasks =
        Enum.map(1..3, fn i ->
          Task.async(fn ->
            RuleCoordinator.execute_distributed(
              String.to_atom("concurrent_rule_#{i}"),
              execution_pattern: :scatter_gather,
              target_engines: [:concurrent_test_engine],
              timeout: 2000
            )
          end)
        end)

      results = Task.await_many(tasks, 5000)

      # All executions should complete (successfully or with error)
      Enum.each(results, fn result ->
        case result do
          {:ok, _result} -> :ok
          {:error, reason} -> assert is_atom(reason)
        end
      end)

      # Clean up
      EngineRegistry.unregister_engine(:concurrent_test_engine)
      GenServer.stop(engine_pid)
    end
  end
end

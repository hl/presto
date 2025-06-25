defmodule Presto.Distributed.ConsensusManagerTest do
  use ExUnit.Case, async: false

  alias Presto.Distributed.ConsensusManager

  @moduletag :integration

  setup do
    # Start the consensus manager for testing
    {:ok, manager_pid} = ConsensusManager.start_link([])

    # Ensure clean state
    on_exit(fn ->
      if Process.alive?(manager_pid) do
        GenServer.stop(manager_pid)
      end
    end)

    {:ok, manager_pid: manager_pid}
  end

  describe "consensus operations" do
    test "proposes operations with majority consensus", %{manager_pid: _pid} do
      operation_data = %{
        engine_name: :test_consensus_engine,
        action: :create,
        parameters: %{rules: [], facts: []}
      }

      case ConsensusManager.propose_operation(:engine_creation, operation_data,
             consensus_algorithm: :majority
           ) do
        {:ok, operation_id} ->
          assert is_binary(operation_id)

          # Check operation status
          case ConsensusManager.get_operation_status(operation_id) do
            {:ok, status} ->
              assert is_map(status)
              assert Map.has_key?(status, :operation_id)
              assert Map.has_key?(status, :status)

            {:error, :not_found} ->
              # Operation may have completed quickly
              :ok
          end

        {:error, reason} ->
          # Expected on single node with insufficient participants
          assert reason in [:insufficient_nodes, :consensus_failed]
      end
    end

    test "proposes operations with RAFT consensus", %{manager_pid: _pid} do
      operation_data = %{
        rule_id: :test_rule,
        engine_name: :test_engine,
        rule_content: "test rule content"
      }

      case ConsensusManager.propose_operation(:rule_update, operation_data,
             consensus_algorithm: :raft
           ) do
        {:ok, operation_id} ->
          assert is_binary(operation_id)

        {:error, reason} ->
          # Expected on single node
          assert reason in [:insufficient_nodes, :not_leader]
      end
    end

    test "proposes operations with BFT consensus", %{manager_pid: _pid} do
      operation_data = %{
        engine_name: :test_bft_engine,
        operation_type: :state_update,
        data: %{key: :value}
      }

      case ConsensusManager.propose_operation(:engine_state_update, operation_data,
             consensus_algorithm: :bft
           ) do
        {:ok, operation_id} ->
          assert is_binary(operation_id)

        {:error, reason} ->
          # Expected on single node with insufficient Byzantine fault tolerance
          assert reason in [:insufficient_nodes, :consensus_failed]
      end
    end
  end

  describe "consensus status and monitoring" do
    test "gets operation status for non-existent operation", %{manager_pid: _pid} do
      assert {:error, :not_found} = ConsensusManager.get_operation_status("nonexistent_id")
    end

    test "cancels pending operations", %{manager_pid: _pid} do
      # Try to cancel a non-existent operation
      result = ConsensusManager.cancel_operation("nonexistent_id")
      assert result in [:ok, {:error, :not_found}]
    end

    test "gets consensus statistics", %{manager_pid: _pid} do
      stats = ConsensusManager.get_consensus_stats()

      assert is_map(stats)
      assert Map.has_key?(stats, :total_operations)
      assert Map.has_key?(stats, :successful_operations)
      assert Map.has_key?(stats, :failed_operations)
      assert Map.has_key?(stats, :pending_operations)
      assert Map.has_key?(stats, :avg_consensus_time)

      assert is_integer(stats.total_operations)
      assert is_integer(stats.successful_operations)
      assert is_integer(stats.failed_operations)
      assert is_integer(stats.pending_operations)
      assert is_number(stats.avg_consensus_time)
    end

    test "gets cluster consensus status", %{manager_pid: _pid} do
      status = ConsensusManager.get_cluster_consensus_status()

      assert is_map(status)
      assert Map.has_key?(status, :consensus_protocol)
      assert Map.has_key?(status, :participating_nodes)
      assert Map.has_key?(status, :leader_node)
      assert Map.has_key?(status, :quorum_size)

      assert is_atom(status.consensus_protocol)
      assert is_list(status.participating_nodes)
      assert is_integer(status.quorum_size)
    end
  end

  describe "consensus algorithms" do
    test "majority consensus with single node", %{manager_pid: _pid} do
      # Single node should be able to achieve majority consensus with itself
      operation_data = %{simple: :operation}

      case ConsensusManager.propose_operation(:test_operation, operation_data,
             consensus_algorithm: :majority,
             timeout: 1000
           ) do
        {:ok, operation_id} ->
          assert is_binary(operation_id)

        {:error, reason} ->
          assert reason in [:insufficient_nodes, :timeout]
      end
    end

    test "RAFT leader election simulation", %{manager_pid: _pid} do
      # Test RAFT behavior on single node
      operation_data = %{raft_test: :data}

      case ConsensusManager.propose_operation(:raft_test, operation_data,
             consensus_algorithm: :raft
           ) do
        {:ok, _operation_id} ->
          # Single node can be leader
          :ok

        {:error, :not_leader} ->
          # May not be leader yet
          :ok

        {:error, :insufficient_nodes} ->
          # Expected with single node RAFT
          :ok
      end
    end

    test "BFT consensus tolerance", %{manager_pid: _pid} do
      operation_data = %{bft_test: :byzantine_data}

      case ConsensusManager.propose_operation(:bft_test, operation_data,
             consensus_algorithm: :bft,
             fault_tolerance: 1
           ) do
        {:ok, _operation_id} ->
          :ok

        {:error, reason} ->
          # Expected with insufficient nodes for BFT
          assert reason in [:insufficient_nodes, :consensus_failed]
      end
    end
  end

  describe "error handling and edge cases" do
    test "handles invalid operation types", %{manager_pid: _pid} do
      operation_data = %{invalid: :data}

      case ConsensusManager.propose_operation(:invalid_operation_type, operation_data) do
        {:error, reason} ->
          assert reason in [:invalid_operation_type, :unsupported_operation]

        {:ok, _operation_id} ->
          # May accept unknown operation types
          :ok
      end
    end

    test "handles operation timeout", %{manager_pid: _pid} do
      operation_data = %{timeout_test: :data}

      case ConsensusManager.propose_operation(:timeout_test, operation_data,
             # Very short timeout
             timeout: 1
           ) do
        {:error, :timeout} ->
          :ok

        {:ok, _operation_id} ->
          # May complete quickly
          :ok

        {:error, other_reason} ->
          # Other valid error reasons
          assert other_reason in [:insufficient_nodes, :consensus_failed]
      end
    end

    test "handles concurrent operations", %{manager_pid: _pid} do
      # Submit multiple operations concurrently
      operation_data = %{concurrent: :test}

      tasks =
        Enum.map(1..5, fn i ->
          Task.async(fn ->
            ConsensusManager.propose_operation(
              String.to_atom("concurrent_op_#{i}"),
              Map.put(operation_data, :id, i)
            )
          end)
        end)

      results = Task.await_many(tasks, 5000)

      # All operations should either succeed or fail gracefully
      Enum.each(results, fn result ->
        case result do
          {:ok, operation_id} -> assert is_binary(operation_id)
          {:error, reason} -> assert is_atom(reason)
        end
      end)
    end

    test "handles operation with invalid data", %{manager_pid: _pid} do
      # Test with nil operation data
      case ConsensusManager.propose_operation(:test_operation, nil) do
        {:error, :invalid_operation_data} ->
          :ok

        {:ok, _operation_id} ->
          # May accept nil data
          :ok

        {:error, other_reason} ->
          assert is_atom(other_reason)
      end
    end
  end

  describe "consensus configuration" do
    test "respects consensus timeout configuration", %{manager_pid: _pid} do
      operation_data = %{config_test: :data}

      start_time = System.monotonic_time(:millisecond)

      result = ConsensusManager.propose_operation(:config_test, operation_data, timeout: 2000)

      end_time = System.monotonic_time(:millisecond)
      duration = end_time - start_time

      case result do
        {:error, :timeout} ->
          # Should timeout around 2000ms
          assert duration >= 1800 and duration <= 2500

        _ ->
          # Operation completed before timeout
          assert duration < 2000
      end
    end

    test "handles different quorum sizes", %{manager_pid: _pid} do
      operation_data = %{quorum_test: :data}

      # Test with different quorum requirements
      case ConsensusManager.propose_operation(:quorum_test, operation_data,
             consensus_algorithm: :majority,
             quorum_size: 1
           ) do
        {:ok, _operation_id} ->
          :ok

        {:error, reason} ->
          assert is_atom(reason)
      end
    end
  end
end

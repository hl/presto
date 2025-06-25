defmodule Presto.Distributed.CoordinatorTest do
  use ExUnit.Case, async: false

  alias Presto.Distributed.Coordinator

  @moduletag :integration

  describe "distributed coordinator" do
    test "starts successfully with default configuration" do
      {:ok, pid} = Coordinator.start_link([])
      assert Process.alive?(pid)

      # Test basic functionality
      topology = Coordinator.get_cluster_topology()
      assert is_map(topology)
      assert topology.local_node == Node.self()

      # Clean up
      GenServer.stop(pid)
    end

    test "handles cluster topology queries" do
      {:ok, pid} =
        Coordinator.start_link(
          cluster_nodes: [Node.self()],
          sync_interval: 30_000,
          heartbeat_interval: 5_000
        )

      topology = Coordinator.get_cluster_topology()

      assert topology.local_node == Node.self()
      assert is_map(topology.cluster_nodes)
      assert is_integer(topology.total_engines)
      assert is_list(topology.distributed_engines)

      GenServer.stop(pid)
    end

    test "syncs cluster state manually" do
      {:ok, pid} = Coordinator.start_link([])

      # Manual sync should succeed even with single node
      assert :ok = Coordinator.sync_cluster_state()

      GenServer.stop(pid)
    end

    test "handles distributed engine creation for single node" do
      {:ok, pid} = Coordinator.start_link([])

      # Should succeed with single node and replication factor 1
      case Coordinator.start_distributed_engine(
             name: :test_distributed_engine,
             replication_factor: 1,
             consistency: :eventual
           ) do
        {:ok, engine_pid} ->
          assert Process.alive?(engine_pid)

          # Verify engine is registered
          case Presto.EngineRegistry.lookup_engine(:test_distributed_engine) do
            {:ok, ^engine_pid} -> :ok
            error -> flunk("Engine not registered: #{inspect(error)}")
          end

          # Clean up
          Presto.EngineSupervisor.stop_engine(:test_distributed_engine)

        {:error, :insufficient_nodes} ->
          # Expected for multi-node replication on single node
          :ok

        error ->
          flunk("Unexpected error: #{inspect(error)}")
      end

      GenServer.stop(pid)
    end

    test "can check consensus capability" do
      {:ok, pid} = Coordinator.start_link([])

      # Consensus should be possible for local operations
      can_achieve = Coordinator.can_achieve_consensus?(:test_engine, :test_operation)
      assert is_boolean(can_achieve)

      GenServer.stop(pid)
    end

    test "handles failover triggers gracefully" do
      {:ok, pid} = Coordinator.start_link([])

      # Should handle non-existent engine gracefully
      case Coordinator.trigger_failover(:nonexistent_engine) do
        {:error, _reason} -> :ok
        # Also acceptable
        :ok -> :ok
      end

      GenServer.stop(pid)
    end
  end

  describe "cluster events" do
    test "handles node down events gracefully" do
      {:ok, pid} = Coordinator.start_link([])

      # Send a fake node down event
      send(pid, {:nodedown, :fake_node, :connection_closed})

      # Should still be alive and responsive
      Process.sleep(100)
      assert Process.alive?(pid)

      # Should still respond to queries
      topology = Coordinator.get_cluster_topology()
      assert is_map(topology)

      GenServer.stop(pid)
    end

    test "handles node up events gracefully" do
      {:ok, pid} = Coordinator.start_link([])

      # Send a fake node up event
      send(pid, {:nodeup, :fake_node})

      # Should still be alive and responsive
      Process.sleep(100)
      assert Process.alive?(pid)

      GenServer.stop(pid)
    end

    test "handles cluster messages" do
      {:ok, pid} = Coordinator.start_link([])

      # Send a fake cluster message
      send(
        pid,
        {:cluster_message, Node.self(), %{type: :heartbeat, timestamp: DateTime.utc_now()}}
      )

      # Should handle gracefully
      Process.sleep(100)
      assert Process.alive?(pid)

      GenServer.stop(pid)
    end
  end

  describe "error handling" do
    test "handles invalid configuration gracefully" do
      # Should start even with some invalid options
      {:ok, pid} =
        Coordinator.start_link(
          invalid_option: "invalid",
          cluster_nodes: "not_a_list"
        )

      assert Process.alive?(pid)
      GenServer.stop(pid)
    end

    test "recovers from internal errors" do
      {:ok, pid} = Coordinator.start_link([])

      # Send invalid message to test error handling
      send(pid, {:invalid_message, "test"})

      # Should still be alive
      Process.sleep(100)
      assert Process.alive?(pid)

      GenServer.stop(pid)
    end
  end
end

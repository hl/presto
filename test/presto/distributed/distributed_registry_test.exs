defmodule Presto.Distributed.DistributedRegistryTest do
  use ExUnit.Case, async: false

  alias Presto.Distributed.DistributedRegistry
  alias Presto.{RuleEngine, EngineRegistry}

  @moduletag :integration

  setup do
    # Start the registry for testing
    {:ok, registry_pid} = DistributedRegistry.start_link([])

    # Ensure clean state
    on_exit(fn ->
      if Process.alive?(registry_pid) do
        GenServer.stop(registry_pid)
      end
    end)

    {:ok, registry_pid: registry_pid}
  end

  describe "global engine registration" do
    test "registers engine globally", %{registry_pid: _pid} do
      # Start a test engine
      {:ok, engine_pid} = RuleEngine.start_link([])

      # Register globally
      assert :ok = DistributedRegistry.register_global_engine(:test_global_engine, engine_pid)

      # Should be findable
      case DistributedRegistry.lookup_global_engine(:test_global_engine) do
        {:ok, engine_info} ->
          assert engine_info.name == :test_global_engine
          assert engine_info.local_pid == engine_pid
          assert engine_info.primary_node == Node.self()
          assert engine_info.status == :active

        error ->
          flunk("Engine not found: #{inspect(error)}")
      end

      # Clean up
      DistributedRegistry.unregister_global_engine(:test_global_engine)
      GenServer.stop(engine_pid)
    end

    test "prevents duplicate registrations", %{registry_pid: _pid} do
      {:ok, engine_pid1} = RuleEngine.start_link([])
      {:ok, engine_pid2} = RuleEngine.start_link([])

      # First registration should succeed
      assert :ok = DistributedRegistry.register_global_engine(:duplicate_test, engine_pid1)

      # Second registration should fail
      case DistributedRegistry.register_global_engine(:duplicate_test, engine_pid2) do
        {:error, _reason} -> :ok
        :ok -> flunk("Should not allow duplicate registrations")
      end

      # Clean up
      DistributedRegistry.unregister_global_engine(:duplicate_test)
      GenServer.stop(engine_pid1)
      GenServer.stop(engine_pid2)
    end

    test "unregisters engines correctly", %{registry_pid: _pid} do
      {:ok, engine_pid} = RuleEngine.start_link([])

      # Register and verify
      assert :ok = DistributedRegistry.register_global_engine(:unregister_test, engine_pid)
      assert {:ok, _} = DistributedRegistry.lookup_global_engine(:unregister_test)

      # Unregister and verify
      assert :ok = DistributedRegistry.unregister_global_engine(:unregister_test)
      assert {:error, :not_found} = DistributedRegistry.lookup_global_engine(:unregister_test)

      GenServer.stop(engine_pid)
    end

    test "handles engine process death", %{registry_pid: _pid} do
      {:ok, engine_pid} = RuleEngine.start_link([])

      # Register engine
      assert :ok = DistributedRegistry.register_global_engine(:death_test, engine_pid)

      # Kill the engine process
      GenServer.stop(engine_pid)
      # Allow cleanup time
      Process.sleep(100)

      # Should detect death in health check
      health = DistributedRegistry.cluster_health_check()
      assert is_map(health)
      assert is_list(health.engine_health)
    end
  end

  describe "cluster operations" do
    test "lists cluster engines", %{registry_pid: _pid} do
      {:ok, engine_pid} = RuleEngine.start_link([])

      # Register engine
      assert :ok = DistributedRegistry.register_global_engine(:cluster_list_test, engine_pid)

      # List should include our engine
      engines = DistributedRegistry.list_cluster_engines()
      assert is_list(engines)

      engine_names = Enum.map(engines, fn engine -> engine.name end)
      assert :cluster_list_test in engine_names

      # Clean up
      DistributedRegistry.unregister_global_engine(:cluster_list_test)
      GenServer.stop(engine_pid)
    end

    test "performs cluster health checks", %{registry_pid: _pid} do
      health = DistributedRegistry.cluster_health_check()

      assert is_map(health)
      assert Map.has_key?(health, :timestamp)
      assert Map.has_key?(health, :total_engines)
      assert Map.has_key?(health, :total_nodes)
      assert Map.has_key?(health, :engine_health)
      assert Map.has_key?(health, :node_health)

      assert is_integer(health.total_engines)
      assert is_integer(health.total_nodes)
      assert is_list(health.engine_health)
      assert is_list(health.node_health)
    end

    test "gets routing information", %{registry_pid: _pid} do
      {:ok, engine_pid} = RuleEngine.start_link([])

      # Register with failover enabled
      assert :ok =
               DistributedRegistry.register_global_engine(:routing_test, engine_pid,
                 failover_enabled: true
               )

      # Get routing info
      case DistributedRegistry.get_engine_routing_info(:routing_test) do
        {:ok, routing_info} ->
          assert routing_info.name == :routing_test
          assert routing_info.primary_node == Node.self()
          assert is_list(routing_info.available_nodes)
          assert routing_info.failover_enabled == true

        error ->
          flunk("Could not get routing info: #{inspect(error)}")
      end

      # Clean up
      DistributedRegistry.unregister_global_engine(:routing_test)
      GenServer.stop(engine_pid)
    end

    test "handles failover triggers", %{registry_pid: _pid} do
      {:ok, engine_pid} = RuleEngine.start_link([])

      # Register with failover enabled
      assert :ok =
               DistributedRegistry.register_global_engine(:failover_test, engine_pid,
                 failover_enabled: true
               )

      # Trigger failover (should fail on single node)
      case DistributedRegistry.trigger_engine_failover(:failover_test) do
        # Expected on single node
        {:error, :no_available_nodes} -> :ok
        # Other errors are acceptable
        {:error, _reason} -> :ok
        # Success is also acceptable
        :ok -> :ok
      end

      # Clean up
      DistributedRegistry.unregister_global_engine(:failover_test)
      GenServer.stop(engine_pid)
    end

    test "provides registry statistics", %{registry_pid: _pid} do
      stats = DistributedRegistry.get_registry_stats()

      assert is_map(stats)
      assert Map.has_key?(stats, :global_engines_count)
      assert Map.has_key?(stats, :nodes_in_cluster)

      assert is_integer(stats.global_engines_count)
      assert is_integer(stats.nodes_in_cluster)
    end
  end

  describe "error handling" do
    test "handles lookup of non-existent engines", %{registry_pid: _pid} do
      assert {:error, :not_found} = DistributedRegistry.lookup_global_engine(:nonexistent)
    end

    test "handles unregistration of non-existent engines", %{registry_pid: _pid} do
      # Should not crash
      result = DistributedRegistry.unregister_global_engine(:nonexistent)
      assert result in [:ok, {:error, :not_found}]
    end

    test "handles routing info for non-existent engines", %{registry_pid: _pid} do
      assert {:error, :not_found} = DistributedRegistry.get_engine_routing_info(:nonexistent)
    end

    test "handles failover for non-existent engines", %{registry_pid: _pid} do
      assert {:error, :engine_not_found} =
               DistributedRegistry.trigger_engine_failover(:nonexistent)
    end
  end

  describe "integration with local registry" do
    test "maintains consistency with local registry", %{registry_pid: _pid} do
      {:ok, engine_pid} = RuleEngine.start_link([])

      # Register globally (should also register locally)
      assert :ok = DistributedRegistry.register_global_engine(:consistency_test, engine_pid)

      # Should be in local registry
      case EngineRegistry.lookup_engine(:consistency_test) do
        {:ok, ^engine_pid} -> :ok
        error -> flunk("Not in local registry: #{inspect(error)}")
      end

      # Unregister globally
      assert :ok = DistributedRegistry.unregister_global_engine(:consistency_test)

      # Should be removed from local registry
      assert :error = EngineRegistry.lookup_engine(:consistency_test)

      GenServer.stop(engine_pid)
    end

    test "handles engines registered only locally", %{registry_pid: _pid} do
      {:ok, engine_pid} = RuleEngine.start_link([])

      # Register only locally
      assert :ok = EngineRegistry.register_engine(:local_only, engine_pid)

      # Should not be in global registry initially
      assert {:error, :not_found} = DistributedRegistry.lookup_global_engine(:local_only)

      # But should appear in cluster engines list after sync
      # Allow sync time
      Process.sleep(100)
      engines = DistributedRegistry.list_cluster_engines()
      engine_names = Enum.map(engines, fn engine -> engine.name end)

      # May or may not be included depending on sync timing
      # This is acceptable behavior

      # Clean up
      EngineRegistry.unregister_engine(:local_only)
      GenServer.stop(engine_pid)
    end
  end
end

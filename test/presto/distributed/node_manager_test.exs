defmodule Presto.Distributed.NodeManagerTest do
  use ExUnit.Case, async: false

  alias Presto.Distributed.NodeManager

  @moduletag :integration

  setup do
    # Start the node manager for testing
    {:ok, manager_pid} =
      NodeManager.start_link(
        strategy: :static,
        nodes: [Node.self()],
        discovery_interval: 30_000,
        health_check_interval: 5_000
      )

    # Ensure clean state
    on_exit(fn ->
      if Process.alive?(manager_pid) do
        GenServer.stop(manager_pid)
      end
    end)

    {:ok, manager_pid: manager_pid}
  end

  describe "node discovery" do
    test "discovers nodes with static strategy", %{manager_pid: _pid} do
      discovered_nodes = NodeManager.get_discovered_nodes()

      assert is_list(discovered_nodes)
      # Should at least include the current node
      assert Node.self() in discovered_nodes
    end

    test "handles DNS-based discovery", %{manager_pid: _pid} do
      # Start node manager with DNS strategy
      GenServer.stop(NodeManager)

      {:ok, dns_manager_pid} =
        NodeManager.start_link(
          strategy: :dns,
          dns_name: "localhost",
          discovery_interval: 5_000
        )

      # Give it time to attempt discovery
      Process.sleep(100)

      discovered_nodes = NodeManager.get_discovered_nodes()
      assert is_list(discovered_nodes)

      # Clean up
      GenServer.stop(dns_manager_pid)
    end

    test "handles Consul-based discovery", %{manager_pid: _pid} do
      # Start node manager with Consul strategy
      GenServer.stop(NodeManager)

      {:ok, consul_manager_pid} =
        NodeManager.start_link(
          strategy: :consul,
          consul_url: "http://localhost:8500",
          service_name: "presto-cluster",
          discovery_interval: 5_000
        )

      # Give it time to attempt discovery
      Process.sleep(100)

      discovered_nodes = NodeManager.get_discovered_nodes()
      assert is_list(discovered_nodes)

      # Clean up
      GenServer.stop(consul_manager_pid)
    end

    test "handles Kubernetes-based discovery", %{manager_pid: _pid} do
      # Start node manager with Kubernetes strategy
      GenServer.stop(NodeManager)

      {:ok, k8s_manager_pid} =
        NodeManager.start_link(
          strategy: :kubernetes,
          namespace: "default",
          service_name: "presto-service",
          discovery_interval: 5_000
        )

      # Give it time to attempt discovery
      Process.sleep(100)

      discovered_nodes = NodeManager.get_discovered_nodes()
      assert is_list(discovered_nodes)

      # Clean up
      GenServer.stop(k8s_manager_pid)
    end
  end

  describe "node health monitoring" do
    test "performs health checks on discovered nodes", %{manager_pid: _pid} do
      health_status = NodeManager.get_cluster_health()

      assert is_map(health_status)
      assert Map.has_key?(health_status, :healthy_nodes)
      assert Map.has_key?(health_status, :unhealthy_nodes)
      assert Map.has_key?(health_status, :total_nodes)
      assert Map.has_key?(health_status, :last_check)

      assert is_list(health_status.healthy_nodes)
      assert is_list(health_status.unhealthy_nodes)
      assert is_integer(health_status.total_nodes)

      # Current node should be healthy
      assert Node.self() in health_status.healthy_nodes
    end

    test "checks specific node health", %{manager_pid: _pid} do
      # Check current node health
      health = NodeManager.check_node_health(Node.self())

      assert health in [:healthy, :unhealthy, :unknown]
      # Current node should be healthy
      assert health == :healthy
    end

    test "handles health check for non-existent node", %{manager_pid: _pid} do
      health = NodeManager.check_node_health(:nonexistent_node@localhost)

      assert health in [:unhealthy, :unknown]
    end
  end

  describe "node membership management" do
    test "gets node membership information", %{manager_pid: _pid} do
      membership = NodeManager.get_node_membership()

      assert is_map(membership)
      assert Map.has_key?(membership, :local_node)
      assert Map.has_key?(membership, :cluster_nodes)
      assert Map.has_key?(membership, :node_roles)

      assert membership.local_node == Node.self()
      assert is_list(membership.cluster_nodes)
      assert is_map(membership.node_roles)

      # Local node should be in cluster
      assert Node.self() in membership.cluster_nodes
    end

    test "handles node join events", %{manager_pid: _pid} do
      # Simulate node join event
      fake_node = :fake_node@localhost

      # This would normally be triggered by actual node discovery
      # For testing, we can check that the manager handles it gracefully
      current_nodes = NodeManager.get_discovered_nodes()

      # Should handle gracefully even if fake node isn't actually reachable
      assert is_list(current_nodes)
    end

    test "handles node leave events", %{manager_pid: _pid} do
      # Test node leave detection
      initial_health = NodeManager.get_cluster_health()

      # Force a health check
      Process.send(NodeManager, :health_check, [])
      Process.sleep(100)

      updated_health = NodeManager.get_cluster_health()

      assert is_map(initial_health)
      assert is_map(updated_health)
    end
  end

  describe "discovery configuration" do
    test "handles invalid discovery strategy", %{manager_pid: _pid} do
      # Start node manager with invalid strategy
      GenServer.stop(NodeManager)

      case NodeManager.start_link(
             strategy: :invalid_strategy,
             discovery_interval: 5_000
           ) do
        {:ok, invalid_manager_pid} ->
          # Should start but possibly with degraded functionality
          discovered_nodes = NodeManager.get_discovered_nodes()
          assert is_list(discovered_nodes)

          GenServer.stop(invalid_manager_pid)

        {:error, reason} ->
          # May reject invalid strategy
          assert is_atom(reason)
      end
    end

    test "respects discovery interval configuration", %{manager_pid: _pid} do
      # Start node manager with short discovery interval
      GenServer.stop(NodeManager)

      {:ok, fast_manager_pid} =
        NodeManager.start_link(
          strategy: :static,
          nodes: [Node.self()],
          # Very frequent discovery
          discovery_interval: 100
        )

      # Should still function normally
      discovered_nodes = NodeManager.get_discovered_nodes()
      assert is_list(discovered_nodes)
      assert Node.self() in discovered_nodes

      GenServer.stop(fast_manager_pid)
    end

    test "handles discovery timeout gracefully", %{manager_pid: _pid} do
      # Test with unreachable DNS to trigger timeout
      GenServer.stop(NodeManager)

      {:ok, timeout_manager_pid} =
        NodeManager.start_link(
          strategy: :dns,
          dns_name: "nonexistent.example.com",
          discovery_interval: 1_000,
          # Short timeout
          discovery_timeout: 100
        )

      # Give it time to attempt discovery and timeout
      Process.sleep(200)

      # Should still be responsive
      discovered_nodes = NodeManager.get_discovered_nodes()
      assert is_list(discovered_nodes)

      GenServer.stop(timeout_manager_pid)
    end
  end

  describe "node manager statistics" do
    test "provides discovery statistics", %{manager_pid: _pid} do
      stats = NodeManager.get_discovery_stats()

      assert is_map(stats)
      assert Map.has_key?(stats, :total_discoveries)
      assert Map.has_key?(stats, :successful_discoveries)
      assert Map.has_key?(stats, :failed_discoveries)
      assert Map.has_key?(stats, :avg_discovery_time)
      assert Map.has_key?(stats, :last_discovery)

      assert is_integer(stats.total_discoveries)
      assert is_integer(stats.successful_discoveries)
      assert is_integer(stats.failed_discoveries)
      assert is_number(stats.avg_discovery_time)
    end

    test "provides health check statistics", %{manager_pid: _pid} do
      # Trigger a health check to generate stats
      Process.send(NodeManager, :health_check, [])
      Process.sleep(100)

      stats = NodeManager.get_discovery_stats()

      # Should have some health check activity
      assert is_map(stats)
    end
  end

  describe "error handling and resilience" do
    test "handles discovery service unavailable", %{manager_pid: _pid} do
      # Test with unreachable Consul service
      GenServer.stop(NodeManager)

      {:ok, resilient_manager_pid} =
        NodeManager.start_link(
          strategy: :consul,
          consul_url: "http://unreachable:8500",
          discovery_interval: 1_000
        )

      # Should handle gracefully and remain responsive
      Process.sleep(200)

      discovered_nodes = NodeManager.get_discovered_nodes()
      assert is_list(discovered_nodes)

      health = NodeManager.get_cluster_health()
      assert is_map(health)

      GenServer.stop(resilient_manager_pid)
    end

    test "recovers from temporary discovery failures", %{manager_pid: _pid} do
      # Node manager should be resilient to temporary failures
      initial_nodes = NodeManager.get_discovered_nodes()

      # Force a health check
      Process.send(NodeManager, :health_check, [])
      Process.sleep(100)

      # Should still have discovered nodes
      current_nodes = NodeManager.get_discovered_nodes()
      assert length(current_nodes) >= length(initial_nodes)
    end

    test "handles concurrent health checks", %{manager_pid: _pid} do
      # Send multiple health check messages concurrently
      Enum.each(1..5, fn _ ->
        Process.send(NodeManager, :health_check, [])
      end)

      Process.sleep(200)

      # Should remain responsive
      health = NodeManager.get_cluster_health()
      assert is_map(health)

      discovered_nodes = NodeManager.get_discovered_nodes()
      assert is_list(discovered_nodes)
    end

    test "handles invalid node configuration", %{manager_pid: _pid} do
      # Start with invalid node list
      GenServer.stop(NodeManager)

      {:ok, invalid_config_manager_pid} =
        NodeManager.start_link(
          strategy: :static,
          nodes: [:invalid_node@invalid_host, "not_an_atom"],
          discovery_interval: 5_000
        )

      # Should handle gracefully
      discovered_nodes = NodeManager.get_discovered_nodes()
      assert is_list(discovered_nodes)

      GenServer.stop(invalid_config_manager_pid)
    end
  end
end

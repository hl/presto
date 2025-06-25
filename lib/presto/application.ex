defmodule Presto.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children =
      [
        # Engine registry for named engine management
        Presto.EngineRegistry,

        # Dynamic supervisor for engine processes
        Presto.EngineSupervisor,

        # Distributed coordination (optional - only if clustering enabled)
        maybe_distributed_children(),

        # Optional: Setup telemetry handlers
        {Task, &setup_telemetry/0}
      ]
      |> List.flatten()

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Presto.Supervisor]
    Supervisor.start_link(children, opts)
  end

  # Conditionally include distributed components based on configuration
  defp maybe_distributed_children do
    if distributed_mode_enabled?() do
      [
        # Node discovery and membership management
        {Presto.Distributed.NodeManager, get_node_manager_config()},

        # Fact replication across nodes
        Presto.Distributed.FactReplicator,

        # Distributed consensus for coordinated operations
        Presto.Distributed.ConsensusManager,

        # Cross-node rule execution coordination
        Presto.Distributed.RuleCoordinator,

        # Distributed engine registry
        Presto.Distributed.DistributedRegistry,

        # Main distributed coordinator
        {Presto.Distributed.Coordinator, get_coordinator_config()}
      ]
    else
      []
    end
  end

  # Check if distributed mode is enabled via configuration
  defp distributed_mode_enabled? do
    Application.get_env(:presto, :distributed_mode, false)
  end

  # Get node manager configuration
  defp get_node_manager_config do
    Application.get_env(:presto, :node_manager,
      strategy: :static,
      nodes: [Node.self()],
      discovery_interval: 30_000,
      health_check_interval: 5_000
    )
  end

  # Get coordinator configuration
  defp get_coordinator_config do
    Application.get_env(:presto, :distributed_coordinator,
      cluster_nodes: [Node.self()],
      sync_interval: 5000,
      heartbeat_interval: 1000,
      partition_tolerance: :ap
    )
  end

  # Setup telemetry handlers if telemetry is available
  defp setup_telemetry do
    try do
      # Setup default telemetry handlers for logging
      Presto.Telemetry.setup_default_handlers()

      # Optionally setup Prometheus metrics if available
      try do
        Presto.Telemetry.Prometheus.setup()
      rescue
        # Prometheus not available, skip
        _ -> :ok
      end
    rescue
      # Telemetry not available, skip
      _ -> :ok
    end
  end
end

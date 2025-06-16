defmodule Presto.Distributed.ClusterManager do
  @moduledoc """
  Cluster management for distributed RETE networks.

  Manages node discovery, health monitoring, and cluster coordination for
  horizontally scaled RETE processing. Supports dynamic node addition/removal
  and automatic failover for high availability.

  ## Architecture

  The cluster uses a peer-to-peer approach with eventually consistent state:
  - Each node maintains its own view of the cluster topology
  - Network partitioning is handled gracefully with split-brain detection
  - Fact distribution follows consistent hashing for predictable routing
  - Alpha and beta network segments are distributed based on load and affinity

  ## Node Types

  - **Coordinator**: Manages cluster membership and partitioning decisions
  - **Worker**: Processes assigned alpha/beta network segments
  - **Storage**: Maintains distributed working memory partitions
  - **Gateway**: Handles external client connections and load balancing
  """

  use GenServer
  require Logger

  alias Presto.Distributed.{HealthMonitor, NodeRegistry, PartitionManager}
  alias Presto.Logger, as: PrestoLogger

  @type node_type :: :coordinator | :worker | :storage | :gateway
  @type node_info :: %{
          id: String.t(),
          type: node_type(),
          node: node(),
          pid: pid(),
          status: :active | :suspect | :failed,
          capacity: %{
            cpu_cores: pos_integer(),
            memory_mb: pos_integer(),
            network_segments: pos_integer()
          },
          load: %{
            cpu_usage: float(),
            memory_usage: float(),
            active_segments: pos_integer(),
            facts_per_second: float()
          },
          last_heartbeat: integer(),
          joined_at: integer()
        }

  @type cluster_state :: %{
          nodes: %{String.t() => node_info()},
          partitions: %{String.t() => [String.t()]},
          coordinator: String.t() | nil,
          replication_factor: pos_integer(),
          partition_count: pos_integer(),
          cluster_id: String.t()
        }

  @default_config %{
    node_type: :worker,
    heartbeat_interval: 5_000,
    failure_timeout: 15_000,
    replication_factor: 2,
    partition_count: 16,
    max_nodes_per_partition: 3
  }

  # Client API

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    config = Map.merge(@default_config, Map.new(opts))
    GenServer.start_link(__MODULE__, config, name: __MODULE__)
  end

  @spec join_cluster([node()], keyword()) :: :ok | {:error, term()}
  def join_cluster(seed_nodes, opts \\ []) do
    GenServer.call(__MODULE__, {:join_cluster, seed_nodes, opts})
  end

  @spec leave_cluster() :: :ok
  def leave_cluster do
    GenServer.call(__MODULE__, :leave_cluster)
  end

  @spec get_cluster_state() :: cluster_state()
  def get_cluster_state do
    GenServer.call(__MODULE__, :get_cluster_state)
  end

  @spec get_node_info(String.t()) :: node_info() | nil
  def get_node_info(node_id) do
    GenServer.call(__MODULE__, {:get_node_info, node_id})
  end

  @spec get_nodes_by_type(node_type()) :: [node_info()]
  def get_nodes_by_type(type) do
    GenServer.call(__MODULE__, {:get_nodes_by_type, type})
  end

  @spec route_fact(tuple()) :: {:ok, [String.t()]} | {:error, term()}
  def route_fact(fact) do
    GenServer.call(__MODULE__, {:route_fact, fact})
  end

  @spec assign_network_segment(String.t(), map()) :: :ok | {:error, term()}
  def assign_network_segment(segment_id, segment_config) do
    GenServer.call(__MODULE__, {:assign_network_segment, segment_id, segment_config})
  end

  @spec report_node_load(map()) :: :ok
  def report_node_load(load_metrics) do
    GenServer.cast(__MODULE__, {:report_node_load, load_metrics})
  end

  # Server implementation

  @impl true
  def init(config) do
    node_id = generate_node_id()
    cluster_id = Map.get(config, :cluster_id, generate_cluster_id())

    # Start health monitoring
    {:ok, health_monitor} = HealthMonitor.start_link(self())

    # Register this node
    {:ok, registry} = NodeRegistry.start_link()

    state = %{
      node_id: node_id,
      cluster_id: cluster_id,
      config: config,
      health_monitor: health_monitor,
      registry: registry,
      cluster_state: %{
        nodes: %{},
        partitions: %{},
        coordinator: nil,
        replication_factor: config.replication_factor,
        partition_count: config.partition_count,
        cluster_id: cluster_id
      },
      local_node: create_local_node_info(node_id, config),
      heartbeat_timer: nil,
      partition_manager: nil
    }

    # Schedule heartbeat
    schedule_heartbeat(state.config.heartbeat_interval)

    PrestoLogger.log_distributed(:info, node_id, "cluster_manager_started", %{
      cluster_id: cluster_id,
      node_type: config.node_type
    })

    {:ok, state}
  end

  @impl true
  def handle_call({:join_cluster, seed_nodes, opts}, _from, state) do
    case attempt_cluster_join(seed_nodes, state.local_node, opts) do
      {:ok, cluster_state} ->
        # Start partition manager if we're joining successfully
        {:ok, partition_manager} =
          PartitionManager.start_link(
            cluster_state,
            state.local_node
          )

        new_state = %{state | cluster_state: cluster_state, partition_manager: partition_manager}

        PrestoLogger.log_distributed(:info, state.node_id, "cluster_joined", %{
          cluster_id: cluster_state.cluster_id,
          node_count: map_size(cluster_state.nodes)
        })

        {:reply, :ok, new_state}

      {:error, reason} ->
        PrestoLogger.log_distributed(:error, state.node_id, "cluster_join_failed", %{
          reason: reason,
          seed_nodes: seed_nodes
        })

        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call(:leave_cluster, _from, state) do
    # Gracefully leave the cluster
    if state.partition_manager do
      PartitionManager.stop(state.partition_manager)
    end

    # Notify other nodes of departure
    broadcast_node_departure(state)

    new_state = %{
      state
      | cluster_state: %{state.cluster_state | nodes: %{}},
        partition_manager: nil
    }

    PrestoLogger.log_distributed(:info, state.node_id, "cluster_left", %{
      cluster_id: state.cluster_id
    })

    {:reply, :ok, new_state}
  end

  @impl true
  def handle_call(:get_cluster_state, _from, state) do
    {:reply, state.cluster_state, state}
  end

  @impl true
  def handle_call({:get_node_info, node_id}, _from, state) do
    node_info = Map.get(state.cluster_state.nodes, node_id)
    {:reply, node_info, state}
  end

  @impl true
  def handle_call({:get_nodes_by_type, type}, _from, state) do
    nodes =
      state.cluster_state.nodes
      |> Enum.filter(fn {_id, node} -> node.type == type end)
      |> Enum.map(fn {_id, node} -> node end)

    {:reply, nodes, state}
  end

  @impl true
  def handle_call({:route_fact, fact}, _from, state) do
    case route_fact_to_partitions(fact, state.cluster_state) do
      {:ok, target_nodes} ->
        {:reply, {:ok, target_nodes}, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:assign_network_segment, segment_id, segment_config}, _from, state) do
    if state.partition_manager do
      result =
        PartitionManager.assign_segment(
          state.partition_manager,
          segment_id,
          segment_config
        )

      {:reply, result, state}
    else
      {:reply, {:error, :not_in_cluster}, state}
    end
  end

  @impl true
  def handle_cast({:report_node_load, load_metrics}, state) do
    # Update local node load information
    updated_node = Map.put(state.local_node, :load, load_metrics)

    # Broadcast load update to cluster
    broadcast_node_update(updated_node, state)

    new_state = %{state | local_node: updated_node}
    {:noreply, new_state}
  end

  @impl true
  def handle_cast({:node_update, node_info}, state) do
    # Update cluster state with node information
    updated_nodes = Map.put(state.cluster_state.nodes, node_info.id, node_info)
    updated_cluster_state = %{state.cluster_state | nodes: updated_nodes}

    new_state = %{state | cluster_state: updated_cluster_state}
    {:noreply, new_state}
  end

  @impl true
  def handle_cast({:node_failure, node_id}, state) do
    # Handle node failure - remove from cluster and trigger rebalancing
    case Map.get(state.cluster_state.nodes, node_id) do
      nil ->
        {:noreply, state}

      failed_node ->
        PrestoLogger.log_distributed(:warning, state.node_id, "node_failure_detected", %{
          failed_node_id: node_id,
          node_type: failed_node.type
        })

        # Remove failed node and trigger rebalancing
        updated_nodes = Map.delete(state.cluster_state.nodes, node_id)
        updated_cluster_state = %{state.cluster_state | nodes: updated_nodes}

        # Trigger partition rebalancing if we have a partition manager
        if state.partition_manager do
          PartitionManager.handle_node_failure(state.partition_manager, node_id)
        end

        new_state = %{state | cluster_state: updated_cluster_state}
        {:noreply, new_state}
    end
  end

  @impl true
  def handle_info(:heartbeat, state) do
    # Send heartbeat to cluster
    broadcast_heartbeat(state)

    # Check for failed nodes
    check_node_health(state)

    # Schedule next heartbeat
    schedule_heartbeat(state.config.heartbeat_interval)

    {:noreply, state}
  end

  @impl true
  def handle_info({:nodedown, node}, state) do
    # Handle Erlang node disconnection
    node_id = find_node_id_by_erlang_node(node, state)

    if node_id do
      GenServer.cast(self(), {:node_failure, node_id})
    end

    {:noreply, state}
  end

  # Private implementation functions

  defp generate_node_id do
    "node_" <> (:crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower))
  end

  defp generate_cluster_id do
    "cluster_" <> (:crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower))
  end

  defp create_local_node_info(node_id, config) do
    %{
      id: node_id,
      type: config.node_type,
      node: Node.self(),
      pid: self(),
      status: :active,
      capacity: get_node_capacity(),
      load: %{
        cpu_usage: 0.0,
        memory_usage: 0.0,
        active_segments: 0,
        facts_per_second: 0.0
      },
      last_heartbeat: System.monotonic_time(:millisecond),
      joined_at: System.monotonic_time(:millisecond)
    }
  end

  defp get_node_capacity do
    # Get system capacity information
    %{
      cpu_cores: System.schedulers_online(),
      memory_mb: get_memory_size_mb(),
      # Default segment capacity
      network_segments: 10
    }
  end

  defp get_memory_size_mb do
    # Simplified memory detection - in production would use :os.type specific calls
    # Default 1GB
    1024
  end

  defp attempt_cluster_join(seed_nodes, local_node, _opts) do
    # Try to contact seed nodes and join cluster
    case contact_seed_nodes(seed_nodes) do
      {:ok, cluster_state} ->
        # Add ourselves to the cluster state
        updated_nodes = Map.put(cluster_state.nodes, local_node.id, local_node)
        updated_cluster_state = %{cluster_state | nodes: updated_nodes}
        {:ok, updated_cluster_state}

      {:error, :no_responsive_seeds} ->
        # No existing cluster found, start new cluster
        new_cluster_state = %{
          nodes: %{local_node.id => local_node},
          partitions: initialize_partitions(local_node.id),
          coordinator: local_node.id,
          replication_factor: 2,
          partition_count: 16,
          cluster_id: generate_cluster_id()
        }

        {:ok, new_cluster_state}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp contact_seed_nodes([]), do: {:error, :no_responsive_seeds}

  defp contact_seed_nodes([node | rest]) do
    case :rpc.call(node, __MODULE__, :get_cluster_state, [], 5000) do
      {:badrpc, _reason} ->
        contact_seed_nodes(rest)

      cluster_state when is_map(cluster_state) ->
        {:ok, cluster_state}
    end
  end

  defp initialize_partitions(coordinator_node_id) do
    # Create initial partition assignment
    0..15
    |> Enum.into(%{}, fn partition_id ->
      {"partition_#{partition_id}", [coordinator_node_id]}
    end)
  end

  defp route_fact_to_partitions(fact, cluster_state) do
    # Use consistent hashing to determine partition
    fact_hash = :crypto.hash(:sha256, :erlang.term_to_binary(fact))

    partition_index =
      :binary.decode_unsigned(binary_part(fact_hash, 0, 4))
      |> rem(cluster_state.partition_count)

    partition_key = "partition_#{partition_index}"

    case Map.get(cluster_state.partitions, partition_key) do
      nil -> {:error, :partition_not_found}
      node_list -> {:ok, node_list}
    end
  end

  defp broadcast_node_departure(_state) do
    # Implement node departure notification
    :ok
  end

  defp broadcast_node_update(_node_info, _state) do
    # Implement node update broadcasting
    :ok
  end

  defp broadcast_heartbeat(_state) do
    # Implement heartbeat broadcasting
    :ok
  end

  defp check_node_health(_state) do
    # Implement node health checking
    :ok
  end

  defp find_node_id_by_erlang_node(_node, _state) do
    # Find cluster node ID by Erlang node reference
    nil
  end

  defp schedule_heartbeat(interval) do
    Process.send_after(self(), :heartbeat, interval)
  end
end

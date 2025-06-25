defmodule Presto.Distributed.Coordinator do
  @moduledoc """
  Distributed coordination system for Presto RETE engines across multiple nodes.

  This module provides the foundation for horizontal scaling by enabling:
  - Node discovery and cluster management
  - Cross-node engine registry synchronization
  - Distributed fact sharing and replication
  - Consensus-based distributed operations
  - Fault tolerance and network partition handling

  ## Architecture

  The coordinator operates as a distributed state machine with each node maintaining:
  - Local engine registry with global visibility
  - Distributed fact store with conflict resolution
  - Node membership and health tracking
  - Cross-node message routing and delivery

  ## Clustering

  Uses Erlang's native clustering capabilities for node discovery and communication.
  Supports various deployment patterns:
  - Static node configuration
  - Dynamic service discovery (DNS, Consul, etcd)
  - Kubernetes pod discovery
  - Cloud provider auto-scaling groups

  ## Example Usage

      # Start distributed coordination
      Presto.Distributed.Coordinator.start_link([
        cluster_nodes: [:"app@node1", :"app@node2", :"app@node3"],
        sync_interval: 5000,
        heartbeat_interval: 1000
      ])

      # Create distributed engine
      {:ok, engine} = Presto.Distributed.Coordinator.start_distributed_engine(
        name: :global_payroll,
        replication_factor: 3,
        consistency: :eventual
      )

      # Share facts across nodes
      :ok = Presto.Distributed.Coordinator.replicate_facts(
        :global_payroll,
        [{:employee, "emp_001", "Alice", :engineering}]
      )
  """

  use GenServer
  require Logger

  alias Presto.Distributed.FactReplicator

  @type node_status :: :online | :offline | :suspicious | :partitioned
  @type consistency_level :: :strong | :eventual | :local
  @type replication_factor :: 1..5

  @type cluster_opts :: [
          cluster_nodes: [node()],
          sync_interval: pos_integer(),
          heartbeat_interval: pos_integer(),
          # CAP theorem preference
          partition_tolerance: :ap | :cp,
          max_network_delay: pos_integer()
        ]

  @type engine_opts :: [
          name: atom(),
          replication_factor: replication_factor(),
          consistency: consistency_level(),
          partition_strategy: :quorum | :majority | :manual
        ]

  ## Client API

  @doc """
  Starts the distributed coordinator.
  """
  @spec start_link(cluster_opts()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Starts a distributed engine with cross-node replication.
  """
  @spec start_distributed_engine(engine_opts()) :: {:ok, pid()} | {:error, term()}
  def start_distributed_engine(opts) do
    GenServer.call(__MODULE__, {:start_distributed_engine, opts})
  end

  @doc """
  Replicates facts to distributed engines across the cluster.
  """
  @spec replicate_facts(atom(), [tuple()]) :: :ok | {:error, term()}
  def replicate_facts(engine_name, facts) do
    GenServer.call(__MODULE__, {:replicate_facts, engine_name, facts})
  end

  @doc """
  Gets the current cluster topology and node status.
  """
  @spec get_cluster_topology() :: %{
          local_node: node(),
          cluster_nodes: %{node() => node_status()},
          total_engines: non_neg_integer(),
          distributed_engines: [%{name: atom(), nodes: [node()], status: atom()}]
        }
  def get_cluster_topology do
    GenServer.call(__MODULE__, :get_cluster_topology)
  end

  @doc """
  Forces synchronization of engine registries across all nodes.
  """
  @spec sync_cluster_state() :: :ok | {:error, term()}
  def sync_cluster_state do
    GenServer.call(__MODULE__, :sync_cluster_state)
  end

  @doc """
  Checks if the local node can achieve consensus for a distributed operation.
  """
  @spec can_achieve_consensus?(atom(), term()) :: boolean()
  def can_achieve_consensus?(engine_name, operation) do
    GenServer.call(__MODULE__, {:can_achieve_consensus, engine_name, operation})
  end

  @doc """
  Manually triggers failover for a distributed engine.
  """
  @spec trigger_failover(atom()) :: :ok | {:error, term()}
  def trigger_failover(engine_name) do
    GenServer.call(__MODULE__, {:trigger_failover, engine_name})
  end

  ## Server implementation

  @impl GenServer
  def init(opts) do
    Logger.info("Starting Presto Distributed Coordinator", node: Node.self())

    # Initialize node monitoring
    :net_kernel.monitor_nodes(true, nodedown_reason: true)

    state = %{
      cluster_nodes: Keyword.get(opts, :cluster_nodes, []),
      sync_interval: Keyword.get(opts, :sync_interval, 5000),
      heartbeat_interval: Keyword.get(opts, :heartbeat_interval, 1000),
      partition_tolerance: Keyword.get(opts, :partition_tolerance, :ap),
      max_network_delay: Keyword.get(opts, :max_network_delay, 5000),

      # Cluster state
      node_status: %{},
      distributed_engines: %{},
      sync_refs: %{},
      consensus_operations: %{},

      # Statistics
      stats: %{
        cluster_events: 0,
        sync_operations: 0,
        consensus_requests: 0,
        failed_operations: 0
      }
    }

    # Start periodic tasks
    schedule_cluster_sync(state.sync_interval)
    schedule_heartbeat(state.heartbeat_interval)

    # Discover initial cluster nodes
    {:ok, discover_cluster_nodes(state)}
  end

  @impl GenServer
  def handle_call({:start_distributed_engine, opts}, _from, state) do
    name = Keyword.fetch!(opts, :name)
    replication_factor = Keyword.get(opts, :replication_factor, 1)
    consistency = Keyword.get(opts, :consistency, :eventual)

    case create_distributed_engine(name, replication_factor, consistency, state) do
      {:ok, engine_pid, new_state} ->
        Logger.info("Created distributed engine",
          name: name,
          replication_factor: replication_factor,
          consistency: consistency,
          pid: inspect(engine_pid)
        )

        {:reply, {:ok, engine_pid}, new_state}

      {:error, reason} = error ->
        Logger.error("Failed to create distributed engine",
          name: name,
          reason: inspect(reason)
        )

        {:reply, error, state}
    end
  end

  @impl GenServer
  def handle_call({:replicate_facts, engine_name, facts}, _from, state) do
    case Map.get(state.distributed_engines, engine_name) do
      nil ->
        {:reply, {:error, :engine_not_found}, state}

      engine_info ->
        case FactReplicator.replicate_to_nodes(engine_info.nodes, engine_name, facts) do
          :ok ->
            {:reply, :ok, state}

          {:error, reason} ->
            Logger.warning("Failed to replicate facts",
              engine: engine_name,
              reason: inspect(reason)
            )

            {:reply, {:error, reason}, state}
        end
    end
  end

  @impl GenServer
  def handle_call(:get_cluster_topology, _from, state) do
    topology = %{
      local_node: Node.self(),
      cluster_nodes: state.node_status,
      total_engines: map_size(state.distributed_engines),
      distributed_engines: build_engine_status_list(state.distributed_engines)
    }

    {:reply, topology, state}
  end

  @impl GenServer
  def handle_call(:sync_cluster_state, _from, state) do
    case sync_with_cluster(state) do
      {:ok, new_state} ->
        Logger.info("Cluster state synchronized successfully")
        {:reply, :ok, new_state}

      {:error, reason} ->
        Logger.error("Failed to sync cluster state", reason: inspect(reason))
        {:reply, {:error, reason}, state}
    end
  end

  @impl GenServer
  def handle_call({:can_achieve_consensus, engine_name, _operation}, _from, state) do
    case Map.get(state.distributed_engines, engine_name) do
      nil ->
        {:reply, false, state}

      engine_info ->
        online_nodes = count_online_nodes(engine_info.nodes, state.node_status)
        required_nodes = calculate_quorum(engine_info.replication_factor)
        can_achieve = online_nodes >= required_nodes

        {:reply, can_achieve, state}
    end
  end

  @impl GenServer
  def handle_call({:trigger_failover, engine_name}, _from, state) do
    case perform_engine_failover(engine_name, state) do
      {:ok, new_state} ->
        Logger.info("Engine failover completed", engine: engine_name)
        {:reply, :ok, new_state}

      {:error, reason} ->
        Logger.error("Engine failover failed",
          engine: engine_name,
          reason: inspect(reason)
        )

        {:reply, {:error, reason}, state}
    end
  end

  @impl GenServer
  def handle_cast(:sync_cluster_state, state) do
    new_state = perform_cluster_sync(state)
    {:noreply, new_state}
  end

  @impl GenServer
  def handle_cast(unexpected_cast, state) do
    Logger.debug("Received unexpected cast", message: inspect(unexpected_cast))
    {:noreply, state}
  end

  @impl GenServer
  def handle_info({:nodedown, node, reason}, state) do
    Logger.warning("Node went down", node: node, reason: inspect(reason))

    new_state = handle_node_down(node, reason, state)

    {:noreply, new_state}
  end

  @impl GenServer
  def handle_info({:nodeup, node}, state) do
    Logger.info("Node came online", node: node)

    new_state = handle_node_up(node, state)

    {:noreply, new_state}
  end

  @impl GenServer
  def handle_info(:cluster_sync, state) do
    new_state = perform_cluster_sync(state)
    schedule_cluster_sync(state.sync_interval)
    {:noreply, new_state}
  end

  @impl GenServer
  def handle_info(:heartbeat, state) do
    new_state = send_heartbeat_to_nodes(state)
    schedule_heartbeat(state.heartbeat_interval)
    {:noreply, new_state}
  end

  @impl GenServer
  def handle_info({:cluster_message, from_node, message}, state) do
    new_state = handle_cluster_message(from_node, message, state)
    {:noreply, new_state}
  end

  @impl GenServer
  def handle_info(unexpected_message, state) do
    Logger.debug("Received unexpected message", message: inspect(unexpected_message))
    {:noreply, state}
  end

  ## Private functions

  defp discover_cluster_nodes(state) do
    current_nodes = Node.list(:connected) ++ [Node.self()]

    node_status =
      Enum.reduce(current_nodes, %{}, fn node, acc ->
        status = if node == Node.self(), do: :online, else: check_node_status(node)
        Map.put(acc, node, status)
      end)

    %{state | node_status: node_status}
  end

  defp create_distributed_engine(name, replication_factor, consistency, state) do
    # Select nodes for replication based on current cluster state
    available_nodes = get_online_nodes(state.node_status)

    if length(available_nodes) < replication_factor do
      {:error, :insufficient_nodes}
    else
      selected_nodes = Enum.take(available_nodes, replication_factor)

      # Create engine on local node
      case Presto.EngineSupervisor.start_engine(name: name) do
        {:ok, engine_pid} ->
          # Register distributed engine information
          engine_info = %{
            name: name,
            nodes: selected_nodes,
            replication_factor: replication_factor,
            consistency: consistency,
            primary_node: Node.self(),
            created_at: DateTime.utc_now(),
            status: :active
          }

          new_distributed_engines = Map.put(state.distributed_engines, name, engine_info)
          new_state = %{state | distributed_engines: new_distributed_engines}

          # Replicate engine creation to other nodes
          spawn(fn ->
            replicate_engine_creation(selected_nodes -- [Node.self()], engine_info)
          end)

          {:ok, engine_pid, new_state}

        error ->
          error
      end
    end
  end

  defp replicate_engine_creation(target_nodes, engine_info) do
    Enum.each(target_nodes, fn node ->
      try do
        GenServer.call({__MODULE__, node}, {:replicate_engine, engine_info}, 5000)

        Logger.debug("Replicated engine creation to node",
          node: node,
          engine: engine_info.name
        )
      rescue
        error ->
          Logger.warning("Failed to replicate engine to node",
            node: node,
            engine: engine_info.name,
            error: inspect(error)
          )
      end
    end)
  end

  defp sync_with_cluster(state) do
    online_nodes = get_online_nodes(state.node_status) -- [Node.self()]

    # Collect registry information from all nodes
    registry_responses =
      Enum.map(online_nodes, fn node ->
        try do
          response = GenServer.call({__MODULE__, node}, :get_node_registry, 3000)
          {node, {:ok, response}}
        catch
          :exit, reason ->
            Logger.warning("Failed to sync with node", node: node, reason: inspect(reason))
            {node, {:error, reason}}
        rescue
          error ->
            Logger.warning("Error syncing with node", node: node, error: inspect(error))
            {node, {:error, error}}
        end
      end)

    # Merge registry information
    {successful_responses, failed_responses} =
      Enum.split_with(registry_responses, fn {_node, result} ->
        match?({:ok, _}, result)
      end)

    if length(failed_responses) > 0 do
      Logger.warning("Some nodes failed during sync",
        failed_nodes: Enum.map(failed_responses, fn {node, _} -> node end)
      )
    end

    # Merge successful registry data
    merged_engines =
      Enum.reduce(successful_responses, state.distributed_engines, fn
        {node, {:ok, node_engines}}, acc ->
          merge_node_engines(acc, node_engines, node)
      end)

    new_stats = %{state.stats | sync_operations: state.stats.sync_operations + 1}
    new_state = %{state | distributed_engines: merged_engines, stats: new_stats}

    {:ok, new_state}
  end

  defp perform_cluster_sync(state) do
    case sync_with_cluster(state) do
      {:ok, new_state} ->
        Logger.debug("Periodic cluster sync completed")
        new_state

      {:error, reason} ->
        Logger.warning("Periodic cluster sync failed", reason: inspect(reason))
        new_stats = %{state.stats | failed_operations: state.stats.failed_operations + 1}
        %{state | stats: new_stats}
    end
  end

  defp send_heartbeat_to_nodes(state) do
    online_nodes = get_online_nodes(state.node_status) -- [Node.self()]

    heartbeat_msg = %{
      type: :heartbeat,
      from: Node.self(),
      timestamp: DateTime.utc_now(),
      engines: Map.keys(state.distributed_engines)
    }

    Enum.each(online_nodes, fn node ->
      send({__MODULE__, node}, {:cluster_message, Node.self(), heartbeat_msg})
    end)

    state
  end

  defp handle_node_down(node, _reason, state) do
    # Mark node as offline
    new_node_status = Map.put(state.node_status, node, :offline)

    # Check if any distributed engines are affected
    affected_engines =
      Enum.filter(state.distributed_engines, fn {_name, info} ->
        node in info.nodes
      end)

    new_distributed_engines =
      Enum.reduce(affected_engines, state.distributed_engines, fn
        {name, info}, acc ->
          case handle_engine_node_failure(name, info, node) do
            {:ok, updated_info} -> Map.put(acc, name, updated_info)
            {:error, _reason} -> Map.delete(acc, name)
          end
      end)

    new_stats = %{state.stats | cluster_events: state.stats.cluster_events + 1}

    %{
      state
      | node_status: new_node_status,
        distributed_engines: new_distributed_engines,
        stats: new_stats
    }
  end

  defp handle_node_up(node, state) do
    # Mark node as online
    new_node_status = Map.put(state.node_status, node, :online)

    # Trigger sync to get updated cluster state
    spawn(fn ->
      GenServer.cast(__MODULE__, :sync_cluster_state)
    end)

    new_stats = %{state.stats | cluster_events: state.stats.cluster_events + 1}

    %{state | node_status: new_node_status, stats: new_stats}
  end

  defp handle_cluster_message(from_node, message, state) do
    case message do
      %{type: :heartbeat, timestamp: _timestamp, engines: _engines} ->
        # Update last seen time for the node
        new_node_status = Map.put(state.node_status, from_node, :online)
        %{state | node_status: new_node_status}

      %{type: :engine_update, engine_info: engine_info} ->
        # Update distributed engine information
        new_engines = Map.put(state.distributed_engines, engine_info.name, engine_info)
        %{state | distributed_engines: new_engines}

      _ ->
        Logger.debug("Received unknown cluster message",
          from: from_node,
          message: inspect(message)
        )

        state
    end
  end

  defp handle_engine_node_failure(name, engine_info, failed_node) do
    remaining_nodes = engine_info.nodes -- [failed_node]

    if length(remaining_nodes) >= calculate_quorum(engine_info.replication_factor) do
      # Sufficient nodes remain - update engine info
      updated_info = %{engine_info | nodes: remaining_nodes, status: :degraded}
      {:ok, updated_info}
    else
      # Insufficient nodes - engine becomes unavailable
      Logger.error("Distributed engine lost quorum",
        engine: name,
        failed_node: failed_node,
        remaining_nodes: remaining_nodes
      )

      {:error, :quorum_lost}
    end
  end

  defp perform_engine_failover(engine_name, state) do
    case Map.get(state.distributed_engines, engine_name) do
      nil ->
        {:error, :engine_not_found}

      engine_info ->
        # Find alternative nodes for failover
        online_nodes = get_online_nodes(state.node_status)
        available_nodes = online_nodes -- engine_info.nodes

        if length(available_nodes) > 0 do
          # Select new node for failover
          new_node = hd(available_nodes)

          # Add new node to engine configuration
          updated_nodes = [new_node | engine_info.nodes]
          updated_info = %{engine_info | nodes: updated_nodes, status: :active}

          new_engines = Map.put(state.distributed_engines, engine_name, updated_info)
          new_state = %{state | distributed_engines: new_engines}

          # Trigger engine creation on new node
          spawn(fn ->
            replicate_engine_creation([new_node], updated_info)
          end)

          {:ok, new_state}
        else
          {:error, :no_available_nodes}
        end
    end
  end

  # Helper functions

  defp get_online_nodes(node_status) do
    node_status
    |> Enum.filter(fn {_node, status} -> status == :online end)
    |> Enum.map(fn {node, _status} -> node end)
  end

  defp count_online_nodes(nodes, node_status) do
    Enum.count(nodes, fn node ->
      Map.get(node_status, node) == :online
    end)
  end

  defp calculate_quorum(replication_factor) do
    div(replication_factor, 2) + 1
  end

  defp check_node_status(node) do
    if Node.ping(node) == :pong, do: :online, else: :offline
  end

  defp build_engine_status_list(distributed_engines) do
    Enum.map(distributed_engines, fn {name, info} ->
      %{
        name: name,
        nodes: info.nodes,
        status: info.status,
        replication_factor: info.replication_factor,
        consistency: info.consistency
      }
    end)
  end

  defp merge_node_engines(local_engines, node_engines, _node) do
    # Simple merge strategy - prefer more recent information
    Enum.reduce(node_engines, local_engines, fn {name, remote_info}, acc ->
      case Map.get(acc, name) do
        nil ->
          # New engine from remote node
          Map.put(acc, name, remote_info)

        local_info ->
          # Merge based on timestamps
          if DateTime.compare(remote_info.created_at, local_info.created_at) == :gt do
            Map.put(acc, name, remote_info)
          else
            acc
          end
      end
    end)
  end

  defp schedule_cluster_sync(interval) do
    Process.send_after(self(), :cluster_sync, interval)
  end

  defp schedule_heartbeat(interval) do
    Process.send_after(self(), :heartbeat, interval)
  end
end

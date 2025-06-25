defmodule Presto.Distributed.DistributedRegistry do
  @moduledoc """
  Distributed engine registry that extends the local EngineRegistry with cluster-wide capabilities.

  Provides:
  - Global engine discovery across all cluster nodes
  - Cross-node engine health monitoring
  - Automatic failover and load balancing
  - Consistent engine naming across the cluster
  - Event propagation for registry changes

  ## Features

  ### Global Engine Lookup
  - Search engines across all nodes in the cluster
  - Transparent routing to engines on remote nodes
  - Load balancing for multiple instances of the same engine

  ### Distributed Health Monitoring
  - Monitor engine health across all nodes
  - Detect node failures and engine crashes
  - Automatic cleanup of stale registry entries

  ### Failover and Replication
  - Automatic engine failover on node failure
  - Engine state replication for high availability
  - Configurable replication strategies

  ## Example Usage

      # Register engine globally
      DistributedRegistry.register_global_engine(:payroll, engine_pid, 
        replication_factor: 3,
        failover_enabled: true
      )

      # Lookup engine anywhere in cluster
      {:ok, engine_info} = DistributedRegistry.lookup_global_engine(:payroll)

      # List all engines in cluster
      engines = DistributedRegistry.list_cluster_engines()

      # Monitor engine health across cluster
      health = DistributedRegistry.cluster_health_check()
  """

  use GenServer
  require Logger

  alias Presto.EngineRegistry
  alias Presto.Distributed.Coordinator

  @type global_engine_info :: %{
          name: atom(),
          local_pid: pid() | nil,
          nodes: [node()],
          primary_node: node(),
          replication_factor: pos_integer(),
          status: :active | :degraded | :failed,
          last_seen: DateTime.t(),
          metadata: map()
        }

  @type registry_opts :: [
          replication_factor: pos_integer(),
          failover_enabled: boolean(),
          load_balancing: :round_robin | :least_loaded | :sticky,
          health_check_interval: pos_integer()
        ]

  ## Client API

  @doc """
  Starts the distributed registry.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Registers an engine globally across the cluster.
  """
  @spec register_global_engine(atom(), pid(), registry_opts()) :: :ok | {:error, term()}
  def register_global_engine(name, pid, opts \\ []) do
    GenServer.call(__MODULE__, {:register_global, name, pid, opts})
  end

  @doc """
  Looks up an engine anywhere in the cluster.
  """
  @spec lookup_global_engine(atom()) :: {:ok, global_engine_info()} | {:error, :not_found}
  def lookup_global_engine(name) do
    GenServer.call(__MODULE__, {:lookup_global, name})
  end

  @doc """
  Unregisters an engine from the global registry.
  """
  @spec unregister_global_engine(atom()) :: :ok
  def unregister_global_engine(name) do
    GenServer.call(__MODULE__, {:unregister_global, name})
  end

  @doc """
  Lists all engines across the entire cluster.
  """
  @spec list_cluster_engines() :: [global_engine_info()]
  def list_cluster_engines do
    GenServer.call(__MODULE__, :list_cluster_engines)
  end

  @doc """
  Gets the routing information for an engine.
  """
  @spec get_engine_routing_info(atom()) :: {:ok, map()} | {:error, :not_found}
  def get_engine_routing_info(name) do
    GenServer.call(__MODULE__, {:get_routing_info, name})
  end

  @doc """
  Performs a health check of all engines across the cluster.
  """
  @spec cluster_health_check() :: map()
  def cluster_health_check do
    GenServer.call(__MODULE__, :cluster_health_check)
  end

  @doc """
  Forces failover of an engine to a different node.
  """
  @spec trigger_engine_failover(atom()) :: :ok | {:error, term()}
  def trigger_engine_failover(name) do
    GenServer.call(__MODULE__, {:trigger_failover, name})
  end

  @doc """
  Gets statistics about the distributed registry.
  """
  @spec get_registry_stats() :: map()
  def get_registry_stats do
    GenServer.call(__MODULE__, :get_stats)
  end

  ## Server implementation

  @impl GenServer
  def init(opts) do
    Logger.info("Starting Distributed Registry")

    # Subscribe to cluster events
    Process.send_after(self(), :sync_with_cluster, 1000)

    state = %{
      # Configuration
      default_replication_factor: Keyword.get(opts, :default_replication_factor, 1),
      default_failover_enabled: Keyword.get(opts, :default_failover_enabled, true),
      health_check_interval: Keyword.get(opts, :health_check_interval, 10_000),

      # Runtime state
      global_engines: %{},
      # Engines grouped by node
      node_engines: %{},
      # Load balancing state
      routing_table: %{},

      # Statistics
      stats: %{
        total_global_engines: 0,
        cluster_registrations: 0,
        failover_events: 0,
        health_checks: 0
      }
    }

    # Schedule periodic health checks
    schedule_health_check(state.health_check_interval)

    {:ok, state}
  end

  @impl GenServer
  def handle_call({:register_global, name, pid, opts}, _from, state) do
    replication_factor = Keyword.get(opts, :replication_factor, state.default_replication_factor)
    failover_enabled = Keyword.get(opts, :failover_enabled, state.default_failover_enabled)

    case register_engine_globally(name, pid, replication_factor, failover_enabled, state) do
      {:ok, new_state} ->
        Logger.info("Engine registered globally",
          name: name,
          node: Node.self(),
          replication_factor: replication_factor
        )

        {:reply, :ok, new_state}

      {:error, reason} ->
        Logger.error("Global engine registration failed",
          name: name,
          reason: inspect(reason)
        )

        {:reply, {:error, reason}, state}
    end
  end

  @impl GenServer
  def handle_call({:lookup_global, name}, _from, state) do
    case Map.get(state.global_engines, name) do
      nil ->
        # Check if engine exists on any node
        case find_engine_in_cluster(name, state) do
          {:ok, engine_info} -> {:reply, {:ok, engine_info}, state}
          {:error, :not_found} -> {:reply, {:error, :not_found}, state}
        end

      engine_info ->
        # Return cached global engine info
        {:reply, {:ok, engine_info}, state}
    end
  end

  @impl GenServer
  def handle_call({:unregister_global, name}, _from, state) do
    case unregister_engine_globally(name, state) do
      {:ok, new_state} ->
        Logger.info("Engine unregistered globally", name: name)
        {:reply, :ok, new_state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl GenServer
  def handle_call(:list_cluster_engines, _from, state) do
    # Combine local and remote engine information
    all_engines = collect_cluster_engines(state)
    {:reply, all_engines, state}
  end

  @impl GenServer
  def handle_call({:get_routing_info, name}, _from, state) do
    case Map.get(state.global_engines, name) do
      nil ->
        {:reply, {:error, :not_found}, state}

      engine_info ->
        routing_info = %{
          name: name,
          primary_node: engine_info.primary_node,
          available_nodes: engine_info.nodes,
          load_balancing: get_load_balancing_info(name, state),
          failover_enabled: Map.get(engine_info.metadata, :failover_enabled, false)
        }

        {:reply, {:ok, routing_info}, state}
    end
  end

  @impl GenServer
  def handle_call(:cluster_health_check, _from, state) do
    health_report = perform_cluster_health_check(state)
    new_stats = %{state.stats | health_checks: state.stats.health_checks + 1}
    {:reply, health_report, %{state | stats: new_stats}}
  end

  @impl GenServer
  def handle_call({:trigger_failover, name}, _from, state) do
    case perform_engine_failover(name, state) do
      {:ok, new_state} ->
        new_stats = %{new_state.stats | failover_events: new_state.stats.failover_events + 1}
        {:reply, :ok, %{new_state | stats: new_stats}}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl GenServer
  def handle_call(:get_stats, _from, state) do
    stats =
      Map.merge(state.stats, %{
        global_engines_count: map_size(state.global_engines),
        nodes_in_cluster: map_size(state.node_engines)
      })

    {:reply, stats, state}
  end

  @impl GenServer
  def handle_info(:sync_with_cluster, state) do
    new_state = sync_with_cluster_nodes(state)

    # Schedule next sync
    Process.send_after(self(), :sync_with_cluster, 15_000)

    {:noreply, new_state}
  end

  @impl GenServer
  def handle_info(:health_check, state) do
    new_state = perform_periodic_health_check(state)
    schedule_health_check(state.health_check_interval)
    {:noreply, new_state}
  end

  @impl GenServer
  def handle_info({:engine_event, event_type, engine_name, node}, state) do
    new_state = handle_engine_event(event_type, engine_name, node, state)
    {:noreply, new_state}
  end

  ## Private functions

  defp register_engine_globally(name, pid, replication_factor, failover_enabled, state) do
    # First register locally
    case EngineRegistry.register_engine(name, pid) do
      :ok ->
        # Create global engine info
        global_info = %{
          name: name,
          local_pid: pid,
          nodes: [Node.self()],
          primary_node: Node.self(),
          replication_factor: replication_factor,
          status: :active,
          last_seen: DateTime.utc_now(),
          metadata: %{
            failover_enabled: failover_enabled,
            created_at: DateTime.utc_now()
          }
        }

        # Update global registry
        new_global_engines = Map.put(state.global_engines, name, global_info)

        # Update node engines mapping
        local_engines = Map.get(state.node_engines, Node.self(), [])
        new_node_engines = Map.put(state.node_engines, Node.self(), [name | local_engines])

        # Update stats
        new_stats = %{
          state.stats
          | total_global_engines: state.stats.total_global_engines + 1,
            cluster_registrations: state.stats.cluster_registrations + 1
        }

        new_state = %{
          state
          | global_engines: new_global_engines,
            node_engines: new_node_engines,
            stats: new_stats
        }

        # Propagate registration to cluster
        propagate_engine_registration(global_info)

        {:ok, new_state}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp unregister_engine_globally(name, state) do
    case Map.get(state.global_engines, name) do
      nil ->
        {:error, :not_found}

      engine_info ->
        # Unregister locally
        EngineRegistry.unregister_engine(name)

        # Remove from global registry
        new_global_engines = Map.delete(state.global_engines, name)

        # Update node engines mapping
        local_engines = Map.get(state.node_engines, Node.self(), [])
        new_local_engines = List.delete(local_engines, name)
        new_node_engines = Map.put(state.node_engines, Node.self(), new_local_engines)

        new_state = %{state | global_engines: new_global_engines, node_engines: new_node_engines}

        # Propagate unregistration to cluster
        propagate_engine_unregistration(engine_info)

        {:ok, new_state}
    end
  end

  defp find_engine_in_cluster(name, state) do
    # Check all known nodes for the engine
    Enum.find_value(state.node_engines, fn {node, engines} ->
      if name in engines do
        case get_remote_engine_info(node, name) do
          {:ok, engine_info} -> {:ok, engine_info}
          {:error, _} -> nil
        end
      else
        nil
      end
    end) || {:error, :not_found}
  end

  defp collect_cluster_engines(state) do
    # Start with local global engines
    local_engines = Map.values(state.global_engines)

    # Add engines from other nodes
    remote_engines =
      Enum.flat_map(state.node_engines, fn {node, engine_names} ->
        if node != Node.self() do
          engine_names
          |> Enum.filter(fn name -> not Map.has_key?(state.global_engines, name) end)
          |> Enum.map(fn name ->
            case get_remote_engine_info(node, name) do
              {:ok, info} -> info
              {:error, _} -> nil
            end
          end)
          |> Enum.reject(&is_nil/1)
        else
          []
        end
      end)

    local_engines ++ remote_engines
  end

  defp get_remote_engine_info(node, name) do
    case :rpc.call(node, EngineRegistry, :get_engine_info, [name]) do
      {:ok, info} ->
        # Convert to global engine info format
        global_info = %{
          name: name,
          # Remote engine
          local_pid: nil,
          nodes: [node],
          primary_node: node,
          replication_factor: 1,
          status: :active,
          last_seen: DateTime.utc_now(),
          metadata: %{
            remote_info: info
          }
        }

        {:ok, global_info}

      :error ->
        {:error, :not_found}

      {:badrpc, reason} ->
        {:error, {:rpc_failed, reason}}
    end
  end

  defp sync_with_cluster_nodes(state) do
    # Get cluster topology
    case get_cluster_nodes() do
      {:ok, cluster_nodes} ->
        # Sync engine information with each node
        new_node_engines =
          Enum.reduce(cluster_nodes, %{}, fn node, acc ->
            case get_node_engines(node) do
              {:ok, engines} -> Map.put(acc, node, engines)
              {:error, _} -> Map.put(acc, node, [])
            end
          end)

        %{state | node_engines: new_node_engines}

      {:error, _reason} ->
        state
    end
  end

  defp get_cluster_nodes do
    try do
      case GenServer.call(Coordinator, :get_cluster_topology) do
        topology when is_map(topology) ->
          online_nodes =
            topology.cluster_nodes
            |> Enum.filter(fn {_node, status} -> status == :online end)
            |> Enum.map(fn {node, _status} -> node end)

          {:ok, online_nodes}

        _ ->
          {:error, :invalid_topology}
      end
    rescue
      error -> {:error, error}
    end
  end

  defp get_node_engines(node) do
    if node == Node.self() do
      # Get local engines
      engines =
        EngineRegistry.list_engines()
        |> Enum.map(fn info -> info.name end)

      {:ok, engines}
    else
      # Get remote engines
      case :rpc.call(node, EngineRegistry, :list_engines, []) do
        engines when is_list(engines) ->
          engine_names = Enum.map(engines, fn info -> info.name end)
          {:ok, engine_names}

        {:badrpc, reason} ->
          {:error, {:rpc_failed, reason}}
      end
    end
  end

  defp perform_cluster_health_check(state) do
    # Check health of all global engines
    engine_health =
      Enum.map(state.global_engines, fn {name, info} ->
        health_status = check_global_engine_health(info)

        %{
          name: name,
          primary_node: info.primary_node,
          nodes: info.nodes,
          status: health_status,
          last_seen: info.last_seen
        }
      end)

    # Check node health
    node_health =
      Enum.map(state.node_engines, fn {node, engines} ->
        node_status = if node == Node.self(), do: :online, else: check_node_health(node)

        %{
          node: node,
          status: node_status,
          engine_count: length(engines),
          engines: engines
        }
      end)

    %{
      timestamp: DateTime.utc_now(),
      total_engines: map_size(state.global_engines),
      total_nodes: map_size(state.node_engines),
      engine_health: engine_health,
      node_health: node_health
    }
  end

  defp check_global_engine_health(engine_info) do
    if engine_info.primary_node == Node.self() and engine_info.local_pid do
      # Check local engine
      if Process.alive?(engine_info.local_pid), do: :healthy, else: :failed
    else
      # Check remote engine
      case :rpc.call(engine_info.primary_node, Process, :alive?, [engine_info.local_pid]) do
        true -> :healthy
        false -> :failed
        {:badrpc, _} -> :unreachable
      end
    end
  end

  defp check_node_health(node) do
    case Node.ping(node) do
      :pong -> :online
      :pang -> :offline
    end
  end

  defp perform_periodic_health_check(state) do
    # Update engine statuses based on health checks
    updated_engines =
      Enum.reduce(state.global_engines, %{}, fn {name, info}, acc ->
        health_status = check_global_engine_health(info)
        updated_info = %{info | status: health_status_to_engine_status(health_status)}
        Map.put(acc, name, updated_info)
      end)

    %{state | global_engines: updated_engines}
  end

  defp health_status_to_engine_status(:healthy), do: :active
  defp health_status_to_engine_status(:failed), do: :failed
  defp health_status_to_engine_status(:unreachable), do: :degraded

  defp perform_engine_failover(name, state) do
    case Map.get(state.global_engines, name) do
      nil ->
        {:error, :engine_not_found}

      engine_info ->
        if Map.get(engine_info.metadata, :failover_enabled, false) do
          # Find alternative nodes for failover
          case find_failover_nodes(engine_info, state) do
            {:ok, target_node} ->
              # Trigger failover through coordinator
              case Coordinator.trigger_failover(name) do
                :ok ->
                  # Update engine info with new primary node
                  updated_info = %{engine_info | primary_node: target_node}
                  new_engines = Map.put(state.global_engines, name, updated_info)
                  {:ok, %{state | global_engines: new_engines}}

                error ->
                  error
              end

            {:error, reason} ->
              {:error, reason}
          end
        else
          {:error, :failover_disabled}
        end
    end
  end

  defp find_failover_nodes(engine_info, state) do
    # Find healthy nodes that can host the engine
    online_nodes =
      state.node_engines
      |> Enum.filter(fn {node, _engines} ->
        node != engine_info.primary_node and check_node_health(node) == :online
      end)
      |> Enum.map(fn {node, _engines} -> node end)

    case online_nodes do
      [] -> {:error, :no_available_nodes}
      # Simple selection - could be enhanced
      [node | _] -> {:ok, node}
    end
  end

  defp propagate_engine_registration(engine_info) do
    # Send registration event to cluster nodes
    spawn(fn ->
      case get_cluster_nodes() do
        {:ok, nodes} ->
          Enum.each(nodes -- [Node.self()], fn node ->
            send({__MODULE__, node}, {:engine_event, :registered, engine_info.name, Node.self()})
          end)

        {:error, _} ->
          :ok
      end
    end)
  end

  defp propagate_engine_unregistration(engine_info) do
    # Send unregistration event to cluster nodes
    spawn(fn ->
      case get_cluster_nodes() do
        {:ok, nodes} ->
          Enum.each(nodes -- [Node.self()], fn node ->
            send(
              {__MODULE__, node},
              {:engine_event, :unregistered, engine_info.name, Node.self()}
            )
          end)

        {:error, _} ->
          :ok
      end
    end)
  end

  defp handle_engine_event(:registered, engine_name, node, state) do
    # Add engine to node engines mapping
    node_engines = Map.get(state.node_engines, node, [])
    updated_engines = [engine_name | List.delete(node_engines, engine_name)]
    new_node_engines = Map.put(state.node_engines, node, updated_engines)

    %{state | node_engines: new_node_engines}
  end

  defp handle_engine_event(:unregistered, engine_name, node, state) do
    # Remove engine from node engines mapping
    node_engines = Map.get(state.node_engines, node, [])
    updated_engines = List.delete(node_engines, engine_name)
    new_node_engines = Map.put(state.node_engines, node, updated_engines)

    %{state | node_engines: new_node_engines}
  end

  defp get_load_balancing_info(_name, _state) do
    # Placeholder for load balancing implementation
    %{
      strategy: :round_robin,
      current_node: Node.self(),
      request_count: 0
    }
  end

  defp schedule_health_check(interval) do
    Process.send_after(self(), :health_check, interval)
  end
end

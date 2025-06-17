defmodule Presto.Distributed.HealthMonitor do
  @moduledoc """
  Health monitoring and failure detection for distributed RETE nodes.

  Implements adaptive failure detection with configurable timeouts,
  gossip-based health propagation, and split-brain detection.
  Provides early warning for node degradation and network partitions.
  """

  use GenServer
  require Logger

  alias Presto.Logger, as: PrestoLogger

  @type node_id :: String.t()
  @type health_status :: :healthy | :suspect | :failed | :recovering
  @type health_metric :: %{
          cpu_usage: float(),
          memory_usage: float(),
          network_latency: non_neg_integer(),
          active_connections: non_neg_integer(),
          error_rate: float(),
          last_heartbeat: integer()
        }

  @type node_health :: %{
          node_id: node_id(),
          status: health_status(),
          metrics: health_metric(),
          failure_count: non_neg_integer(),
          last_status_change: integer(),
          detection_history: [detection_event()],
          gossip_version: non_neg_integer()
        }

  @type detection_event :: %{
          event_type: :heartbeat_missed | :high_latency | :error_spike | :recovery,
          timestamp: integer(),
          details: map()
        }

  @type monitor_state :: %{
          local_node_id: node_id(),
          cluster_manager: pid(),
          monitored_nodes: %{node_id() => node_health()},
          pid_to_node: %{pid() => node_id()},
          config: monitor_config(),
          gossip_timer: reference() | nil,
          health_check_timer: reference() | nil
        }

  @type monitor_config :: %{
          heartbeat_interval: pos_integer(),
          failure_timeout: pos_integer(),
          suspect_timeout: pos_integer(),
          recovery_timeout: pos_integer(),
          max_missed_heartbeats: pos_integer(),
          gossip_interval: pos_integer(),
          health_check_interval: pos_integer(),
          network_partition_threshold: pos_integer()
        }

  @default_config %{
    heartbeat_interval: 5_000,
    failure_timeout: 15_000,
    suspect_timeout: 10_000,
    recovery_timeout: 30_000,
    max_missed_heartbeats: 3,
    gossip_interval: 2_000,
    health_check_interval: 10_000,
    network_partition_threshold: 5
  }

  # Client API

  @spec start_link(pid(), keyword()) :: GenServer.on_start()
  def start_link(cluster_manager, opts \\ []) do
    GenServer.start_link(__MODULE__, {cluster_manager, opts}, name: __MODULE__)
  end

  @spec monitor_node(GenServer.server(), node_id(), pid() | nil) :: :ok
  def monitor_node(pid, node_id, node_pid \\ nil) do
    GenServer.call(pid, {:monitor_node, node_id, node_pid})
  end

  @spec unmonitor_node(GenServer.server(), node_id()) :: :ok
  def unmonitor_node(pid, node_id) do
    GenServer.call(pid, {:unmonitor_node, node_id})
  end

  @spec get_node_health(GenServer.server(), node_id()) :: node_health() | nil
  def get_node_health(pid, node_id) do
    GenServer.call(pid, {:get_node_health, node_id})
  end

  @spec get_all_health_status(GenServer.server()) :: %{node_id() => health_status()}
  def get_all_health_status(pid) do
    GenServer.call(pid, :get_all_health_status)
  end

  @spec report_health_metrics(GenServer.server(), node_id(), health_metric()) :: :ok
  def report_health_metrics(pid, node_id, metrics) do
    GenServer.cast(pid, {:report_health_metrics, node_id, metrics})
  end

  @spec force_health_check(GenServer.server()) :: :ok
  def force_health_check(pid) do
    GenServer.call(pid, :force_health_check)
  end

  # Server implementation

  @impl true
  def init({cluster_manager, opts}) do
    config = Map.merge(@default_config, Map.new(opts))
    local_node_id = Keyword.get(opts, :local_node_id, generate_node_id())

    state = %{
      local_node_id: local_node_id,
      cluster_manager: cluster_manager,
      monitored_nodes: %{},
      pid_to_node: %{},
      config: config,
      gossip_timer: nil,
      health_check_timer: nil
    }

    # Start periodic health checks and gossip
    gossip_timer = schedule_gossip(config.gossip_interval)
    health_check_timer = schedule_health_check(config.health_check_interval)

    new_state = %{state | gossip_timer: gossip_timer, health_check_timer: health_check_timer}

    PrestoLogger.log_distributed(:info, local_node_id, "health_monitor_started", %{
      heartbeat_interval: config.heartbeat_interval,
      failure_timeout: config.failure_timeout
    })

    {:ok, new_state}
  end

  @impl true
  def handle_call({:monitor_node, node_id}, from, state) do
    handle_call({:monitor_node, node_id, nil}, from, state)
  end

  @impl true
  def handle_call({:monitor_node, node_id, node_pid}, _from, state) do
    case Map.get(state.monitored_nodes, node_id) do
      nil ->
        # Add new node to monitoring
        initial_health = create_initial_node_health(node_id)
        updated_nodes = Map.put(state.monitored_nodes, node_id, initial_health)

        # Set up process monitoring if PID is provided
        updated_pid_mapping =
          if node_pid do
            Process.monitor(node_pid)
            Map.put(state.pid_to_node, node_pid, node_id)
          else
            state.pid_to_node
          end

        new_state = %{state | monitored_nodes: updated_nodes, pid_to_node: updated_pid_mapping}

        PrestoLogger.log_distributed(:info, state.local_node_id, "monitoring_node", %{
          target_node_id: node_id,
          has_pid: not is_nil(node_pid)
        })

        {:reply, :ok, new_state}

      _existing ->
        # Already monitoring
        {:reply, :ok, state}
    end
  end

  @impl true
  def handle_call({:unmonitor_node, node_id}, _from, state) do
    updated_nodes = Map.delete(state.monitored_nodes, node_id)

    # Clean up PID mappings for this node
    updated_pid_mapping =
      state.pid_to_node
      |> Enum.reject(fn {_pid, mapped_node_id} -> mapped_node_id == node_id end)
      |> Map.new()

    new_state = %{state | monitored_nodes: updated_nodes, pid_to_node: updated_pid_mapping}

    PrestoLogger.log_distributed(:info, state.local_node_id, "stopped_monitoring_node", %{
      target_node_id: node_id
    })

    {:reply, :ok, new_state}
  end

  @impl true
  def handle_call({:get_node_health, node_id}, _from, state) do
    health_info = Map.get(state.monitored_nodes, node_id)
    {:reply, health_info, state}
  end

  @impl true
  def handle_call(:get_all_health_status, _from, state) do
    health_status =
      state.monitored_nodes
      |> Enum.into(%{}, fn {node_id, health} -> {node_id, health.status} end)

    {:reply, health_status, state}
  end

  @impl true
  def handle_call(:force_health_check, _from, state) do
    new_state = perform_health_checks(state)
    {:reply, :ok, new_state}
  end

  @impl true
  def handle_cast({:report_health_metrics, node_id, metrics}, state) do
    case Map.get(state.monitored_nodes, node_id) do
      nil ->
        # Not monitoring this node
        {:noreply, state}

      existing_health ->
        # Update health metrics and analyze
        updated_health = update_node_health_with_metrics(existing_health, metrics, state.config)
        updated_nodes = Map.put(state.monitored_nodes, node_id, updated_health)
        new_state = %{state | monitored_nodes: updated_nodes}

        # Check if status changed and notify if needed
        if updated_health.status != existing_health.status do
          notify_status_change(node_id, existing_health.status, updated_health.status, state)
        end

        {:noreply, new_state}
    end
  end

  @impl true
  def handle_cast({:gossip_health_update, gossip_data}, state) do
    new_state = process_gossip_health_update(gossip_data, state)
    {:noreply, new_state}
  end

  @impl true
  def handle_info(:perform_gossip, state) do
    # Send gossip messages to other nodes
    perform_gossip_round(state)

    # Schedule next gossip round
    gossip_timer = schedule_gossip(state.config.gossip_interval)
    new_state = %{state | gossip_timer: gossip_timer}

    {:noreply, new_state}
  end

  @impl true
  def handle_info(:perform_health_check, state) do
    # Perform active health checks
    new_state = perform_health_checks(state)

    # Schedule next health check
    health_check_timer = schedule_health_check(state.config.health_check_interval)
    updated_state = %{new_state | health_check_timer: health_check_timer}

    {:noreply, updated_state}
  end

  @impl true
  def handle_info({:heartbeat, node_id, heartbeat_data}, state) do
    new_state = process_heartbeat(node_id, heartbeat_data, state)
    {:noreply, new_state}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, reason}, state) do
    # Handle monitored process death
    case find_node_by_pid(pid, state) do
      nil ->
        {:noreply, state}

      node_id ->
        new_state = handle_node_process_death(node_id, reason, state)
        {:noreply, new_state}
    end
  end

  # Private implementation functions

  defp generate_node_id do
    "monitor_" <> (:crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower))
  end

  defp create_initial_node_health(node_id) do
    now = System.monotonic_time(:millisecond)

    %{
      node_id: node_id,
      status: :healthy,
      metrics: %{
        cpu_usage: 0.0,
        memory_usage: 0.0,
        network_latency: 0,
        active_connections: 0,
        error_rate: 0.0,
        last_heartbeat: now
      },
      failure_count: 0,
      last_status_change: now,
      detection_history: [],
      gossip_version: 0
    }
  end

  defp update_node_health_with_metrics(health, metrics, config) do
    now = System.monotonic_time(:millisecond)
    updated_metrics = Map.merge(health.metrics, metrics)

    # Analyze metrics to determine new status
    new_status = determine_health_status_from_metrics(updated_metrics, health, config)

    # Update detection history if status is concerning
    detection_history =
      if new_status in [:suspect, :failed] do
        event = create_detection_event(new_status, metrics, now)
        # Keep last 10 events
        [event | health.detection_history] |> Enum.take(10)
      else
        health.detection_history
      end

    # Update failure count
    failure_count =
      if new_status == :failed do
        health.failure_count + 1
      else
        health.failure_count
      end

    # Update status change timestamp if changed
    last_status_change =
      if new_status != health.status do
        now
      else
        health.last_status_change
      end

    %{
      health
      | metrics: updated_metrics,
        status: new_status,
        failure_count: failure_count,
        last_status_change: last_status_change,
        detection_history: detection_history,
        gossip_version: health.gossip_version + 1
    }
  end

  defp determine_health_status_from_metrics(metrics, health, config) do
    now = System.monotonic_time(:millisecond)
    time_since_heartbeat = now - metrics.last_heartbeat

    cond do
      failed?(time_since_heartbeat, config) -> :failed
      suspect?(time_since_heartbeat, metrics, config) -> :suspect
      recovering?(health, time_since_heartbeat, metrics, config) -> :recovering
      fully_recovered?(health, now, config) -> :healthy
      true -> health.status
    end
  end

  defp failed?(time_since_heartbeat, config) do
    time_since_heartbeat > config.failure_timeout
  end

  defp suspect?(time_since_heartbeat, metrics, config) do
    time_since_heartbeat > config.suspect_timeout or
      high_resource_usage?(metrics) or
      high_error_rate?(metrics)
  end

  defp recovering?(health, time_since_heartbeat, metrics, config) do
    health.status in [:failed, :suspect] and
      recent_heartbeat?(time_since_heartbeat, config) and
      acceptable_resource_usage?(metrics) and
      low_error_rate?(metrics)
  end

  defp fully_recovered?(health, now, config) do
    health.status == :recovering and
      now - health.last_status_change > config.recovery_timeout
  end

  defp high_resource_usage?(metrics) do
    metrics.cpu_usage > 0.9 or metrics.memory_usage > 0.9
  end

  defp high_error_rate?(metrics) do
    metrics.error_rate > 0.1
  end

  defp recent_heartbeat?(time_since_heartbeat, config) do
    time_since_heartbeat < config.heartbeat_interval * 2
  end

  defp acceptable_resource_usage?(metrics) do
    metrics.cpu_usage < 0.7 and metrics.memory_usage < 0.7
  end

  defp low_error_rate?(metrics) do
    metrics.error_rate < 0.05
  end

  defp create_detection_event(status, metrics, timestamp) do
    event_type =
      case status do
        :suspect -> determine_suspect_reason(metrics)
        :failed -> :heartbeat_missed
        _ -> :recovery
      end

    %{
      event_type: event_type,
      timestamp: timestamp,
      details: Map.take(metrics, [:cpu_usage, :memory_usage, :network_latency, :error_rate])
    }
  end

  defp determine_suspect_reason(metrics) do
    cond do
      metrics.network_latency > 1000 -> :high_latency
      metrics.error_rate > 0.1 -> :error_spike
      metrics.cpu_usage > 0.9 or metrics.memory_usage > 0.9 -> :resource_exhaustion
      true -> :heartbeat_missed
    end
  end

  defp process_heartbeat(node_id, heartbeat_data, state) do
    case Map.get(state.monitored_nodes, node_id) do
      nil ->
        # Not monitoring this node, ignore
        state

      existing_health ->
        # Update last heartbeat time and extract metrics
        now = System.monotonic_time(:millisecond)
        heartbeat_metrics = Map.get(heartbeat_data, :metrics, %{})

        updated_metrics =
          Map.merge(existing_health.metrics, %{
            last_heartbeat: now
          })
          |> Map.merge(heartbeat_metrics)

        # Determine new status based on heartbeat
        new_status =
          if existing_health.status == :failed do
            # Node is coming back online
            :recovering
          else
            :healthy
          end

        updated_health = %{
          existing_health
          | metrics: updated_metrics,
            status: new_status,
            gossip_version: existing_health.gossip_version + 1
        }

        updated_nodes = Map.put(state.monitored_nodes, node_id, updated_health)

        # Notify if status changed
        if new_status != existing_health.status do
          notify_status_change(node_id, existing_health.status, new_status, state)
        end

        %{state | monitored_nodes: updated_nodes}
    end
  end

  defp perform_health_checks(state) do
    now = System.monotonic_time(:millisecond)

    updated_nodes =
      state.monitored_nodes
      |> Enum.into(%{}, fn {node_id, health} ->
        # Check if heartbeat is overdue
        time_since_heartbeat = now - health.metrics.last_heartbeat

        new_status =
          cond do
            time_since_heartbeat > state.config.failure_timeout ->
              :failed

            time_since_heartbeat > state.config.suspect_timeout ->
              :suspect

            true ->
              health.status
          end

        # Update health if status changed
        updated_health =
          if new_status != health.status do
            event = %{
              event_type: :heartbeat_missed,
              timestamp: now,
              details: %{time_since_heartbeat: time_since_heartbeat}
            }

            updated_history = [event | health.detection_history] |> Enum.take(10)

            notify_status_change(node_id, health.status, new_status, state)

            %{
              health
              | status: new_status,
                last_status_change: now,
                detection_history: updated_history,
                gossip_version: health.gossip_version + 1
            }
          else
            health
          end

        {node_id, updated_health}
      end)

    %{state | monitored_nodes: updated_nodes}
  end

  defp perform_gossip_round(state) do
    # Create gossip payload with our view of node health
    gossip_payload = create_gossip_payload(state)

    # Send to other monitoring nodes (simplified - would use actual network communication)
    broadcast_gossip(gossip_payload, state)
  end

  defp create_gossip_payload(state) do
    %{
      sender_id: state.local_node_id,
      timestamp: System.monotonic_time(:millisecond),
      health_updates:
        state.monitored_nodes
        |> Enum.into(%{}, fn {node_id, health} ->
          {node_id,
           %{
             status: health.status,
             gossip_version: health.gossip_version,
             last_heartbeat: health.metrics.last_heartbeat
           }}
        end)
    }
  end

  defp broadcast_gossip(_payload, _state) do
    # Placeholder for actual gossip network implementation
    :ok
  end

  defp process_gossip_health_update(gossip_data, state) do
    # Process incoming gossip and merge with local view
    health_updates = gossip_data.health_updates

    updated_nodes =
      Enum.reduce(health_updates, state.monitored_nodes, fn {node_id, remote_health}, acc ->
        merge_gossip_health_for_node(node_id, remote_health, acc)
      end)

    %{state | monitored_nodes: updated_nodes}
  end

  defp merge_gossip_health_for_node(node_id, remote_health, monitored_nodes) do
    case Map.get(monitored_nodes, node_id) do
      nil ->
        # Not monitoring this node locally
        monitored_nodes

      local_health ->
        merge_health_versions(node_id, local_health, remote_health, monitored_nodes)
    end
  end

  defp merge_health_versions(node_id, local_health, remote_health, monitored_nodes) do
    # Merge based on gossip version (higher version wins)
    if remote_health.gossip_version > local_health.gossip_version do
      updated_health = %{
        local_health
        | status: remote_health.status,
          gossip_version: remote_health.gossip_version
      }

      Map.put(monitored_nodes, node_id, updated_health)
    else
      monitored_nodes
    end
  end

  defp notify_status_change(node_id, old_status, new_status, state) do
    # Notify cluster manager of status change
    GenServer.cast(state.cluster_manager, {:node_status_change, node_id, old_status, new_status})

    PrestoLogger.log_distributed(:info, state.local_node_id, "node_status_changed", %{
      target_node_id: node_id,
      old_status: old_status,
      new_status: new_status
    })
  end

  defp find_node_by_pid(pid, state) do
    Map.get(state.pid_to_node, pid)
  end

  defp handle_node_process_death(node_id, reason, state) do
    case Map.get(state.monitored_nodes, node_id) do
      nil ->
        state

      health ->
        # Mark node as failed due to process death
        now = System.monotonic_time(:millisecond)

        event = %{
          event_type: :process_death,
          timestamp: now,
          details: %{reason: reason}
        }

        updated_health = %{
          health
          | status: :failed,
            last_status_change: now,
            detection_history: [event | health.detection_history] |> Enum.take(10),
            failure_count: health.failure_count + 1,
            gossip_version: health.gossip_version + 1
        }

        updated_nodes = Map.put(state.monitored_nodes, node_id, updated_health)

        notify_status_change(node_id, health.status, :failed, state)

        %{state | monitored_nodes: updated_nodes}
    end
  end

  defp schedule_gossip(interval) do
    Process.send_after(self(), :perform_gossip, interval)
  end

  defp schedule_health_check(interval) do
    Process.send_after(self(), :perform_health_check, interval)
  end
end

defmodule Presto.Distributed.LoadBalancer do
  @moduledoc """
  Load balancing across distributed nodes for optimal RETE network performance.

  Implements dynamic load balancing strategies for distributed RETE processing:
  - Adaptive workload distribution based on real-time metrics
  - Auto-scaling triggers based on demand patterns
  - Request routing optimization for minimal latency
  - Resource-aware scheduling for CPU and memory efficiency
  - Geographic and network topology considerations
  """

  use GenServer
  require Logger

  alias Presto.Distributed.{ClusterManager, NodeRegistry, PartitionManager, HealthMonitor}
  alias Presto.Logger, as: PrestoLogger

  @type node_id :: String.t()
  @type workload_type :: :fact_processing | :rule_evaluation | :join_operations | :aggregation
  @type balancing_strategy ::
          :round_robin
          | :least_connections
          | :weighted_response_time
          | :resource_aware
          | :adaptive

  @type load_metrics :: %{
          cpu_utilization: float(),
          memory_utilization: float(),
          active_connections: non_neg_integer(),
          request_queue_depth: non_neg_integer(),
          response_time_p95: non_neg_integer(),
          throughput_ops_per_sec: float(),
          error_rate: float(),
          network_latency_ms: non_neg_integer()
        }

  @type node_capacity :: %{
          max_cpu_cores: pos_integer(),
          max_memory_mb: pos_integer(),
          max_concurrent_requests: pos_integer(),
          network_bandwidth_mbps: pos_integer(),
          storage_iops: pos_integer(),
          geographic_zone: String.t(),
          node_affinity_groups: [String.t()]
        }

  @type routing_decision :: %{
          selected_node: node_id(),
          routing_strategy: balancing_strategy(),
          decision_factors: map(),
          estimated_response_time: pos_integer(),
          confidence_score: float()
        }

  @type load_balancer_state :: %{
          local_node_id: node_id(),
          cluster_manager: pid(),
          node_registry: pid(),
          partition_manager: pid(),
          health_monitor: pid(),
          node_metrics: %{node_id() => load_metrics()},
          node_capacities: %{node_id() => node_capacity()},
          routing_history: [routing_decision()],
          active_strategies: %{workload_type() => balancing_strategy()},
          auto_scaling_state: auto_scaling_state(),
          performance_baseline: performance_baseline(),
          config: load_balancer_config()
        }

  @type auto_scaling_state :: %{
          scale_up_triggers: [scaling_trigger()],
          scale_down_triggers: [scaling_trigger()],
          last_scaling_action: integer(),
          scaling_cooldown_until: integer(),
          pending_scale_requests: [scale_request()]
        }

  @type scaling_trigger :: %{
          metric: String.t(),
          threshold: float(),
          duration_ms: pos_integer(),
          action: :scale_up | :scale_down,
          enabled: boolean()
        }

  @type scale_request :: %{
          action: :scale_up | :scale_down,
          target_capacity: pos_integer(),
          requested_at: integer(),
          estimated_completion: integer()
        }

  @type performance_baseline :: %{
          avg_response_time: float(),
          avg_throughput: float(),
          avg_cpu_utilization: float(),
          avg_memory_utilization: float(),
          baseline_established_at: integer(),
          sample_count: pos_integer()
        }

  @type load_balancer_config :: %{
          metrics_collection_interval: pos_integer(),
          routing_history_size: pos_integer(),
          auto_scaling_enabled: boolean(),
          scaling_cooldown_period: pos_integer(),
          performance_window_size: pos_integer(),
          latency_sensitivity: float(),
          throughput_weight: float(),
          resource_weight: float(),
          geographic_preference_weight: float()
        }

  @default_config %{
    metrics_collection_interval: 5_000,
    routing_history_size: 1000,
    auto_scaling_enabled: true,
    # 5 minutes
    scaling_cooldown_period: 300_000,
    performance_window_size: 100,
    latency_sensitivity: 0.4,
    throughput_weight: 0.3,
    resource_weight: 0.2,
    geographic_preference_weight: 0.1
  }

  # Client API

  @spec start_link(pid(), pid(), pid(), pid(), keyword()) :: GenServer.on_start()
  def start_link(cluster_manager, node_registry, partition_manager, health_monitor, opts \\ []) do
    GenServer.start_link(
      __MODULE__,
      {cluster_manager, node_registry, partition_manager, health_monitor, opts},
      name: __MODULE__
    )
  end

  @spec route_request(GenServer.server(), workload_type(), map()) ::
          {:ok, routing_decision()} | {:error, term()}
  def route_request(pid, workload_type, request_metadata \\ %{}) do
    GenServer.call(pid, {:route_request, workload_type, request_metadata})
  end

  @spec update_node_metrics(GenServer.server(), node_id(), load_metrics()) :: :ok
  def update_node_metrics(pid, node_id, metrics) do
    GenServer.cast(pid, {:update_node_metrics, node_id, metrics})
  end

  @spec register_node_capacity(GenServer.server(), node_id(), node_capacity()) :: :ok
  def register_node_capacity(pid, node_id, capacity) do
    GenServer.cast(pid, {:register_node_capacity, node_id, capacity})
  end

  @spec get_cluster_load_status(GenServer.server()) :: map()
  def get_cluster_load_status(pid) do
    GenServer.call(pid, :get_cluster_load_status)
  end

  @spec set_balancing_strategy(GenServer.server(), workload_type(), balancing_strategy()) :: :ok
  def set_balancing_strategy(pid, workload_type, strategy) do
    GenServer.call(pid, {:set_balancing_strategy, workload_type, strategy})
  end

  @spec trigger_scaling(GenServer.server(), :scale_up | :scale_down, pos_integer()) ::
          :ok | {:error, term()}
  def trigger_scaling(pid, action, target_capacity) do
    GenServer.call(pid, {:trigger_scaling, action, target_capacity})
  end

  @spec get_performance_metrics(GenServer.server()) :: map()
  def get_performance_metrics(pid) do
    GenServer.call(pid, :get_performance_metrics)
  end

  # Server implementation

  @impl true
  def init({cluster_manager, node_registry, partition_manager, health_monitor, opts}) do
    config = Map.merge(@default_config, Map.new(opts))
    local_node_id = Keyword.get(opts, :local_node_id, generate_node_id())

    state = %{
      local_node_id: local_node_id,
      cluster_manager: cluster_manager,
      node_registry: node_registry,
      partition_manager: partition_manager,
      health_monitor: health_monitor,
      node_metrics: %{},
      node_capacities: %{},
      routing_history: [],
      active_strategies: initialize_default_strategies(),
      auto_scaling_state: initialize_auto_scaling_state(),
      performance_baseline: initialize_performance_baseline(),
      config: config
    }

    # Schedule periodic tasks
    schedule_metrics_collection(config.metrics_collection_interval)
    schedule_auto_scaling_check(30_000)
    schedule_performance_analysis(60_000)

    PrestoLogger.log_distributed(:info, local_node_id, "load_balancer_started", %{
      auto_scaling: config.auto_scaling_enabled,
      strategies: Map.keys(state.active_strategies)
    })

    {:ok, state}
  end

  @impl true
  def handle_call({:route_request, workload_type, request_metadata}, _from, state) do
    case select_optimal_node(workload_type, request_metadata, state) do
      {:ok, routing_decision} ->
        # Record routing decision
        updated_history =
          add_routing_decision(routing_decision, state.routing_history, state.config)

        new_state = %{state | routing_history: updated_history}

        {:reply, {:ok, routing_decision}, new_state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call(:get_cluster_load_status, _from, state) do
    status = %{
      total_nodes: map_size(state.node_metrics),
      avg_cpu_utilization: calculate_average_cpu_utilization(state),
      avg_memory_utilization: calculate_average_memory_utilization(state),
      total_active_connections: calculate_total_connections(state),
      cluster_throughput: calculate_cluster_throughput(state),
      auto_scaling_status: state.auto_scaling_state
    }

    {:reply, status, state}
  end

  @impl true
  def handle_call({:set_balancing_strategy, workload_type, strategy}, _from, state) do
    updated_strategies = Map.put(state.active_strategies, workload_type, strategy)
    new_state = %{state | active_strategies: updated_strategies}

    PrestoLogger.log_distributed(:info, state.local_node_id, "balancing_strategy_updated", %{
      workload_type: workload_type,
      strategy: strategy
    })

    {:reply, :ok, new_state}
  end

  @impl true
  def handle_call({:trigger_scaling, action, target_capacity}, _from, state) do
    case can_perform_scaling_action?(action, state) do
      true ->
        case execute_scaling_action(action, target_capacity, state) do
          {:ok, new_state} ->
            {:reply, :ok, new_state}

          {:error, reason} ->
            {:reply, {:error, reason}, state}
        end

      false ->
        {:reply, {:error, :scaling_cooldown_active}, state}
    end
  end

  @impl true
  def handle_call(:get_performance_metrics, _from, state) do
    metrics = %{
      routing_decisions_last_hour: count_recent_routing_decisions(state, 3_600_000),
      avg_response_time: calculate_avg_response_time(state),
      performance_baseline: state.performance_baseline,
      load_distribution: calculate_load_distribution(state)
    }

    {:reply, metrics, state}
  end

  @impl true
  def handle_cast({:update_node_metrics, node_id, metrics}, state) do
    updated_metrics = Map.put(state.node_metrics, node_id, metrics)
    new_state = %{state | node_metrics: updated_metrics}

    # Check for auto-scaling triggers
    scaling_state =
      if state.config.auto_scaling_enabled do
        check_auto_scaling_triggers(new_state)
      else
        new_state
      end

    {:noreply, scaling_state}
  end

  @impl true
  def handle_cast({:register_node_capacity, node_id, capacity}, state) do
    updated_capacities = Map.put(state.node_capacities, node_id, capacity)
    new_state = %{state | node_capacities: updated_capacities}

    PrestoLogger.log_distributed(:info, state.local_node_id, "node_capacity_registered", %{
      node_id: node_id,
      max_cpu_cores: capacity.max_cpu_cores,
      max_memory_mb: capacity.max_memory_mb
    })

    {:noreply, new_state}
  end

  @impl true
  def handle_info(:collect_metrics, state) do
    new_state = collect_cluster_metrics(state)
    schedule_metrics_collection(state.config.metrics_collection_interval)
    {:noreply, new_state}
  end

  @impl true
  def handle_info(:auto_scaling_check, state) do
    new_state =
      if state.config.auto_scaling_enabled do
        perform_auto_scaling_check(state)
      else
        state
      end

    schedule_auto_scaling_check(30_000)
    {:noreply, new_state}
  end

  @impl true
  def handle_info(:performance_analysis, state) do
    new_state = update_performance_baseline(state)
    schedule_performance_analysis(60_000)
    {:noreply, new_state}
  end

  @impl true
  def handle_info({:scaling_completed, action, result}, state) do
    new_state = handle_scaling_completion(action, result, state)
    {:noreply, new_state}
  end

  # Private implementation functions

  defp generate_node_id() do
    "lb_" <> (:crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower))
  end

  defp initialize_default_strategies() do
    %{
      fact_processing: :resource_aware,
      rule_evaluation: :weighted_response_time,
      join_operations: :least_connections,
      aggregation: :adaptive
    }
  end

  defp initialize_auto_scaling_state() do
    %{
      scale_up_triggers: [
        %{
          metric: "avg_cpu_utilization",
          threshold: 0.8,
          # 5 minutes
          duration_ms: 300_000,
          action: :scale_up,
          enabled: true
        },
        %{
          metric: "avg_response_time",
          # 1 second
          threshold: 1000.0,
          # 3 minutes
          duration_ms: 180_000,
          action: :scale_up,
          enabled: true
        }
      ],
      scale_down_triggers: [
        %{
          metric: "avg_cpu_utilization",
          threshold: 0.3,
          # 10 minutes
          duration_ms: 600_000,
          action: :scale_down,
          enabled: true
        }
      ],
      last_scaling_action: 0,
      scaling_cooldown_until: 0,
      pending_scale_requests: []
    }
  end

  defp initialize_performance_baseline() do
    %{
      avg_response_time: 0.0,
      avg_throughput: 0.0,
      avg_cpu_utilization: 0.0,
      avg_memory_utilization: 0.0,
      baseline_established_at: System.monotonic_time(:millisecond),
      sample_count: 0
    }
  end

  defp select_optimal_node(workload_type, request_metadata, state) do
    strategy = Map.get(state.active_strategies, workload_type, :adaptive)
    available_nodes = get_available_nodes(state)

    if Enum.empty?(available_nodes) do
      {:error, :no_available_nodes}
    else
      case apply_balancing_strategy(
             strategy,
             workload_type,
             available_nodes,
             request_metadata,
             state
           ) do
        {:ok, selected_node} ->
          routing_decision =
            create_routing_decision(selected_node, strategy, workload_type, state)

          {:ok, routing_decision}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defp get_available_nodes(state) do
    case NodeRegistry.get_nodes_by_status(state.node_registry, :active) do
      nodes when is_list(nodes) ->
        Enum.map(nodes, fn node -> node.node_id end)
        |> Enum.reject(fn node_id -> node_id == state.local_node_id end)
        |> Enum.filter(fn node_id -> node_healthy?(node_id, state) end)

      _ ->
        []
    end
  end

  defp node_healthy?(node_id, state) do
    case Map.get(state.node_metrics, node_id) do
      nil ->
        false

      metrics ->
        metrics.cpu_utilization < 0.95 and
          metrics.memory_utilization < 0.95 and
          metrics.error_rate < 0.1
    end
  end

  defp apply_balancing_strategy(strategy, workload_type, available_nodes, request_metadata, state) do
    case strategy do
      :round_robin ->
        apply_round_robin_strategy(available_nodes, state)

      :least_connections ->
        apply_least_connections_strategy(available_nodes, state)

      :weighted_response_time ->
        apply_weighted_response_time_strategy(available_nodes, state)

      :resource_aware ->
        apply_resource_aware_strategy(available_nodes, workload_type, state)

      :adaptive ->
        apply_adaptive_strategy(available_nodes, workload_type, request_metadata, state)

      _ ->
        {:error, {:unsupported_strategy, strategy}}
    end
  end

  defp apply_round_robin_strategy(available_nodes, state) do
    # Simple round-robin based on routing history
    last_routing_count = length(state.routing_history)
    node_index = rem(last_routing_count, length(available_nodes))
    selected_node = Enum.at(available_nodes, node_index)
    {:ok, selected_node}
  end

  defp apply_least_connections_strategy(available_nodes, state) do
    # Select node with fewest active connections
    node_with_least_connections =
      available_nodes
      |> Enum.min_by(fn node_id ->
        case Map.get(state.node_metrics, node_id) do
          # High penalty for missing metrics
          nil -> 999_999
          metrics -> metrics.active_connections
        end
      end)

    {:ok, node_with_least_connections}
  end

  defp apply_weighted_response_time_strategy(available_nodes, state) do
    # Select node with best response time, weighted by current load
    node_scores =
      Enum.map(available_nodes, fn node_id ->
        score = calculate_response_time_score(node_id, state)
        {node_id, score}
      end)

    case Enum.min_by(node_scores, fn {_node, score} -> score end) do
      {selected_node, _score} -> {:ok, selected_node}
      nil -> {:error, :no_suitable_node}
    end
  end

  defp apply_resource_aware_strategy(available_nodes, workload_type, state) do
    # Select node based on resource requirements and availability
    resource_requirements = get_workload_resource_requirements(workload_type)

    suitable_nodes =
      Enum.filter(available_nodes, fn node_id ->
        has_sufficient_resources?(node_id, resource_requirements, state)
      end)

    case suitable_nodes do
      [] ->
        # Fallback to least loaded node
        apply_least_connections_strategy(available_nodes, state)

      nodes ->
        # Select node with best resource utilization ratio
        best_node =
          Enum.min_by(nodes, fn node_id ->
            calculate_resource_utilization_score(node_id, state)
          end)

        {:ok, best_node}
    end
  end

  defp apply_adaptive_strategy(available_nodes, workload_type, request_metadata, state) do
    # Combine multiple factors for optimal selection
    node_scores =
      Enum.map(available_nodes, fn node_id ->
        score = calculate_adaptive_score(node_id, workload_type, request_metadata, state)
        {node_id, score}
      end)

    case Enum.min_by(node_scores, fn {_node, score} -> score end) do
      {selected_node, _score} -> {:ok, selected_node}
      nil -> {:error, :no_suitable_node}
    end
  end

  defp calculate_response_time_score(node_id, state) do
    case Map.get(state.node_metrics, node_id) do
      nil ->
        999_999.0

      metrics ->
        # Combine response time with current load
        base_score = metrics.response_time_p95
        load_penalty = metrics.cpu_utilization * 1000 + metrics.memory_utilization * 500
        queue_penalty = metrics.request_queue_depth * 100

        base_score + load_penalty + queue_penalty
    end
  end

  defp get_workload_resource_requirements(workload_type) do
    case workload_type do
      :fact_processing -> %{cpu_weight: 0.3, memory_weight: 0.7}
      :rule_evaluation -> %{cpu_weight: 0.8, memory_weight: 0.2}
      :join_operations -> %{cpu_weight: 0.6, memory_weight: 0.4}
      :aggregation -> %{cpu_weight: 0.4, memory_weight: 0.6}
    end
  end

  defp has_sufficient_resources?(node_id, requirements, state) do
    case {Map.get(state.node_metrics, node_id), Map.get(state.node_capacities, node_id)} do
      {nil, _} ->
        false

      # Assume sufficient if capacity unknown
      {_, nil} ->
        true

      {metrics, _capacity} ->
        cpu_available = 1.0 - metrics.cpu_utilization
        memory_available = 1.0 - metrics.memory_utilization

        # 20% resource reservation
        cpu_needed = requirements.cpu_weight * 0.2
        memory_needed = requirements.memory_weight * 0.2

        cpu_available >= cpu_needed and memory_available >= memory_needed
    end
  end

  defp calculate_resource_utilization_score(node_id, state) do
    case Map.get(state.node_metrics, node_id) do
      nil ->
        999_999.0

      metrics ->
        # Lower score is better
        metrics.cpu_utilization * 0.6 + metrics.memory_utilization * 0.4
    end
  end

  defp calculate_adaptive_score(node_id, workload_type, request_metadata, state) do
    config = state.config

    # Combine multiple scoring factors
    response_time_score =
      calculate_response_time_score(node_id, state) * config.latency_sensitivity

    resource_score = calculate_resource_utilization_score(node_id, state) * config.resource_weight
    throughput_score = calculate_throughput_score(node_id, state) * config.throughput_weight

    geographic_score =
      calculate_geographic_score(node_id, request_metadata, state) *
        config.geographic_preference_weight

    response_time_score + resource_score + throughput_score + geographic_score
  end

  defp calculate_throughput_score(node_id, state) do
    case Map.get(state.node_metrics, node_id) do
      nil ->
        0.0

      metrics ->
        # Higher throughput = lower score (better)
        # Normalize against expected max
        max_throughput = 1000.0
        (max_throughput - metrics.throughput_ops_per_sec) / max_throughput
    end
  end

  defp calculate_geographic_score(node_id, request_metadata, state) do
    case {Map.get(state.node_capacities, node_id), Map.get(request_metadata, :client_zone)} do
      {nil, _} ->
        0.0

      {_, nil} ->
        0.0

      {capacity, client_zone} ->
        if capacity.geographic_zone == client_zone do
          # Perfect geographic match
          0.0
        else
          # Geographic penalty
          1.0
        end
    end
  end

  defp create_routing_decision(selected_node, strategy, workload_type, state) do
    metrics = Map.get(state.node_metrics, selected_node, %{})

    %{
      selected_node: selected_node,
      routing_strategy: strategy,
      decision_factors: %{
        workload_type: workload_type,
        node_cpu: Map.get(metrics, :cpu_utilization, 0.0),
        node_memory: Map.get(metrics, :memory_utilization, 0.0),
        node_connections: Map.get(metrics, :active_connections, 0),
        decision_timestamp: System.monotonic_time(:millisecond)
      },
      estimated_response_time: Map.get(metrics, :response_time_p95, 100),
      confidence_score: calculate_decision_confidence(selected_node, state)
    }
  end

  defp calculate_decision_confidence(node_id, state) do
    case Map.get(state.node_metrics, node_id) do
      # Low confidence without metrics
      nil ->
        0.1

      metrics ->
        # Base confidence on stability of metrics
        base_confidence = 0.8

        # Reduce confidence based on high utilization
        utilization_penalty = (metrics.cpu_utilization + metrics.memory_utilization) * 0.2

        # Reduce confidence based on error rate
        error_penalty = metrics.error_rate * 0.5

        max(0.1, base_confidence - utilization_penalty - error_penalty)
    end
  end

  defp add_routing_decision(decision, history, config) do
    updated_history = [decision | history]
    Enum.take(updated_history, config.routing_history_size)
  end

  defp collect_cluster_metrics(state) do
    # Collect metrics from all registered nodes
    active_nodes = get_available_nodes(state)

    # Request fresh metrics from health monitor
    Enum.each(active_nodes, fn node_id ->
      # In a real implementation, this would actively poll nodes
      # For now, we rely on nodes reporting their metrics
      :ok
    end)

    state
  end

  defp check_auto_scaling_triggers(state) do
    now = System.monotonic_time(:millisecond)

    # Check if we're in cooldown period
    if now < state.auto_scaling_state.scaling_cooldown_until do
      state
    else
      # Check scale-up triggers
      scale_up_triggered =
        Enum.any?(state.auto_scaling_state.scale_up_triggers, fn trigger ->
          trigger.enabled and evaluate_scaling_trigger(trigger, state)
        end)

      # Check scale-down triggers
      scale_down_triggered =
        Enum.any?(state.auto_scaling_state.scale_down_triggers, fn trigger ->
          trigger.enabled and evaluate_scaling_trigger(trigger, state)
        end)

      cond do
        scale_up_triggered ->
          execute_auto_scaling(:scale_up, state)

        scale_down_triggered ->
          execute_auto_scaling(:scale_down, state)

        true ->
          state
      end
    end
  end

  defp evaluate_scaling_trigger(trigger, state) do
    current_value =
      case trigger.metric do
        "avg_cpu_utilization" -> calculate_average_cpu_utilization(state)
        "avg_memory_utilization" -> calculate_average_memory_utilization(state)
        "avg_response_time" -> calculate_avg_response_time(state)
        _ -> 0.0
      end

    case trigger.action do
      :scale_up -> current_value > trigger.threshold
      :scale_down -> current_value < trigger.threshold
    end
  end

  defp execute_auto_scaling(action, state) do
    PrestoLogger.log_distributed(:info, state.local_node_id, "auto_scaling_triggered", %{
      action: action
    })

    # Calculate target capacity
    current_capacity = map_size(state.node_metrics)

    target_capacity =
      case action do
        # Scale up by ~33%
        :scale_up -> current_capacity + max(1, div(current_capacity, 3))
        # Scale down by 1 node
        :scale_down -> max(1, current_capacity - 1)
      end

    case execute_scaling_action(action, target_capacity, state) do
      {:ok, new_state} -> new_state
      {:error, _reason} -> state
    end
  end

  defp can_perform_scaling_action?(_action, state) do
    now = System.monotonic_time(:millisecond)
    now >= state.auto_scaling_state.scaling_cooldown_until
  end

  defp execute_scaling_action(action, target_capacity, state) do
    now = System.monotonic_time(:millisecond)

    # Create scale request
    scale_request = %{
      action: action,
      target_capacity: target_capacity,
      requested_at: now,
      # 1 minute estimate
      estimated_completion: now + 60_000
    }

    # Update auto-scaling state
    updated_auto_scaling = %{
      state.auto_scaling_state
      | last_scaling_action: now,
        scaling_cooldown_until: now + state.config.scaling_cooldown_period,
        pending_scale_requests: [scale_request | state.auto_scaling_state.pending_scale_requests]
    }

    # Simulate scaling action (in real implementation, would trigger node provisioning)
    spawn(fn ->
      # Simulate scaling delay
      :timer.sleep(30_000)
      send(self(), {:scaling_completed, action, :success})
    end)

    new_state = %{state | auto_scaling_state: updated_auto_scaling}

    PrestoLogger.log_distributed(:info, state.local_node_id, "scaling_action_initiated", %{
      action: action,
      target_capacity: target_capacity,
      estimated_completion: scale_request.estimated_completion
    })

    {:ok, new_state}
  end

  defp handle_scaling_completion(action, result, state) do
    PrestoLogger.log_distributed(:info, state.local_node_id, "scaling_action_completed", %{
      action: action,
      result: result
    })

    # Remove completed request from pending
    updated_pending =
      Enum.reject(state.auto_scaling_state.pending_scale_requests, fn request ->
        request.action == action
      end)

    updated_auto_scaling = %{state.auto_scaling_state | pending_scale_requests: updated_pending}

    %{state | auto_scaling_state: updated_auto_scaling}
  end

  defp perform_auto_scaling_check(state) do
    check_auto_scaling_triggers(state)
  end

  defp update_performance_baseline(state) do
    # Calculate current performance metrics
    current_metrics = %{
      avg_response_time: calculate_avg_response_time(state),
      avg_throughput: calculate_cluster_throughput(state),
      avg_cpu_utilization: calculate_average_cpu_utilization(state),
      avg_memory_utilization: calculate_average_memory_utilization(state)
    }

    # Update baseline with exponential moving average
    baseline = state.performance_baseline
    # Smoothing factor
    alpha = 0.1

    updated_baseline = %{
      avg_response_time:
        baseline.avg_response_time * (1 - alpha) + current_metrics.avg_response_time * alpha,
      avg_throughput:
        baseline.avg_throughput * (1 - alpha) + current_metrics.avg_throughput * alpha,
      avg_cpu_utilization:
        baseline.avg_cpu_utilization * (1 - alpha) + current_metrics.avg_cpu_utilization * alpha,
      avg_memory_utilization:
        baseline.avg_memory_utilization * (1 - alpha) +
          current_metrics.avg_memory_utilization * alpha,
      baseline_established_at: baseline.baseline_established_at,
      sample_count: baseline.sample_count + 1
    }

    %{state | performance_baseline: updated_baseline}
  end

  # Calculation helper functions

  defp calculate_average_cpu_utilization(state) do
    if map_size(state.node_metrics) == 0 do
      0.0
    else
      total_cpu =
        state.node_metrics
        |> Map.values()
        |> Enum.sum(fn metrics -> metrics.cpu_utilization end)

      total_cpu / map_size(state.node_metrics)
    end
  end

  defp calculate_average_memory_utilization(state) do
    if map_size(state.node_metrics) == 0 do
      0.0
    else
      total_memory =
        state.node_metrics
        |> Map.values()
        |> Enum.sum(fn metrics -> metrics.memory_utilization end)

      total_memory / map_size(state.node_metrics)
    end
  end

  defp calculate_total_connections(state) do
    state.node_metrics
    |> Map.values()
    |> Enum.sum(fn metrics -> metrics.active_connections end)
  end

  defp calculate_cluster_throughput(state) do
    state.node_metrics
    |> Map.values()
    |> Enum.sum(fn metrics -> metrics.throughput_ops_per_sec end)
  end

  defp calculate_avg_response_time(state) do
    if map_size(state.node_metrics) == 0 do
      0.0
    else
      total_response_time =
        state.node_metrics
        |> Map.values()
        |> Enum.sum(fn metrics -> metrics.response_time_p95 end)

      total_response_time / map_size(state.node_metrics)
    end
  end

  defp count_recent_routing_decisions(state, time_window_ms) do
    cutoff_time = System.monotonic_time(:millisecond) - time_window_ms

    Enum.count(state.routing_history, fn decision ->
      decision.decision_factors.decision_timestamp > cutoff_time
    end)
  end

  defp calculate_load_distribution(state) do
    state.node_metrics
    |> Enum.into(%{}, fn {node_id, metrics} ->
      {node_id,
       %{
         cpu_utilization: metrics.cpu_utilization,
         memory_utilization: metrics.memory_utilization,
         active_connections: metrics.active_connections,
         throughput: metrics.throughput_ops_per_sec
       }}
    end)
  end

  defp schedule_metrics_collection(interval) do
    Process.send_after(self(), :collect_metrics, interval)
  end

  defp schedule_auto_scaling_check(interval) do
    Process.send_after(self(), :auto_scaling_check, interval)
  end

  defp schedule_performance_analysis(interval) do
    Process.send_after(self(), :performance_analysis, interval)
  end
end

defmodule Presto.Distributed.NetworkPartitioner do
  @moduledoc """
  Network partitioning strategies for optimal load distribution in distributed RETE.

  Implements intelligent partitioning of RETE network segments (alpha and beta nodes)
  across cluster nodes based on load patterns, data locality, and performance characteristics.
  Supports dynamic repartitioning and segment migration for optimal resource utilization.
  """

  use GenServer
  require Logger

  alias Presto.Distributed.ClusterManager
  alias Presto.Logger, as: PrestoLogger

  @type segment_id :: String.t()
  @type node_id :: String.t()
  @type partition_strategy :: :hash_based | :load_balanced | :locality_aware | :hybrid

  @type network_segment :: %{
          id: segment_id(),
          type: :alpha_node | :beta_node | :join_node | :aggregation_node,
          patterns: [tuple()],
          estimated_selectivity: float(),
          memory_requirement: non_neg_integer(),
          cpu_requirement: float(),
          network_dependencies: [segment_id()],
          data_locality_hints: [tuple()],
          current_assignment: [node_id()],
          performance_metrics: segment_metrics()
        }

  @type segment_metrics :: %{
          facts_processed_per_second: float(),
          memory_usage_mb: non_neg_integer(),
          cpu_utilization: float(),
          network_io_bytes_per_second: non_neg_integer(),
          latency_p99_ms: non_neg_integer(),
          error_rate: float()
        }

  @type partitioning_plan :: %{
          strategy: partition_strategy(),
          segment_assignments: %{segment_id() => [node_id()]},
          migration_plan: [migration_operation()],
          estimated_improvement: %{
            load_balance_score: float(),
            network_efficiency: float(),
            resource_utilization: float()
          }
        }

  @type migration_operation :: %{
          segment_id: segment_id(),
          from_nodes: [node_id()],
          to_nodes: [node_id()],
          migration_type: :move | :replicate | :split,
          estimated_duration: pos_integer(),
          rollback_plan: map()
        }

  @type partitioner_state :: %{
          local_node_id: node_id(),
          cluster_manager: pid(),
          partition_manager: pid(),
          node_registry: pid(),
          network_segments: %{segment_id() => network_segment()},
          current_partitioning_plan: partitioning_plan() | nil,
          active_migrations: %{segment_id() => migration_operation()},
          performance_history: [performance_snapshot()],
          config: partitioner_config()
        }

  @type performance_snapshot :: %{
          timestamp: integer(),
          overall_load_balance: float(),
          network_efficiency: float(),
          resource_utilization: %{node_id() => %{cpu: float(), memory: float()}},
          segment_performance: %{segment_id() => segment_metrics()}
        }

  @type partitioner_config :: %{
          rebalancing_threshold: float(),
          migration_batch_size: pos_integer(),
          performance_history_size: pos_integer(),
          locality_weight: float(),
          load_balance_weight: float(),
          network_efficiency_weight: float(),
          auto_rebalancing: boolean(),
          rebalancing_interval: pos_integer()
        }

  @default_config %{
    rebalancing_threshold: 0.2,
    migration_batch_size: 3,
    performance_history_size: 100,
    locality_weight: 0.3,
    load_balance_weight: 0.4,
    network_efficiency_weight: 0.3,
    auto_rebalancing: true,
    rebalancing_interval: 30_000
  }

  # Client API

  @spec start_link(pid(), pid(), pid(), keyword()) :: GenServer.on_start()
  def start_link(cluster_manager, partition_manager, node_registry, opts \\ []) do
    GenServer.start_link(__MODULE__, {cluster_manager, partition_manager, node_registry, opts},
      name: __MODULE__
    )
  end

  @spec register_network_segment(GenServer.server(), network_segment()) :: :ok | {:error, term()}
  def register_network_segment(pid, segment) do
    GenServer.call(pid, {:register_network_segment, segment})
  end

  @spec unregister_network_segment(GenServer.server(), segment_id()) :: :ok
  def unregister_network_segment(pid, segment_id) do
    GenServer.call(pid, {:unregister_network_segment, segment_id})
  end

  @spec get_segment_assignment(GenServer.server(), segment_id()) ::
          {:ok, [node_id()]} | {:error, term()}
  def get_segment_assignment(pid, segment_id) do
    GenServer.call(pid, {:get_segment_assignment, segment_id})
  end

  @spec trigger_rebalancing(GenServer.server(), partition_strategy()) :: :ok | {:error, term()}
  def trigger_rebalancing(pid, strategy \\ :hybrid) do
    GenServer.call(pid, {:trigger_rebalancing, strategy})
  end

  @spec update_segment_metrics(GenServer.server(), segment_id(), segment_metrics()) :: :ok
  def update_segment_metrics(pid, segment_id, metrics) do
    GenServer.cast(pid, {:update_segment_metrics, segment_id, metrics})
  end

  @spec get_partitioning_status(GenServer.server()) :: map()
  def get_partitioning_status(pid) do
    GenServer.call(pid, :get_partitioning_status)
  end

  @spec migrate_segment(GenServer.server(), segment_id(), [node_id()], keyword()) ::
          :ok | {:error, term()}
  def migrate_segment(pid, segment_id, target_nodes, opts \\ []) do
    GenServer.call(pid, {:migrate_segment, segment_id, target_nodes, opts})
  end

  # Server implementation

  @impl true
  def init({cluster_manager, partition_manager, node_registry, opts}) do
    config = Map.merge(@default_config, Map.new(opts))
    local_node_id = Keyword.get(opts, :local_node_id, generate_node_id())

    state = %{
      local_node_id: local_node_id,
      cluster_manager: cluster_manager,
      partition_manager: partition_manager,
      node_registry: node_registry,
      network_segments: %{},
      current_partitioning_plan: nil,
      active_migrations: %{},
      performance_history: [],
      config: config
    }

    # Schedule automatic rebalancing if enabled
    if config.auto_rebalancing do
      schedule_rebalancing_check(config.rebalancing_interval)
    end

    PrestoLogger.log_distributed(:info, local_node_id, "network_partitioner_started", %{
      strategy: :hybrid,
      auto_rebalancing: config.auto_rebalancing
    })

    {:ok, state}
  end

  @impl true
  def handle_call({:register_network_segment, segment}, _from, state) do
    # Assign initial nodes for this segment
    case determine_initial_assignment(segment, state) do
      {:ok, assigned_nodes} ->
        updated_segment = %{segment | current_assignment: assigned_nodes}
        updated_segments = Map.put(state.network_segments, segment.id, updated_segment)
        new_state = %{state | network_segments: updated_segments}

        PrestoLogger.log_distributed(:info, state.local_node_id, "network_segment_registered", %{
          segment_id: segment.id,
          segment_type: segment.type,
          assigned_nodes: assigned_nodes
        })

        {:reply, :ok, new_state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:unregister_network_segment, segment_id}, _from, state) do
    updated_segments = Map.delete(state.network_segments, segment_id)
    updated_migrations = Map.delete(state.active_migrations, segment_id)

    new_state = %{
      state
      | network_segments: updated_segments,
        active_migrations: updated_migrations
    }

    PrestoLogger.log_distributed(:info, state.local_node_id, "network_segment_unregistered", %{
      segment_id: segment_id
    })

    {:reply, :ok, new_state}
  end

  @impl true
  def handle_call({:get_segment_assignment, segment_id}, _from, state) do
    case Map.get(state.network_segments, segment_id) do
      nil ->
        {:reply, {:error, :segment_not_found}, state}

      segment ->
        {:reply, {:ok, segment.current_assignment}, state}
    end
  end

  @impl true
  def handle_call({:trigger_rebalancing, strategy}, _from, state) do
    case compute_rebalancing_plan(strategy, state) do
      {:ok, rebalancing_plan} ->
        case execute_rebalancing_plan(rebalancing_plan, state) do
          {:ok, new_state} ->
            PrestoLogger.log_distributed(:info, state.local_node_id, "rebalancing_completed", %{
              strategy: strategy,
              migrations_executed: length(rebalancing_plan.migration_plan)
            })

            {:reply, :ok, new_state}

        end
    end
  end

  @impl true
  def handle_call(:get_partitioning_status, _from, state) do
    status = %{
      total_segments: map_size(state.network_segments),
      active_migrations: map_size(state.active_migrations),
      current_plan: state.current_partitioning_plan,
      performance_score: calculate_current_performance_score(state),
      load_distribution: analyze_current_load_distribution(state)
    }

    {:reply, status, state}
  end

  @impl true
  def handle_call({:migrate_segment, segment_id, target_nodes, opts}, _from, state) do
    case Map.get(state.network_segments, segment_id) do
      nil ->
        {:reply, {:error, :segment_not_found}, state}

      segment ->
        migration_type = Keyword.get(opts, :migration_type, :move)

        migration_op = %{
          segment_id: segment_id,
          from_nodes: segment.current_assignment,
          to_nodes: target_nodes,
          migration_type: migration_type,
          estimated_duration: estimate_migration_duration(segment, target_nodes),
          rollback_plan: create_rollback_plan(segment)
        }

        case execute_migration(migration_op, state) do
          {:ok, new_state} ->
            {:reply, :ok, new_state}

        end
    end
  end

  @impl true
  def handle_cast({:update_segment_metrics, segment_id, metrics}, state) do
    case Map.get(state.network_segments, segment_id) do
      nil ->
        {:noreply, state}

      segment ->
        updated_segment = %{segment | performance_metrics: metrics}
        updated_segments = Map.put(state.network_segments, segment_id, updated_segment)
        new_state = %{state | network_segments: updated_segments}

        # Record performance snapshot if significant change
        if should_record_performance_snapshot?(metrics, segment.performance_metrics) do
          snapshot = create_performance_snapshot(new_state)

          updated_history =
            add_performance_snapshot(snapshot, new_state.performance_history, state.config)

          final_state = %{new_state | performance_history: updated_history}
          {:noreply, final_state}
        else
          {:noreply, new_state}
        end
    end
  end

  @impl true
  def handle_info(:rebalancing_check, state) do
    new_state =
      if should_trigger_auto_rebalancing?(state) do
        case compute_rebalancing_plan(:hybrid, state) do
          {:ok, plan} ->
            case execute_rebalancing_plan(plan, state) do
              {:ok, rebalanced_state} ->
                PrestoLogger.log_distributed(
                  :info,
                  state.local_node_id,
                  "auto_rebalancing_completed",
                  %{
                    improvements: plan.estimated_improvement
                  }
                )

                rebalanced_state

              {:error, reason} ->
                PrestoLogger.log_distributed(
                  :warning,
                  state.local_node_id,
                  "auto_rebalancing_failed",
                  %{
                    reason: reason
                  }
                )

                state
            end

        end
      else
        state
      end

    # Schedule next check
    schedule_rebalancing_check(state.config.rebalancing_interval)

    {:noreply, new_state}
  end

  @impl true
  def handle_info({:migration_completed, segment_id, result}, state) do
    case Map.get(state.active_migrations, segment_id) do
      nil ->
        {:noreply, state}

      migration_op ->
        updated_migrations = Map.delete(state.active_migrations, segment_id)

        new_state =
          case result do
            :success ->
              # Update segment assignment
              case Map.get(state.network_segments, segment_id) do
                nil ->
                  %{state | active_migrations: updated_migrations}

                segment ->
                  updated_segment = %{segment | current_assignment: migration_op.to_nodes}
                  updated_segments = Map.put(state.network_segments, segment_id, updated_segment)

                  PrestoLogger.log_distributed(
                    :info,
                    state.local_node_id,
                    "migration_completed",
                    %{
                      segment_id: segment_id,
                      from_nodes: migration_op.from_nodes,
                      to_nodes: migration_op.to_nodes
                    }
                  )

                  %{
                    state
                    | network_segments: updated_segments,
                      active_migrations: updated_migrations
                  }
              end

            {:error, reason} ->
              PrestoLogger.log_distributed(:error, state.local_node_id, "migration_failed", %{
                segment_id: segment_id,
                reason: reason
              })

              # Execute rollback plan if available
              execute_rollback_plan(migration_op.rollback_plan, state)
              %{state | active_migrations: updated_migrations}
          end

        {:noreply, new_state}
    end
  end

  # Private implementation functions

  defp generate_node_id() do
    "partitioner_" <> (:crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower))
  end

  defp determine_initial_assignment(segment, state) do
    # Get available nodes from cluster
    cluster_state = ClusterManager.get_cluster_state()

    available_nodes =
      cluster_state.nodes
      |> Enum.filter(fn {_id, node_info} -> node_info.status == :active end)
      |> Enum.map(fn {id, _info} -> id end)

    if Enum.empty?(available_nodes) do
      {:error, :no_available_nodes}
    else
      # Use load-based assignment for initial placement
      assigned_nodes = select_optimal_nodes_for_segment(segment, available_nodes, state)
      {:ok, assigned_nodes}
    end
  end

  defp select_optimal_nodes_for_segment(segment, available_nodes, state) do
    # Calculate score for each node based on current load and segment requirements
    node_scores =
      Enum.map(available_nodes, fn node_id ->
        score = calculate_node_suitability_score(node_id, segment, state)
        {node_id, score}
      end)

    # Select the best nodes (lower score is better)
    # Default to 2 replicas
    replication_factor = min(length(available_nodes), 2)

    node_scores
    |> Enum.sort_by(fn {_node, score} -> score end)
    |> Enum.take(replication_factor)
    |> Enum.map(fn {node_id, _score} -> node_id end)
  end

  defp calculate_node_suitability_score(node_id, segment, state) do
    # Get current load for this node
    current_load = get_node_current_load(node_id, state)

    # Factor in segment requirements
    cpu_score = current_load.cpu_utilization + segment.cpu_requirement
    memory_score = (current_load.memory_usage + segment.memory_requirement) / 1000.0

    # Factor in network locality
    locality_score = calculate_locality_score(node_id, segment, state)

    # Combine scores with weights
    cpu_score * 0.4 + memory_score * 0.4 + locality_score * 0.2
  end

  defp get_node_current_load(node_id, state) do
    # Calculate current load from assigned segments
    assigned_segments =
      state.network_segments
      |> Enum.filter(fn {_id, segment} -> node_id in segment.current_assignment end)
      |> Enum.map(fn {_id, segment} -> segment end)

    total_cpu = Enum.sum(Enum.map(assigned_segments, & &1.cpu_requirement))
    total_memory = Enum.sum(Enum.map(assigned_segments, & &1.memory_requirement))

    %{
      cpu_utilization: total_cpu,
      memory_usage: total_memory,
      segment_count: length(assigned_segments)
    }
  end

  defp calculate_locality_score(node_id, segment, state) do
    # Analyze network dependencies and data locality
    dependency_scores =
      Enum.map(segment.network_dependencies, fn dep_segment_id ->
        case Map.get(state.network_segments, dep_segment_id) do
          # Unknown dependency, assume no locality
          nil ->
            1.0

          dep_segment ->
            if node_id in dep_segment.current_assignment do
              # Perfect locality
              0.0
            else
              # Some network cost
              0.5
            end
        end
      end)

    case dependency_scores do
      # No dependencies
      [] -> 0.0
      scores -> Enum.sum(scores) / length(scores)
    end
  end

  defp compute_rebalancing_plan(strategy, state) do
    _current_performance = calculate_current_performance_score(state)

    case strategy do
      :hash_based ->
        compute_hash_based_plan(state)

      :load_balanced ->
        compute_load_balanced_plan(state)

      :locality_aware ->
        compute_locality_aware_plan(state)

      :hybrid ->
        compute_hybrid_plan(state)
    end
  end

  defp compute_hash_based_plan(state) do
    # Simple hash-based partitioning
    cluster_state = ClusterManager.get_cluster_state()
    available_nodes = Map.keys(cluster_state.nodes)

    segment_assignments =
      state.network_segments
      |> Enum.into(%{}, fn {segment_id, _segment} ->
        # Hash segment ID to determine target nodes
        hash = :crypto.hash(:sha256, segment_id)

        node_index =
          :binary.decode_unsigned(binary_part(hash, 0, 4))
          |> rem(length(available_nodes))

        target_node = Enum.at(available_nodes, node_index)

        {segment_id, [target_node]}
      end)

    migration_plan = compute_migration_operations(segment_assignments, state)

    {:ok,
     %{
       strategy: :hash_based,
       segment_assignments: segment_assignments,
       migration_plan: migration_plan,
       estimated_improvement: %{
         load_balance_score: 0.5,
         network_efficiency: 0.3,
         resource_utilization: 0.4
       }
     }}
  end

  defp compute_load_balanced_plan(state) do
    cluster_state = ClusterManager.get_cluster_state()
    available_nodes = Map.keys(cluster_state.nodes)

    # Sort nodes by current load
    node_loads =
      Enum.map(available_nodes, fn node_id ->
        load = get_node_current_load(node_id, state)
        {node_id, load.cpu_utilization + load.memory_usage / 1000.0}
      end)
      |> Enum.sort_by(fn {_node, load} -> load end)

    # Reassign segments to balance load
    segment_assignments = balance_segments_across_nodes(state.network_segments, node_loads)
    migration_plan = compute_migration_operations(segment_assignments, state)

    {:ok,
     %{
       strategy: :load_balanced,
       segment_assignments: segment_assignments,
       migration_plan: migration_plan,
       estimated_improvement: %{
         load_balance_score: 0.8,
         network_efficiency: 0.4,
         resource_utilization: 0.7
       }
     }}
  end

  defp compute_locality_aware_plan(state) do
    # Group segments by network dependencies
    dependency_groups = group_segments_by_dependencies(state.network_segments)

    # Assign each group to nodes that can co-locate them
    segment_assignments = assign_dependency_groups_to_nodes(dependency_groups, state)
    migration_plan = compute_migration_operations(segment_assignments, state)

    {:ok,
     %{
       strategy: :locality_aware,
       segment_assignments: segment_assignments,
       migration_plan: migration_plan,
       estimated_improvement: %{
         load_balance_score: 0.4,
         network_efficiency: 0.9,
         resource_utilization: 0.5
       }
     }}
  end

  defp compute_hybrid_plan(state) do
    # Combine multiple strategies with weights
    {:ok, load_plan} = compute_load_balanced_plan(state)
    {:ok, locality_plan} = compute_locality_aware_plan(state)

    # Merge plans based on configuration weights
    merged_assignments =
      merge_assignment_plans(
        load_plan.segment_assignments,
        locality_plan.segment_assignments,
        state.config
      )

    migration_plan = compute_migration_operations(merged_assignments, state)

    {:ok,
     %{
       strategy: :hybrid,
       segment_assignments: merged_assignments,
       migration_plan: migration_plan,
       estimated_improvement: %{
         load_balance_score: 0.7,
         network_efficiency: 0.7,
         resource_utilization: 0.6
       }
     }}
  end

  defp balance_segments_across_nodes(segments, node_loads) do
    # Simple round-robin assignment based on load
    sorted_nodes = Enum.map(node_loads, fn {node_id, _load} -> node_id end)

    segments
    |> Enum.with_index()
    |> Enum.into(%{}, fn {{segment_id, _segment}, index} ->
      node_index = rem(index, length(sorted_nodes))
      target_node = Enum.at(sorted_nodes, node_index)
      {segment_id, [target_node]}
    end)
  end

  defp group_segments_by_dependencies(segments) do
    # Create dependency graph and find connected components
    segments
    |> Enum.group_by(fn {_id, segment} ->
      if Enum.empty?(segment.network_dependencies) do
        :independent
      else
        # Simple grouping by first dependency
        hd(segment.network_dependencies)
      end
    end)
  end

  defp assign_dependency_groups_to_nodes(dependency_groups, _state) do
    cluster_state = ClusterManager.get_cluster_state()
    available_nodes = Map.keys(cluster_state.nodes)

    # Assign each group to nodes
    dependency_groups
    |> Enum.with_index()
    |> Enum.flat_map(fn {{_group_key, segments}, group_index} ->
      node_index = rem(group_index, length(available_nodes))
      target_node = Enum.at(available_nodes, node_index)

      Enum.map(segments, fn {segment_id, _segment} ->
        {segment_id, [target_node]}
      end)
    end)
    |> Enum.into(%{})
  end

  defp merge_assignment_plans(load_plan, locality_plan, config) do
    load_weight = config.load_balance_weight
    locality_weight = config.network_efficiency_weight

    # For each segment, choose between plans based on weights
    all_segments =
      MapSet.union(MapSet.new(Map.keys(load_plan)), MapSet.new(Map.keys(locality_plan)))

    Enum.into(all_segments, %{}, fn segment_id ->
      load_assignment = Map.get(load_plan, segment_id, [])
      locality_assignment = Map.get(locality_plan, segment_id, [])

      # Simple selection based on primary weight
      chosen_assignment =
        if load_weight > locality_weight do
          load_assignment
        else
          locality_assignment
        end

      {segment_id, chosen_assignment}
    end)
  end

  defp compute_migration_operations(target_assignments, state) do
    state.network_segments
    |> Enum.map(fn {segment_id, segment} ->
      current_assignment = segment.current_assignment
      target_assignment = Map.get(target_assignments, segment_id, current_assignment)

      if current_assignment != target_assignment do
        %{
          segment_id: segment_id,
          from_nodes: current_assignment,
          to_nodes: target_assignment,
          migration_type: :move,
          estimated_duration: estimate_migration_duration(segment, target_assignment),
          rollback_plan: create_rollback_plan(segment)
        }
      else
        nil
      end
    end)
    |> Enum.reject(&is_nil/1)
  end

  defp execute_rebalancing_plan(plan, state) do
    # Execute migrations in batches
    migration_batches = Enum.chunk_every(plan.migration_plan, state.config.migration_batch_size)

    try do
      final_state =
        Enum.reduce(migration_batches, state, fn batch, acc_state ->
          batch_results =
            Enum.map(batch, fn migration ->
              execute_migration(migration, acc_state)
            end)

          # Check if all migrations in batch succeeded
          if Enum.all?(batch_results, fn {result, _state} -> result == :ok end) do
            # Get the final state from the last successful migration
            {_result, final_batch_state} = List.last(batch_results)
            final_batch_state
          else
            # If any migration failed, return current state
            acc_state
          end
        end)

      updated_state = %{final_state | current_partitioning_plan: plan}
      {:ok, updated_state}
    rescue
      error ->
        {:error, {:execution_failed, error}}
    end
  end

  defp execute_migration(migration_op, state) do
    # Add to active migrations
    updated_migrations = Map.put(state.active_migrations, migration_op.segment_id, migration_op)
    intermediate_state = %{state | active_migrations: updated_migrations}

    # Simulate migration (in real implementation, this would coordinate with nodes)
    spawn(fn ->
      :timer.sleep(migration_op.estimated_duration)
      send(self(), {:migration_completed, migration_op.segment_id, :success})
    end)

    {:ok, intermediate_state}
  end

  defp estimate_migration_duration(segment, _target_nodes) do
    # Estimate based on segment size and type
    # 1 second base
    base_duration = 1000
    # Scale by memory requirement
    size_factor = segment.memory_requirement / 1000

    type_factor =
      case segment.type do
        :alpha_node -> 1.0
        :beta_node -> 1.5
        :join_node -> 2.0
        :aggregation_node -> 2.5
      end

    round(base_duration * size_factor * type_factor)
  end

  defp create_rollback_plan(segment) do
    %{
      original_assignment: segment.current_assignment,
      segment_backup: segment
    }
  end

  defp execute_rollback_plan(_rollback_plan, state) do
    # Implementation for rollback would restore original assignment
    state
  end

  defp calculate_current_performance_score(state) do
    if map_size(state.network_segments) == 0 do
      1.0
    else
      # Calculate aggregate performance metrics
      _total_segments = map_size(state.network_segments)

      # Load balance score (how evenly distributed segments are)
      load_balance_score = calculate_load_balance_score(state)

      # Network efficiency score (how well co-located dependencies are)
      network_efficiency_score = calculate_network_efficiency_score(state)

      # Resource utilization score
      resource_utilization_score = calculate_resource_utilization_score(state)

      # Weighted average
      config = state.config

      load_balance_score * config.load_balance_weight +
        network_efficiency_score * config.network_efficiency_weight +
        resource_utilization_score *
          (1.0 - config.load_balance_weight - config.network_efficiency_weight)
    end
  end

  defp calculate_load_balance_score(state) do
    cluster_state = ClusterManager.get_cluster_state()

    node_loads =
      Enum.map(Map.keys(cluster_state.nodes), fn node_id ->
        get_node_current_load(node_id, state).segment_count
      end)

    if Enum.empty?(node_loads) do
      1.0
    else
      avg_load = Enum.sum(node_loads) / length(node_loads)
      max_load = Enum.max(node_loads)
      min_load = Enum.min(node_loads)

      if max_load == min_load do
        # Perfect balance
        1.0
      else
        1.0 - (max_load - min_load) / (avg_load + 1)
      end
    end
  end

  defp calculate_network_efficiency_score(state) do
    # Calculate how many network dependencies are co-located
    _total_dependencies = 0
    _co_located_dependencies = 0

    {total, co_located} =
      Enum.reduce(state.network_segments, {0, 0}, fn {_id, segment}, {tot_acc, col_acc} ->
        dep_count = length(segment.network_dependencies)

        co_located_count =
          Enum.count(segment.network_dependencies, fn dep_id ->
            case Map.get(state.network_segments, dep_id) do
              nil ->
                false

              dep_segment ->
                not MapSet.disjoint?(
                  MapSet.new(segment.current_assignment),
                  MapSet.new(dep_segment.current_assignment)
                )
            end
          end)

        {tot_acc + dep_count, col_acc + co_located_count}
      end)

    if total == 0 do
      1.0
    else
      co_located / total
    end
  end

  defp calculate_resource_utilization_score(state) do
    cluster_state = ClusterManager.get_cluster_state()

    if map_size(cluster_state.nodes) == 0 do
      0.0
    else
      # Average utilization across all nodes
      utilizations =
        Enum.map(Map.keys(cluster_state.nodes), fn node_id ->
          load = get_node_current_load(node_id, state)
          (load.cpu_utilization + load.memory_usage / 1000.0) / 2.0
        end)

      Enum.sum(utilizations) / length(utilizations)
    end
  end

  defp analyze_current_load_distribution(state) do
    cluster_state = ClusterManager.get_cluster_state()

    Map.keys(cluster_state.nodes)
    |> Enum.into(%{}, fn node_id ->
      load = get_node_current_load(node_id, state)

      {node_id,
       %{
         segment_count: load.segment_count,
         cpu_utilization: load.cpu_utilization,
         memory_usage: load.memory_usage
       }}
    end)
  end

  defp should_trigger_auto_rebalancing?(state) do
    current_score = calculate_current_performance_score(state)
    current_score < 1.0 - state.config.rebalancing_threshold
  end

  defp should_record_performance_snapshot?(_new_metrics, _old_metrics) do
    # For now, record all updates
    true
  end

  defp create_performance_snapshot(state) do
    %{
      timestamp: System.monotonic_time(:millisecond),
      overall_load_balance: calculate_load_balance_score(state),
      network_efficiency: calculate_network_efficiency_score(state),
      resource_utilization: analyze_current_load_distribution(state),
      segment_performance:
        state.network_segments
        |> Enum.into(%{}, fn {id, segment} -> {id, segment.performance_metrics} end)
    }
  end

  defp add_performance_snapshot(snapshot, history, config) do
    updated_history = [snapshot | history]
    Enum.take(updated_history, config.performance_history_size)
  end

  defp schedule_rebalancing_check(interval) do
    Process.send_after(self(), :rebalancing_check, interval)
  end
end

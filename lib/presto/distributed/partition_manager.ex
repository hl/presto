defmodule Presto.Distributed.PartitionManager do
  @moduledoc """
  Partition management for distributed RETE networks.

  Manages data partitioning across cluster nodes with consistent hashing,
  automatic rebalancing, and fault tolerance. Ensures optimal distribution
  of facts and network segments across available nodes.
  """

  use GenServer
  require Logger

  alias Presto.Logger, as: PrestoLogger

  @type partition_id :: String.t()
  @type segment_id :: String.t()
  @type node_id :: String.t()

  @type partition_info :: %{
          id: partition_id(),
          primary_nodes: [node_id()],
          replica_nodes: [node_id()],
          segment_assignments: %{segment_id() => [node_id()]},
          fact_count: non_neg_integer(),
          load_metric: float(),
          last_rebalanced: integer()
        }

  @type rebalancing_strategy :: :load_based | :capacity_based | :geographic | :manual

  @type partition_state :: %{
          partitions: %{partition_id() => partition_info()},
          cluster_state: map(),
          replication_factor: pos_integer(),
          partition_count: pos_integer(),
          rebalancing_strategy: rebalancing_strategy(),
          rebalancing_threshold: float(),
          consistent_hash_ring: map(),
          segment_registry: %{segment_id() => partition_id()}
        }

  @default_config %{
    replication_factor: 2,
    partition_count: 16,
    rebalancing_strategy: :load_based,
    rebalancing_threshold: 0.3,
    auto_rebalance: true,
    rebalance_interval: 60_000
  }

  # Client API

  @spec start_link(map(), map(), keyword()) :: GenServer.on_start()
  def start_link(cluster_state, local_node, opts \\ []) do
    config = Map.merge(@default_config, Map.new(opts))
    init_args = %{cluster_state: cluster_state, local_node: local_node, config: config}
    GenServer.start_link(__MODULE__, init_args, name: __MODULE__)
  end

  @spec assign_segment(GenServer.server(), segment_id(), map()) :: :ok | {:error, term()}
  def assign_segment(pid, segment_id, segment_config) do
    GenServer.call(pid, {:assign_segment, segment_id, segment_config})
  end

  @spec unassign_segment(GenServer.server(), segment_id()) :: :ok
  def unassign_segment(pid, segment_id) do
    GenServer.call(pid, {:unassign_segment, segment_id})
  end

  @spec get_partition_for_fact(GenServer.server(), tuple()) ::
          {:ok, partition_id()} | {:error, term()}
  def get_partition_for_fact(pid, fact) do
    GenServer.call(pid, {:get_partition_for_fact, fact})
  end

  @spec get_nodes_for_partition(GenServer.server(), partition_id()) :: [node_id()]
  def get_nodes_for_partition(pid, partition_id) do
    GenServer.call(pid, {:get_nodes_for_partition, partition_id})
  end

  @spec get_partition_info(GenServer.server(), partition_id()) :: partition_info() | nil
  def get_partition_info(pid, partition_id) do
    GenServer.call(pid, {:get_partition_info, partition_id})
  end

  @spec get_all_partitions(GenServer.server()) :: [partition_info()]
  def get_all_partitions(pid) do
    GenServer.call(pid, :get_all_partitions)
  end

  @spec trigger_rebalance(GenServer.server()) :: :ok
  def trigger_rebalance(pid) do
    GenServer.call(pid, :trigger_rebalance)
  end

  @spec handle_node_failure(GenServer.server(), node_id()) :: :ok
  def handle_node_failure(pid, failed_node_id) do
    GenServer.call(pid, {:handle_node_failure, failed_node_id})
  end

  @spec update_partition_load(GenServer.server(), partition_id(), float()) :: :ok
  def update_partition_load(pid, partition_id, load_metric) do
    GenServer.cast(pid, {:update_partition_load, partition_id, load_metric})
  end

  @spec stop(GenServer.server()) :: :ok
  def stop(pid) do
    GenServer.stop(pid, :normal)
  end

  # Server implementation

  @impl true
  def init(%{cluster_state: cluster_state, local_node: local_node, config: config}) do
    # Initialize partition ring
    consistent_hash_ring = build_consistent_hash_ring(cluster_state, config.partition_count)

    # Initialize partitions
    partitions = initialize_partitions(cluster_state, config)

    state = %{
      partitions: partitions,
      cluster_state: cluster_state,
      local_node: local_node,
      replication_factor: config.replication_factor,
      partition_count: config.partition_count,
      rebalancing_strategy: config.rebalancing_strategy,
      rebalancing_threshold: config.rebalancing_threshold,
      consistent_hash_ring: consistent_hash_ring,
      segment_registry: %{},
      config: config
    }

    # Schedule automatic rebalancing if enabled
    if config.auto_rebalance do
      schedule_rebalance_check(config.rebalance_interval)
    end

    PrestoLogger.log_distributed(:info, local_node.id, "partition_manager_started", %{
      partition_count: config.partition_count,
      replication_factor: config.replication_factor
    })

    {:ok, state}
  end

  @impl true
  def handle_call({:assign_segment, segment_id, segment_config}, _from, state) do
    # Determine best partition for this segment
    partition_id = select_optimal_partition(segment_config, state)

    case assign_segment_to_partition(segment_id, partition_id, segment_config, state) do
      {:ok, new_state} ->
        PrestoLogger.log_distributed(:info, state.local_node.id, "segment_assigned", %{
          segment_id: segment_id,
          partition_id: partition_id
        })

        {:reply, :ok, new_state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:unassign_segment, segment_id}, _from, state) do
    case Map.get(state.segment_registry, segment_id) do
      nil ->
        {:reply, :ok, state}

      partition_id ->
        new_state = remove_segment_from_partition(segment_id, partition_id, state)
        {:reply, :ok, new_state}
    end
  end

  @impl true
  def handle_call({:get_partition_for_fact, fact}, _from, state) do
    partition_id = hash_fact_to_partition(fact, state.consistent_hash_ring, state.partition_count)
    {:reply, {:ok, partition_id}, state}
  end

  @impl true
  def handle_call({:get_nodes_for_partition, partition_id}, _from, state) do
    case Map.get(state.partitions, partition_id) do
      nil ->
        {:reply, [], state}

      partition_info ->
        all_nodes = partition_info.primary_nodes ++ partition_info.replica_nodes
        {:reply, Enum.uniq(all_nodes), state}
    end
  end

  @impl true
  def handle_call({:get_partition_info, partition_id}, _from, state) do
    partition_info = Map.get(state.partitions, partition_id)
    {:reply, partition_info, state}
  end

  @impl true
  def handle_call(:get_all_partitions, _from, state) do
    partitions = Map.values(state.partitions)
    {:reply, partitions, state}
  end

  @impl true
  def handle_call(:trigger_rebalance, _from, state) do
    {:ok, new_state} = perform_rebalance(state)

    PrestoLogger.log_distributed(:info, state.local_node.id, "rebalance_completed", %{
      partition_count: map_size(new_state.partitions)
    })

    {:reply, :ok, new_state}
  end

  @impl true
  def handle_call({:handle_node_failure, failed_node_id}, _from, state) do
    {:ok, new_state} = handle_node_failure_internal(failed_node_id, state)

    PrestoLogger.log_distributed(:info, state.local_node.id, "node_failure_handled", %{
      failed_node_id: failed_node_id,
      affected_partitions: count_affected_partitions(failed_node_id, state)
    })

    {:reply, :ok, new_state}
  end

  @impl true
  def handle_cast({:update_partition_load, partition_id, load_metric}, state) do
    case Map.get(state.partitions, partition_id) do
      nil ->
        {:noreply, state}

      partition_info ->
        updated_partition = %{partition_info | load_metric: load_metric}
        updated_partitions = Map.put(state.partitions, partition_id, updated_partition)
        new_state = %{state | partitions: updated_partitions}

        {:noreply, new_state}
    end
  end

  @impl true
  def handle_info(:rebalance_check, state) do
    # Check if rebalancing is needed
    should_rebalance = should_trigger_rebalance?(state)

    new_state =
      if should_rebalance do
        {:ok, rebalanced_state} = perform_rebalance(state)

        PrestoLogger.log_distributed(
          :info,
          state.local_node.id,
          "auto_rebalance_completed",
          %{}
        )

        rebalanced_state
      else
        state
      end

    # Schedule next check
    schedule_rebalance_check(state.config.rebalance_interval)

    {:noreply, new_state}
  end

  # Private implementation functions

  defp build_consistent_hash_ring(_cluster_state, partition_count) do
    # Create hash ring with virtual nodes for better distribution
    virtual_nodes_per_partition = 150

    0..(partition_count - 1)
    |> Enum.flat_map(fn partition_index ->
      0..(virtual_nodes_per_partition - 1)
      |> Enum.map(fn vnode_index ->
        key = "partition_#{partition_index}_vnode_#{vnode_index}"
        hash = :crypto.hash(:sha256, key)
        hash_value = :binary.decode_unsigned(binary_part(hash, 0, 8))
        {hash_value, "partition_#{partition_index}"}
      end)
    end)
    |> Enum.sort_by(fn {hash_value, _partition} -> hash_value end)
    |> Map.new()
  end

  defp initialize_partitions(cluster_state, config) do
    available_nodes = Map.keys(cluster_state.nodes)

    0..(config.partition_count - 1)
    |> Enum.into(%{}, fn partition_index ->
      partition_id = "partition_#{partition_index}"

      # Assign nodes for this partition using round-robin
      {primary_nodes, replica_nodes} =
        assign_nodes_to_partition(
          available_nodes,
          partition_index,
          config.replication_factor
        )

      partition_info = %{
        id: partition_id,
        primary_nodes: primary_nodes,
        replica_nodes: replica_nodes,
        segment_assignments: %{},
        fact_count: 0,
        load_metric: 0.0,
        last_rebalanced: System.monotonic_time(:millisecond)
      }

      {partition_id, partition_info}
    end)
  end

  defp assign_nodes_to_partition(available_nodes, partition_index, replication_factor) do
    node_count = length(available_nodes)

    if node_count == 0 do
      {[], []}
    else
      # Use consistent assignment based on partition index
      primary_index = rem(partition_index, node_count)
      primary_node = Enum.at(available_nodes, primary_index)

      # Select replica nodes (avoiding the primary)
      replica_nodes =
        available_nodes
        |> Enum.with_index()
        |> Enum.filter(fn {_node, index} -> index != primary_index end)
        |> Enum.take(replication_factor - 1)
        |> Enum.map(fn {node, _index} -> node end)

      {[primary_node], replica_nodes}
    end
  end

  defp select_optimal_partition(_segment_config, state) do
    # Select partition based on current load and capacity
    state.partitions
    |> Enum.min_by(fn {_id, partition_info} ->
      # Consider load, segment count, and node availability
      load_factor = partition_info.load_metric
      segment_factor = map_size(partition_info.segment_assignments) * 0.1
      availability_factor = calculate_availability_factor(partition_info, state)

      load_factor + segment_factor + availability_factor
    end)
    |> elem(0)
  end

  defp calculate_availability_factor(partition_info, _state) do
    # Simple availability calculation based on node count
    total_nodes = length(partition_info.primary_nodes) + length(partition_info.replica_nodes)
    # Prefer partitions with more nodes
    max(0.0, 1.0 - total_nodes / 5.0)
  end

  defp assign_segment_to_partition(segment_id, partition_id, segment_config, state) do
    case Map.get(state.partitions, partition_id) do
      nil ->
        {:error, :partition_not_found}

      partition_info ->
        # Determine which nodes should handle this segment
        assigned_nodes = select_nodes_for_segment(partition_info, segment_config)

        # Update partition
        updated_segment_assignments =
          Map.put(
            partition_info.segment_assignments,
            segment_id,
            assigned_nodes
          )

        updated_partition = %{partition_info | segment_assignments: updated_segment_assignments}
        updated_partitions = Map.put(state.partitions, partition_id, updated_partition)
        updated_segment_registry = Map.put(state.segment_registry, segment_id, partition_id)

        new_state = %{
          state
          | partitions: updated_partitions,
            segment_registry: updated_segment_registry
        }

        {:ok, new_state}
    end
  end

  defp select_nodes_for_segment(partition_info, _segment_config) do
    # For now, assign to all available nodes in the partition
    partition_info.primary_nodes ++ partition_info.replica_nodes
  end

  defp remove_segment_from_partition(segment_id, partition_id, state) do
    case Map.get(state.partitions, partition_id) do
      nil ->
        state

      partition_info ->
        updated_segment_assignments = Map.delete(partition_info.segment_assignments, segment_id)
        updated_partition = %{partition_info | segment_assignments: updated_segment_assignments}
        updated_partitions = Map.put(state.partitions, partition_id, updated_partition)
        updated_segment_registry = Map.delete(state.segment_registry, segment_id)

        %{state | partitions: updated_partitions, segment_registry: updated_segment_registry}
    end
  end

  defp hash_fact_to_partition(fact, consistent_hash_ring, _partition_count) do
    # Hash the fact and find the appropriate partition
    fact_hash = :crypto.hash(:sha256, :erlang.term_to_binary(fact))
    hash_value = :binary.decode_unsigned(binary_part(fact_hash, 0, 8))

    # Find the first hash ring entry >= our hash value
    ring_entries = Map.keys(consistent_hash_ring) |> Enum.sort()

    target_ring_hash = Enum.find(ring_entries, fn ring_hash -> ring_hash >= hash_value end)
    # Wrap around
    target_ring_hash = target_ring_hash || hd(ring_entries)

    Map.get(consistent_hash_ring, target_ring_hash)
  end

  defp perform_rebalance(state) do
    # Analyze current distribution
    load_distribution = analyze_load_distribution(state)

    # Determine if rebalancing is beneficial
    if requires_rebalancing?(load_distribution, state.rebalancing_threshold) do
      # Execute rebalancing based on strategy
      execute_rebalancing_strategy(state.rebalancing_strategy, state, load_distribution)
    else
      {:ok, state}
    end
  end

  defp analyze_load_distribution(state) do
    state.partitions
    |> Enum.map(fn {partition_id, partition_info} ->
      %{
        partition_id: partition_id,
        load_metric: partition_info.load_metric,
        segment_count: map_size(partition_info.segment_assignments),
        node_count: length(partition_info.primary_nodes) + length(partition_info.replica_nodes)
      }
    end)
  end

  defp requires_rebalancing?(load_distribution, threshold) do
    loads = Enum.map(load_distribution, & &1.load_metric)

    if Enum.empty?(loads) do
      false
    else
      avg_load = Enum.sum(loads) / length(loads)
      max_load = Enum.max(loads)
      min_load = Enum.min(loads)

      # Check if load imbalance exceeds threshold
      (max_load - min_load) / max(avg_load, 0.001) > threshold
    end
  end

  defp execute_rebalancing_strategy(:load_based, state, load_distribution) do
    # Move segments from high-load to low-load partitions
    perform_load_based_rebalancing(state, load_distribution)
  end

  defp execute_rebalancing_strategy(:capacity_based, state, load_distribution) do
    # Rebalance based on node capacity and resource availability
    perform_capacity_based_rebalancing(state, load_distribution)
  end

  defp execute_rebalancing_strategy(:geographic, state, load_distribution) do
    # Rebalance considering geographic locality and network topology
    perform_geographic_rebalancing(state, load_distribution)
  end

  defp execute_rebalancing_strategy(:manual, state, _load_distribution) do
    # Manual rebalancing requires explicit configuration
    perform_manual_rebalancing(state)
  end

  defp execute_rebalancing_strategy(_strategy, state, _load_distribution) do
    # Fallback for unknown strategies
    PrestoLogger.log_distributed(:warning, state.local_node_id, "unknown_rebalancing_strategy", %{
      strategy: state.rebalancing_strategy
    })

    {:ok, state}
  end

  defp perform_load_based_rebalancing(state, load_distribution) do
    # Calculate target distribution based on node capacities
    rebalancing_plan = calculate_rebalancing_plan(state, load_distribution)

    case execute_rebalancing_plan(rebalancing_plan, state) do
      {:ok, new_state} ->
        # Update rebalancing timestamps for affected partitions
        final_state = update_rebalancing_timestamps(new_state, rebalancing_plan)

        PrestoLogger.log_distributed(:info, state.local_node_id, "load_rebalancing_completed", %{
          moved_segments: count_moved_segments(rebalancing_plan),
          affected_partitions: length(rebalancing_plan.partition_moves)
        })

        {:ok, final_state}

      {:error, reason} ->
        PrestoLogger.log_distributed(:error, state.local_node_id, "rebalancing_failed", %{
          reason: reason
        })

        {:error, reason}
    end
  end

  defp calculate_rebalancing_plan(state, _load_distribution) do
    # Get current node loads and capacities
    node_loads = calculate_current_node_loads(state)
    node_capacities = get_node_capacities(state)

    # Calculate target loads per node based on total load and capacities
    total_load = Enum.sum(Map.values(node_loads))
    total_capacity = Enum.sum(Map.values(node_capacities))

    target_loads =
      Enum.into(node_capacities, %{}, fn {node_id, capacity} ->
        target_load = capacity / total_capacity * total_load
        {node_id, target_load}
      end)

    # Find nodes that are overloaded and underloaded
    {overloaded_nodes, underloaded_nodes} = categorise_nodes_by_load(node_loads, target_loads)

    # Generate partition movement plan
    partition_moves = plan_partition_movements(overloaded_nodes, underloaded_nodes, state)

    %{
      partition_moves: partition_moves,
      target_loads: target_loads,
      estimated_improvement: calculate_load_balance_improvement(node_loads, target_loads),
      movement_cost: estimate_movement_cost(partition_moves)
    }
  end

  defp execute_rebalancing_plan(plan, state) do
    # Execute partition movements sequentially to maintain consistency
    final_state =
      Enum.reduce(plan.partition_moves, state, fn move, acc_state ->
        execute_partition_move(move, acc_state)
      end)

    {:ok, final_state}
  rescue
    error ->
      {:error, {:rebalancing_execution_failed, error}}
  end

  defp execute_partition_move(move, state) do
    %{partition_id: partition_id, from_node: from_node, to_node: to_node, segments: segments} =
      move

    # Get current partition info
    partition_info = Map.get(state.partitions, partition_id)

    # Move segments from source to target node
    updated_partition = move_segments_between_nodes(partition_info, segments, from_node, to_node)

    # Update partition state
    updated_partitions = Map.put(state.partitions, partition_id, updated_partition)

    PrestoLogger.log_distributed(:debug, state.local_node_id, "partition_move_executed", %{
      partition_id: partition_id,
      from_node: from_node,
      to_node: to_node,
      segments_moved: length(segments)
    })

    %{state | partitions: updated_partitions}
  end

  defp move_segments_between_nodes(partition_info, segments, from_node, to_node) do
    # Update segment assignments
    updated_segment_assignments =
      Enum.reduce(segments, partition_info.segment_assignments, fn segment_id, assignments ->
        Map.put(assignments, segment_id, to_node)
      end)

    # Update node assignments
    from_node_segments = Map.get(partition_info.node_assignments, from_node, [])
    to_node_segments = Map.get(partition_info.node_assignments, to_node, [])

    updated_from_segments = from_node_segments -- segments
    updated_to_segments = to_node_segments ++ segments

    updated_node_assignments =
      partition_info.node_assignments
      |> Map.put(from_node, updated_from_segments)
      |> Map.put(to_node, updated_to_segments)

    # Update replication info if needed
    updated_replication_map =
      update_replication_assignments(
        partition_info.replication_map,
        segments,
        from_node,
        to_node
      )

    %{
      partition_info
      | segment_assignments: updated_segment_assignments,
        node_assignments: updated_node_assignments,
        replication_map: updated_replication_map,
        last_modified: System.monotonic_time(:millisecond)
    }
  end

  defp update_replication_assignments(replication_map, segments, from_node, to_node) do
    Enum.reduce(segments, replication_map, fn segment_id, acc_map ->
      update_segment_replication(acc_map, segment_id, from_node, to_node)
    end)
  end

  defp update_segment_replication(replication_map, segment_id, from_node, to_node) do
    case Map.get(replication_map, segment_id) do
      nil ->
        replication_map

      replicas ->
        updated_replicas = update_replica_assignments(replicas, from_node, to_node)
        Map.put(replication_map, segment_id, updated_replicas)
    end
  end

  defp update_replica_assignments(replicas, from_node, to_node) do
    # Update primary replica if it was on the from_node
    Enum.map(replicas, fn
      %{node_id: ^from_node, role: _role} = replica ->
        %{replica | node_id: to_node}

      replica ->
        replica
    end)
  end

  defp calculate_current_node_loads(state) do
    # Calculate load based on number of segments and their sizes
    Enum.reduce(state.partitions, %{}, fn {_partition_id, partition_info}, acc ->
      Enum.reduce(partition_info.node_assignments, acc, fn {node_id, segments}, node_acc ->
        segment_load = calculate_segments_load(segments, partition_info)
        Map.update(node_acc, node_id, segment_load, &(&1 + segment_load))
      end)
    end)
  end

  defp calculate_segments_load(segments, partition_info) do
    # Simple load calculation based on segment count and metadata
    base_load_per_segment = 1.0

    segments
    |> Enum.map(fn segment_id ->
      segment_metadata = Map.get(partition_info.segment_metadata, segment_id, %{})
      size_multiplier = Map.get(segment_metadata, :size_factor, 1.0)
      complexity_multiplier = Map.get(segment_metadata, :complexity_factor, 1.0)

      base_load_per_segment * size_multiplier * complexity_multiplier
    end)
    |> Enum.sum()
  end

  defp get_node_capacities(state) do
    # Get node capacities from configuration or default values
    state.node_capacities
    |> Map.new(fn {node_id, capacity_info} ->
      total_capacity =
        Map.get(capacity_info, :cpu_capacity, 1.0) *
          Map.get(capacity_info, :memory_capacity, 1.0) *
          Map.get(capacity_info, :network_capacity, 1.0)

      {node_id, total_capacity}
    end)
  end

  defp categorise_nodes_by_load(current_loads, target_loads) do
    # 10% tolerance
    threshold = 0.1

    {overloaded, underloaded} =
      Enum.split_with(target_loads, fn {node_id, target_load} ->
        current_load = Map.get(current_loads, node_id, 0.0)
        current_load > target_load * (1 + threshold)
      end)

    overloaded_nodes =
      Enum.map(overloaded, fn {node_id, target_load} ->
        current_load = Map.get(current_loads, node_id, 0.0)
        excess_load = current_load - target_load
        {node_id, excess_load}
      end)

    underloaded_nodes =
      Enum.map(underloaded, fn {node_id, target_load} ->
        current_load = Map.get(current_loads, node_id, 0.0)
        available_capacity = target_load - current_load
        {node_id, available_capacity}
      end)
      |> Enum.filter(fn {_node_id, capacity} -> capacity > 0 end)

    {overloaded_nodes, underloaded_nodes}
  end

  defp plan_partition_movements(overloaded_nodes, underloaded_nodes, state) do
    # Generate partition movement plan using a greedy algorithm
    Enum.flat_map(overloaded_nodes, fn {overloaded_node, excess_load} ->
      plan_movements_for_node(overloaded_node, excess_load, underloaded_nodes, state)
    end)
  end

  defp plan_movements_for_node(from_node, excess_load, underloaded_nodes, state) do
    # Find segments that can be moved from this node
    moveable_segments = find_moveable_segments(from_node, state)

    # Greedily assign segments to underloaded nodes
    {_remaining_load, movements} =
      Enum.reduce(moveable_segments, {excess_load, []}, fn segment_info,
                                                           {remaining_load, acc_moves} ->
        plan_segment_movement(
          segment_info,
          from_node,
          remaining_load,
          acc_moves,
          underloaded_nodes,
          state
        )
      end)

    movements
  end

  defp plan_segment_movement(
         segment_info,
         from_node,
         remaining_load,
         acc_moves,
         underloaded_nodes,
         state
       ) do
    if remaining_load <= 0 do
      {remaining_load, acc_moves}
    else
      attempt_segment_movement(
        segment_info,
        from_node,
        remaining_load,
        acc_moves,
        underloaded_nodes,
        state
      )
    end
  end

  defp attempt_segment_movement(
         segment_info,
         from_node,
         remaining_load,
         acc_moves,
         underloaded_nodes,
         state
       ) do
    case find_best_target_node(segment_info, underloaded_nodes, state) do
      nil ->
        {remaining_load, acc_moves}

      {target_node, _capacity} ->
        create_movement_plan(segment_info, from_node, target_node, remaining_load, acc_moves)
    end
  end

  defp create_movement_plan(segment_info, from_node, target_node, remaining_load, acc_moves) do
    segment_load = Map.get(segment_info, :load, 1.0)

    movement = %{
      partition_id: segment_info.partition_id,
      from_node: from_node,
      to_node: target_node,
      segments: [segment_info.segment_id],
      estimated_load: segment_load
    }

    {remaining_load - segment_load, [movement | acc_moves]}
  end

  defp find_moveable_segments(node_id, state) do
    # Find segments on this node that can be moved (not critical system segments)
    Enum.flat_map(state.partitions, fn {partition_id, partition_info} ->
      node_segments = Map.get(partition_info.node_assignments, node_id, [])

      node_segments
      |> Enum.filter(fn segment_id ->
        segment_metadata = Map.get(partition_info.segment_metadata, segment_id, %{})
        Map.get(segment_metadata, :moveable, true)
      end)
      |> Enum.map(fn segment_id ->
        load = calculate_segments_load([segment_id], partition_info)

        %{
          partition_id: partition_id,
          segment_id: segment_id,
          load: load,
          metadata: Map.get(partition_info.segment_metadata, segment_id, %{})
        }
      end)
    end)
  end

  defp find_best_target_node(segment_info, underloaded_nodes, _state) do
    # Find the underloaded node with the most available capacity
    underloaded_nodes
    |> Enum.filter(fn {_node_id, available_capacity} ->
      available_capacity >= segment_info.load
    end)
    |> Enum.max_by(fn {_node_id, available_capacity} -> available_capacity end, fn -> nil end)
  end

  defp calculate_load_balance_improvement(current_loads, target_loads) do
    # Calculate standard deviation reduction as improvement metric
    current_values = Map.values(current_loads)
    target_values = Map.values(target_loads)

    current_std_dev = calculate_standard_deviation(current_values)
    target_std_dev = calculate_standard_deviation(target_values)

    if current_std_dev > 0 do
      (current_std_dev - target_std_dev) / current_std_dev
    else
      0.0
    end
  end

  defp calculate_standard_deviation(values) do
    count = length(values)

    if count <= 1 do
      0.0
    else
      mean = Enum.sum(values) / count
      variance = Enum.sum(Enum.map(values, fn x -> :math.pow(x - mean, 2) end)) / count
      :math.sqrt(variance)
    end
  end

  defp estimate_movement_cost(partition_moves) do
    # Estimate cost based on data that needs to be moved
    Enum.sum(
      Enum.map(partition_moves, fn move ->
        # Base cost per movement
        base_cost = 1.0
        load_factor = Map.get(move, :estimated_load, 1.0)
        base_cost * load_factor
      end)
    )
  end

  defp count_moved_segments(plan) do
    Enum.sum(Enum.map(plan.partition_moves, fn move -> length(move.segments) end))
  end

  defp update_rebalancing_timestamps(state, plan) do
    affected_partition_ids =
      plan.partition_moves
      |> Enum.map(& &1.partition_id)
      |> Enum.uniq()

    updated_partitions =
      Enum.reduce(affected_partition_ids, state.partitions, fn partition_id, acc ->
        case Map.get(acc, partition_id) do
          nil ->
            acc

          partition_info ->
            updated_partition = %{
              partition_info
              | last_rebalanced: System.monotonic_time(:millisecond)
            }

            Map.put(acc, partition_id, updated_partition)
        end
      end)

    %{state | partitions: updated_partitions}
  end

  defp perform_capacity_based_rebalancing(state, _load_distribution) do
    # Get node capacities from cluster configuration
    node_capacities = get_node_capacity_information(state)

    # Calculate capacity utilization for each node
    capacity_utilization = calculate_capacity_utilization(state, node_capacities)

    # Create rebalancing plan based on capacity constraints
    capacity_plan = create_capacity_based_plan(state, capacity_utilization, node_capacities)

    case execute_rebalancing_plan(capacity_plan, state) do
      {:ok, new_state} ->
        final_state = update_rebalancing_timestamps(new_state, capacity_plan)

        PrestoLogger.log_distributed(
          :info,
          state.local_node_id,
          "capacity_rebalancing_completed",
          %{
            moved_segments: count_moved_segments(capacity_plan),
            capacity_improvement:
              calculate_capacity_improvement(capacity_utilization, final_state)
          }
        )

        {:ok, final_state}

      {:error, reason} ->
        PrestoLogger.log_distributed(
          :error,
          state.local_node_id,
          "capacity_rebalancing_failed",
          %{
            reason: reason
          }
        )

        {:error, reason}
    end
  end

  defp perform_geographic_rebalancing(state, _load_distribution) do
    # Get geographic topology information
    geographic_topology = get_geographic_topology(state)

    # Calculate geographic affinity scores
    affinity_scores = calculate_geographic_affinity(state, geographic_topology)

    # Create rebalancing plan based on geographic constraints
    geographic_plan = create_geographic_plan(state, affinity_scores, geographic_topology)

    case execute_rebalancing_plan(geographic_plan, state) do
      {:ok, new_state} ->
        final_state = update_rebalancing_timestamps(new_state, geographic_plan)

        PrestoLogger.log_distributed(
          :info,
          state.local_node_id,
          "geographic_rebalancing_completed",
          %{
            moved_segments: count_moved_segments(geographic_plan),
            locality_improvement: calculate_locality_improvement(affinity_scores, final_state)
          }
        )

        {:ok, final_state}

      {:error, reason} ->
        PrestoLogger.log_distributed(
          :error,
          state.local_node_id,
          "geographic_rebalancing_failed",
          %{
            reason: reason
          }
        )

        {:error, reason}
    end
  end

  defp perform_manual_rebalancing(state) do
    # Get manual rebalancing configuration
    manual_config = get_manual_rebalancing_config(state)

    case validate_manual_rebalancing_config(manual_config, state) do
      {:ok, validated_config} ->
        # Create plan from manual configuration
        manual_plan = create_manual_rebalancing_plan(validated_config, state)

        case execute_rebalancing_plan(manual_plan, state) do
          {:ok, new_state} ->
            final_state = update_rebalancing_timestamps(new_state, manual_plan)

            PrestoLogger.log_distributed(
              :info,
              state.local_node_id,
              "manual_rebalancing_completed",
              %{
                executed_moves: count_moved_segments(manual_plan),
                configuration_applied: true
              }
            )

            {:ok, final_state}

          {:error, reason} ->
            PrestoLogger.log_distributed(
              :error,
              state.local_node_id,
              "manual_rebalancing_failed",
              %{
                reason: reason
              }
            )

            {:error, reason}
        end

      {:error, validation_error} ->
        PrestoLogger.log_distributed(
          :error,
          state.local_node_id,
          "manual_rebalancing_config_invalid",
          %{
            error: validation_error
          }
        )

        {:error, {:invalid_manual_config, validation_error}}
    end
  end

  defp handle_node_failure_internal(failed_node_id, state) do
    # Find partitions affected by node failure
    affected_partitions = find_partitions_with_node(failed_node_id, state)

    # Remove failed node and reassign if necessary
    updated_partitions =
      Enum.reduce(affected_partitions, state.partitions, fn partition_id, acc ->
        case Map.get(acc, partition_id) do
          nil ->
            acc

          partition_info ->
            updated_partition = remove_node_from_partition(partition_info, failed_node_id, state)
            Map.put(acc, partition_id, updated_partition)
        end
      end)

    new_state = %{state | partitions: updated_partitions}
    {:ok, new_state}
  end

  defp find_partitions_with_node(node_id, state) do
    state.partitions
    |> Enum.filter(fn {_partition_id, partition_info} ->
      node_id in partition_info.primary_nodes or node_id in partition_info.replica_nodes
    end)
    |> Enum.map(fn {partition_id, _partition_info} -> partition_id end)
  end

  defp remove_node_from_partition(partition_info, failed_node_id, _state) do
    updated_primary_nodes = List.delete(partition_info.primary_nodes, failed_node_id)
    updated_replica_nodes = List.delete(partition_info.replica_nodes, failed_node_id)

    %{partition_info | primary_nodes: updated_primary_nodes, replica_nodes: updated_replica_nodes}
  end

  defp count_affected_partitions(node_id, state) do
    length(find_partitions_with_node(node_id, state))
  end

  defp should_trigger_rebalance?(state) do
    load_distribution = analyze_load_distribution(state)
    requires_rebalancing?(load_distribution, state.rebalancing_threshold)
  end

  defp schedule_rebalance_check(interval) do
    Process.send_after(self(), :rebalance_check, interval)
  end

  # Helper functions for additional rebalancing strategies

  ## Capacity-based rebalancing helpers

  defp get_node_capacity_information(state) do
    # Get node capacity from cluster configuration or defaults
    default_capacity = %{
      cpu_cores: 4,
      memory_gb: 16,
      network_bandwidth_mbps: 1000,
      storage_gb: 500
    }

    # In a real implementation, this would query the cluster manager
    state.cluster_state.nodes
    |> Map.new(fn {node_id, node_info} ->
      capacity = Map.get(node_info, :capacity, default_capacity)
      {node_id, capacity}
    end)
  end

  defp calculate_capacity_utilization(state, node_capacities) do
    state.partitions
    |> Enum.flat_map(fn {_partition_id, partition_info} ->
      # Calculate utilization for each node in this partition
      all_nodes = partition_info.primary_nodes ++ partition_info.replica_nodes

      Enum.map(all_nodes, fn node_id ->
        {node_id, calculate_node_partition_utilization(partition_info, node_capacities[node_id])}
      end)
    end)
    |> Enum.group_by(fn {node_id, _utilization} -> node_id end)
    |> Map.new(fn {node_id, utilizations} ->
      total_utilization = Enum.sum(Enum.map(utilizations, fn {_node, util} -> util end))
      capacity = Map.get(node_capacities, node_id, %{cpu_cores: 4})
      utilization_ratio = total_utilization / capacity.cpu_cores
      {node_id, %{total_utilization: total_utilization, utilization_ratio: utilization_ratio}}
    end)
  end

  defp calculate_node_partition_utilization(_partition_info, node_capacity) do
    # Simplified utilization calculation - in practice would be more sophisticated
    # Each partition uses 10% of a core by default
    base_utilization = 0.1
    # Normalize to 16GB baseline
    memory_factor = node_capacity.memory_gb / 16.0
    # Normalize to 4 core baseline
    cpu_factor = node_capacity.cpu_cores / 4.0

    base_utilization / max(cpu_factor * memory_factor, 0.1)
  end

  defp create_capacity_based_plan(state, capacity_utilization, _node_capacities) do
    # Find overutilized and underutilized nodes
    # 80% capacity threshold
    {overutilized_nodes, underutilized_nodes} =
      categorize_nodes_by_capacity(capacity_utilization, 0.8)

    # Create partition moves to balance capacity
    partition_moves =
      plan_capacity_based_movements(overutilized_nodes, underutilized_nodes, state)

    %{
      partition_moves: partition_moves,
      strategy: :capacity_based,
      estimated_improvement:
        calculate_capacity_balance_improvement(capacity_utilization, partition_moves),
      movement_cost: estimate_movement_cost(partition_moves)
    }
  end

  defp categorize_nodes_by_capacity(capacity_utilization, threshold) do
    Enum.split_with(capacity_utilization, fn {_node_id, utilization} ->
      utilization.utilization_ratio > threshold
    end)
  end

  defp plan_capacity_based_movements(overutilized_nodes, underutilized_nodes, state) do
    # Simple greedy algorithm to move partitions from overutilized to underutilized nodes
    Enum.flat_map(overutilized_nodes, fn {overloaded_node, utilization} ->
      excess_utilization = utilization.utilization_ratio - 0.8

      # Find partitions this node participates in
      node_partitions = find_partitions_for_node(overloaded_node, state)

      # Select partitions to move (simple selection by ID)
      partitions_to_move = Enum.take(node_partitions, max(1, round(excess_utilization * 2)))

      # Find target nodes with available capacity
      Enum.map(partitions_to_move, fn partition_id ->
        target_node = select_target_node_by_capacity(underutilized_nodes)

        %{
          partition_id: partition_id,
          from_node: overloaded_node,
          to_node: target_node,
          # Simplified
          segments: ["segment_#{partition_id}"],
          estimated_load: excess_utilization / length(partitions_to_move)
        }
      end)
    end)
  end

  defp find_partitions_for_node(node_id, state) do
    state.partitions
    |> Enum.filter(fn {_partition_id, partition_info} ->
      node_id in partition_info.primary_nodes or node_id in partition_info.replica_nodes
    end)
    |> Enum.map(fn {partition_id, _partition_info} -> partition_id end)
  end

  defp select_target_node_by_capacity(underutilized_nodes) do
    # Select the node with the lowest utilization
    case Enum.min_by(
           underutilized_nodes,
           fn {_node_id, utilization} -> utilization.utilization_ratio end,
           fn -> nil end
         ) do
      {node_id, _utilization} -> node_id
      # Fallback
      nil -> "default_node"
    end
  end

  defp calculate_capacity_improvement(_capacity_utilization, _final_state) do
    # Calculate improvement in capacity balance
    # Placeholder - 15% improvement
    0.15
  end

  defp calculate_capacity_balance_improvement(_capacity_utilization, _partition_moves) do
    # Placeholder - 20% improvement
    0.2
  end

  ## Geographic rebalancing helpers

  defp get_geographic_topology(state) do
    # Get geographic information about nodes from cluster configuration
    default_topology = %{
      regions: %{
        "us-east-1" => ["node1", "node2"],
        "us-west-1" => ["node3", "node4"],
        "eu-west-1" => ["node5", "node6"]
      },
      availability_zones: %{
        "us-east-1a" => ["node1"],
        "us-east-1b" => ["node2"],
        "us-west-1a" => ["node3"],
        "us-west-1b" => ["node4"],
        "eu-west-1a" => ["node5"],
        "eu-west-1b" => ["node6"]
      },
      network_latency: %{
        # ms
        {"us-east-1", "us-west-1"} => 70,
        # ms
        {"us-east-1", "eu-west-1"} => 120,
        # ms
        {"us-west-1", "eu-west-1"} => 140
      }
    }

    # In practice, this would come from cluster configuration
    Map.get(state.config, :geographic_topology, default_topology)
  end

  defp calculate_geographic_affinity(state, geographic_topology) do
    # Calculate affinity scores between nodes based on geographic proximity
    regions = geographic_topology.regions

    state.partitions
    |> Map.new(fn {partition_id, partition_info} ->
      all_nodes = partition_info.primary_nodes ++ partition_info.replica_nodes

      # Calculate geographic spread of this partition
      node_regions =
        Enum.map(all_nodes, fn node_id ->
          find_node_region(node_id, regions)
        end)

      unique_regions = Enum.uniq(node_regions)
      geographic_spread = length(unique_regions)

      # Lower spread is better for locality
      affinity_score = 1.0 / max(geographic_spread, 1)

      {partition_id,
       %{
         nodes: all_nodes,
         regions: node_regions,
         geographic_spread: geographic_spread,
         affinity_score: affinity_score
       }}
    end)
  end

  defp find_node_region(node_id, regions) do
    case Enum.find(regions, fn {_region, nodes} -> node_id in nodes end) do
      {region, _nodes} -> region
      nil -> "unknown"
    end
  end

  defp create_geographic_plan(_state, affinity_scores, geographic_topology) do
    # Find partitions with poor geographic locality
    poorly_located_partitions =
      affinity_scores
      |> Enum.filter(fn {_partition_id, scores} -> scores.affinity_score < 0.5 end)
      # Limit moves to avoid disruption
      |> Enum.take(5)

    # Create moves to improve geographic locality
    partition_moves =
      Enum.map(poorly_located_partitions, fn {partition_id, scores} ->
        # Find better geographic placement
        target_region = select_optimal_region(scores, geographic_topology)
        target_node = select_node_in_region(target_region, geographic_topology)
        current_nodes = scores.nodes

        %{
          partition_id: partition_id,
          # Move from first node
          from_node: hd(current_nodes),
          to_node: target_node,
          segments: ["segment_#{partition_id}"],
          estimated_load: 1.0,
          geographic_improvement: 0.5 - scores.affinity_score
        }
      end)

    %{
      partition_moves: partition_moves,
      strategy: :geographic,
      estimated_improvement: calculate_geographic_improvement(affinity_scores),
      movement_cost: estimate_movement_cost(partition_moves)
    }
  end

  defp select_optimal_region(_scores, geographic_topology) do
    # Select region with most available capacity
    regions = Map.keys(geographic_topology.regions)
    # Simple selection - could be more sophisticated
    Enum.random(regions)
  end

  defp select_node_in_region(region, geographic_topology) do
    nodes = Map.get(geographic_topology.regions, region, [])

    case nodes do
      [] -> "default_node"
      [node | _] -> node
    end
  end

  defp calculate_locality_improvement(_affinity_scores, _final_state) do
    # Placeholder - 25% locality improvement
    0.25
  end

  defp calculate_geographic_improvement(_affinity_scores) do
    # Placeholder - 30% geographic improvement
    0.3
  end

  ## Manual rebalancing helpers

  defp get_manual_rebalancing_config(state) do
    # Get manual rebalancing configuration from state or config
    Map.get(state.config, :manual_rebalancing, %{
      enabled: false,
      moves: []
    })
  end

  defp validate_manual_rebalancing_config(config, state) do
    case config do
      %{enabled: true, moves: moves} when is_list(moves) ->
        case validate_manual_moves(moves, state) do
          :ok -> {:ok, config}
          {:error, reason} -> {:error, reason}
        end

      %{enabled: false} ->
        {:error, :manual_rebalancing_disabled}

      _ ->
        {:error, :invalid_manual_config_format}
    end
  end

  defp validate_manual_moves(moves, state) do
    # Validate that all specified moves are valid
    invalid_moves =
      Enum.filter(moves, fn move ->
        not valid_manual_move?(move, state)
      end)

    if Enum.empty?(invalid_moves) do
      :ok
    else
      {:error, {:invalid_moves, invalid_moves}}
    end
  end

  defp valid_manual_move?(move, state) do
    required_fields = [:partition_id, :from_node, :to_node]

    # Check required fields exist
    has_required_fields = Enum.all?(required_fields, &Map.has_key?(move, &1))

    # Check partition exists
    partition_exists = Map.has_key?(state.partitions, move.partition_id)

    # Check nodes exist in cluster
    all_nodes = Map.keys(state.cluster_state.nodes)
    from_node_exists = move.from_node in all_nodes
    to_node_exists = move.to_node in all_nodes

    has_required_fields and partition_exists and from_node_exists and to_node_exists
  end

  defp create_manual_rebalancing_plan(config, _state) do
    # Convert manual configuration to standard rebalancing plan format
    partition_moves =
      Enum.map(config.moves, fn move ->
        %{
          partition_id: move.partition_id,
          from_node: move.from_node,
          to_node: move.to_node,
          segments: Map.get(move, :segments, ["segment_#{move.partition_id}"]),
          estimated_load: Map.get(move, :estimated_load, 1.0),
          manual_move: true
        }
      end)

    %{
      partition_moves: partition_moves,
      strategy: :manual,
      # No automatic improvement calculation for manual
      estimated_improvement: 0.0,
      movement_cost: estimate_movement_cost(partition_moves)
    }
  end
end

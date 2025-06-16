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

  defp execute_rebalancing_strategy(_strategy, state, _load_distribution) do
    # Other strategies not implemented yet
    {:ok, state}
  end

  defp perform_load_based_rebalancing(state, _load_distribution) do
    # For now, just mark rebalancing timestamp - full implementation would move segments
    updated_partitions =
      state.partitions
      |> Enum.into(%{}, fn {partition_id, partition_info} ->
        updated_partition = %{
          partition_info
          | last_rebalanced: System.monotonic_time(:millisecond)
        }

        {partition_id, updated_partition}
      end)

    new_state = %{state | partitions: updated_partitions}
    {:ok, new_state}
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
end

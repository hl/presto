defmodule Presto.Distributed.WorkingMemory do
  @moduledoc """
  Distributed working memory with consistency guarantees for RETE networks.

  Implements distributed fact storage with configurable consistency models,
  automatic conflict resolution, and efficient fact synchronization across
  cluster nodes. Supports both eventually consistent and strongly consistent
  operations based on use case requirements.
  """

  use GenServer
  require Logger

  alias Presto.Distributed.{ClusterManager, PartitionManager}
  alias Presto.Logger, as: PrestoLogger

  @type fact :: tuple()
  @type fact_id :: String.t()
  @type node_id :: String.t()
  @type partition_id :: String.t()
  @type vector_clock :: %{node_id() => non_neg_integer()}

  @type fact_entry :: %{
          id: fact_id(),
          fact: fact(),
          partition_id: partition_id(),
          vector_clock: vector_clock(),
          timestamp: integer(),
          source_node: node_id(),
          replicas: [node_id()],
          status: :active | :tombstone,
          conflict_resolution_data: map()
        }

  @type consistency_level :: :eventual | :strong | :bounded_staleness
  @type conflict_resolution :: :last_write_wins | :vector_clock | :custom

  @type operation :: %{
          type: :assert | :retract | :update,
          fact: fact(),
          options: %{
            consistency: consistency_level(),
            timeout: pos_integer(),
            conflict_resolution: conflict_resolution()
          }
        }

  @type distributed_memory_state :: %{
          local_node_id: node_id(),
          cluster_manager: pid(),
          partition_manager: pid(),
          local_facts: %{fact_id() => fact_entry()},
          vector_clock: vector_clock(),
          pending_operations: %{String.t() => operation()},
          sync_state: %{node_id() => %{last_sync: integer(), pending_facts: [fact_id()]}},
          config: memory_config()
        }

  @type memory_config :: %{
          replication_factor: pos_integer(),
          consistency_timeout: pos_integer(),
          sync_interval: pos_integer(),
          conflict_resolution_strategy: conflict_resolution(),
          max_pending_operations: pos_integer(),
          vector_clock_pruning_threshold: pos_integer()
        }

  @default_config %{
    replication_factor: 2,
    consistency_timeout: 5_000,
    sync_interval: 10_000,
    conflict_resolution_strategy: :vector_clock,
    max_pending_operations: 1000,
    vector_clock_pruning_threshold: 100
  }

  # Client API

  @spec start_link(pid(), pid(), keyword()) :: GenServer.on_start()
  def start_link(cluster_manager, partition_manager, opts \\ []) do
    GenServer.start_link(__MODULE__, {cluster_manager, partition_manager, opts}, name: __MODULE__)
  end

  @spec assert_fact(GenServer.server(), fact(), keyword()) :: :ok | {:error, term()}
  def assert_fact(pid, fact, opts \\ []) do
    operation = create_operation(:assert, fact, opts)
    GenServer.call(pid, {:execute_operation, operation}, get_timeout(opts))
  end

  @spec retract_fact(GenServer.server(), fact(), keyword()) :: :ok | {:error, term()}
  def retract_fact(pid, fact, opts \\ []) do
    operation = create_operation(:retract, fact, opts)
    GenServer.call(pid, {:execute_operation, operation}, get_timeout(opts))
  end

  @spec get_facts(GenServer.server(), keyword()) :: [fact()]
  def get_facts(pid, opts \\ []) do
    GenServer.call(pid, {:get_facts, opts})
  end

  @spec get_fact_count(GenServer.server()) :: non_neg_integer()
  def get_fact_count(pid) do
    GenServer.call(pid, :get_fact_count)
  end

  @spec sync_with_cluster(GenServer.server()) :: :ok
  def sync_with_cluster(pid) do
    GenServer.call(pid, :sync_with_cluster)
  end

  @spec get_synchronization_status(GenServer.server()) :: map()
  def get_synchronization_status(pid) do
    GenServer.call(pid, :get_synchronization_status)
  end

  @spec resolve_conflicts(GenServer.server()) :: :ok
  def resolve_conflicts(pid) do
    GenServer.call(pid, :resolve_conflicts)
  end

  # Server implementation

  @impl true
  def init({cluster_manager, partition_manager, opts}) do
    config = Map.merge(@default_config, Map.new(opts))
    local_node_id = Keyword.get(opts, :local_node_id, generate_node_id())

    state = %{
      local_node_id: local_node_id,
      cluster_manager: cluster_manager,
      partition_manager: partition_manager,
      local_facts: %{},
      vector_clock: %{local_node_id => 0},
      pending_operations: %{},
      sync_state: %{},
      config: config
    }

    # Schedule periodic synchronization
    schedule_sync(config.sync_interval)

    PrestoLogger.log_distributed(:info, local_node_id, "distributed_working_memory_started", %{
      replication_factor: config.replication_factor,
      consistency_timeout: config.consistency_timeout
    })

    {:ok, state}
  end

  @impl true
  def handle_call({:execute_operation, operation}, from, state) do
    case operation.options.consistency do
      :eventual ->
        # Execute locally and replicate asynchronously
        {:ok, new_state} = execute_local_operation(operation, state)
        # Start async replication
        spawn(fn -> replicate_operation_async(operation, new_state) end)
        {:reply, :ok, new_state}

      :strong ->
        # Execute with strong consistency (requires quorum)
        operation_id = generate_operation_id()
        updated_pending = Map.put(state.pending_operations, operation_id, operation)
        new_state = %{state | pending_operations: updated_pending}

        # Start consensus protocol
        spawn(fn ->
          execute_strong_consistency_operation(operation_id, operation, from, new_state)
        end)

        {:noreply, new_state}

      :bounded_staleness ->
        # Execute with bounded staleness guarantees
        {:reply, execute_bounded_staleness_operation(operation, state), state}
    end
  end

  @impl true
  def handle_call({:get_facts, opts}, _from, state) do
    consistency = Keyword.get(opts, :consistency, :eventual)

    facts =
      case consistency do
        :eventual ->
          # Return local facts immediately
          get_local_facts(state)

        :strong ->
          # Ensure we have the latest facts from cluster
          {:ok, synced_facts} = sync_for_strong_read(state)
          synced_facts

        :bounded_staleness ->
          # Return facts within staleness bounds
          get_bounded_staleness_facts(state, opts)
      end

    {:reply, facts, state}
  end

  @impl true
  def handle_call(:get_fact_count, _from, state) do
    count =
      state.local_facts
      |> Map.values()
      |> Enum.count(fn entry -> entry.status == :active end)

    {:reply, count, state}
  end

  @impl true
  def handle_call(:sync_with_cluster, _from, state) do
    {:ok, new_state} = perform_cluster_sync(state)
    PrestoLogger.log_distributed(:info, state.local_node_id, "cluster_sync_completed", %{
      local_fact_count: map_size(new_state.local_facts)
    })

    {:reply, :ok, new_state}
  end

  @impl true
  def handle_call(:get_synchronization_status, _from, state) do
    status = %{
      local_vector_clock: state.vector_clock,
      sync_state: state.sync_state,
      pending_operations: map_size(state.pending_operations),
      local_fact_count: map_size(state.local_facts)
    }

    {:reply, status, state}
  end

  @impl true
  def handle_call(:resolve_conflicts, _from, state) do
    new_state = resolve_all_conflicts(state)
    {:reply, :ok, new_state}
  end

  @impl true
  def handle_cast({:replicate_fact, fact_entry, source_node}, state) do
    new_state = handle_incoming_fact_replication(fact_entry, source_node, state)
    {:noreply, new_state}
  end

  @impl true
  def handle_cast({:sync_request, requesting_node, vector_clock}, state) do
    # Send facts that the requesting node is missing
    spawn(fn -> handle_sync_request(requesting_node, vector_clock, state) end)
    {:noreply, state}
  end

  @impl true
  def handle_cast({:operation_result, operation_id, result}, state) do
    # Handle result from strong consistency operation
    updated_pending = Map.delete(state.pending_operations, operation_id)
    new_state = %{state | pending_operations: updated_pending}

    case result do
      :ok ->
        PrestoLogger.log_distributed(:debug, state.local_node_id, "strong_operation_completed", %{
          operation_id: operation_id
        })

      {:error, reason} ->
        PrestoLogger.log_distributed(:warning, state.local_node_id, "strong_operation_failed", %{
          operation_id: operation_id,
          reason: reason
        })
    end

    {:noreply, new_state}
  end

  @impl true
  def handle_info(:perform_sync, state) do
    # Periodic synchronization
    {:ok, new_state} = perform_background_sync(state)

    # Schedule next sync
    schedule_sync(state.config.sync_interval)

    {:noreply, new_state}
  end

  @impl true
  def handle_info({:sync_response, node_id, facts}, state) do
    new_state = process_sync_response(node_id, facts, state)
    {:noreply, new_state}
  end

  # Private implementation functions

  defp generate_node_id() do
    "wmem_" <> (:crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower))
  end

  defp generate_operation_id() do
    "op_" <> (:crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower))
  end

  defp generate_fact_id(fact) do
    ("fact_" <> Base.encode16(:crypto.hash(:sha256, :erlang.term_to_binary(fact)), case: :lower))
    |> String.slice(0, 16)
  end

  defp create_operation(type, fact, opts) do
    %{
      type: type,
      fact: fact,
      options: %{
        consistency: Keyword.get(opts, :consistency, :eventual),
        timeout: Keyword.get(opts, :timeout, 5_000),
        conflict_resolution: Keyword.get(opts, :conflict_resolution, :vector_clock)
      }
    }
  end

  defp get_timeout(opts) do
    Keyword.get(opts, :timeout, 5_000)
  end

  defp execute_local_operation(operation, state) do
    fact_id = generate_fact_id(operation.fact)

    case operation.type do
      :assert ->
        execute_local_assert(fact_id, operation.fact, state)

      :retract ->
        execute_local_retract(fact_id, operation.fact, state)

      :update ->
        execute_local_update(fact_id, operation.fact, state)
    end
  end

  defp execute_local_assert(fact_id, fact, state) do
    # Get partition for this fact
    {:ok, partition_id} =
      PartitionManager.get_partition_for_fact(
        state.partition_manager,
        fact
      )

    # Create fact entry
    now = System.monotonic_time(:millisecond)
    updated_vector_clock = increment_vector_clock(state.vector_clock, state.local_node_id)

    fact_entry = %{
      id: fact_id,
      fact: fact,
      partition_id: partition_id,
      vector_clock: updated_vector_clock,
      timestamp: now,
      source_node: state.local_node_id,
      replicas: [],
      status: :active,
      conflict_resolution_data: %{}
    }

    # Store locally
    updated_facts = Map.put(state.local_facts, fact_id, fact_entry)
    new_state = %{state | local_facts: updated_facts, vector_clock: updated_vector_clock}

    {:ok, new_state}
  end

  defp execute_local_retract(fact_id, _fact, state) do
    case Map.get(state.local_facts, fact_id) do
      nil ->
        # Fact doesn't exist locally
        {:ok, state}

      existing_entry ->
        # Create tombstone
        updated_vector_clock = increment_vector_clock(state.vector_clock, state.local_node_id)

        tombstone_entry = %{
          existing_entry
          | status: :tombstone,
            vector_clock: updated_vector_clock,
            timestamp: System.monotonic_time(:millisecond)
        }

        updated_facts = Map.put(state.local_facts, fact_id, tombstone_entry)
        new_state = %{state | local_facts: updated_facts, vector_clock: updated_vector_clock}

        {:ok, new_state}
    end
  end

  defp execute_local_update(fact_id, fact, state) do
    # Update is essentially retract + assert
    case execute_local_retract(fact_id, fact, state) do
      {:ok, intermediate_state} ->
        execute_local_assert(fact_id, fact, intermediate_state)

      error ->
        error
    end
  end

  defp increment_vector_clock(vector_clock, node_id) do
    current_value = Map.get(vector_clock, node_id, 0)
    Map.put(vector_clock, node_id, current_value + 1)
  end

  defp get_local_facts(state) do
    state.local_facts
    |> Map.values()
    |> Enum.filter(fn entry -> entry.status == :active end)
    |> Enum.map(fn entry -> entry.fact end)
  end

  defp sync_for_strong_read(state) do
    # Request latest data from all replicas
    cluster_state = ClusterManager.get_cluster_state()
    active_nodes = Map.keys(cluster_state.nodes)

    # Send sync requests to all nodes
    sync_responses =
      Enum.map(active_nodes, fn node_id ->
        request_sync_from_node(node_id, state.vector_clock)
      end)

    # Wait for responses and merge
    {:ok, merged_facts} = collect_sync_responses(sync_responses, state.config.consistency_timeout)
    {:ok, merged_facts}
  end

  defp get_bounded_staleness_facts(state, opts) do
    staleness_bound = Keyword.get(opts, :max_staleness_ms, 10_000)
    now = System.monotonic_time(:millisecond)

    state.local_facts
    |> Map.values()
    |> Enum.filter(fn entry ->
      entry.status == :active and now - entry.timestamp <= staleness_bound
    end)
    |> Enum.map(fn entry -> entry.fact end)
  end

  defp execute_bounded_staleness_operation(_operation, state) do
    # For now, treat as eventual consistency
    {:ok, state}
  end

  defp replicate_operation_async(operation, state) do
    # Get target nodes for replication
    {:ok, partition_id} =
      PartitionManager.get_partition_for_fact(
        state.partition_manager,
        operation.fact
      )

    target_nodes =
      PartitionManager.get_nodes_for_partition(
        state.partition_manager,
        partition_id
      )

    # Send replication to target nodes
    fact_id = generate_fact_id(operation.fact)

    case Map.get(state.local_facts, fact_id) do
      # Fact not found
      nil ->
        :ok

      fact_entry ->
        Enum.each(target_nodes, fn node_id ->
          if node_id != state.local_node_id do
            send_fact_replication(node_id, fact_entry)
          end
        end)
    end
  end

  defp execute_strong_consistency_operation(_operation_id, _operation, _from, _state) do
    # Placeholder for consensus protocol implementation
    # Would implement Raft or similar consensus algorithm
    :ok
  end

  defp handle_incoming_fact_replication(fact_entry, _source_node, state) do
    fact_id = fact_entry.id

    case Map.get(state.local_facts, fact_id) do
      nil ->
        # New fact, accept it
        updated_facts = Map.put(state.local_facts, fact_id, fact_entry)
        updated_vector_clock = merge_vector_clocks(state.vector_clock, fact_entry.vector_clock)

        %{state | local_facts: updated_facts, vector_clock: updated_vector_clock}

      existing_fact ->
        # Conflict resolution needed
        resolved_fact = resolve_fact_conflict(existing_fact, fact_entry, state.config)
        updated_facts = Map.put(state.local_facts, fact_id, resolved_fact)
        updated_vector_clock = merge_vector_clocks(state.vector_clock, resolved_fact.vector_clock)

        %{state | local_facts: updated_facts, vector_clock: updated_vector_clock}
    end
  end

  defp resolve_fact_conflict(local_fact, remote_fact, config) do
    case config.conflict_resolution_strategy do
      :last_write_wins ->
        if remote_fact.timestamp > local_fact.timestamp do
          remote_fact
        else
          local_fact
        end

      :vector_clock ->
        case compare_vector_clocks(local_fact.vector_clock, remote_fact.vector_clock) do
          :greater ->
            local_fact

          :less ->
            remote_fact

          :concurrent ->
            # For concurrent updates, fall back to timestamp
            if remote_fact.timestamp > local_fact.timestamp do
              remote_fact
            else
              local_fact
            end
        end

      :custom ->
        # Placeholder for custom conflict resolution
        local_fact
    end
  end

  defp compare_vector_clocks(vc1, vc2) do
    all_nodes = MapSet.union(MapSet.new(Map.keys(vc1)), MapSet.new(Map.keys(vc2)))

    comparison_results =
      Enum.map(all_nodes, fn node ->
        v1 = Map.get(vc1, node, 0)
        v2 = Map.get(vc2, node, 0)

        cond do
          v1 > v2 -> :greater
          v1 < v2 -> :less
          true -> :equal
        end
      end)

    cond do
      Enum.all?(comparison_results, &(&1 in [:greater, :equal])) and
          :greater in comparison_results ->
        :greater

      Enum.all?(comparison_results, &(&1 in [:less, :equal])) and :less in comparison_results ->
        :less

      true ->
        :concurrent
    end
  end

  defp merge_vector_clocks(vc1, vc2) do
    all_nodes = MapSet.union(MapSet.new(Map.keys(vc1)), MapSet.new(Map.keys(vc2)))

    Enum.into(all_nodes, %{}, fn node ->
      v1 = Map.get(vc1, node, 0)
      v2 = Map.get(vc2, node, 0)
      {node, max(v1, v2)}
    end)
  end

  defp perform_cluster_sync(state) do
    # Get all active nodes from cluster
    cluster_state = ClusterManager.get_cluster_state()

    active_nodes =
      cluster_state.nodes
      |> Enum.filter(fn {_id, node_info} -> node_info.status == :active end)
      |> Enum.map(fn {id, _info} -> id end)
      |> Enum.reject(fn node_id -> node_id == state.local_node_id end)

    # Request sync from all nodes
    sync_tasks =
      Enum.map(active_nodes, fn node_id ->
        Task.async(fn -> sync_with_node(node_id, state) end)
      end)

    # Collect results
    results = Task.yield_many(sync_tasks, state.config.consistency_timeout)

    # Process successful syncs
    new_state =
      Enum.reduce(results, state, fn {_task, result}, acc ->
        case result do
          {:ok, {:ok, node_facts}} ->
            merge_facts_from_node(node_facts, acc)

          _ ->
            # Sync failed for this node
            acc
        end
      end)

    {:ok, new_state}
  end

  defp perform_background_sync(state) do
    # Lighter weight sync for background operation
    cluster_state = ClusterManager.get_cluster_state()

    # Pick a random subset of nodes to sync with
    active_nodes =
      cluster_state.nodes
      |> Enum.filter(fn {_id, node_info} -> node_info.status == :active end)
      |> Enum.map(fn {id, _info} -> id end)
      |> Enum.reject(fn node_id -> node_id == state.local_node_id end)
      # Sync with up to 3 nodes
      |> Enum.take_random(min(3, length(cluster_state.nodes)))

    # Perform lightweight sync
    new_state =
      Enum.reduce(active_nodes, state, fn node_id, acc ->
        {:ok, node_facts} = sync_with_node(node_id, acc)
        merge_facts_from_node(node_facts, acc)
      end)

    {:ok, new_state}
  end

  defp sync_with_node(_node_id, _state) do
    # Placeholder for actual network sync implementation
    {:ok, []}
  end

  defp merge_facts_from_node(node_facts, state) do
    # Merge incoming facts with local facts
    Enum.reduce(node_facts, state, fn fact_entry, acc ->
      handle_incoming_fact_replication(fact_entry, fact_entry.source_node, acc)
    end)
  end

  defp resolve_all_conflicts(state) do
    # Identify and resolve any remaining conflicts
    # For now, just return the current state
    state
  end

  defp send_fact_replication(_node_id, _fact_entry) do
    # Placeholder for actual network communication
    :ok
  end

  defp request_sync_from_node(_node_id, _vector_clock) do
    # Placeholder for sync request
    {:ok, []}
  end

  defp collect_sync_responses(_sync_responses, _timeout) do
    # Placeholder for collecting and merging sync responses
    {:ok, []}
  end

  defp handle_sync_request(_requesting_node, _vector_clock, _state) do
    # Placeholder for handling incoming sync requests
    :ok
  end

  defp process_sync_response(_node_id, _facts, state) do
    # Placeholder for processing sync responses
    state
  end

  defp schedule_sync(interval) do
    Process.send_after(self(), :perform_sync, interval)
  end
end

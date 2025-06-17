defmodule Presto.BetaNetwork do
  @moduledoc """
  Beta network implementation for the RETE algorithm.

  The beta network handles joining facts from different sources (alpha nodes or other beta nodes)
  and stores partial matches for efficient incremental processing.
  """

  use GenServer

  alias Presto.AlphaNetwork
  alias Presto.Optimization.AdvancedIndexing
  alias Presto.Optimization.JoinOptimizer
  alias Presto.Optimization.SharedMemoryManager

  @type join_condition :: {:join, String.t(), String.t(), atom() | [atom()]}
  @type beta_node :: %{
          id: String.t(),
          left_input: String.t(),
          right_input: String.t(),
          join_keys: [atom()],
          left_type: :alpha | :beta,
          right_type: :alpha | :beta,
          # Shared memory optimization fields
          shared_memory_ref: String.t() | nil,
          memory_signature: term() | nil,
          # Advanced indexing fields
          advanced_index: map() | nil,
          index_enabled: boolean()
        }

  # Client API

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @spec create_beta_node(GenServer.server(), join_condition()) :: {:ok, String.t()}
  def create_beta_node(pid, join_condition) do
    GenServer.call(pid, {:create_beta_node, join_condition})
  end

  @spec remove_beta_node(GenServer.server(), String.t()) :: :ok
  def remove_beta_node(pid, node_id) do
    GenServer.call(pid, {:remove_beta_node, node_id})
  end

  @spec get_beta_nodes(GenServer.server()) :: %{String.t() => beta_node()}
  def get_beta_nodes(pid) do
    GenServer.call(pid, :get_beta_nodes)
  end

  @spec get_beta_node_info(GenServer.server(), String.t()) :: beta_node() | nil
  def get_beta_node_info(pid, node_id) do
    GenServer.call(pid, {:get_beta_node_info, node_id})
  end

  @spec process_alpha_changes(GenServer.server()) :: :ok
  def process_alpha_changes(pid) do
    GenServer.cast(pid, :process_alpha_changes)
  end

  @spec mark_alpha_node_changed(GenServer.server(), String.t()) :: :ok
  def mark_alpha_node_changed(pid, alpha_node_id) do
    GenServer.cast(pid, {:mark_alpha_node_changed, alpha_node_id})
  end

  @spec configure_join_optimization(GenServer.server(), keyword()) :: :ok
  def configure_join_optimization(pid, opts) do
    GenServer.call(pid, {:configure_join_optimization, opts})
  end

  @spec get_beta_memory(GenServer.server(), String.t()) :: [map()]
  def get_beta_memory(pid, node_id) do
    GenServer.call(pid, {:get_beta_memory, node_id})
  end

  @spec get_partial_matches(GenServer.server(), String.t()) :: [map()]
  def get_partial_matches(pid, node_id) do
    GenServer.call(pid, {:get_partial_matches, node_id})
  end

  @spec clear_beta_memory(GenServer.server(), String.t()) :: :ok
  def clear_beta_memory(pid, node_id) do
    GenServer.call(pid, {:clear_beta_memory, node_id})
  end

  @spec get_process_count(GenServer.server()) :: integer()
  def get_process_count(pid) do
    GenServer.call(pid, :get_process_count)
  end

  # Server implementation

  @impl true
  def init(opts) do
    alpha_network = Keyword.fetch!(opts, :alpha_network)

    state = %{
      alpha_network: alpha_network,
      beta_nodes: %{},
      # ETS table for beta memories (complete joins)
      beta_memories: :ets.new(:beta_memories, [:set, :private]),
      # ETS table for partial matches (incomplete joins)
      partial_matches: :ets.new(:partial_matches, [:set, :private]),
      # Track processing for performance monitoring
      process_count: 0,
      # Configuration for join optimization
      join_config: %{
        # Min dataset size for hash joins
        optimization_threshold: 10,
        # Max entries in join index
        max_index_size: 10_000,
        # Join ordering optimization settings
        enable_join_reordering: true,
        cost_threshold: 100,
        selectivity_learning: true,
        # Advanced indexing settings
        enable_advanced_indexing: true,
        indexing_threshold: 50,
        index_rebuild_frequency: 1000
      },
      # Track which alpha nodes have changed for incremental processing
      changed_alpha_nodes: MapSet.new(),
      # Dependency graph cache for efficient change propagation
      dependency_cache: :ets.new(:dependency_cache, [:set, :private])
    }

    {:ok, state}
  end

  @impl true
  def handle_call({:create_beta_node, join_condition}, _from, state) do
    node_id = generate_node_id()

    case parse_join_condition(join_condition) do
      {:ok, beta_node} ->
        node = Map.put(beta_node, :id, node_id)

        # Add optimization fields
        node =
          Map.merge(node, %{
            shared_memory_ref: nil,
            memory_signature: nil,
            advanced_index: nil,
            index_enabled: state.join_config.enable_advanced_indexing
          })

        new_nodes = Map.put(state.beta_nodes, node_id, node)

        # Initialize empty memories for this node
        :ets.insert(state.beta_memories, {node_id, []})
        :ets.insert(state.partial_matches, {node_id, []})

        new_state = %{state | beta_nodes: new_nodes}
        {:reply, {:ok, node_id}, new_state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:remove_beta_node, node_id}, _from, state) do
    # Clean up shared memory reference if exists
    case Map.get(state.beta_nodes, node_id) do
      %{shared_memory_ref: ref} when not is_nil(ref) ->
        if shared_memory_available?() do
          SharedMemoryManager.release_shared_memory(ref)
        end

      _ ->
        :ok
    end

    new_nodes = Map.delete(state.beta_nodes, node_id)
    :ets.delete(state.beta_memories, node_id)
    :ets.delete(state.partial_matches, node_id)

    new_state = %{state | beta_nodes: new_nodes}
    {:reply, :ok, new_state}
  end

  @impl true
  def handle_call(:get_beta_nodes, _from, state) do
    {:reply, state.beta_nodes, state}
  end

  @impl true
  def handle_call({:get_beta_node_info, node_id}, _from, state) do
    node_info = Map.get(state.beta_nodes, node_id)
    {:reply, node_info, state}
  end

  @impl true
  def handle_call({:get_beta_memory, node_id}, _from, state) do
    case :ets.lookup(state.beta_memories, node_id) do
      [{^node_id, matches}] -> {:reply, matches, state}
      [] -> {:reply, [], state}
    end
  end

  @impl true
  def handle_call({:get_partial_matches, node_id}, _from, state) do
    case :ets.lookup(state.partial_matches, node_id) do
      [{^node_id, matches}] -> {:reply, matches, state}
      [] -> {:reply, [], state}
    end
  end

  @impl true
  def handle_call({:clear_beta_memory, node_id}, _from, state) do
    :ets.insert(state.beta_memories, {node_id, []})
    :ets.insert(state.partial_matches, {node_id, []})
    {:reply, :ok, state}
  end

  @impl true
  def handle_call(:get_process_count, _from, state) do
    {:reply, state.process_count, state}
  end

  @impl true
  def handle_call({:configure_join_optimization, opts}, _from, state) do
    new_config = Map.merge(state.join_config, Map.new(opts))
    new_state = %{state | join_config: new_config}
    {:reply, :ok, new_state}
  end

  @impl true
  def handle_cast(:process_alpha_changes, state) do
    # Use incremental processing if changes are tracked, otherwise full processing
    new_state =
      if MapSet.size(state.changed_alpha_nodes) > 0 do
        process_incremental_joins(state)
      else
        # Fallback to full processing for backward compatibility
        process_all_joins(state)
      end

    {:noreply, new_state}
  end

  @impl true
  def handle_cast({:mark_alpha_node_changed, alpha_node_id}, state) do
    new_changed_nodes = MapSet.put(state.changed_alpha_nodes, alpha_node_id)
    new_state = %{state | changed_alpha_nodes: new_changed_nodes}
    {:noreply, new_state}
  end

  @impl true
  def terminate(_reason, state) do
    :ets.delete(state.beta_memories)
    :ets.delete(state.partial_matches)
    :ets.delete(state.dependency_cache)
    :ok
  end

  # Private functions

  defp generate_node_id do
    "beta_" <> Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)
  end

  defp parse_join_condition({:join, left_input, right_input, join_keys}) do
    # Determine input types (alpha or beta)
    left_type = if String.starts_with?(left_input, "alpha_"), do: :alpha, else: :beta
    right_type = if String.starts_with?(right_input, "alpha_"), do: :alpha, else: :beta

    # Normalize join keys to list
    normalized_keys =
      case join_keys do
        key when is_atom(key) -> [key]
        keys when is_list(keys) -> keys
        _ -> {:error, :invalid_join_keys}
      end

    case normalized_keys do
      {:error, reason} ->
        {:error, reason}

      keys ->
        {:ok,
         %{
           left_input: left_input,
           right_input: right_input,
           join_keys: keys,
           left_type: left_type,
           right_type: right_type
         }}
    end
  end

  defp parse_join_condition(_) do
    {:error, :invalid_join_condition}
  end

  # Incremental join processing - only process affected nodes
  defp process_incremental_joins(state) do
    if MapSet.size(state.changed_alpha_nodes) == 0 do
      # No changes, no processing needed
      state
    else
      # Get cached dependency graph
      dependency_graph = get_cached_dependency_graph(state)

      # Find beta nodes affected by changed alpha nodes
      affected_beta_nodes = find_affected_beta_nodes(state.changed_alpha_nodes, dependency_graph)

      # Process only affected nodes in dependency order
      ordered_affected =
        order_affected_nodes_by_dependencies(affected_beta_nodes, dependency_graph)

      new_state =
        Enum.reduce(ordered_affected, state, fn {node_id, node}, acc_state ->
          process_beta_node_joins(node_id, node, acc_state)
        end)

      # Clear change tracking
      %{new_state | process_count: new_state.process_count + 1, changed_alpha_nodes: MapSet.new()}
    end
  end

  # Fallback: Process all joins when needed (e.g., first run, cache invalidation)
  defp process_all_joins(state) do
    # Use selectivity-based join ordering optimization
    ordered_nodes =
      if state.join_config.enable_join_reordering do
        optimize_join_order(state.beta_nodes, state.join_config)
      else
        order_beta_nodes_by_dependencies(state.beta_nodes)
      end

    new_state =
      Enum.reduce(ordered_nodes, state, fn {node_id, node}, acc_state ->
        process_beta_node_joins(node_id, node, acc_state)
      end)

    %{new_state | process_count: new_state.process_count + 1, changed_alpha_nodes: MapSet.new()}
  end

  defp order_beta_nodes_by_dependencies(beta_nodes) do
    # Sort nodes so that those with alpha inputs come before those with beta inputs
    Enum.sort_by(beta_nodes, fn {_node_id, node} ->
      case {node.left_type, node.right_type} do
        # Both inputs are alpha nodes - process first
        {:alpha, :alpha} -> 0
        # One input is beta - process later
        {:alpha, :beta} -> 1
        # One input is beta - process later
        {:beta, :alpha} -> 1
        # Both inputs are beta - process last
        {:beta, :beta} -> 2
      end
    end)
  end

  defp process_beta_node_joins(node_id, node, state) do
    # Get input data from left and right sources
    left_data = get_input_data(node.left_input, node.left_type, state)
    right_data = get_input_data(node.right_input, node.right_type, state)

    # Create memory signature for shared memory optimization
    memory_signature = create_memory_signature(left_data, right_data, node.join_keys)

    # Try to use shared memory for identical join patterns
    {joins, updated_node} =
      get_or_compute_joins(
        memory_signature,
        left_data,
        right_data,
        node,
        state
      )

    # Update selectivity statistics for join optimization learning
    if state.join_config.selectivity_learning do
      combined_input_data = left_data ++ right_data
      JoinOptimizer.update_selectivity_statistics(node_id, combined_input_data, joins)
    end

    # Update beta memory
    :ets.insert(state.beta_memories, {node_id, joins})

    # Update partial matches (for now, simple implementation)
    partial_matches = find_partial_matches(left_data, right_data, node.join_keys)
    :ets.insert(state.partial_matches, {node_id, partial_matches})

    # Update node in state if shared memory reference changed
    new_nodes =
      if updated_node.shared_memory_ref != node.shared_memory_ref do
        Map.put(state.beta_nodes, node_id, updated_node)
      else
        state.beta_nodes
      end

    %{state | beta_nodes: new_nodes}
  end

  defp get_input_data(input_id, :alpha, state) do
    # Get data from alpha network
    AlphaNetwork.get_alpha_memory(state.alpha_network, input_id)
  end

  defp get_input_data(input_id, :beta, state) do
    # Get data from another beta node
    case :ets.lookup(state.beta_memories, input_id) do
      [{^input_id, matches}] -> matches
      [] -> []
    end
  end

  # Main join function that chooses strategy based on data size
  @spec perform_joins([map()], [map()], [atom()], map(), beta_node()) :: [map()]
  defp perform_joins(left_data, right_data, join_keys, state, node) do
    left_size = length(left_data)
    right_size = length(right_data)
    threshold = state.join_config.optimization_threshold

    if left_size >= threshold and right_size >= threshold do
      hash_join(left_data, right_data, join_keys, node, state)
    else
      # Use cartesian join for small datasets where overhead isn't worth it
      cartesian_join(left_data, right_data, join_keys)
    end
  end

  # Optimized O(N+M) hash join implementation with advanced indexing
  @spec hash_join([map()], [map()], [atom()], beta_node(), map()) :: [map()]
  defp hash_join(left_data, right_data, join_keys, node, state) do
    # Check if we should use advanced indexing
    if should_use_advanced_indexing?(left_data, right_data, join_keys, node, state) do
      advanced_hash_join(left_data, right_data, join_keys, node, state)
    else
      # Use standard hash join
      standard_hash_join(left_data, right_data, join_keys)
    end
  end

  defp standard_hash_join(left_data, right_data, join_keys) do
    # Choose smaller dataset for index to minimize memory usage
    {build_side, probe_side, left_is_build} =
      if length(left_data) <= length(right_data) do
        {left_data, right_data, true}
      else
        {right_data, left_data, false}
      end

    # Build hash index on join keys
    hash_index = build_join_index(build_side, join_keys)

    # Probe index with other dataset
    probe_side
    |> Enum.flat_map(&probe_hash_join(&1, hash_index, join_keys, left_is_build))
  end

  @spec advanced_hash_join([map()], [map()], [atom()], beta_node(), map()) :: [map()]
  defp advanced_hash_join(left_data, right_data, join_keys, node, state) do
    # Use advanced indexing for complex joins
    {build_side, probe_side, left_is_build} = choose_build_probe_sides(left_data, right_data)

    # Create or update advanced index
    updated_node = ensure_advanced_index(node, build_side, join_keys, state)

    # Use advanced index for lookup
    results =
      probe_advanced_index(probe_side, updated_node.advanced_index, join_keys, left_is_build)

    results
  end

  defp probe_hash_join(probe_record, hash_index, join_keys, left_is_build) do
    join_key_values = extract_join_key_values(probe_record, join_keys)

    case Map.get(hash_index, join_key_values) do
      nil -> []
      matching_records -> merge_matching_records(matching_records, probe_record, left_is_build)
    end
  end

  defp merge_matching_records(matching_records, probe_record, left_is_build) do
    Enum.map(matching_records, &merge_record_pair(&1, probe_record, left_is_build))
  end

  defp merge_record_pair(build_record, probe_record, true),
    do: Map.merge(build_record, probe_record)

  defp merge_record_pair(build_record, probe_record, false),
    do: Map.merge(probe_record, build_record)

  # Build hash index on join keys
  defp build_join_index(records, join_keys) do
    Enum.reduce(records, %{}, fn record, acc ->
      key_values = extract_join_key_values(record, join_keys)

      # Only index records that have all join keys
      if valid_join_keys?(record, join_keys) do
        Map.update(acc, key_values, [record], &[record | &1])
      else
        acc
      end
    end)
  end

  # Extract join key values for indexing
  defp extract_join_key_values(record, join_keys) do
    Enum.map(join_keys, &Map.get(record, &1))
  end

  # Validate that record has all required join keys
  defp valid_join_keys?(record, join_keys) do
    Enum.all?(join_keys, &Map.has_key?(record, &1))
  end

  # Rename existing cartesian join for small datasets
  defp cartesian_join(left_data, right_data, join_keys) do
    # Original cartesian product implementation for small datasets
    for left_match <- left_data,
        right_match <- right_data,
        join_condition_met?(left_match, right_match, join_keys) do
      # Merge the matches
      Map.merge(left_match, right_match)
    end
  end

  defp join_condition_met?(left_match, right_match, join_keys) do
    Enum.all?(join_keys, fn key ->
      Map.get(left_match, key) == Map.get(right_match, key) and
        Map.has_key?(left_match, key) and
        Map.has_key?(right_match, key)
    end)
  end

  defp find_partial_matches(left_data, right_data, join_keys) do
    # Find left-side matches that don't have corresponding right-side matches
    left_without_right =
      for left_match <- left_data,
          not has_matching_right?(left_match, right_data, join_keys) do
        left_match
      end

    # Find right-side matches that don't have corresponding left-side matches
    right_without_left =
      for right_match <- right_data,
          not has_matching_left?(right_match, left_data, join_keys) do
        right_match
      end

    left_without_right ++ right_without_left
  end

  defp has_matching_right?(left_match, right_data, join_keys) do
    Enum.any?(right_data, fn right_match ->
      join_condition_met?(left_match, right_match, join_keys)
    end)
  end

  defp has_matching_left?(right_match, left_data, join_keys) do
    Enum.any?(left_data, fn left_match ->
      join_condition_met?(left_match, right_match, join_keys)
    end)
  end

  # Helper functions for incremental processing

  defp get_cached_dependency_graph(state) do
    case :ets.lookup(state.dependency_cache, :graph) do
      [{:graph, cached_graph}] ->
        cached_graph

      [] ->
        graph = build_dependency_graph(state.beta_nodes)
        :ets.insert(state.dependency_cache, {:graph, graph})
        graph
    end
  end

  defp build_dependency_graph(beta_nodes) do
    # Build a map of alpha_node -> [dependent_beta_nodes]
    Enum.reduce(beta_nodes, %{}, fn {node_id, node}, acc ->
      acc
      |> add_dependency_if_alpha(node.left_input, node.left_type, {node_id, node})
      |> add_dependency_if_alpha(node.right_input, node.right_type, {node_id, node})
    end)
  end

  defp add_dependency_if_alpha(dependency_map, input_id, :alpha, beta_node) do
    Map.update(dependency_map, input_id, [beta_node], &[beta_node | &1])
  end

  defp add_dependency_if_alpha(dependency_map, _input_id, :beta, _beta_node) do
    dependency_map
  end

  defp find_affected_beta_nodes(changed_alpha_nodes, dependency_graph) do
    changed_alpha_nodes
    |> Enum.flat_map(fn alpha_node_id ->
      Map.get(dependency_graph, alpha_node_id, [])
    end)
    |> Enum.uniq()
  end

  defp order_affected_nodes_by_dependencies(affected_nodes, _dependency_graph) do
    # For now, use the same ordering as process_all_joins
    # In a more advanced implementation, we could do topological sorting
    Enum.sort_by(affected_nodes, fn {_node_id, node} ->
      case {node.left_type, node.right_type} do
        {:alpha, :alpha} -> 0
        {:alpha, :beta} -> 1
        {:beta, :alpha} -> 1
        {:beta, :beta} -> 2
      end
    end)
  end

  # Selectivity-based join ordering optimization
  defp optimize_join_order(beta_nodes, join_config) do
    # Convert to list for optimization
    nodes_list = Enum.map(beta_nodes, fn {_id, node} -> node end)

    # Get optimization plan from JoinOptimizer
    optimization_plan = JoinOptimizer.optimize_join_order(nodes_list, join_config)

    if optimization_plan.optimisation_applied do
      # Use optimized order
      optimization_plan.ordered_nodes
      |> Enum.map(fn node_id -> {node_id, beta_nodes[node_id]} end)
      |> Enum.filter(fn {_id, node} -> not is_nil(node) end)
    else
      # Fall back to dependency-based ordering
      order_beta_nodes_by_dependencies(beta_nodes)
    end
  end

  # Helper functions for shared memory optimization

  defp create_memory_signature(left_data, right_data, join_keys) do
    # Create a deterministic signature based on input data and join keys
    # This is used to identify identical join computations across different nodes
    left_signature = create_data_signature(left_data)
    right_signature = create_data_signature(right_data)
    join_signature = :crypto.hash(:sha256, :erlang.term_to_binary(join_keys))

    {left_signature, right_signature, join_signature}
  end

  defp create_data_signature(data) when is_list(data) do
    # Create a hash of the data content for signature matching
    # Sort by a canonical representation to ensure consistent signatures
    canonical_data = Enum.sort_by(data, &:erlang.term_to_binary/1)
    :crypto.hash(:sha256, :erlang.term_to_binary(canonical_data))
  end

  defp get_or_compute_joins(memory_signature, left_data, right_data, node, state) do
    shared_memory_key = {memory_signature, node.join_keys}

    # Check if we already have this computation in shared memory
    case node.shared_memory_ref do
      nil ->
        # No shared memory reference, try to create one
        compute_and_share_joins(shared_memory_key, left_data, right_data, node, state)

      existing_ref ->
        handle_existing_shared_memory(
          existing_ref,
          memory_signature,
          shared_memory_key,
          left_data,
          right_data,
          node,
          state
        )
    end
  end

  defp handle_existing_shared_memory(
         existing_ref,
         memory_signature,
         shared_memory_key,
         left_data,
         right_data,
         node,
         state
       ) do
    # Check if shared memory is still valid for this signature
    if signature_matches_and_available?(node, memory_signature) do
      try_reuse_shared_memory(existing_ref, shared_memory_key, left_data, right_data, node, state)
    else
      recompute_after_cleanup(existing_ref, shared_memory_key, left_data, right_data, node, state)
    end
  end

  defp signature_matches_and_available?(node, memory_signature) do
    node.memory_signature == memory_signature and shared_memory_available?()
  end

  defp try_reuse_shared_memory(
         existing_ref,
         shared_memory_key,
         left_data,
         right_data,
         node,
         state
       ) do
    case SharedMemoryManager.get_shared_memory(existing_ref) do
      {:ok, shared_joins} ->
        {shared_joins, node}

      {:error, :not_found} ->
        # Shared memory was cleaned up, recompute
        compute_and_share_joins(shared_memory_key, left_data, right_data, node, state)
    end
  end

  defp recompute_after_cleanup(
         existing_ref,
         shared_memory_key,
         left_data,
         right_data,
         node,
         state
       ) do
    # Signature changed or shared memory not available, need to recompute
    if shared_memory_available?() do
      SharedMemoryManager.release_shared_memory(existing_ref)
    end

    compute_and_share_joins(shared_memory_key, left_data, right_data, node, state)
  end

  defp compute_and_share_joins(shared_memory_key, left_data, right_data, node, state) do
    # Perform the actual join computation
    joins = perform_joins(left_data, right_data, node.join_keys, state, node)

    # Store in shared memory if beneficial and available
    if should_share_memory?(joins) and shared_memory_available?() do
      case SharedMemoryManager.get_or_create_shared_memory(shared_memory_key, joins) do
        {:ok, memory_ref} ->
          # Update node with shared memory reference
          updated_node = %{
            node
            | shared_memory_ref: memory_ref,
              memory_signature: elem(shared_memory_key, 0)
          }

          {joins, updated_node}

        {:error, _reason} ->
          # Failed to create shared memory, use local storage
          {joins, node}
      end
    else
      # Result too small to benefit from sharing or shared memory not available
      {joins, node}
    end
  end

  defp should_share_memory?(joins) when is_list(joins) do
    # Enable shared memory for moderately large result sets
    # Too small: overhead isn't worth it
    # Too large: may consume too much memory
    join_count = length(joins)
    join_count >= 10 and join_count <= 1000
  end

  defp shared_memory_available? do
    # Check if SharedMemoryManager is available by trying to get its PID
    case GenServer.whereis(SharedMemoryManager) do
      nil -> false
      _pid -> true
    end
  end

  # Advanced indexing helper functions

  defp should_use_advanced_indexing?(left_data, right_data, join_keys, node, state) do
    # Use advanced indexing if:
    # 1. Node has indexing enabled
    # 2. Dataset is large enough
    # 3. Multiple join keys (complex join)
    # 4. State has advanced indexing enabled

    node_supports_indexing = node && node.index_enabled

    data_size_sufficient =
      length(left_data) + length(right_data) >=
        ((state && state.join_config.indexing_threshold) || 50)

    complex_join = length(join_keys) > 1

    node_supports_indexing && data_size_sufficient && complex_join
  end

  defp choose_build_probe_sides(left_data, right_data) do
    # Choose smaller dataset for index building
    if length(left_data) <= length(right_data) do
      {left_data, right_data, true}
    else
      {right_data, left_data, false}
    end
  end

  @spec ensure_advanced_index(beta_node(), [map()], [atom()], map()) :: beta_node()
  defp ensure_advanced_index(node, build_data, join_keys, state) when is_list(join_keys) do
    # Create or update advanced index for this node
    if node.advanced_index == nil do
      # Create new advanced index
      # Ensure join_keys are atoms for type safety
      atom_join_keys = ensure_atom_keys(join_keys)

      index_config = %{
        type: :composite,
        keys: atom_join_keys,
        threshold: state.join_config.indexing_threshold,
        max_size: state.join_config.max_index_size,
        rebuild_frequency: state.join_config.index_rebuild_frequency
      }

      advanced_index = AdvancedIndexing.create_multi_level_index(atom_join_keys, index_config)
      updated_index = AdvancedIndexing.index_data(advanced_index, build_data)

      %{node | advanced_index: updated_index}
    else
      # Update existing index with new data
      updated_index = AdvancedIndexing.rebuild_indexes_if_needed(node.advanced_index, build_data)
      %{node | advanced_index: updated_index}
    end
  end

  defp probe_advanced_index(probe_data, advanced_index, join_keys, left_is_build) do
    # Use advanced index to find matching records
    Enum.flat_map(probe_data, fn probe_record ->
      process_probe_record(probe_record, advanced_index, join_keys, left_is_build)
    end)
  end

  defp process_probe_record(probe_record, advanced_index, join_keys, left_is_build) do
    # Create lookup criteria from probe record
    lookup_criteria = create_lookup_criteria(probe_record, join_keys)

    # Lookup using advanced index
    matching_records =
      AdvancedIndexing.lookup_with_advanced_index(advanced_index, lookup_criteria)

    # Merge probe record with matching records
    merge_probe_with_matches(probe_record, matching_records, left_is_build)
  end

  defp create_lookup_criteria(probe_record, join_keys) do
    Enum.reduce(join_keys, %{}, fn key, acc ->
      if Map.has_key?(probe_record, key) do
        Map.put(acc, key, Map.get(probe_record, key))
      else
        acc
      end
    end)
  end

  defp merge_probe_with_matches(probe_record, matching_records, left_is_build) do
    Enum.map(matching_records, fn match_record ->
      if left_is_build do
        Map.merge(match_record, probe_record)
      else
        Map.merge(probe_record, match_record)
      end
    end)
  end

  # Helper function to validate join keys are atoms for type safety
  @spec ensure_atom_keys([atom()]) :: [atom()]
  defp ensure_atom_keys(keys) when is_list(keys) do
    # Since the type spec requires [atom()], we should only get atoms
    # This function validates that assumption
    Enum.each(keys, fn
      key when is_atom(key) -> :ok
      key -> raise ArgumentError, "Expected atom, got: #{inspect(key)}"
    end)

    keys
  end
end

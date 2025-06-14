defmodule Presto.BetaNetwork do
  @moduledoc """
  Beta network implementation for the RETE algorithm.

  The beta network handles joining facts from different sources (alpha nodes or other beta nodes)
  and stores partial matches for efficient incremental processing.
  """

  use GenServer

  alias Presto.AlphaNetwork

  @type join_condition :: {:join, String.t(), String.t(), atom() | [atom()]}
  @type beta_node :: %{
          id: String.t(),
          left_input: String.t(),
          right_input: String.t(),
          join_keys: [atom()],
          left_type: :alpha | :beta,
          right_type: :alpha | :beta
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
      # NEW: Configuration for join optimization
      join_config: %{
        # Min dataset size for hash joins
        optimization_threshold: 10,
        # Max entries in join index
        max_index_size: 10_000
      },
      # NEW: Track which alpha nodes have changed for incremental processing
      changed_alpha_nodes: MapSet.new(),
      # NEW: Dependency graph cache for efficient change propagation
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

  # NEW: Incremental join processing - only process affected nodes
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
    # Process beta nodes in dependency order (nodes that don't depend on other betas first)
    ordered_nodes = order_beta_nodes_by_dependencies(state.beta_nodes)

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

    # Perform joins using optimized algorithm
    joins = perform_joins(left_data, right_data, node.join_keys, state)

    # Update beta memory
    :ets.insert(state.beta_memories, {node_id, joins})

    # Update partial matches (for now, simple implementation)
    partial_matches = find_partial_matches(left_data, right_data, node.join_keys)
    :ets.insert(state.partial_matches, {node_id, partial_matches})

    state
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

  # NEW: Main join function that chooses strategy based on data size
  defp perform_joins(left_data, right_data, join_keys, state) do
    left_size = length(left_data)
    right_size = length(right_data)
    threshold = state.join_config.optimization_threshold

    if left_size >= threshold and right_size >= threshold do
      hash_join(left_data, right_data, join_keys)
    else
      # Use cartesian join for small datasets where overhead isn't worth it
      cartesian_join(left_data, right_data, join_keys)
    end
  end

  # NEW: Optimized O(N+M) hash join implementation
  defp hash_join(left_data, right_data, join_keys) do
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
    |> Enum.flat_map(fn probe_record ->
      join_key_values = extract_join_key_values(probe_record, join_keys)

      case Map.get(hash_index, join_key_values) do
        nil ->
          []

        matching_records ->
          Enum.map(matching_records, fn build_record ->
            if left_is_build do
              Map.merge(build_record, probe_record)
            else
              Map.merge(probe_record, build_record)
            end
          end)
      end
    end)
  end

  # NEW: Build hash index on join keys
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

  # NEW: Extract join key values for indexing
  defp extract_join_key_values(record, join_keys) do
    Enum.map(join_keys, &Map.get(record, &1))
  end

  # NEW: Validate that record has all required join keys
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

  # NEW: Helper functions for incremental processing

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
end

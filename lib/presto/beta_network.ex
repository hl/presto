defmodule Presto.BetaNetwork do
  @moduledoc """
  Beta network implementation for the RETE algorithm.

  The beta network handles joining facts from different sources (alpha nodes or other beta nodes)
  and stores partial matches for efficient incremental processing.
  """

  use GenServer

  @type join_condition :: {:join, String.t(), String.t(), atom() | [atom()]}
  @type aggregation_spec :: {:aggregate, String.t(), [atom()], atom(), atom() | nil}
  @type node_type :: :join | :aggregation

  @type beta_node :: %{
          id: String.t(),
          type: node_type(),
          # Join node fields
          left_input: String.t() | nil,
          right_input: String.t() | nil,
          join_keys: [atom()],
          left_type: :alpha | :beta | nil,
          right_type: :alpha | :beta | nil,
          # Aggregation node fields
          input_source: String.t() | nil,
          group_by: [atom()],
          aggregate_fn: atom() | nil,
          aggregate_field: atom() | nil
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

  @spec create_aggregation_node(GenServer.server(), aggregation_spec()) :: {:ok, String.t()}
  def create_aggregation_node(pid, aggregation_spec) do
    GenServer.call(pid, {:create_aggregation_node, aggregation_spec})
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
    rule_engine = Keyword.fetch!(opts, :rule_engine)
    alpha_memories_table = Keyword.fetch!(opts, :alpha_memories_table)

    state = %{
      rule_engine: rule_engine,
      alpha_memories_table: alpha_memories_table,
      beta_nodes: %{},
      # ETS table for beta memories (complete joins) - optimized for concurrent access
      beta_memories: :ets.new(:beta_memories, [
        :set, :public, 
        {:read_concurrency, true}, 
        {:write_concurrency, true}
      ]),
      # ETS table for partial matches (incomplete joins) - optimized for concurrent access
      partial_matches: :ets.new(:partial_matches, [
        :set, :public, 
        {:read_concurrency, true}, 
        {:write_concurrency, true}
      ]),
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
        node = Map.put(beta_node, :id, node_id) |> Map.put(:type, :join)

        # Simple node without optimization fields

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
  def handle_call({:create_aggregation_node, aggregation_spec}, _from, state) do
    node_id = generate_node_id()

    case parse_aggregation_spec(aggregation_spec) do
      {:ok, aggregation_node} ->
        node = Map.put(aggregation_node, :id, node_id) |> Map.put(:type, :aggregation)

        new_nodes = Map.put(state.beta_nodes, node_id, node)

        # Initialize empty memory for aggregation results
        :ets.insert(state.beta_memories, {node_id, []})

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

  defp parse_aggregation_spec({:aggregate, input_source, group_by, aggregate_fn, aggregate_field}) do
    # Validate aggregate function
    valid_aggregate_fns = [:sum, :count, :avg, :min, :max, :collect]

    if aggregate_fn in valid_aggregate_fns do
      {:ok,
       %{
         input_source: input_source,
         group_by: List.wrap(group_by),
         aggregate_fn: aggregate_fn,
         aggregate_field: aggregate_field,
         # Set nil for join-specific fields
         left_input: nil,
         right_input: nil,
         join_keys: [],
         left_type: nil,
         right_type: nil
       }}
    else
      {:error, {:invalid_aggregate_function, aggregate_fn}}
    end
  end

  defp parse_aggregation_spec(_) do
    {:error, :invalid_aggregation_spec}
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
      case Map.get(node, :type) do
        :aggregation ->
          # Aggregation nodes should be processed after their inputs
          # but we treat them like alpha-input nodes for now
          0

        _ ->
          # Join nodes
          case {node.left_type, node.right_type} do
            # Both inputs are alpha nodes - process first
            {:alpha, :alpha} -> 0
            # One input is beta - process later
            {:alpha, :beta} -> 1
            # One input is beta - process later
            {:beta, :alpha} -> 1
            # Both inputs are beta - process last
            {:beta, :beta} -> 2
            # Handle nil case (shouldn't happen but let's be safe)
            _ -> 0
          end
      end
    end)
  end

  defp process_beta_node_joins(node_id, node, state) do
    case Map.get(node, :type) do
      :join ->
        process_join_node(node_id, node, state)

      :aggregation ->
        process_aggregation_node(node_id, node, state)

      nil ->
        # Legacy support - assume join node
        process_join_node(node_id, node, state)
    end
  end

  defp process_join_node(node_id, node, state) do
    # Get input data from left and right sources
    left_data = get_input_data(node.left_input, node.left_type, state)
    right_data = get_input_data(node.right_input, node.right_type, state)

    # Perform simple joins
    joins = perform_joins(left_data, right_data, node.join_keys, state, node)

    # Update beta memory
    :ets.insert(state.beta_memories, {node_id, joins})

    # Update partial matches
    partial_matches = find_partial_matches(left_data, right_data, node.join_keys)
    :ets.insert(state.partial_matches, {node_id, partial_matches})

    state
  end

  defp process_aggregation_node(node_id, node, state) do
    # Get input data from source
    input_type = if String.starts_with?(node.input_source, "alpha_"), do: :alpha, else: :beta
    input_data = get_input_data(node.input_source, input_type, state)

    # Perform aggregation
    aggregated_results =
      perform_aggregation(
        input_data,
        node.group_by,
        node.aggregate_fn,
        node.aggregate_field
      )

    # Update beta memory with aggregation results
    :ets.insert(state.beta_memories, {node_id, aggregated_results})

    state
  end

  defp get_input_data(input_id, :alpha, state) do
    # Get data directly from rule engine's alpha_memories ETS table to avoid deadlock
    # The rule engine stores alpha memories in a public ETS table
    case :ets.lookup(state.alpha_memories_table, input_id) do
      [{^input_id, matches}] -> matches
      [] -> []
    end
  end

  defp get_input_data(input_id, :beta, state) do
    # Get data from another beta node
    case :ets.lookup(state.beta_memories, input_id) do
      [{^input_id, matches}] -> matches
      [] -> []
    end
  end

  # Simplified join function
  @spec perform_joins([map()], [map()], [atom()], map(), beta_node()) :: [map()]
  defp perform_joins(left_data, right_data, join_keys, _state, _node) do
    # Simple cartesian join approach
    cartesian_join(left_data, right_data, join_keys)
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
      case Map.get(node, :type) do
        :aggregation ->
          # Aggregation nodes should be processed after their inputs
          0

        _ ->
          # Join nodes
          case {node.left_type, node.right_type} do
            {:alpha, :alpha} -> 0
            {:alpha, :beta} -> 1
            {:beta, :alpha} -> 1
            {:beta, :beta} -> 2
            _ -> 0
          end
      end
    end)
  end

  # Simplified join ordering (removed optimization dependency)
  defp optimize_join_order(beta_nodes, _join_config) do
    # Fall back to dependency-based ordering
    order_beta_nodes_by_dependencies(beta_nodes)
  end

  # Aggregation functions

  defp perform_aggregation(input_data, group_by, aggregate_fn, aggregate_field) do
    # Group data by the specified fields
    grouped_data =
      Enum.group_by(input_data, fn row ->
        Enum.map(group_by, &Map.get(row, &1))
      end)

    # Apply aggregation function to each group
    Enum.map(grouped_data, fn {group_values, rows} ->
      # Create result map with group keys
      result = Enum.zip(group_by, group_values) |> Map.new()

      # Add aggregated value
      aggregated_value = apply_aggregate_fn(rows, aggregate_fn, aggregate_field)

      # Add the result with a key based on the aggregate function
      Map.put(result, aggregate_result_key(aggregate_fn, aggregate_field), aggregated_value)
    end)
  end

  defp apply_aggregate_fn(rows, :count, _field) do
    length(rows)
  end

  defp apply_aggregate_fn(rows, :sum, field) do
    rows
    |> Enum.map(&Map.get(&1, field, 0))
    |> Enum.sum()
  end

  defp apply_aggregate_fn(rows, :avg, field) do
    values = Enum.map(rows, &Map.get(&1, field, 0))

    if length(values) > 0 do
      Enum.sum(values) / length(values)
    else
      0
    end
  end

  defp apply_aggregate_fn(rows, :min, field) do
    rows
    |> Enum.map(&Map.get(&1, field))
    |> Enum.min(fn -> nil end)
  end

  defp apply_aggregate_fn(rows, :max, field) do
    rows
    |> Enum.map(&Map.get(&1, field))
    |> Enum.max(fn -> nil end)
  end

  defp apply_aggregate_fn(rows, :collect, field) do
    Enum.map(rows, &Map.get(&1, field))
  end

  defp aggregate_result_key(:count, _field), do: :count
  defp aggregate_result_key(agg_fn, field), do: :"#{agg_fn}_#{field}"
end

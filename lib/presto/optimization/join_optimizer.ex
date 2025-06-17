defmodule Presto.Optimization.JoinOptimizer do
  @moduledoc """
  Selectivity-based join ordering optimisation for RETE networks.

  This module implements RETE-II/RETE-NT style join optimisation by analyzing
  the selectivity of different patterns and reordering joins to process the
  most selective (filtering) conditions first. This can significantly improve
  performance by reducing the number of tuples that need to be processed
  in subsequent join operations.
  """

  @type selectivity_stats :: %{
          pattern_id: String.t(),
          input_size: non_neg_integer(),
          output_size: non_neg_integer(),
          selectivity: float(),
          estimated_cost: float(),
          last_updated: integer()
        }

  @type join_plan :: %{
          ordered_nodes: [String.t()],
          estimated_total_cost: float(),
          selectivity_order: [float()],
          optimisation_applied: boolean()
        }

  # Default selectivity estimates for different pattern types
  @default_selectivities %{
    # = conditions are highly selective
    equality: 0.1,
    # >, <, >=, <= conditions moderately selective
    range: 0.3,
    # != conditions less selective
    inequality: 0.8,
    # general pattern matching
    pattern_match: 0.5,
    # no filtering conditions
    no_conditions: 1.0
  }

  @spec analyze_node_selectivity(map(), [map()]) :: selectivity_stats()
  def analyze_node_selectivity(node, input_data) do
    input_size = calculate_input_size(input_data)

    # Estimate output size based on join conditions
    estimated_output_size = estimate_output_size(node, input_data)

    # Calculate selectivity (output/input ratio)
    selectivity =
      if input_size > 0 do
        estimated_output_size / input_size
      else
        1.0
      end

    # Estimate processing cost based on complexity
    estimated_cost = estimate_processing_cost(node, input_size)

    %{
      pattern_id: node.id,
      input_size: input_size,
      output_size: estimated_output_size,
      selectivity: selectivity,
      estimated_cost: estimated_cost,
      last_updated: System.monotonic_time(:millisecond)
    }
  end

  @spec optimize_join_order([map()], map()) :: join_plan()
  def optimize_join_order(nodes, optimisation_config \\ %{}) do
    enable_optimisation = Map.get(optimisation_config, :enable_join_reordering, true)
    cost_threshold = Map.get(optimisation_config, :cost_threshold, 100)

    if enable_optimisation and should_optimize?(nodes, cost_threshold) do
      perform_join_optimisation(nodes)
    else
      # Return original order
      %{
        ordered_nodes: Enum.map(nodes, & &1.id),
        estimated_total_cost: estimate_total_cost(nodes),
        selectivity_order: Enum.map(nodes, fn _ -> 1.0 end),
        optimisation_applied: false
      }
    end
  end

  @spec update_selectivity_statistics(String.t(), [map()], [map()]) :: true
  def update_selectivity_statistics(node_id, input_data, output_data) do
    # Store actual selectivity data for future optimisation
    input_size = length(input_data)
    output_size = length(output_data)

    actual_selectivity =
      if input_size > 0 do
        output_size / input_size
      else
        1.0
      end

    # Calculate improvement if we have previous estimates
    estimated_size = get_statistic({:estimated_size, node_id}, output_size)

    improvement =
      if estimated_size > 0, do: abs(estimated_size - output_size) / estimated_size, else: 0.0

    # Record the improvement for statistics
    if improvement > 0, do: record_optimisation_improvement(improvement)

    # Update selectivity learning (implement adaptive learning)
    update_learned_selectivity(node_id, actual_selectivity)

    # Store the actual output size for future comparison
    store_statistic({:actual_size, node_id}, output_size)
  end

  @spec get_optimisation_statistics() :: %{
          total_optimisations: non_neg_integer(),
          average_improvement: float(),
          best_case_improvement: float(),
          worst_case_improvement: float()
        }
  def get_optimisation_statistics do
    ensure_statistics_table_exists()

    # Aggregate statistics from persistent storage
    total_optimisations = get_statistic(:total_optimisations, 0)
    improvements = get_all_improvements()

    {average_improvement, best_case, worst_case} =
      if Enum.empty?(improvements) do
        {0.0, 0.0, 0.0}
      else
        avg = Enum.sum(improvements) / length(improvements)
        best = Enum.max(improvements)
        worst = Enum.min(improvements)
        {avg, best, worst}
      end

    %{
      total_optimisations: total_optimisations,
      average_improvement: average_improvement,
      best_case_improvement: best_case,
      worst_case_improvement: worst_case
    }
  end

  # Private functions

  defp calculate_input_size(input_data) when is_list(input_data) do
    length(input_data)
  end

  defp calculate_input_size({left_data, right_data})
       when is_list(left_data) and is_list(right_data) do
    length(left_data) + length(right_data)
  end

  defp calculate_input_size(_), do: 0

  defp estimate_output_size(node, input_data) do
    # Estimate output size based on join conditions and pattern analysis
    base_size = calculate_input_size(input_data)
    condition_selectivity = analyze_condition_selectivity(node.join_keys, node)

    # Apply selectivity multiplier, ensuring at least 1 output
    max(1, round(base_size * condition_selectivity))
  end

  defp analyze_condition_selectivity(join_keys, node) do
    # Use learned selectivity if available, otherwise fall back to defaults
    node_id = get_node_id(node)

    case length(join_keys) do
      0 ->
        # No join conditions - cartesian product
        @default_selectivities.no_conditions

      1 ->
        # Single join key - use learned selectivity if available
        get_learned_selectivity(node_id)

      multiple when multiple > 1 ->
        # Multiple join keys - compound conditions are more selective
        base_selectivity = @default_selectivities.equality
        # Each additional condition makes it more selective
        :math.pow(base_selectivity, multiple)
    end
  end

  defp estimate_processing_cost(node, input_size) do
    # Estimate the computational cost of processing this node
    # Base cost per input element
    base_cost = input_size * 0.001

    # Factor in join complexity
    join_complexity_cost = length(node.join_keys) * 0.0001

    # Factor in node type (alpha vs beta)
    type_cost =
      case {node.left_type, node.right_type} do
        # Simple join
        {:alpha, :alpha} -> 1.0
        # More complex
        {:alpha, :beta} -> 1.5
        # More complex
        {:beta, :alpha} -> 1.5
        # Most complex
        {:beta, :beta} -> 2.0
      end

    base_cost + join_complexity_cost + type_cost
  end

  defp should_optimize?(nodes, cost_threshold) do
    # Only optimize if the estimated benefit exceeds the threshold
    total_estimated_cost = estimate_total_cost(nodes)
    total_estimated_cost > cost_threshold
  end

  defp estimate_total_cost(nodes) do
    Enum.reduce(nodes, 0.0, fn node, acc ->
      # Rough cost estimation for deciding whether to optimize
      node_cost = Map.get(node, :estimated_cost, 1.0)
      acc + node_cost
    end)
  end

  defp perform_join_optimisation(nodes) do
    # Calculate selectivity for each node
    nodes_with_selectivity =
      Enum.map(nodes, fn node ->
        # Estimate selectivity based on pattern analysis
        estimated_selectivity = estimate_node_selectivity(node)
        Map.put(node, :estimated_selectivity, estimated_selectivity)
      end)

    # Sort by selectivity (most selective first) and dependency constraints
    optimized_order = sort_by_selectivity_and_dependencies(nodes_with_selectivity)

    # Calculate optimisation benefit
    original_cost = estimate_execution_cost(nodes)
    optimized_cost = estimate_execution_cost(optimized_order)

    %{
      ordered_nodes: Enum.map(optimized_order, & &1.id),
      estimated_total_cost: optimized_cost,
      selectivity_order: Enum.map(optimized_order, & &1.estimated_selectivity),
      optimisation_applied: true,
      estimated_improvement: (original_cost - optimized_cost) / original_cost
    }
  end

  defp estimate_node_selectivity(node) do
    # Estimate selectivity without actual data (static analysis)
    join_key_count = length(node.join_keys)

    # More join keys generally mean higher selectivity (fewer results)
    base_selectivity =
      case join_key_count do
        0 -> 1.0
        1 -> 0.1
        2 -> 0.05
        _ -> 0.01
      end

    # Factor in node complexity
    complexity_factor =
      case {node.left_type, node.right_type} do
        {:alpha, :alpha} -> 1.0
        # Beta inputs tend to be more filtered
        {:alpha, :beta} -> 0.8
        {:beta, :alpha} -> 0.8
        # Both inputs are filtered
        {:beta, :beta} -> 0.6
      end

    base_selectivity * complexity_factor
  end

  defp sort_by_selectivity_and_dependencies(nodes) do
    # First, separate nodes by dependency level
    level_0 =
      Enum.filter(nodes, fn node ->
        node.left_type == :alpha and node.right_type == :alpha
      end)

    level_1 =
      Enum.filter(nodes, fn node ->
        (node.left_type == :alpha and node.right_type == :beta) or
          (node.left_type == :beta and node.right_type == :alpha)
      end)

    level_2 =
      Enum.filter(nodes, fn node ->
        node.left_type == :beta and node.right_type == :beta
      end)

    # Sort each level by selectivity (most selective first)
    sort_by_selectivity = fn level ->
      Enum.sort_by(level, & &1.estimated_selectivity)
    end

    sort_by_selectivity.(level_0) ++
      sort_by_selectivity.(level_1) ++
      sort_by_selectivity.(level_2)
  end

  defp estimate_execution_cost(nodes) do
    # Estimate total execution cost considering data flow
    {_final_size, total_cost} =
      Enum.reduce(nodes, {1000, 0.0}, fn node, {input_size, cost_acc} ->
        # Get selectivity from node or estimate it
        selectivity = Map.get(node, :estimated_selectivity, estimate_node_selectivity(node))

        # Estimate output size after this node
        output_size = round(input_size * selectivity)

        # Add processing cost for this node
        node_cost = input_size * 0.001 + length(node.join_keys) * 0.0001

        {max(output_size, 1), cost_acc + node_cost}
      end)

    total_cost
  end

  defp update_learned_selectivity(node_id, actual_selectivity) do
    ensure_statistics_table_exists()

    # Update learned selectivity using exponential moving average
    # Learning rate
    alpha = 0.1
    current_selectivity = get_statistic({:selectivity, node_id}, @default_selectivities.equality)

    new_selectivity = alpha * actual_selectivity + (1 - alpha) * current_selectivity

    # Store updated selectivity
    store_statistic({:selectivity, node_id}, new_selectivity)

    # Update optimisation count
    increment_statistic(:total_optimisations)

    :ok
  end

  # Persistent storage implementation using ETS

  @table_name :presto_join_optimizer_stats

  defp ensure_statistics_table_exists do
    case :ets.whereis(@table_name) do
      :undefined ->
        :ets.new(@table_name, [:named_table, :public, :set, {:read_concurrency, true}])

      _ ->
        :ok
    end
  end

  defp store_statistic(key, value) do
    ensure_statistics_table_exists()
    :ets.insert(@table_name, {key, value})
  end

  defp get_statistic(key, default) do
    ensure_statistics_table_exists()

    case :ets.lookup(@table_name, key) do
      [{^key, value}] -> value
      [] -> default
    end
  end

  defp increment_statistic(key) do
    ensure_statistics_table_exists()
    :ets.update_counter(@table_name, key, {2, 1}, {key, 0})
  end

  defp store_improvement(improvement) do
    timestamp = System.system_time(:microsecond)
    store_statistic({:improvement, timestamp}, improvement)
  end

  defp get_all_improvements do
    ensure_statistics_table_exists()

    # Get all improvement records
    :ets.select(@table_name, [
      {{{:improvement, :"$1"}, :"$2"}, [], [:"$2"]}
    ])
  end

  defp get_node_id(node) do
    # Generate a unique identifier for the node based on its properties
    node_signature = %{
      join_keys: Map.get(node, :join_keys, []),
      left_input: Map.get(node, :left_input),
      right_input: Map.get(node, :right_input)
    }

    :crypto.hash(:sha256, :erlang.term_to_binary(node_signature))
    |> Base.encode16(case: :lower)
    # Use first 16 chars for readability
    |> String.slice(0, 16)
  end

  @doc """
  Records an optimisation improvement for statistical tracking.
  """
  def record_optimisation_improvement(improvement) when is_number(improvement) do
    store_improvement(improvement)
    increment_statistic(:total_optimisations)
  end

  @doc """
  Clears all optimisation statistics (for testing or maintenance).
  """
  def clear_optimisation_statistics do
    ensure_statistics_table_exists()
    :ets.delete_all_objects(@table_name)
  end

  @doc """
  Gets learned selectivity for a specific node.
  """
  def get_learned_selectivity(node_id) do
    get_statistic({:selectivity, node_id}, @default_selectivities.equality)
  end
end

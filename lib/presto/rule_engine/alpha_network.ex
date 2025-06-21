defmodule Presto.RuleEngine.AlphaNetwork do
  @moduledoc """
  Alpha network processing for the Presto Rule Engine.

  This module handles all alpha network operations, including:
  - Fact processing through alpha nodes
  - Pattern matching and test condition evaluation
  - Alpha node creation and management
  - Alpha memory operations
  - Fact type indexing for efficient processing

  The alpha network is the first stage of the RETE algorithm, responsible for
  filtering facts through single-pattern conditions before they proceed to
  the beta network for multi-pattern joins.
  """

  require Logger
  alias Presto.RuleEngine.State
  alias Presto.PatternMatching
  alias Presto.Logger, as: PrestoLogger

  @doc """
  Processes a fact assertion through the alpha network.

  This function finds all alpha nodes that might match the fact and processes
  the fact through each relevant node, updating alpha memories as needed.
  """
  @spec process_fact_assertion(State.t(), tuple()) :: State.t()
  def process_fact_assertion(%State{} = state, fact) do
    fact_type = elem(fact, 0)
    relevant_nodes = Map.get(state.fact_type_index, fact_type, [])

    PrestoLogger.log_fact_processing(
      :debug,
      fact_type,
      "processing_fact_through_alpha_network",
      %{
        fact: fact,
        relevant_nodes_count: length(relevant_nodes)
      }
    )

    Enum.reduce(relevant_nodes, state, fn node_id, acc_state ->
      process_alpha_node_for_fact(acc_state, node_id, fact)
    end)
  end

  @doc """
  Processes a fact retraction through the alpha network.

  This function removes the fact from all alpha memories where it was stored.
  """
  @spec process_fact_retraction(State.t(), tuple()) :: State.t()
  def process_fact_retraction(%State{} = state, fact) do
    fact_type = elem(fact, 0)
    relevant_nodes = Map.get(state.fact_type_index, fact_type, [])

    PrestoLogger.log_fact_processing(
      :debug,
      fact_type,
      "processing_fact_retraction_through_alpha_network",
      %{
        fact: fact,
        relevant_nodes_count: length(relevant_nodes)
      }
    )

    Enum.reduce(relevant_nodes, state, fn node_id, acc_state ->
      alpha_remove_from_memory(acc_state, node_id, fact)
    end)
  end

  @doc """
  Creates a new alpha node in the state.

  Alpha nodes represent single-pattern conditions in rules. Each node contains
  a pattern to match against facts and optional test conditions.
  """
  @spec create_alpha_node(State.t(), tuple()) :: {:ok, String.t(), State.t()}
  def create_alpha_node(%State{} = state, condition) do
    node_id = generate_alpha_node_id()

    # Extract pattern from condition
    pattern = extract_pattern_from_condition(condition)

    alpha_node = %{
      id: node_id,
      pattern: pattern,
      test_function: nil,
      conditions: [condition]
    }

    PrestoLogger.log_fact_processing(:debug, elem(pattern, 0), "creating_alpha_node", %{
      node_id: node_id,
      pattern: pattern,
      condition: condition
    })

    # Add to alpha nodes map
    new_alpha_nodes = Map.put(state.alpha_nodes, node_id, alpha_node)

    # Initialize empty memory for this node
    :ets.insert(State.get_alpha_memories(state), {node_id, []})

    # Update fact type index
    fact_type = elem(pattern, 0)
    existing_nodes = Map.get(state.fact_type_index, fact_type, [])
    new_fact_type_index = Map.put(state.fact_type_index, fact_type, [node_id | existing_nodes])

    new_state = %{state | alpha_nodes: new_alpha_nodes, fact_type_index: new_fact_type_index}

    PrestoLogger.log_fact_processing(:debug, fact_type, "alpha_node_created", %{
      node_id: node_id,
      total_nodes_for_type: length([node_id | existing_nodes])
    })

    {:ok, node_id, new_state}
  end

  @doc """
  Gets alpha memory for a given node ID.
  """
  @spec get_alpha_memory(State.t(), String.t()) :: [map()]
  def get_alpha_memory(%State{} = state, node_id) do
    case :ets.lookup(State.get_alpha_memories(state), node_id) do
      [{^node_id, matches}] -> matches
      [] -> []
    end
  end

  @doc """
  Gets alpha node info for a given node ID.
  """
  @spec get_alpha_node_info(State.t(), String.t()) :: map() | nil
  def get_alpha_node_info(%State{} = state, node_id) do
    Map.get(state.alpha_nodes, node_id)
  end

  @doc """
  Removes an alpha node from the state.
  """
  @spec remove_alpha_node(State.t(), String.t()) :: State.t()
  def remove_alpha_node(%State{} = state, node_id) do
    case Map.get(state.alpha_nodes, node_id) do
      nil ->
        state

      alpha_node ->
        PrestoLogger.log_fact_processing(
          :debug,
          elem(alpha_node.pattern, 0),
          "removing_alpha_node",
          %{
            node_id: node_id
          }
        )

        # Remove from alpha nodes
        new_alpha_nodes = Map.delete(state.alpha_nodes, node_id)

        # Remove from fact type index
        fact_type = elem(alpha_node.pattern, 0)
        existing_nodes = Map.get(state.fact_type_index, fact_type, [])
        new_nodes = List.delete(existing_nodes, node_id)

        new_fact_type_index =
          if new_nodes == [] do
            Map.delete(state.fact_type_index, fact_type)
          else
            Map.put(state.fact_type_index, fact_type, new_nodes)
          end

        # Remove from alpha memories
        :ets.delete(State.get_alpha_memories(state), node_id)

        PrestoLogger.log_fact_processing(:debug, fact_type, "alpha_node_removed", %{
          node_id: node_id,
          remaining_nodes_for_type: length(new_nodes)
        })

        %{state | alpha_nodes: new_alpha_nodes, fact_type_index: new_fact_type_index}
    end
  end

  @doc """
  Gets all alpha nodes for a specific fact type.
  """
  @spec get_alpha_nodes_for_fact_type(State.t(), atom()) :: [String.t()]
  def get_alpha_nodes_for_fact_type(%State{} = state, fact_type) do
    Map.get(state.fact_type_index, fact_type, [])
  end

  @doc """
  Gets statistics about the alpha network.
  """
  @spec get_alpha_network_stats(State.t()) :: map()
  def get_alpha_network_stats(%State{} = state) do
    total_nodes = map_size(state.alpha_nodes)
    fact_types = Map.keys(state.fact_type_index)
    total_memories = :ets.info(State.get_alpha_memories(state), :size)

    %{
      total_alpha_nodes: total_nodes,
      fact_types_covered: length(fact_types),
      total_memories: total_memories,
      avg_nodes_per_type:
        if(length(fact_types) > 0, do: total_nodes / length(fact_types), else: 0)
    }
  end

  # Private functions

  defp process_alpha_node_for_fact(state, node_id, fact) do
    case Map.get(state.alpha_nodes, node_id) do
      nil ->
        state

      alpha_node ->
        if alpha_node_matches?(alpha_node, fact) do
          alpha_add_to_memory(state, node_id, fact)
        else
          state
        end
    end
  end

  defp alpha_node_matches?(alpha_node, fact) do
    # Use compiled test function if available, otherwise pattern match + test conditions
    case Map.get(alpha_node, :test_function) do
      nil -> alpha_pattern_match_with_tests(alpha_node, fact)
      test_fn -> test_fn.(fact)
    end
  end

  defp alpha_pattern_match_with_tests(alpha_node, fact) do
    # First check if pattern matches and extract variable bindings
    case alpha_basic_pattern_match_with_bindings(alpha_node.pattern, fact) do
      {:ok, bindings} ->
        # Pattern matches, now evaluate test conditions with bindings
        evaluate_test_conditions(alpha_node.conditions, bindings)

      false ->
        false
    end
  end

  defp alpha_basic_pattern_match_with_bindings(pattern, fact)
       when tuple_size(pattern) != tuple_size(fact) do
    false
  end

  defp alpha_basic_pattern_match_with_bindings(pattern, fact) do
    pattern_list = Tuple.to_list(pattern)
    fact_list = Tuple.to_list(fact)

    Enum.zip(pattern_list, fact_list)
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, %{}}, &match_pattern_element/2)
    |> case do
      {:ok, final_bindings} -> {:ok, final_bindings}
      false -> false
    end
  end

  defp match_pattern_element({{pattern_elem, fact_elem}, 0}, {:ok, acc_bindings}) do
    # First element (fact type) must match exactly
    if pattern_elem == fact_elem do
      {:cont, {:ok, acc_bindings}}
    else
      {:halt, false}
    end
  end

  defp match_pattern_element({{pattern_elem, fact_elem}, _index}, {:ok, acc_bindings}) do
    match_non_type_element(pattern_elem, fact_elem, acc_bindings)
  end

  defp match_non_type_element(:_, _fact_elem, acc_bindings) do
    # Wildcard matches anything
    {:cont, {:ok, acc_bindings}}
  end

  defp match_non_type_element(pattern_elem, fact_elem, acc_bindings)
       when pattern_elem == fact_elem do
    # Exact match
    {:cont, {:ok, acc_bindings}}
  end

  defp match_non_type_element(pattern_elem, fact_elem, acc_bindings) do
    if PatternMatching.variable?(pattern_elem) do
      # Variable binding
      new_bindings = Map.put(acc_bindings, pattern_elem, fact_elem)
      {:cont, {:ok, new_bindings}}
    else
      # No match
      {:halt, false}
    end
  end

  defp evaluate_test_conditions(conditions, bindings) do
    # Extract test conditions from the alpha node conditions
    test_conditions = extract_test_conditions_from_alpha_conditions(conditions)

    # Evaluate each test condition with variable bindings
    Enum.all?(test_conditions, fn test_condition ->
      evaluate_single_test_condition(test_condition, bindings)
    end)
  end

  defp extract_test_conditions_from_alpha_conditions(conditions) do
    Enum.flat_map(conditions, &extract_tests_from_condition/1)
  end

  defp extract_tests_from_condition({_pattern, tests}) when is_list(tests) do
    # Pattern with tests: {{:person, :name, :age}, [test_conditions]}
    tests
  end

  defp extract_tests_from_condition(condition_tuple) when is_tuple(condition_tuple) do
    # Pattern with tests appended: {:person, :name, :age, [test_conditions]}
    extract_tests_from_tuple(condition_tuple)
  end

  defp extract_tests_from_condition(_condition) do
    # Just a pattern, no tests
    []
  end

  defp extract_tests_from_tuple(condition_tuple) do
    case Tuple.to_list(condition_tuple) do
      list when length(list) > 2 ->
        extract_tests_from_last_element(List.last(list))

      _ ->
        []
    end
  end

  defp extract_tests_from_last_element(last_elem) when is_list(last_elem) do
    last_elem
  end

  defp extract_tests_from_last_element(_last_elem) do
    []
  end

  defp evaluate_single_test_condition({variable, operator, value}, bindings) do
    case Map.get(bindings, variable) do
      # Variable not bound
      nil -> false
      bound_value -> evaluate_operator(operator, bound_value, value)
    end
  end

  defp evaluate_single_test_condition(_, _), do: false

  defp evaluate_operator(:>, bound_value, value), do: bound_value > value
  defp evaluate_operator(:<, bound_value, value), do: bound_value < value
  defp evaluate_operator(:>=, bound_value, value), do: bound_value >= value
  defp evaluate_operator(:<=, bound_value, value), do: bound_value <= value
  defp evaluate_operator(:==, bound_value, value), do: bound_value == value
  defp evaluate_operator(:!=, bound_value, value), do: bound_value != value
  defp evaluate_operator(_, _bound_value, _value), do: false

  defp alpha_add_to_memory(state, node_id, fact) do
    bindings = alpha_extract_bindings(state.alpha_nodes[node_id].pattern, fact)

    # Get current matches and add new binding
    current_matches =
      case :ets.lookup(State.get_alpha_memories(state), node_id) do
        [{^node_id, matches}] -> matches
        [] -> []
      end

    # Add new binding if not already present
    new_matches =
      if bindings in current_matches do
        current_matches
      else
        [bindings | current_matches]
      end

    :ets.insert(State.get_alpha_memories(state), {node_id, new_matches})
    state
  end

  defp alpha_remove_from_memory(state, node_id, fact) do
    case Map.get(state.alpha_nodes, node_id) do
      nil ->
        state

      alpha_node ->
        bindings = alpha_extract_bindings(alpha_node.pattern, fact)

        # Get current matches and remove the specific binding
        case :ets.lookup(State.get_alpha_memories(state), node_id) do
          [{^node_id, current_matches}] ->
            new_matches = List.delete(current_matches, bindings)
            :ets.insert(State.get_alpha_memories(state), {node_id, new_matches})

          [] ->
            # No memory for this node
            :ok
        end

        state
    end
  end

  defp alpha_extract_bindings(pattern, fact) do
    pattern_list = Tuple.to_list(pattern)
    fact_list = Tuple.to_list(fact)

    Enum.zip(pattern_list, fact_list)
    |> Enum.with_index()
    |> Enum.reduce(%{}, fn {{pattern_elem, fact_elem}, _index}, bindings ->
      if PatternMatching.variable?(pattern_elem) do
        Map.put(bindings, pattern_elem, fact_elem)
      else
        bindings
      end
    end)
  end

  defp generate_alpha_node_id do
    "alpha_#{:erlang.unique_integer([:positive])}"
  end

  defp extract_pattern_from_condition({pattern, _tests}) when is_tuple(pattern) do
    pattern
  end

  defp extract_pattern_from_condition(condition) when is_tuple(condition) do
    condition
  end
end

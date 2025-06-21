defmodule Presto.RuleEngine.AlphaNodeManager do
  @moduledoc """
  Alpha node management for the Presto Rule Engine.

  This module has a single responsibility: managing the lifecycle of alpha nodes
  including creation, retrieval, removal, and fact type indexing.

  Single Responsibility: Alpha node CRUD operations and indexing
  """

  require Logger
  alias Presto.Logger, as: PrestoLogger
  alias Presto.RuleEngine.State

  @doc """
  Creates a new alpha node in the state.
  """
  @spec create_alpha_node(State.t(), tuple()) :: {:ok, String.t(), State.t()}
  def create_alpha_node(%State{} = state, condition) do
    node_id = generate_alpha_node_id()
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

        new_state = %{state | alpha_nodes: new_alpha_nodes, fact_type_index: new_fact_type_index}

        PrestoLogger.log_fact_processing(:debug, fact_type, "alpha_node_removed", %{
          node_id: node_id,
          remaining_nodes_for_type: length(new_nodes)
        })

        new_state
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
  Gets statistics about alpha nodes.
  """
  @spec get_alpha_node_stats(State.t()) :: map()
  def get_alpha_node_stats(%State{} = state) do
    total_nodes = map_size(state.alpha_nodes)
    fact_types = Map.keys(state.fact_type_index)

    %{
      total_alpha_nodes: total_nodes,
      fact_types_covered: length(fact_types),
      avg_nodes_per_type:
        if(length(fact_types) > 0, do: total_nodes / length(fact_types), else: 0)
    }
  end

  # Private functions

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

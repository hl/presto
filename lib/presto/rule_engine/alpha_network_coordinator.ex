defmodule Presto.RuleEngine.AlphaNetworkCoordinator do
  @moduledoc """
  Alpha network coordination for the Presto Rule Engine.

  This module has a single responsibility: coordinating fact processing 
  through the alpha network by orchestrating pattern matching, node 
  management, and memory operations.

  Single Responsibility: Alpha network processing coordination
  """

  require Logger
  alias Presto.Logger, as: PrestoLogger
  alias Presto.RuleEngine.AlphaMemoryManager
  alias Presto.RuleEngine.AlphaNodeManager
  alias Presto.RuleEngine.PatternMatcher
  alias Presto.RuleEngine.State

  @doc """
  Processes a fact assertion through the alpha network.
  """
  @spec process_fact_assertion(State.t(), tuple()) :: State.t()
  def process_fact_assertion(%State{} = state, fact) do
    fact_type = elem(fact, 0)
    relevant_nodes = AlphaNodeManager.get_alpha_nodes_for_fact_type(state, fact_type)

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
  """
  @spec process_fact_retraction(State.t(), tuple()) :: State.t()
  def process_fact_retraction(%State{} = state, fact) do
    fact_type = elem(fact, 0)
    relevant_nodes = AlphaNodeManager.get_alpha_nodes_for_fact_type(state, fact_type)

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
      AlphaMemoryManager.remove_from_memory(acc_state, node_id, fact)
    end)
  end

  @doc """
  Creates a new alpha node and initializes its memory.
  """
  @spec create_alpha_node(State.t(), tuple()) :: {:ok, String.t(), State.t()}
  def create_alpha_node(%State{} = state, condition) do
    {:ok, node_id, new_state} = AlphaNodeManager.create_alpha_node(state, condition)
    AlphaMemoryManager.initialize_alpha_memory(new_state, node_id)
    {:ok, node_id, new_state}
  end

  @doc """
  Removes an alpha node and its memory.
  """
  @spec remove_alpha_node(State.t(), String.t()) :: State.t()
  def remove_alpha_node(%State{} = state, node_id) do
    AlphaMemoryManager.delete_memory(state, node_id)
    AlphaNodeManager.remove_alpha_node(state, node_id)
  end

  @doc """
  Gets alpha memory for a given node ID.
  """
  @spec get_alpha_memory(State.t(), String.t()) :: [map()]
  def get_alpha_memory(%State{} = state, node_id) do
    AlphaMemoryManager.get_alpha_memory(state, node_id)
  end

  @doc """
  Gets alpha node info for a given node ID.
  """
  @spec get_alpha_node_info(State.t(), String.t()) :: map() | nil
  def get_alpha_node_info(%State{} = state, node_id) do
    AlphaNodeManager.get_alpha_node_info(state, node_id)
  end

  @doc """
  Gets comprehensive alpha network statistics.
  """
  @spec get_alpha_network_stats(State.t()) :: map()
  def get_alpha_network_stats(%State{} = state) do
    node_stats = AlphaNodeManager.get_alpha_node_stats(state)
    memory_stats = AlphaMemoryManager.get_alpha_memory_stats(state)

    Map.merge(node_stats, memory_stats)
  end

  # Private functions

  defp process_alpha_node_for_fact(state, node_id, fact) do
    case AlphaNodeManager.get_alpha_node_info(state, node_id) do
      nil ->
        state

      alpha_node ->
        if PatternMatcher.matches?(alpha_node, fact) do
          AlphaMemoryManager.add_to_memory(state, node_id, fact)
        else
          state
        end
    end
  end
end

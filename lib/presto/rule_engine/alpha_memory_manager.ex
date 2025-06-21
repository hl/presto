defmodule Presto.RuleEngine.AlphaMemoryManager do
  @moduledoc """
  Alpha memory management for the Presto Rule Engine.

  This module has a single responsibility: managing alpha memory operations
  including storing and retrieving fact bindings in alpha memories.

  Single Responsibility: Alpha memory operations and storage
  """

  alias Presto.RuleEngine.State
  alias Presto.RuleEngine.PatternMatcher

  @doc """
  Initializes empty memory for an alpha node.
  """
  @spec initialize_alpha_memory(State.t(), String.t()) :: :ok
  def initialize_alpha_memory(%State{} = state, node_id) do
    :ets.insert(State.get_alpha_memories(state), {node_id, []})
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
  Adds a fact binding to alpha memory.
  """
  @spec add_to_memory(State.t(), String.t(), tuple()) :: State.t()
  def add_to_memory(%State{} = state, node_id, fact) do
    case Map.get(state.alpha_nodes, node_id) do
      nil ->
        state

      alpha_node ->
        bindings = PatternMatcher.extract_bindings(alpha_node.pattern, fact)

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
  end

  @doc """
  Removes a fact binding from alpha memory.
  """
  @spec remove_from_memory(State.t(), String.t(), tuple()) :: State.t()
  def remove_from_memory(%State{} = state, node_id, fact) do
    case Map.get(state.alpha_nodes, node_id) do
      nil ->
        state

      alpha_node ->
        bindings = PatternMatcher.extract_bindings(alpha_node.pattern, fact)

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

  @doc """
  Clears all memory for an alpha node.
  """
  @spec clear_memory(State.t(), String.t()) :: :ok
  def clear_memory(%State{} = state, node_id) do
    :ets.insert(State.get_alpha_memories(state), {node_id, []})
  end

  @doc """
  Removes alpha memory when node is deleted.
  """
  @spec delete_memory(State.t(), String.t()) :: :ok
  def delete_memory(%State{} = state, node_id) do
    :ets.delete(State.get_alpha_memories(state), node_id)
  end

  @doc """
  Gets statistics about alpha memories.
  """
  @spec get_alpha_memory_stats(State.t()) :: map()
  def get_alpha_memory_stats(%State{} = state) do
    total_memories = :ets.info(State.get_alpha_memories(state), :size)

    memory_sizes =
      :ets.tab2list(State.get_alpha_memories(state))
      |> Enum.map(fn {_node_id, matches} -> length(matches) end)

    total_bindings = Enum.sum(memory_sizes)
    avg_bindings = if total_memories > 0, do: total_bindings / total_memories, else: 0

    %{
      total_memories: total_memories,
      total_bindings: total_bindings,
      avg_bindings_per_memory: avg_bindings,
      max_bindings: if(length(memory_sizes) > 0, do: Enum.max(memory_sizes), else: 0)
    }
  end
end

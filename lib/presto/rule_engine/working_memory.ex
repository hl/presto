defmodule Presto.RuleEngine.WorkingMemory do
  @moduledoc """
  Working memory operations for the Presto Rule Engine.

  This module handles all fact storage and retrieval operations, including:
  - Fact assertion and retraction
  - Change tracking for incremental processing
  - Working memory queries and management
  - Basic fact operations on ETS tables

  The module operates on the State struct and coordinates with other modules
  for complex operations like alpha network processing.
  """

  require Logger
  alias Presto.Logger, as: PrestoLogger
  alias Presto.RuleEngine.State

  @doc """
  Asserts a fact into the working memory.

  This function stores the fact in the ETS table and tracks changes if enabled.
  Alpha network processing is delegated to AlphaNetwork module.
  """
  @spec assert_fact(State.t(), tuple()) :: State.t()
  def assert_fact(%State{} = state, fact) do
    PrestoLogger.log_fact_processing(:debug, elem(fact, 0), "asserting_fact_to_memory", %{
      fact_type: elem(fact, 0),
      fact_size: tuple_size(fact)
    })

    fact_key = make_fact_key(fact)
    :ets.insert(State.get_facts_table(state), {fact_key, fact})

    # Track change if enabled
    new_state = maybe_track_change({:assert, fact}, state)

    PrestoLogger.log_fact_processing(:debug, elem(fact, 0), "fact_asserted_to_memory", %{
      fact_type: elem(fact, 0),
      change_tracked: new_state.tracking_changes
    })

    new_state
  end

  @doc """
  Retracts a fact from the working memory.

  This function removes the fact from the ETS table and tracks changes if enabled.
  Alpha network processing is delegated to AlphaNetwork module.
  """
  @spec retract_fact(State.t(), tuple()) :: State.t()
  def retract_fact(%State{} = state, fact) do
    PrestoLogger.log_fact_processing(:debug, elem(fact, 0), "retracting_fact_from_memory", %{
      fact_type: elem(fact, 0),
      fact_size: tuple_size(fact)
    })

    fact_key = make_fact_key(fact)
    :ets.delete(State.get_facts_table(state), fact_key)

    # Track change if enabled
    new_state = maybe_track_change({:retract, fact}, state)

    PrestoLogger.log_fact_processing(:debug, elem(fact, 0), "fact_retracted_from_memory", %{
      fact_type: elem(fact, 0),
      change_tracked: new_state.tracking_changes
    })

    new_state
  end

  @doc """
  Gets all facts from the working memory.
  """
  @spec get_facts(State.t()) :: [tuple()]
  def get_facts(%State{} = state) do
    :ets.tab2list(State.get_facts_table(state))
    |> Enum.map(fn {_key, fact} -> fact end)
  end

  @doc """
  Clears all facts from the working memory.
  """
  @spec clear_facts(State.t()) :: State.t()
  def clear_facts(%State{} = state) do
    PrestoLogger.log_fact_processing(:info, :all, "clearing_all_facts", %{})

    :ets.delete_all_objects(State.get_facts_table(state))
    new_state = maybe_track_change({:clear_all}, state)

    PrestoLogger.log_fact_processing(:info, :all, "all_facts_cleared", %{
      change_tracked: new_state.tracking_changes
    })

    new_state
  end

  @doc """
  Enables change tracking for incremental processing.
  """
  @spec enable_change_tracking(State.t()) :: State.t()
  def enable_change_tracking(%State{} = state) do
    %{state | tracking_changes: true}
  end

  @doc """
  Disables change tracking.
  """
  @spec disable_change_tracking(State.t()) :: State.t()
  def disable_change_tracking(%State{} = state) do
    %{state | tracking_changes: false}
  end

  @doc """
  Gets the current change counter value.
  """
  @spec get_change_counter(State.t()) :: non_neg_integer()
  def get_change_counter(%State{} = state), do: state.change_counter

  @doc """
  Gets all tracked changes since a given counter value.
  """
  @spec get_changes_since(State.t(), non_neg_integer()) :: [tuple()]
  def get_changes_since(%State{} = state, since_counter) do
    changes_table = State.get_changes_table(state)

    :ets.tab2list(changes_table)
    |> Enum.filter(fn {counter, _change} -> counter > since_counter end)
    |> Enum.sort_by(fn {counter, _change} -> counter end)
    |> Enum.map(fn {_counter, change} -> change end)
  end

  @doc """
  Clears all tracked changes.
  """
  @spec clear_changes(State.t()) :: State.t()
  def clear_changes(%State{} = state) do
    :ets.delete_all_objects(State.get_changes_table(state))
    %{state | change_counter: 0}
  end

  @doc """
  Gets facts by type from working memory.
  """
  @spec get_facts_by_type(State.t(), atom()) :: [tuple()]
  def get_facts_by_type(%State{} = state, fact_type) do
    get_facts(state)
    |> Enum.filter(fn fact -> elem(fact, 0) == fact_type end)
  end

  @doc """
  Checks if a specific fact exists in working memory.
  """
  @spec fact_exists?(State.t(), tuple()) :: boolean()
  def fact_exists?(%State{} = state, fact) do
    fact_key = make_fact_key(fact)

    case :ets.lookup(State.get_facts_table(state), fact_key) do
      [{^fact_key, ^fact}] -> true
      _ -> false
    end
  end

  @doc """
  Gets the total count of facts in working memory.
  """
  @spec get_fact_count(State.t()) :: non_neg_integer()
  def get_fact_count(%State{} = state) do
    :ets.info(State.get_facts_table(state), :size)
  end

  # Private functions

  defp make_fact_key(fact) do
    # Use the fact itself as the key for simple deduplication
    fact
  end

  defp maybe_track_change(_change, %State{tracking_changes: false} = state) do
    state
  end

  defp maybe_track_change(change, %State{tracking_changes: true} = state) do
    counter = state.change_counter + 1
    :ets.insert(State.get_changes_table(state), {counter, change})
    %{state | change_counter: counter}
  end
end

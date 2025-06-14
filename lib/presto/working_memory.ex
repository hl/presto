defmodule Presto.WorkingMemory do
  @moduledoc """
  ETS-based working memory for storing facts in the RETE algorithm.

  Provides efficient storage and retrieval of facts with concurrent read access,
  pattern matching capabilities, and change tracking for incremental processing.
  """

  use GenServer

  alias Presto.Utils

  @type fact :: tuple()
  @type pattern :: tuple()
  @type change :: {:assert | :retract, fact()}

  # Client API

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts)
  end

  @spec assert_fact(GenServer.server(), fact()) :: :ok
  def assert_fact(pid, fact) do
    GenServer.call(pid, {:assert_fact, fact})
  end

  @spec retract_fact(GenServer.server(), fact()) :: :ok
  def retract_fact(pid, fact) do
    GenServer.call(pid, {:retract_fact, fact})
  end

  @spec get_facts(GenServer.server()) :: [fact()]
  def get_facts(pid) do
    GenServer.call(pid, :get_facts)
  end

  @spec clear_facts(GenServer.server()) :: :ok
  def clear_facts(pid) do
    GenServer.call(pid, :clear_facts)
  end

  @spec match_facts(GenServer.server(), pattern()) :: [fact()]
  def match_facts(pid, pattern) do
    GenServer.call(pid, {:match_facts, pattern})
  end

  @spec match_facts_with_bindings(GenServer.server(), pattern()) :: [map()]
  def match_facts_with_bindings(pid, pattern) do
    GenServer.call(pid, {:match_facts_with_bindings, pattern})
  end

  @spec start_tracking_changes(GenServer.server()) :: :ok
  def start_tracking_changes(pid) do
    GenServer.call(pid, :start_tracking_changes)
  end

  @spec get_changes(GenServer.server()) :: [change()]
  def get_changes(pid) do
    GenServer.call(pid, :get_changes)
  end

  @spec clear_changes(GenServer.server()) :: :ok
  def clear_changes(pid) do
    GenServer.call(pid, :clear_changes)
  end

  # Server implementation

  @impl true
  def init(_opts) do
    facts_table = :ets.new(:facts, [:set, :public, read_concurrency: true])
    changes_table = :ets.new(:changes, [:ordered_set, :private])

    state = %{
      facts_table: facts_table,
      changes_table: changes_table,
      tracking_changes: false,
      change_counter: 0
    }

    {:ok, state}
  end

  @impl true
  def handle_call({:assert_fact, fact}, _from, state) do
    fact_key = make_fact_key(fact)
    :ets.insert(state.facts_table, {fact_key, fact})

    new_state = maybe_track_change({:assert, fact}, state)

    {:reply, :ok, new_state}
  end

  @impl true
  def handle_call({:retract_fact, fact}, _from, state) do
    fact_key = make_fact_key(fact)
    :ets.delete(state.facts_table, fact_key)

    new_state = maybe_track_change({:retract, fact}, state)

    {:reply, :ok, new_state}
  end

  @impl true
  def handle_call(:get_facts, _from, state) do
    facts =
      :ets.tab2list(state.facts_table)
      |> Enum.map(fn {_key, fact} -> fact end)

    {:reply, facts, state}
  end

  @impl true
  def handle_call(:clear_facts, _from, state) do
    :ets.delete_all_objects(state.facts_table)

    new_state = maybe_track_change({:clear_all}, state)

    {:reply, :ok, new_state}
  end

  @impl true
  def handle_call({:match_facts, pattern}, _from, state) do
    matches = match_pattern(state.facts_table, pattern)
    {:reply, matches, state}
  end

  @impl true
  def handle_call({:match_facts_with_bindings, pattern}, _from, state) do
    bindings = match_pattern_with_bindings(state.facts_table, pattern)
    {:reply, bindings, state}
  end

  @impl true
  def handle_call(:start_tracking_changes, _from, state) do
    new_state = %{state | tracking_changes: true}
    {:reply, :ok, new_state}
  end

  @impl true
  def handle_call(:get_changes, _from, state) do
    changes =
      :ets.tab2list(state.changes_table)
      |> Enum.sort_by(fn {counter, _change} -> counter end)
      |> Enum.map(fn {_counter, change} -> change end)

    {:reply, changes, state}
  end

  @impl true
  def handle_call(:clear_changes, _from, state) do
    :ets.delete_all_objects(state.changes_table)
    new_state = %{state | change_counter: 0}
    {:reply, :ok, new_state}
  end

  @impl true
  def terminate(_reason, state) do
    :ets.delete(state.facts_table)
    :ets.delete(state.changes_table)
    :ok
  end

  # Private functions

  defp make_fact_key(fact) do
    # Use the fact itself as the key for simple deduplication
    # In a more complex implementation, might want structured keys
    fact
  end

  defp maybe_track_change(_change, %{tracking_changes: false} = state) do
    state
  end

  defp maybe_track_change(change, %{tracking_changes: true} = state) do
    counter = state.change_counter + 1
    :ets.insert(state.changes_table, {counter, change})
    %{state | change_counter: counter}
  end

  defp match_pattern(table, pattern) do
    # Get all facts and manually match
    facts = :ets.tab2list(table) |> Enum.map(fn {_key, fact} -> fact end)

    Enum.filter(facts, fn fact -> pattern_matches?(pattern, fact) end)
  end

  defp match_pattern_with_bindings(table, pattern) do
    # Get all facts and manually match with variable binding
    facts = :ets.tab2list(table) |> Enum.map(fn {_key, fact} -> fact end)

    facts
    |> Enum.filter(fn fact -> pattern_matches?(pattern, fact) end)
    |> Enum.map(fn fact -> extract_bindings(pattern, fact) end)
  end

  # Simplified pattern matching - removed complex ETS match spec generation

  defp pattern_matches?(pattern, fact) when tuple_size(pattern) != tuple_size(fact) do
    false
  end

  defp pattern_matches?(pattern, fact) do
    pattern_list = Tuple.to_list(pattern)
    fact_list = Tuple.to_list(fact)

    Enum.zip(pattern_list, fact_list)
    |> Enum.with_index()
    |> Enum.all?(fn {{pattern_elem, fact_elem}, index} ->
      case index do
        0 ->
          # First element (fact type) must match exactly
          pattern_elem == fact_elem

        _ ->
          # Other elements can be variables, wildcards, or exact matches
          pattern_elem == :_ or pattern_elem == fact_elem or Utils.variable?(pattern_elem)
      end
    end)
  end

  defp extract_bindings(pattern, fact) do
    pattern_list = Tuple.to_list(pattern)
    fact_list = Tuple.to_list(fact)

    Enum.zip(pattern_list, fact_list)
    |> Enum.with_index()
    |> Enum.reduce(%{}, fn {{pattern_elem, fact_elem}, index}, acc ->
      # Only bind variables, and skip the fact type (index 0)
      if index > 0 and Utils.variable?(pattern_elem) do
        var_name =
          String.trim_leading(Atom.to_string(pattern_elem), ":")
          |> String.to_atom()

        Map.put(acc, var_name, fact_elem)
      else
        acc
      end
    end)
  end
end

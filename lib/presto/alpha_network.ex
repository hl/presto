defmodule Presto.AlphaNetwork do
  @moduledoc """
  Alpha network implementation for the RETE algorithm.

  The alpha network filters individual facts based on single conditions,
  using Elixir's pattern matching and compiled test functions for efficiency.
  """

  use GenServer

  alias Presto.Utils
  # alias Presto.WorkingMemory  # Not currently used

  @type condition :: {atom(), atom(), atom(), [test()]}
  @type test :: {atom(), atom(), any()}
  @type alpha_node :: %{
          id: String.t(),
          pattern: tuple(),
          test_function: function(),
          conditions: [test()]
        }

  # Client API

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @spec create_alpha_node(GenServer.server(), condition()) :: {:ok, String.t()}
  def create_alpha_node(pid, condition) do
    GenServer.call(pid, {:create_alpha_node, condition})
  end

  @spec create_optimized_alpha_node(GenServer.server(), tuple(), function()) :: {:ok, String.t()}
  def create_optimized_alpha_node(pid, pattern, compiled_matcher) do
    GenServer.call(pid, {:create_optimized_alpha_node, pattern, compiled_matcher})
  end

  @spec remove_alpha_node(GenServer.server(), String.t()) :: :ok
  def remove_alpha_node(pid, node_id) do
    GenServer.call(pid, {:remove_alpha_node, node_id})
  end

  @spec get_alpha_nodes(GenServer.server()) :: %{String.t() => alpha_node()}
  def get_alpha_nodes(pid) do
    GenServer.call(pid, :get_alpha_nodes)
  end

  @spec get_alpha_node_info(GenServer.server(), String.t()) :: alpha_node() | nil
  def get_alpha_node_info(pid, node_id) do
    GenServer.call(pid, {:get_alpha_node_info, node_id})
  end

  @spec process_fact_assertion(GenServer.server(), tuple()) :: :ok
  def process_fact_assertion(pid, fact) do
    GenServer.cast(pid, {:process_fact_assertion, fact})
  end

  @spec process_fact_retraction(GenServer.server(), tuple()) :: :ok
  def process_fact_retraction(pid, fact) do
    GenServer.cast(pid, {:process_fact_retraction, fact})
  end

  @spec get_alpha_memory(GenServer.server(), String.t()) :: [map()]
  def get_alpha_memory(pid, node_id) do
    GenServer.call(pid, {:get_alpha_memory, node_id})
  end

  # Server implementation

  @impl true
  def init(opts) do
    working_memory = Keyword.fetch!(opts, :working_memory)

    state = %{
      working_memory: working_memory,
      alpha_nodes: %{},
      # ETS table to store alpha memory for each node
      alpha_memories: :ets.new(:alpha_memories, [:set, :private]),
      # NEW: Index alpha nodes by fact type for O(1) lookup
      fact_type_index: %{},
      # NEW: Compiled pattern cache to avoid repeated compilation
      compiled_patterns: :ets.new(:compiled_patterns, [:set, :private])
    }

    {:ok, state}
  end

  @impl true
  def handle_call({:create_alpha_node, condition}, _from, state) do
    node_id = generate_node_id()

    # NEW: Check pattern cache first
    condition_key = :crypto.hash(:sha256, :erlang.term_to_binary(condition))

    compiled_condition =
      case :ets.lookup(state.compiled_patterns, condition_key) do
        [{^condition_key, cached_result}] ->
          cached_result

        [] ->
          result = compile_condition(condition)
          :ets.insert(state.compiled_patterns, {condition_key, result})
          result
      end

    case compiled_condition do
      {:ok, alpha_node} ->
        node = Map.put(alpha_node, :id, node_id)
        new_nodes = Map.put(state.alpha_nodes, node_id, node)

        # Initialize empty memory for this node
        :ets.insert(state.alpha_memories, {node_id, []})

        # NEW: Index node by fact type for O(1) lookup
        fact_type = extract_fact_type_from_pattern(node.pattern)
        fact_type_nodes = Map.get(state.fact_type_index, fact_type, [])

        new_fact_type_index =
          Map.put(state.fact_type_index, fact_type, [{node_id, node} | fact_type_nodes])

        new_state = %{state | alpha_nodes: new_nodes, fact_type_index: new_fact_type_index}
        {:reply, {:ok, node_id}, new_state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:create_optimized_alpha_node, pattern, compiled_matcher}, _from, state) do
    node_id = generate_node_id()

    # Create optimized alpha node with pre-compiled matcher
    alpha_node = %{
      id: node_id,
      pattern: pattern,
      test_function: compiled_matcher,
      # No separate conditions for optimized nodes
      conditions: [],
      optimization_type: :compile_time_optimized
    }

    new_nodes = Map.put(state.alpha_nodes, node_id, alpha_node)

    # Initialize empty memory for this node
    :ets.insert(state.alpha_memories, {node_id, []})

    # Index node by fact type for O(1) lookup
    fact_type = extract_fact_type_from_pattern(pattern)
    fact_type_nodes = Map.get(state.fact_type_index, fact_type, [])

    new_fact_type_index =
      Map.put(state.fact_type_index, fact_type, [{node_id, alpha_node} | fact_type_nodes])

    new_state = %{state | alpha_nodes: new_nodes, fact_type_index: new_fact_type_index}
    {:reply, {:ok, node_id}, new_state}
  end

  @impl true
  def handle_call({:remove_alpha_node, node_id}, _from, state) do
    # Get the node before deleting to update the fact type index
    node = Map.get(state.alpha_nodes, node_id)
    new_nodes = Map.delete(state.alpha_nodes, node_id)
    :ets.delete(state.alpha_memories, node_id)

    # NEW: Update fact type index
    new_fact_type_index =
      if node do
        fact_type = extract_fact_type_from_pattern(node.pattern)
        current_nodes = Map.get(state.fact_type_index, fact_type, [])
        filtered_nodes = Enum.filter(current_nodes, fn {id, _node} -> id != node_id end)

        if Enum.empty?(filtered_nodes) do
          Map.delete(state.fact_type_index, fact_type)
        else
          Map.put(state.fact_type_index, fact_type, filtered_nodes)
        end
      else
        state.fact_type_index
      end

    new_state = %{state | alpha_nodes: new_nodes, fact_type_index: new_fact_type_index}
    {:reply, :ok, new_state}
  end

  @impl true
  def handle_call(:get_alpha_nodes, _from, state) do
    {:reply, state.alpha_nodes, state}
  end

  @impl true
  def handle_call({:get_alpha_node_info, node_id}, _from, state) do
    node_info = Map.get(state.alpha_nodes, node_id)
    {:reply, node_info, state}
  end

  @impl true
  def handle_call({:get_alpha_memory, node_id}, _from, state) do
    case :ets.lookup(state.alpha_memories, node_id) do
      [{^node_id, matches}] -> {:reply, matches, state}
      [] -> {:reply, [], state}
    end
  end

  @impl true
  def handle_cast({:process_fact_assertion, fact}, state) do
    new_state = process_fact_through_network(fact, :assert, state)
    {:noreply, new_state}
  end

  @impl true
  def handle_cast({:process_fact_retraction, fact}, state) do
    new_state = process_fact_through_network(fact, :retract, state)
    {:noreply, new_state}
  end

  @impl true
  def terminate(_reason, state) do
    :ets.delete(state.alpha_memories)
    :ets.delete(state.compiled_patterns)
    :ok
  end

  # Private functions

  defp generate_node_id do
    "alpha_" <> Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)
  end

  # NEW: Helper function to extract fact type from pattern
  defp extract_fact_type_from_pattern(pattern) do
    # First element is always the fact type
    elem(pattern, 0)
  end

  defp compile_condition({fact_type, field1, field2, tests}) do
    # Handle 3-element fact pattern
    pattern = {fact_type, field1, field2}
    test_function = compile_tests(tests, [field1, field2])

    {:ok,
     %{
       pattern: pattern,
       test_function: test_function,
       conditions: tests,
       arity: 3
     }}
  end

  defp compile_condition({fact_type, field1, field2, field3, tests}) do
    # Handle 4-element fact pattern
    pattern = {fact_type, field1, field2, field3}
    test_function = compile_tests(tests, [field1, field2, field3])

    {:ok,
     %{
       pattern: pattern,
       test_function: test_function,
       conditions: tests,
       arity: 4
     }}
  end

  defp compile_condition({fact_type, field1, tests}) do
    # Handle 2-element fact pattern
    pattern = {fact_type, field1}
    test_function = compile_tests(tests, [field1])

    {:ok,
     %{
       pattern: pattern,
       test_function: test_function,
       conditions: tests,
       arity: 2
     }}
  end

  defp compile_condition(_) do
    {:error, :invalid_condition_format}
  end

  defp compile_tests([], _fields) do
    # No conditions, always passes
    fn _bindings -> true end
  end

  defp compile_tests(tests, _fields) do
    # Compile tests into a single function for efficiency
    fn bindings ->
      Enum.all?(tests, fn test ->
        evaluate_test(test, bindings)
      end)
    end
  end

  defp evaluate_test({field, operator, value}, bindings) do
    field_value = Map.get(bindings, field)

    case operator do
      :> -> field_value > value
      :< -> field_value < value
      :>= -> field_value >= value
      :<= -> field_value <= value
      :== -> field_value == value
      :!= -> field_value != value
      _ -> false
    end
  end

  defp process_fact_through_network(fact, operation, state) do
    fact_type = elem(fact, 0)

    # NEW: Get only relevant alpha nodes for this fact type (O(1) lookup)
    relevant_nodes = Map.get(state.fact_type_index, fact_type, [])

    # Process fact through relevant nodes only
    Enum.reduce(relevant_nodes, state, fn {node_id, node}, acc_state ->
      process_fact_through_node(fact, node_id, node, operation, acc_state)
    end)
  end

  defp process_fact_through_node(fact, node_id, node, operation, state) do
    case fact_matches_pattern?(fact, node.pattern) do
      true ->
        bindings = extract_bindings_from_fact(fact, node.pattern)

        # If no test conditions, always passes
        test_result = if node.conditions == [], do: true, else: node.test_function.(bindings)

        case test_result do
          true ->
            update_alpha_memory(node_id, bindings, operation, state)

          false ->
            state
        end

      false ->
        state
    end
  end

  defp fact_matches_pattern?(fact, pattern) when tuple_size(fact) != tuple_size(pattern) do
    false
  end

  defp fact_matches_pattern?(fact, pattern) do
    fact_size = tuple_size(fact)
    match_elements?(fact, pattern, fact_size, 0)
  end

  # NEW: Optimized element-by-element matching without tuple conversions
  defp match_elements?(_fact, _pattern, size, index) when index >= size, do: true

  defp match_elements?(fact, pattern, size, index) do
    fact_elem = elem(fact, index)
    pattern_elem = elem(pattern, index)

    element_matches?(fact_elem, pattern_elem, index) and
      match_elements?(fact, pattern, size, index + 1)
  end

  defp element_matches?(_fact_elem, :_, _index), do: true

  defp element_matches?(fact_elem, pattern_elem, 0) do
    # Fact type (position 0) must match exactly
    fact_elem == pattern_elem
  end

  defp element_matches?(fact_elem, pattern_elem, _index) when is_atom(pattern_elem) do
    Utils.variable?(pattern_elem) or fact_elem == pattern_elem
  end

  defp element_matches?(fact_elem, pattern_elem, _index) do
    fact_elem == pattern_elem
  end

  defp extract_bindings_from_fact(fact, pattern) do
    fact_size = tuple_size(fact)
    extract_bindings_from_elements(fact, pattern, fact_size, 1, %{})
  end

  # NEW: Optimized binding extraction without tuple conversions
  defp extract_bindings_from_elements(_fact, _pattern, size, index, acc) when index >= size,
    do: acc

  defp extract_bindings_from_elements(fact, pattern, size, index, acc) do
    pattern_elem = elem(pattern, index)

    new_acc =
      if Utils.variable?(pattern_elem) do
        fact_elem = elem(fact, index)
        Map.put(acc, pattern_elem, fact_elem)
      else
        acc
      end

    extract_bindings_from_elements(fact, pattern, size, index + 1, new_acc)
  end

  defp update_alpha_memory(node_id, bindings, operation, state) do
    case :ets.lookup(state.alpha_memories, node_id) do
      [{^node_id, current_matches}] ->
        new_matches = apply_memory_operation(operation, bindings, current_matches)
        :ets.insert(state.alpha_memories, {node_id, new_matches})
        state

      [] ->
        # Node doesn't exist in memory table
        state
    end
  end

  defp apply_memory_operation(:assert, bindings, current_matches) do
    # Add to memory if not already present
    if bindings in current_matches do
      current_matches
    else
      [bindings | current_matches]
    end
  end

  defp apply_memory_operation(:retract, bindings, current_matches) do
    # Remove from memory
    List.delete(current_matches, bindings)
  end
end

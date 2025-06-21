defmodule Presto.RuleEngine do
  @moduledoc """
  Main rule engine GenServer that coordinates the RETE algorithm components.

  Manages rules, coordinates fact processing through alpha and beta networks,
  and handles rule execution with proper error handling and performance monitoring.
  """

  use GenServer
  require Logger

  alias Presto.BetaNetwork
  alias Presto.Logger, as: PrestoLogger
  alias Presto.Rule
  alias Presto.RuleEngine.AlphaNetworkCoordinator
  alias Presto.RuleEngine.BetaNetworkCoordinator
  alias Presto.RuleEngine.Configuration
  alias Presto.RuleEngine.ExecutionTracker
  alias Presto.RuleEngine.FactLineage
  alias Presto.RuleEngine.RuleMetadata
  alias Presto.RuleEngine.RuleStorage
  alias Presto.RuleEngine.State
  alias Presto.RuleEngine.Statistics
  alias Presto.RuleEngine.WorkingMemory
  alias Presto.PatternMatching

  @type rule :: %{
          id: atom(),
          conditions: [condition()],
          action: function(),
          priority: integer()
        }

  @type condition :: tuple()
  @type rule_result :: tuple()
  @type rule_error :: {:error, atom(), Exception.t()}

  # Client API

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    engine_id = Keyword.get(opts, :engine_id, generate_engine_id())
    PrestoLogger.log_engine_lifecycle(:info, engine_id, "starting", %{opts: opts})

    case GenServer.start_link(__MODULE__, Keyword.put(opts, :engine_id, engine_id)) do
      {:ok, pid} = result ->
        PrestoLogger.log_engine_lifecycle(:info, engine_id, "started", %{pid: pid})
        result

      {:error, reason} = error ->
        PrestoLogger.log_engine_lifecycle(:error, engine_id, "start_failed", %{reason: reason})
        error
    end
  end

  @spec add_rule(GenServer.server(), rule()) :: :ok | {:error, term()}
  def add_rule(pid, rule) do
    rule_id = if is_map(rule) and Map.has_key?(rule, :id), do: rule.id, else: :unknown
    PrestoLogger.log_rule_compilation(:info, rule_id, "adding_rule", %{rule: rule})
    GenServer.call(pid, {:add_rule, rule})
  end

  @spec remove_rule(GenServer.server(), atom()) :: :ok
  def remove_rule(pid, rule_id) do
    GenServer.call(pid, {:remove_rule, rule_id})
  end

  @spec get_rules(GenServer.server()) :: %{atom() => rule()}
  def get_rules(pid) do
    GenServer.call(pid, :get_rules)
  end

  @spec assert_fact(GenServer.server(), tuple()) :: :ok
  def assert_fact(pid, fact) do
    PrestoLogger.log_fact_processing(:debug, elem(fact, 0), "asserting_fact", %{fact: fact})
    GenServer.call(pid, {:assert_fact, fact})
  end

  @spec retract_fact(GenServer.server(), tuple()) :: :ok
  def retract_fact(pid, fact) do
    GenServer.call(pid, {:retract_fact, fact})
  end

  @spec get_facts(GenServer.server()) :: [tuple()]
  def get_facts(pid) do
    GenServer.call(pid, :get_facts)
  end

  @spec clear_facts(GenServer.server()) :: :ok
  def clear_facts(pid) do
    GenServer.call(pid, :clear_facts)
  end

  @spec fire_rules(GenServer.server(), keyword()) :: [rule_result()]
  def fire_rules(pid, opts \\ []) do
    PrestoLogger.log_with_timing(
      :info,
      "fire_rules",
      fn ->
        GenServer.call(pid, {:fire_rules, opts}, 30_000)
      end,
      %{opts: opts}
    )
  end

  @spec fire_rules_incremental(GenServer.server()) :: [rule_result()]
  def fire_rules_incremental(pid) do
    GenServer.call(pid, :fire_rules_incremental)
  end

  @spec fire_rules_with_errors(GenServer.server()) :: {:ok, [rule_result()], [rule_error()]}
  def fire_rules_with_errors(pid) do
    GenServer.call(pid, :fire_rules_with_errors)
  end

  @spec get_last_execution_order(GenServer.server()) :: [atom()]
  def get_last_execution_order(pid) do
    GenServer.call(pid, :get_last_execution_order)
  end

  @spec get_rule_statistics(GenServer.server()) :: %{atom() => map()}
  def get_rule_statistics(pid) do
    GenServer.call(pid, :get_rule_statistics)
  end

  @spec get_engine_statistics(GenServer.server()) :: map()
  def get_engine_statistics(pid) do
    GenServer.call(pid, :get_engine_statistics)
  end

  @spec analyze_rule(GenServer.server(), atom()) :: map()
  def analyze_rule(pid, rule_id) do
    GenServer.call(pid, {:analyze_rule, rule_id})
  end

  @spec get_rule_set_analysis(GenServer.server()) :: map()
  def get_rule_set_analysis(pid) do
    GenServer.call(pid, :analyze_rule_set)
  end

  @spec configure_optimization(GenServer.server(), keyword()) :: :ok
  def configure_optimization(pid, opts) do
    GenServer.call(pid, {:configure_optimization, opts})
  end

  @spec get_optimization_config(GenServer.server()) :: map()
  def get_optimization_config(pid) do
    GenServer.call(pid, :get_optimization_config)
  end

  # Private functions

  # Rule Analysis (merged from RuleAnalyzer)
  defp analyze_rule(rule) do
    conditions = Map.get(rule, :conditions, [])
    {patterns, tests} = separate_conditions(conditions)

    pattern_count = length(patterns)
    test_count = length(tests)
    join_count = calculate_join_count(patterns)

    complexity = determine_complexity(pattern_count, test_count, join_count)
    strategy = choose_execution_strategy(complexity, pattern_count, join_count)

    %{
      complexity: complexity,
      strategy: strategy,
      pattern_count: pattern_count,
      test_count: test_count,
      join_count: join_count,
      fact_types: extract_fact_types(patterns),
      variable_count: count_variables(patterns)
    }
  end

  defp determine_complexity(pattern_count, test_count, join_count) do
    total_complexity = pattern_count + test_count + join_count * 2

    cond do
      total_complexity <= 2 and join_count == 0 -> :simple
      total_complexity <= 5 and join_count <= 1 -> :moderate
      true -> :complex
    end
  end

  defp choose_execution_strategy(:simple, pattern_count, join_count)
       when pattern_count <= 2 and join_count == 0 do
    :fast_path
  end

  defp choose_execution_strategy(_complexity, _pattern_count, _join_count) do
    :rete_network
  end

  defp calculate_join_count(patterns) when length(patterns) <= 1, do: 0

  defp calculate_join_count(patterns) do
    # Count potential joins based on shared variables
    patterns
    |> Enum.map(&extract_variables_from_pattern/1)
    |> count_variable_intersections()
  end

  defp count_variable_intersections(variable_sets) do
    variable_sets
    |> Enum.with_index()
    |> Enum.flat_map(fn {vars1, i} ->
      variable_sets
      |> Enum.drop(i + 1)
      |> Enum.map(fn vars2 ->
        length(Enum.filter(vars1, &(&1 in vars2)))
      end)
    end)
    |> Enum.sum()
  end

  defp extract_fact_types(patterns) do
    patterns
    |> Enum.map(&elem(&1, 0))
    |> Enum.uniq()
  end

  defp count_variables(patterns) do
    patterns
    |> Enum.flat_map(&extract_variables_from_pattern/1)
    |> Enum.uniq()
    |> length()
  end

  defp analyze_rule_set(rules) do
    analyses = Enum.map(rules, &analyze_rule/1)

    fast_path_count = Enum.count(analyses, &(&1.strategy == :fast_path))
    total_rules = length(rules)

    %{
      total_rules: total_rules,
      fast_path_eligible: fast_path_count,
      complexity_distribution: %{
        simple: Enum.count(analyses, &(&1.complexity == :simple)),
        moderate: Enum.count(analyses, &(&1.complexity == :moderate)),
        complex: Enum.count(analyses, &(&1.complexity == :complex))
      },
      fact_type_coverage: analyses |> Enum.flat_map(& &1.fact_types) |> Enum.uniq() |> length()
    }
  end

  # Server implementation

  @impl true
  def init(opts) do
    case State.new(opts) do
      {:ok, state} ->
        # Start beta network using the coordinator
        case BetaNetworkCoordinator.start_beta_network(state) do
          {:ok, updated_state} -> {:ok, updated_state}
          {:error, reason} -> {:stop, reason}
        end

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def handle_call({:add_rule, rule}, _from, state) do
    case Rule.validate(rule) do
      :ok -> process_valid_rule(rule, state)
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  catch
    {:error, reason} ->
      {:reply, {:error, reason}, state}
  end

  @impl true
  def handle_call({:remove_rule, rule_id}, _from, state) do
    case RuleMetadata.get_rule_networks(state, rule_id) do
      nil ->
        # Rule doesn't exist, that's fine
        {:reply, :ok, state}

      network_nodes ->
        # Remove network nodes
        new_state = cleanup_rule_network(network_nodes, state)

        final_state =
          new_state
          |> RuleStorage.remove_rule(rule_id)
          |> RuleMetadata.remove_rule_metadata(rule_id)
          |> Statistics.remove_rule_statistics(rule_id)

        {:reply, :ok, final_state}
    end
  end

  @impl true
  def handle_call(:get_rules, _from, state) do
    rules = RuleStorage.get_rules(state)
    {:reply, rules, state}
  end

  @impl true
  def handle_call({:assert_fact, fact}, _from, state) do
    # Assert fact using WorkingMemory module
    new_state = WorkingMemory.assert_fact(state, fact)

    # Process fact through alpha network
    alpha_processed_state = AlphaNetworkCoordinator.process_fact_assertion(new_state, fact)

    # Track fact lineage
    fact_key = FactLineage.create_fact_key(fact)

    lineage_info = %{
      fact: fact,
      generation: alpha_processed_state.fact_generation,
      source: :input,
      derived_from: [],
      derived_by_rule: nil,
      timestamp: System.system_time(:microsecond)
    }

    final_state =
      alpha_processed_state
      |> FactLineage.update_fact_lineage(fact_key, lineage_info)
      |> Statistics.update_total_facts(1)
      |> FactLineage.update_facts_since_incremental([
        fact | alpha_processed_state.facts_since_incremental
      ])

    {:reply, :ok, final_state}
  end

  @impl true
  def handle_call({:retract_fact, fact}, _from, state) do
    # Retract fact using WorkingMemory module
    new_state = WorkingMemory.retract_fact(state, fact)

    # Process fact retraction through alpha network
    alpha_processed_state = AlphaNetworkCoordinator.process_fact_retraction(new_state, fact)

    # Update statistics
    final_state = Statistics.update_total_facts(alpha_processed_state, -1)

    {:reply, :ok, final_state}
  end

  @impl true
  def handle_call(:get_facts, _from, state) do
    facts = WorkingMemory.get_facts(state)
    {:reply, facts, state}
  end

  @impl true
  def handle_call(:clear_facts, _from, state) do
    new_state = WorkingMemory.clear_facts(state)
    final_state = Statistics.reset_total_facts(new_state)

    {:reply, :ok, final_state}
  end

  @impl true
  def handle_call({:fire_rules, opts}, _from, state) do
    {time, {results, updated_state}} =
      :timer.tc(fn ->
        concurrent = Keyword.get(opts, :concurrent, false)
        auto_chain = Keyword.get(opts, :auto_chain, false)

        if auto_chain do
          execute_rules_with_chaining(state, concurrent)
        else
          execute_rules(state, concurrent)
        end
      end)

    # Clear incremental tracking since we've processed all facts
    state_with_stats =
      Statistics.update_execution_statistics(updated_state, time, length(results))

    final_state =
      state_with_stats
      |> FactLineage.update_facts_since_incremental([])
      |> FactLineage.update_incremental_timestamp(System.system_time(:microsecond))

    {:reply, results, final_state}
  end

  @impl true
  def handle_call(:fire_rules_incremental, _from, state) do
    # Incremental implementation - only process new facts
    if Enum.empty?(state.facts_since_incremental) do
      # No new facts since last incremental execution
      {:reply, [], state}
    else
      # Process rules but filter results to only include those involving new facts
      {all_results, updated_state} = execute_rules(state, false)

      # For simple implementation, assume all results are from new facts
      # In a full implementation, we'd track which results involve new facts
      incremental_results =
        filter_incremental_results(all_results, state.facts_since_incremental, state)

      # Clear the new facts list and update timestamp
      final_state =
        updated_state
        |> FactLineage.update_facts_since_incremental([])
        |> FactLineage.update_incremental_timestamp(System.system_time(:microsecond))

      {:reply, incremental_results, final_state}
    end
  end

  @impl true
  def handle_call(:fire_rules_with_errors, _from, state) do
    {results, errors} = execute_rules_with_error_handling(state)
    {:reply, {:ok, results, errors}, state}
  end

  @impl true
  def handle_call(:get_last_execution_order, _from, state) do
    execution_order = ExecutionTracker.get_last_execution_order(state)
    {:reply, execution_order, state}
  end

  @impl true
  def handle_call(:get_rule_statistics, _from, state) do
    statistics = Statistics.get_all_rule_statistics(state)
    {:reply, statistics, state}
  end

  @impl true
  def handle_call(:get_engine_statistics, _from, state) do
    statistics = Statistics.get_engine_statistics(state)
    {:reply, statistics, state}
  end

  @impl true
  def handle_call({:analyze_rule, rule_id}, _from, state) do
    analysis = RuleMetadata.get_rule_analysis(state, rule_id)
    {:reply, analysis, state}
  end

  @impl true
  def handle_call(:analyze_rule_set, _from, state) do
    rules = RuleStorage.get_rules(state) |> Map.values()
    analysis = analyze_rule_set(rules)
    {:reply, analysis, state}
  end

  @impl true
  def handle_call({:configure_optimization, opts}, _from, state) do
    new_state = Configuration.update_optimization_config(state, Map.new(opts))
    {:reply, :ok, new_state}
  end

  @impl true
  def handle_call(:get_optimization_config, _from, state) do
    config = Configuration.get_optimization_config(state)
    {:reply, config, state}
  end

  @impl true
  def handle_call({:get_alpha_memory, node_id}, _from, state) do
    memory = AlphaNetworkCoordinator.get_alpha_memory(state, node_id)
    {:reply, memory, state}
  end

  @impl true
  def terminate(_reason, state) do
    # Stop beta network using coordinator
    BetaNetworkCoordinator.stop_beta_network(state)
    # Clean up state resources
    State.cleanup(state)
  end

  # Private functions

  defp generate_engine_id do
    "engine_" <> (:crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower))
  end

  defp evaluate_operator(:>, bound_value, value), do: bound_value > value
  defp evaluate_operator(:<, bound_value, value), do: bound_value < value
  defp evaluate_operator(:>=, bound_value, value), do: bound_value >= value
  defp evaluate_operator(:<=, bound_value, value), do: bound_value <= value
  defp evaluate_operator(:==, bound_value, value), do: bound_value == value
  defp evaluate_operator(:!=, bound_value, value), do: bound_value != value
  defp evaluate_operator(_, _bound_value, _value), do: false

  defp create_rule_network(rule, rule_analysis, state) do
    if should_use_fast_path?(rule_analysis, state) do
      {%{alpha_nodes: [], beta_nodes: []}, state}
    else
      case compile_rule_to_network(rule, state) do
        {:ok, network_nodes, updated_state} -> {network_nodes, updated_state}
        {:error, reason} -> throw({:error, reason})
      end
    end
  end

  defp update_fast_path_rules_with_metadata(state, rule, rule_analysis) do
    if rule_analysis.strategy == :fast_path do
      fact_type = extract_fact_type_from_rule(rule)
      current_fast_path_rules = Map.get(state.fast_path_rules, fact_type, [])
      RuleMetadata.update_fast_path_rules(state, fact_type, [rule.id | current_fast_path_rules])
    else
      state
    end
  end

  defp update_rule_statistics_with_metadata(state, rule, rule_analysis) do
    initial_stats = %{
      executions: 0,
      total_time: 0,
      average_time: 0,
      facts_processed: 0,
      strategy_used: rule_analysis.strategy,
      complexity: rule_analysis.complexity
    }

    Statistics.initialize_rule_statistics(state, rule.id, initial_stats)
  end

  defp process_valid_rule(rule, state) do
    case Map.get(rule, :type) do
      :aggregation ->
        process_aggregation_rule(rule, state)

      _ ->
        process_standard_rule(rule, state)
    end
  end

  defp process_standard_rule(rule, state) do
    # Phase 1: Standard rule analysis
    rule_analysis = analyze_rule(rule)

    # Phase 2: Create network (simplified - no compile-time optimization)
    {network_nodes, new_state} = create_rule_network(rule, rule_analysis, state)

    # Use SRP-compliant modules to update state
    final_state =
      new_state
      |> RuleStorage.add_rule(rule.id, rule)
      |> RuleMetadata.update_rule_networks(rule.id, network_nodes)
      |> RuleMetadata.update_rule_analyses(rule.id, rule_analysis)
      |> update_fast_path_rules_with_metadata(rule, rule_analysis)
      |> update_rule_statistics_with_metadata(rule, rule_analysis)

    {:reply, :ok, final_state}
  end

  defp process_aggregation_rule(rule, state) do
    # Create alpha nodes for conditions (patterns to match)
    {alpha_nodes, state_with_alpha} = create_alpha_nodes_for_conditions(rule.conditions, state)

    # Create aggregation node in beta network
    input_source =
      if length(alpha_nodes) == 1 do
        # Single alpha node input
        hd(alpha_nodes)
      else
        # Multiple alpha nodes need join first
        {beta_join_nodes, _} = create_beta_nodes_for_rule(alpha_nodes, state_with_alpha)
        # Use the final join result
        List.last(beta_join_nodes)
      end

    # Create aggregation node
    {:ok, agg_node_id} =
      BetaNetworkCoordinator.create_aggregation_node(
        state_with_alpha,
        {:aggregate, input_source, rule.group_by, rule.aggregate, rule.field}
      )

    network_nodes = %{
      alpha_nodes: alpha_nodes,
      beta_nodes: [agg_node_id],
      aggregation_node: agg_node_id
    }

    final_state =
      state_with_alpha
      |> RuleStorage.add_rule(rule.id, rule)
      |> RuleMetadata.update_rule_networks(rule.id, network_nodes)

    {:reply, :ok, final_state}
  end

  defp compile_rule_to_network(rule, state) do
    # Simple implementation: compile conditions to alpha/beta network
    # For now, create alpha nodes for each condition pattern
    # and a final beta node to join them all
    {alpha_nodes, new_state} = create_alpha_nodes_for_conditions(rule.conditions, state)

    # Create beta nodes to join alpha nodes if needed
    {beta_nodes, final_state} = create_beta_nodes_for_rule(alpha_nodes, new_state)

    network_nodes = %{
      alpha_nodes: alpha_nodes,
      beta_nodes: beta_nodes
    }

    {:ok, network_nodes, final_state}
  rescue
    error ->
      {:error, {:compilation_failed, error}}
  end

  defp create_alpha_nodes_for_conditions(conditions, state) do
    # Extract pattern-based conditions and create alpha nodes
    {pattern_conditions, _test_conditions} = separate_conditions(conditions)

    Enum.reduce(pattern_conditions, {[], state}, fn condition, {acc_nodes, acc_state} ->
      {:ok, node_id, new_state} = AlphaNetworkCoordinator.create_alpha_node(acc_state, condition)
      {[node_id | acc_nodes], new_state}
    end)
  end

  defp create_beta_nodes_for_rule(alpha_nodes, state) when length(alpha_nodes) <= 1 do
    # No joins needed for single alpha node
    {[], state}
  end

  defp create_beta_nodes_for_rule([node1, node2], state) do
    # Simple two-node join - determine join key from alpha node patterns
    join_key = determine_join_key(node1, node2, state)

    {:ok, beta_id} =
      BetaNetworkCoordinator.create_beta_node(state, {:join, node1, node2, join_key})

    {[beta_id], state}
  end

  defp create_beta_nodes_for_rule(alpha_nodes, state) do
    # For multiple nodes, create chain of beta nodes sequentially
    # Each beta node joins the previous result with the next alpha node
    create_sequential_joins(alpha_nodes, state)
  end

  defp create_sequential_joins([first_alpha, second_alpha | rest], state) do
    # Create first beta node joining first two alpha nodes
    join_key = determine_join_key(first_alpha, second_alpha, state)

    {:ok, first_beta} =
      BetaNetworkCoordinator.create_beta_node(
        state,
        {:join, first_alpha, second_alpha, join_key}
      )

    # Continue chaining with remaining alpha nodes
    chain_remaining_nodes(first_beta, rest, [first_beta], state)
  end

  defp create_sequential_joins(_alpha_nodes, state) do
    # Less than 2 nodes, no joins needed
    {[], state}
  end

  defp chain_remaining_nodes(_current_beta, [], acc_betas, state) do
    # No more nodes to join
    {acc_betas, state}
  end

  defp chain_remaining_nodes(current_beta, [next_alpha | rest], acc_betas, state) do
    # Join current beta result with next alpha node
    # For beta-alpha joins, we need to find a common variable
    join_key = determine_beta_alpha_join_key(current_beta, next_alpha, state)

    {:ok, new_beta} =
      BetaNetworkCoordinator.create_beta_node(
        state,
        {:join, current_beta, next_alpha, join_key}
      )

    chain_remaining_nodes(new_beta, rest, [new_beta | acc_betas], state)
  end

  # Helper functions to determine join keys based on alpha node patterns
  defp determine_join_key(alpha_node1, alpha_node2, state) do
    # Get alpha node info to determine their patterns
    node1_info = AlphaNetworkCoordinator.get_alpha_node_info(state, alpha_node1)
    node2_info = AlphaNetworkCoordinator.get_alpha_node_info(state, alpha_node2)

    case {node1_info, node2_info} do
      {%{pattern: pattern1}, %{pattern: pattern2}} ->
        find_common_variable(pattern1, pattern2)

      _ ->
        # fallback
        :name
    end
  end

  defp determine_beta_alpha_join_key(_beta_node, alpha_node, state) do
    # For beta-alpha joins, we assume the beta node has all previous variables
    # and we look for any variable that the alpha node also has
    node_info = AlphaNetworkCoordinator.get_alpha_node_info(state, alpha_node)

    case node_info do
      %{pattern: pattern} ->
        extract_first_variable_from_pattern(pattern)

      _ ->
        # fallback
        :name
    end
  end

  defp find_common_variable(pattern1, pattern2) do
    vars1 = extract_variables_from_pattern(pattern1)
    vars2 = extract_variables_from_pattern(pattern2)

    case Enum.find(vars1, fn var -> var in vars2 end) do
      # fallback if no common variable
      nil -> :name
      common_var -> common_var
    end
  end

  defp extract_variables_from_pattern(pattern) do
    pattern
    |> Tuple.to_list()
    # Skip fact type
    |> Enum.drop(1)
    |> Enum.filter(&variable?/1)
  end

  defp extract_first_variable_from_pattern(pattern) do
    case extract_variables_from_pattern(pattern) do
      [first_var | _] -> first_var
      # fallback
      [] -> :name
    end
  end

  defp variable?(atom) when is_atom(atom) do
    # Variables are atoms that are not fact types or literals
    atom != :_ and not is_fact_type?(atom)
  end

  defp variable?(_), do: false

  defp is_fact_type?(atom) when is_atom(atom) do
    # Common fact types in our domain (only the first element of facts)
    atom in [:person, :employment, :number, :employee, :order]
  end

  defp separate_conditions(conditions) do
    # Separate fact patterns from test conditions
    {patterns, tests} = Enum.split_with(conditions, &fact_pattern?/1)

    # Convert patterns to alpha conditions with their tests
    alpha_conditions = Enum.map(patterns, &convert_pattern_to_alpha(&1, tests))

    {alpha_conditions, tests}
  end

  defp fact_pattern?(condition) do
    case condition do
      {variable, operator, _value}
      when operator in [:>, :<, :>=, :<=, :==, :!=] and is_atom(variable) ->
        false

      {fact_type, _field1} when is_atom(fact_type) ->
        true

      {fact_type, _field1, _field2} when is_atom(fact_type) ->
        true

      {fact_type, _field1, _field2, _field3} when is_atom(fact_type) ->
        true

      _ ->
        false
    end
  end

  defp convert_pattern_to_alpha(pattern, tests) do
    # Don't append tests to the pattern tuple - keep them separate
    case pattern do
      {_fact_type, field1} ->
        relevant_tests = find_relevant_tests([field1], tests)
        {pattern, relevant_tests}

      {_fact_type, field1, field2} ->
        relevant_tests = find_relevant_tests([field1, field2], tests)
        {pattern, relevant_tests}

      {_fact_type, field1, field2, field3} ->
        relevant_tests = find_relevant_tests([field1, field2, field3], tests)
        {pattern, relevant_tests}

      _ ->
        # For any other pattern, just return it with all tests
        {pattern, tests}
    end
  end

  defp find_relevant_tests(fields, tests) do
    Enum.filter(tests, fn
      {field, _op, _val} -> field in fields
      _ -> false
    end)
  end

  defp cleanup_rule_network(network_nodes, state) do
    # Remove alpha nodes
    new_state =
      Enum.reduce(network_nodes.alpha_nodes, state, fn node_id, acc_state ->
        AlphaNetworkCoordinator.remove_alpha_node(acc_state, node_id)
      end)

    # Remove beta nodes
    Enum.each(network_nodes.beta_nodes, fn node_id ->
      BetaNetworkCoordinator.remove_beta_node(new_state, node_id)
    end)

    new_state
  end

  defp execute_rules(state, concurrent) do
    # NEW: Use optimized execution strategy
    if Configuration.fast_path_enabled?(state) do
      execute_rules_optimized(state, concurrent)
    else
      execute_rules_traditional(state, concurrent)
    end
  end

  # NEW: Optimized execution with fast-path and batching
  defp execute_rules_optimized(state, concurrent) do
    # Process changes through beta network for RETE rules only
    BetaNetworkCoordinator.process_alpha_changes(state)

    # Separate fast-path rules from RETE rules
    {fast_path_rules, rete_rules} = separate_rules_by_strategy(state)

    # Execute fast-path rules efficiently
    fast_path_results = execute_fast_path_rules(fast_path_rules, state)

    # Execute RETE rules traditionally
    rete_results = execute_rete_rules(rete_rules, state, concurrent)

    # Combine results and update statistics
    all_results = fast_path_results ++ rete_results
    execution_order = extract_execution_order(fast_path_rules, rete_rules)

    updated_state = %{
      state
      | last_execution_order: execution_order,
        engine_statistics:
          update_optimization_statistics(state.engine_statistics, fast_path_rules, rete_rules)
    }

    # Collect rule statistics
    all_rule_ids = Enum.map(fast_path_rules ++ rete_rules, fn {rule_id, _rule} -> rule_id end)
    final_state = Statistics.collect_rule_statistics(updated_state, all_rule_ids)

    {List.flatten(all_results), final_state}
  end

  # Traditional execution without optimizations
  defp execute_rules_traditional(state, concurrent) do
    # Process changes through beta network
    BetaNetworkCoordinator.process_alpha_changes(state)

    # Get rules sorted by priority
    sorted_rules =
      state.rules
      |> Enum.sort_by(
        fn {_id, rule} ->
          Map.get(rule, :priority, 0)
        end,
        :desc
      )

    execution_order = Enum.map(sorted_rules, fn {id, _rule} -> id end)

    # Execute rules
    results =
      if concurrent do
        execute_rules_concurrent(sorted_rules, state)
      else
        execute_rules_sequential(sorted_rules, state)
      end

    # Update execution order tracking and collect rule statistics
    updated_state =
      Enum.reduce(execution_order, state, fn rule_id, acc_state ->
        ExecutionTracker.record_execution(acc_state, rule_id)
      end)

    # Collect updated rule statistics from process dictionary
    rule_ids = Enum.map(sorted_rules, fn {rule_id, _rule} -> rule_id end)
    final_state = Statistics.collect_rule_statistics(updated_state, rule_ids)

    {List.flatten(results), final_state}
  end

  defp execute_rules_sequential(rules, state) do
    Enum.map(rules, fn {rule_id, rule} ->
      execute_single_rule(rule_id, rule, state)
    end)
  end

  defp execute_rules_concurrent(rules, state) do
    # Execute rules in parallel tasks
    tasks =
      Enum.map(rules, fn {rule_id, rule} ->
        Task.async(fn ->
          execute_single_rule(rule_id, rule, state)
        end)
      end)

    Task.await_many(tasks, 30_000)
  end

  # NEW: Execute rules with automatic chaining until convergence
  defp execute_rules_with_chaining(state, concurrent) do
    execute_rules_with_chaining(state, concurrent, [], 0, 10)
  end

  defp execute_rules_with_chaining(state, _concurrent, all_results, cycle, max_cycles)
       when cycle >= max_cycles do
    # Prevent infinite loops - return results after max cycles
    {List.flatten(all_results), state}
  end

  defp execute_rules_with_chaining(state, concurrent, all_results, cycle, max_cycles) do
    # Execute one cycle of rules using traditional execution (avoid recursion)
    {cycle_results, updated_state} = execute_rules_traditional(state, concurrent)

    # If no new facts were produced, we've reached convergence
    if Enum.empty?(cycle_results) do
      {List.flatten(all_results), updated_state}
    else
      # Assert all results back into working memory for next cycle
      new_state =
        Enum.reduce(cycle_results, updated_state, fn fact, acc_state ->
          # Assert fact into working memory using WorkingMemory module
          wm_state = WorkingMemory.assert_fact(acc_state, fact)
          intermediate_state = AlphaNetworkCoordinator.process_fact_assertion(wm_state, fact)

          # Track fact lineage for derived facts (no specific rule context in chaining)
          fact_key = FactLineage.create_fact_key(fact)

          lineage_info = %{
            fact: fact,
            generation: intermediate_state.fact_generation,
            source: :derived,
            # Could be enhanced to track chaining sources
            derived_from: [],
            derived_by_rule: :chaining,
            timestamp: System.system_time(:microsecond)
          }

          # Update fact lineage and statistics
          intermediate_state
          |> FactLineage.update_fact_lineage(fact_key, lineage_info)
          |> Statistics.update_execution_statistics(0, 1)
          |> FactLineage.update_facts_since_incremental([
            fact | intermediate_state.facts_since_incremental
          ])
        end)

      # Continue with next cycle
      execute_rules_with_chaining(
        new_state,
        concurrent,
        [cycle_results | all_results],
        cycle + 1,
        max_cycles
      )
    end
  end

  defp execute_single_rule(rule_id, rule, state) do
    execute_single_rule(rule_id, rule, state, :no_error_handling)
  end

  defp execute_single_rule(rule_id, rule, state, error_handling) do
    case Map.get(rule, :type) do
      :aggregation ->
        execute_aggregation_rule(rule_id, rule, state, error_handling)

      _ ->
        execute_standard_rule(rule_id, rule, state, error_handling)
    end
  end

  defp execute_standard_rule(rule_id, rule, state, :no_error_handling) do
    # Get matching facts for this rule
    # This is simplified - production would use proper network results
    facts = get_rule_matches(rule, state)

    # Execute rule action for each match and track statistics
    {time, results} =
      :timer.tc(fn ->
        Enum.flat_map(facts, fn fact_bindings ->
          try do
            rule.action.(fact_bindings)
          rescue
            _error ->
              []
          end
        end)
      end)

    # Update rule statistics
    Statistics.update_rule_statistics(state, rule_id, time, length(facts))

    results
  end

  defp execute_standard_rule(rule_id, rule, state, :with_error_handling) do
    # Get matching facts for this rule
    facts = get_rule_matches(rule, state)

    # Execute rule action for each match - let exceptions bubble up
    {time, results} =
      :timer.tc(fn ->
        Enum.flat_map(facts, fn fact_bindings ->
          rule.action.(fact_bindings)
        end)
      end)

    # Update rule statistics (only if no exception occurred)
    Statistics.update_rule_statistics(state, rule_id, time, length(facts))

    results
  end

  defp execute_aggregation_rule(rule_id, rule, state, _error_handling) do
    # Get aggregation results from beta network
    network_nodes = Map.get(state.rule_networks, rule_id, %{})
    agg_node_id = Map.get(network_nodes, :aggregation_node)

    {time, results} =
      :timer.tc(fn -> get_and_transform_aggregation_results(agg_node_id, rule.output, state) end)

    # Update rule statistics
    Statistics.update_rule_statistics(state, rule_id, time, length(results))

    results
  end

  defp transform_aggregation_result(result, output_pattern) when is_tuple(output_pattern) do
    # Transform the aggregation result map into the specified output pattern
    output_list = Tuple.to_list(output_pattern)

    # Find the aggregated value key (e.g., :count, :sum_hours, etc.)
    aggregate_key =
      result
      |> Map.keys()
      |> Enum.find(fn key ->
        key_str = Atom.to_string(key)

        String.starts_with?(key_str, "sum_") or
          String.starts_with?(key_str, "avg_") or
          String.starts_with?(key_str, "min_") or
          String.starts_with?(key_str, "max_") or
          String.starts_with?(key_str, "collect_") or
          key == :count
      end)

    aggregate_value = Map.get(result, aggregate_key)

    transformed =
      Enum.map(output_list, fn
        # Replace :value with the aggregated value
        :value ->
          aggregate_value

        # For tuples (like group key tuples), replace atoms with their values
        tuple when is_tuple(tuple) ->
          tuple
          |> Tuple.to_list()
          |> Enum.map(fn
            atom when is_atom(atom) -> Map.get(result, atom, atom)
            other -> other
          end)
          |> List.to_tuple()

        # For direct atoms, get their value from the result
        atom when is_atom(atom) ->
          Map.get(result, atom, atom)

        # Pass through anything else
        other ->
          other
      end)

    List.to_tuple(transformed)
  end

  defp execute_rules_with_error_handling(state) do
    BetaNetwork.process_alpha_changes(state.beta_network)

    sorted_rules =
      state.rules
      |> Enum.sort_by(
        fn {_id, rule} ->
          Map.get(rule, :priority, 0)
        end,
        :desc
      )

    {results, errors} =
      Enum.reduce(sorted_rules, {[], []}, fn {rule_id, rule}, {acc_results, acc_errors} ->
        try do
          rule_results = execute_single_rule(rule_id, rule, state, :with_error_handling)
          {acc_results ++ rule_results, acc_errors}
        rescue
          error ->
            error_info = {:error, rule_id, error}
            {acc_results, [error_info | acc_errors]}
        end
      end)

    {List.flatten(results), Enum.reverse(errors)}
  end

  defp get_rule_matches(rule, state) do
    case Map.get(state.rule_networks, rule.id) do
      nil ->
        []

      network_nodes ->
        get_matches_from_network_nodes(network_nodes, state)
    end
  end

  defp get_matches_from_network_nodes(network_nodes, state) do
    case network_nodes.beta_nodes do
      [_ | _] = beta_nodes ->
        final_beta = find_final_beta_node(beta_nodes, state)
        BetaNetworkCoordinator.get_beta_memory(state, final_beta)

      [] ->
        get_matches_from_alpha_nodes(network_nodes.alpha_nodes, state)
    end
  end

  defp get_matches_from_alpha_nodes([alpha_node | _], state) do
    AlphaNetworkCoordinator.get_alpha_memory(state, alpha_node)
  end

  defp get_matches_from_alpha_nodes([], _state) do
    []
  end

  defp find_final_beta_node(beta_nodes, state) do
    # Find the beta node that is not used as input to any other beta node
    # fallback to first if none found
    Enum.find(beta_nodes, &node_is_not_input?(&1, beta_nodes, state)) || hd(beta_nodes)
  end

  defp node_is_not_input?(node_id, beta_nodes, state) do
    not Enum.any?(beta_nodes, &node_uses_as_input?(&1, node_id, state))
  end

  defp node_uses_as_input?(other_id, node_id, state) when other_id != node_id do
    case BetaNetworkCoordinator.get_beta_node_info(state, other_id) do
      %{left_input: ^node_id} -> true
      %{right_input: ^node_id} -> true
      _ -> false
    end
  end

  defp node_uses_as_input?(_other_id, _node_id, _state), do: false

  defp get_and_transform_aggregation_results(agg_node_id, output, state) do
    if agg_node_id do
      # Get aggregation results from beta memory
      agg_results = BetaNetworkCoordinator.get_beta_memory(state, agg_node_id)

      # Transform aggregation results to output facts
      Enum.map(agg_results, fn result ->
        transform_aggregation_result(result, output)
      end)
    else
      []
    end
  end

  defp filter_incremental_results(all_results, new_facts, state) do
    # Use fact lineage tracking to identify results that involve new facts
    new_fact_keys = Enum.map(new_facts, &FactLineage.create_fact_key/1)

    # Get all facts derived from new facts (transitively)
    derived_facts = get_facts_derived_from_new_facts(new_fact_keys, state)

    # Filter results to only include those that are derived facts or involve new facts
    all_relevant_facts = new_facts ++ derived_facts

    all_relevant_fact_keys =
      Enum.map(all_relevant_facts, &FactLineage.create_fact_key/1) |> MapSet.new()

    Enum.filter(all_results, fn result ->
      result_key = FactLineage.create_fact_key(result)

      MapSet.member?(all_relevant_fact_keys, result_key) or
        result_involves_new_facts?(result, new_facts)
    end)
  end

  defp extract_fact_identifiers(facts) do
    Enum.flat_map(facts, &extract_identifier_from_fact/1)
  end

  defp extract_identifier_from_fact(fact) do
    case fact do
      {:person, name, _age} -> [name]
      {:employment, name, _company} -> [name]
      {:company, name, _industry} -> [name]
      _ -> []
    end
  end

  defp result_involves_identifiers?(result, identifiers) do
    case result do
      {:adult, name} -> name in identifiers
      {:tech_worker, name, _company} -> name in identifiers
      {:senior, name} -> name in identifiers
      {:middle_aged, name} -> name in identifiers
      {_type, name} when is_binary(name) -> name in identifiers
      {_type, name, _extra} when is_binary(name) -> name in identifiers
      _ -> false
    end
  end

  # NEW: Helper functions for Phase 4 optimizations

  defp should_use_fast_path?(rule_analysis, state) do
    Configuration.fast_path_enabled?(state) and
      rule_analysis.strategy == :fast_path
  end

  defp separate_rules_by_strategy(state) do
    state.rules
    |> Enum.split_with(fn {rule_id, _rule} ->
      analysis = Map.get(state.rule_analyses, rule_id)
      analysis && analysis.strategy == :fast_path
    end)
  end

  defp execute_fast_path_rules(fast_path_rules, state) do
    if Configuration.rule_batching_enabled?(state) do
      execute_fast_path_rules_batched(fast_path_rules, state)
    else
      execute_fast_path_rules_individually(fast_path_rules, state)
    end
  end

  # Fast-path execution (merged from FastPathExecutor)
  defp execute_fast_path(rule, state) do
    # Get all facts from working memory
    all_facts = WorkingMemory.get_facts(state)

    # Extract pattern from rule
    conditions = Map.get(rule, :conditions, [])
    {patterns, tests} = separate_conditions(conditions)
    pattern = hd(patterns)
    fact_type = elem(pattern, 0)

    # Filter facts by type
    relevant_facts =
      Enum.filter(all_facts, fn fact ->
        elem(fact, 0) == fact_type
      end)

    # Find matching facts
    matching_bindings = find_fast_path_matches(relevant_facts, pattern, tests)

    # Execute action for each match
    results =
      Enum.flat_map(matching_bindings, fn bindings ->
        try do
          rule.action.(bindings)
        rescue
          _ -> []
        end
      end)

    {:ok, results}
  end

  defp find_fast_path_matches(facts, pattern, tests) do
    facts
    |> Enum.filter(&fact_matches_pattern?(&1, pattern))
    |> Enum.map(&extract_bindings_from_fact(&1, pattern))
    |> Enum.filter(&all_tests_pass?(&1, tests))
  end

  defp fact_matches_pattern?(fact, pattern) when tuple_size(fact) != tuple_size(pattern),
    do: false

  defp fact_matches_pattern?(fact, pattern) do
    fact_matches_pattern_elements?(fact, pattern, 0, tuple_size(fact))
  end

  # Optimized recursive pattern matching avoiding tuple-to-list conversion
  defp fact_matches_pattern_elements?(_fact, _pattern, index, size) when index >= size, do: true

  defp fact_matches_pattern_elements?(fact, pattern, index, size) do
    if element_matches?(
         :erlang.element(index + 1, fact),
         :erlang.element(index + 1, pattern),
         index
       ) do
      fact_matches_pattern_elements?(fact, pattern, index + 1, size)
    else
      false
    end
  end

  defp element_matches?(_fact_elem, :_, _index), do: true
  # Fact type must match
  defp element_matches?(fact_elem, pattern_elem, 0), do: fact_elem == pattern_elem

  defp element_matches?(fact_elem, pattern_elem, _index) when is_atom(pattern_elem) do
    PatternMatching.variable?(pattern_elem) or fact_elem == pattern_elem
  end

  defp element_matches?(fact_elem, pattern_elem, _index), do: fact_elem == pattern_elem

  defp extract_bindings_from_fact(fact, pattern) do
    extract_bindings_elements(fact, pattern, 0, tuple_size(fact), %{})
  end

  # Optimized recursive binding extraction avoiding tuple-to-list conversion
  defp extract_bindings_elements(_fact, _pattern, index, size, acc) when index >= size, do: acc

  defp extract_bindings_elements(fact, pattern, index, size, acc) do
    pattern_elem = :erlang.element(index + 1, pattern)

    new_acc =
      if PatternMatching.variable?(pattern_elem) do
        fact_elem = :erlang.element(index + 1, fact)
        Map.put(acc, pattern_elem, fact_elem)
      else
        acc
      end

    extract_bindings_elements(fact, pattern, index + 1, size, new_acc)
  end

  defp all_tests_pass?(bindings, tests) do
    Enum.all?(tests, fn {var, op, value} ->
      case Map.get(bindings, var) do
        nil -> false
        bound_value -> evaluate_operator(op, bound_value, value)
      end
    end)
  end

  defp execute_fast_path_rules_batched(fast_path_rules, state) do
    # Group rules by fact type for batched execution
    rules_by_fact_type =
      fast_path_rules
      |> Enum.group_by(fn {_rule_id, rule} -> extract_fact_type_from_rule(rule) end)

    # Execute each group as a batch
    Enum.flat_map(rules_by_fact_type, fn {_fact_type, rules} ->
      # Execute each rule in the batch
      Enum.flat_map(rules, fn {_rule_id, rule} ->
        {:ok, results} = execute_fast_path(rule, state)
        results
      end)
    end)
  end

  defp execute_fast_path_rules_individually(fast_path_rules, state) do
    Enum.flat_map(fast_path_rules, fn {_rule_id, rule} ->
      {:ok, results} = execute_fast_path(rule, state)
      results
    end)
  end

  defp execute_rete_rules(rete_rules, state, concurrent) do
    if concurrent do
      execute_rules_concurrent(rete_rules, state)
    else
      execute_rules_sequential(rete_rules, state)
    end
  end

  defp extract_execution_order(fast_path_rules, rete_rules) do
    # Maintain priority-based order across both rule types
    all_rules = fast_path_rules ++ rete_rules

    all_rules
    |> Enum.sort_by(fn {_rule_id, rule} -> Map.get(rule, :priority, 0) end, :desc)
    |> Enum.map(fn {rule_id, _rule} -> rule_id end)
  end

  defp update_optimization_statistics(engine_stats, fast_path_rules, rete_rules) do
    # This can now be replaced with Statistics.update_optimization_statistics
    # but keeping for backward compatibility in this refactor
    engine_stats
    |> Map.update!(:fast_path_executions, &(&1 + length(fast_path_rules)))
    |> Map.update!(:rete_network_executions, &(&1 + length(rete_rules)))
  end

  defp extract_fact_type_from_rule(rule) do
    conditions = Map.get(rule, :conditions, [])
    {patterns, _tests} = separate_conditions(conditions)

    case patterns do
      [pattern | _] -> elem(pattern, 0)
      [] -> nil
    end
  end

  # Fact lineage tracking helper functions

  defp get_facts_derived_from_new_facts(new_fact_keys, state) do
    # Find all facts that were derived from the new facts or their descendants
    all_derived = find_transitive_derived_facts(new_fact_keys, state.fact_lineage, MapSet.new())

    # Convert back to facts
    all_derived
    |> Enum.map(fn fact_key ->
      case Map.get(state.fact_lineage, fact_key) do
        %{fact: fact} -> fact
        nil -> nil
      end
    end)
    |> Enum.reject(&is_nil/1)
  end

  defp find_transitive_derived_facts(fact_keys, fact_lineage, visited) do
    new_keys = Enum.reject(fact_keys, &MapSet.member?(visited, &1))

    if Enum.empty?(new_keys) do
      visited
    else
      updated_visited = Enum.reduce(new_keys, visited, &MapSet.put(&2, &1))

      derived_keys =
        fact_lineage
        |> Enum.filter(fn {_key, lineage} ->
          lineage.source == :derived and
            Enum.any?(lineage.derived_from, &(&1 in new_keys))
        end)
        |> Enum.map(fn {key, _lineage} -> key end)

      find_transitive_derived_facts(derived_keys, fact_lineage, updated_visited)
    end
  end

  defp result_involves_new_facts?(result, new_facts) do
    # Fallback heuristic for results that aren't tracked in lineage
    new_fact_identifiers = extract_fact_identifiers(new_facts)
    result_involves_identifiers?(result, new_fact_identifiers)
  end
end

defmodule Presto.RuleEngine do
  @moduledoc """
  Main rule engine GenServer that coordinates the RETE algorithm components.

  Manages rules, coordinates fact processing through alpha and beta networks,
  and handles rule execution with proper error handling and performance monitoring.
  """

  use GenServer
  require Logger

  alias Presto.AlphaNetwork
  alias Presto.BetaNetwork
  alias Presto.FastPathExecutor
  alias Presto.Logger, as: PrestoLogger
  alias Presto.Optimization.CompileTimeOptimizer
  alias Presto.RuleAnalyzer
  alias Presto.WorkingMemory

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

  @spec analyze_rule_set(GenServer.server()) :: map()
  def analyze_rule_set(pid) do
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

  defp generate_engine_id do
    "engine_" <> (:crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower))
  end

  # Server implementation

  @impl true
  def init(opts) do
    engine_id = Keyword.get(opts, :engine_id, generate_engine_id())
    PrestoLogger.log_engine_lifecycle(:info, engine_id, "initializing", %{opts: opts})
    # Start component processes with error handling
    try do
      {:ok, working_memory} = WorkingMemory.start_link()

      PrestoLogger.log_engine_lifecycle(:debug, engine_id, "working_memory_started", %{
        pid: working_memory
      })

      {:ok, alpha_network} = AlphaNetwork.start_link(working_memory: working_memory)

      PrestoLogger.log_engine_lifecycle(:debug, engine_id, "alpha_network_started", %{
        pid: alpha_network
      })

      {:ok, beta_network} = BetaNetwork.start_link(alpha_network: alpha_network)

      PrestoLogger.log_engine_lifecycle(:debug, engine_id, "beta_network_started", %{
        pid: beta_network
      })

      state = %{
        engine_id: engine_id,
        working_memory: working_memory,
        alpha_network: alpha_network,
        beta_network: beta_network,
        rules: %{},
        # Maps rule_id to network node IDs
        rule_networks: %{},
        # NEW: Maps rule_id to rule analysis
        rule_analyses: %{},
        # NEW: Fast-path eligible rules grouped by fact type
        fast_path_rules: %{},
        last_execution_order: [],
        rule_statistics: %{},
        engine_statistics: %{
          total_facts: 0,
          total_rules: 0,
          total_rule_firings: 0,
          last_execution_time: 0,
          # NEW: Optimization statistics
          fast_path_executions: 0,
          rete_network_executions: 0,
          alpha_nodes_saved_by_sharing: 0
        },
        # Timestamp for incremental processing
        last_incremental_execution: 0,
        # Facts added since last incremental execution
        facts_since_incremental: [],
        # NEW: Optimization configuration
        optimization_config: %{
          enable_fast_path: false,
          enable_alpha_sharing: true,
          enable_rule_batching: true,
          # Max conditions for fast path
          fast_path_threshold: 2,
          # Min rules sharing pattern for alpha node sharing
          sharing_threshold: 2
        },
        # NEW: Compile-time optimization configuration and state
        compile_time_config: %{
          enabled: true,
          pattern_cache_size: 1000,
          guard_optimization: true,
          network_sharing: true,
          code_generation: true,
          dependency_analysis: true
        },
        compiled_rules: %{},
        network_topology: %{},
        shared_patterns: %{}
      }

      PrestoLogger.log_engine_lifecycle(:info, engine_id, "initialized", %{
        state_keys: Map.keys(state)
      })

      {:ok, state}
    rescue
      error ->
        PrestoLogger.log_error(error, %{engine_id: engine_id, stage: "initialization"})
        {:stop, {:initialization_failed, error}}
    end
  end

  @impl true
  def handle_call({:add_rule, rule}, _from, state) do
    case validate_rule(rule) do
      :ok -> process_valid_rule(rule, state)
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  catch
    {:error, reason} ->
      {:reply, {:error, reason}, state}
  end

  @impl true
  def handle_call({:remove_rule, rule_id}, _from, state) do
    case Map.get(state.rule_networks, rule_id) do
      nil ->
        # Rule doesn't exist, that's fine
        {:reply, :ok, state}

      network_nodes ->
        # Remove network nodes
        new_state = cleanup_rule_network(network_nodes, state)

        updated_rules = Map.delete(new_state.rules, rule_id)
        updated_networks = Map.delete(new_state.rule_networks, rule_id)
        updated_statistics = Map.delete(new_state.rule_statistics, rule_id)

        final_state = %{
          new_state
          | rules: updated_rules,
            rule_networks: updated_networks,
            rule_statistics: updated_statistics,
            engine_statistics: Map.update!(new_state.engine_statistics, :total_rules, &(&1 - 1))
        }

        {:reply, :ok, final_state}
    end
  end

  @impl true
  def handle_call(:get_rules, _from, state) do
    {:reply, state.rules, state}
  end

  @impl true
  def handle_call({:assert_fact, fact}, _from, state) do
    WorkingMemory.assert_fact(state.working_memory, fact)
    AlphaNetwork.process_fact_assertion(state.alpha_network, fact)

    new_state = %{
      state
      | engine_statistics: Map.update!(state.engine_statistics, :total_facts, &(&1 + 1)),
        facts_since_incremental: [fact | state.facts_since_incremental]
    }

    {:reply, :ok, new_state}
  end

  @impl true
  def handle_call({:retract_fact, fact}, _from, state) do
    WorkingMemory.retract_fact(state.working_memory, fact)
    AlphaNetwork.process_fact_retraction(state.alpha_network, fact)

    new_state = %{
      state
      | engine_statistics: Map.update!(state.engine_statistics, :total_facts, &(&1 - 1))
    }

    {:reply, :ok, new_state}
  end

  @impl true
  def handle_call(:get_facts, _from, state) do
    facts = WorkingMemory.get_facts(state.working_memory)
    {:reply, facts, state}
  end

  @impl true
  def handle_call(:clear_facts, _from, state) do
    WorkingMemory.clear_facts(state.working_memory)

    new_state = %{state | engine_statistics: Map.put(state.engine_statistics, :total_facts, 0)}

    {:reply, :ok, new_state}
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
    state_with_stats = update_execution_statistics(updated_state, time, length(results))

    final_state = %{
      state_with_stats
      | facts_since_incremental: [],
        last_incremental_execution: System.system_time(:microsecond)
    }

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
      final_state = %{
        updated_state
        | facts_since_incremental: [],
          last_incremental_execution: System.system_time(:microsecond)
      }

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
    {:reply, state.last_execution_order, state}
  end

  @impl true
  def handle_call(:get_rule_statistics, _from, state) do
    {:reply, state.rule_statistics, state}
  end

  @impl true
  def handle_call(:get_engine_statistics, _from, state) do
    {:reply, state.engine_statistics, state}
  end

  @impl true
  def handle_call({:analyze_rule, rule_id}, _from, state) do
    analysis = Map.get(state.rule_analyses, rule_id)
    {:reply, analysis, state}
  end

  @impl true
  def handle_call(:analyze_rule_set, _from, state) do
    rules = Map.values(state.rules)
    analysis = RuleAnalyzer.analyze_rule_set(rules)
    {:reply, analysis, state}
  end

  @impl true
  def handle_call({:configure_optimization, opts}, _from, state) do
    new_config = Map.merge(state.optimization_config, Map.new(opts))
    new_state = %{state | optimization_config: new_config}
    {:reply, :ok, new_state}
  end

  @impl true
  def handle_call(:get_optimization_config, _from, state) do
    {:reply, state.optimization_config, state}
  end

  @impl true
  def terminate(_reason, state) do
    # Clean shutdown of component processes
    GenServer.stop(state.beta_network)
    GenServer.stop(state.alpha_network)
    GenServer.stop(state.working_memory)
    :ok
  end

  # Private functions

  defp create_rule_network(rule, rule_analysis, compiled_rule, state) do
    if should_use_fast_path?(rule_analysis, state) do
      {%{alpha_nodes: [], beta_nodes: []}, state}
    else
      case compile_rule_to_network_optimized(rule, compiled_rule, state) do
        {:ok, network_nodes, updated_state} -> {network_nodes, updated_state}
        {:error, reason} -> throw({:error, reason})
      end
    end
  end

  defp update_fast_path_rules(rule, rule_analysis, state) do
    if rule_analysis.strategy == :fast_path do
      fact_type = extract_fact_type_from_rule(rule)
      fast_path_rules_for_type = Map.get(state.fast_path_rules, fact_type, [])
      Map.put(state.fast_path_rules, fact_type, [rule.id | fast_path_rules_for_type])
    else
      state.fast_path_rules
    end
  end

  defp create_rule_statistics(rule, rule_analysis, state) do
    Map.put(state.rule_statistics, rule.id, %{
      executions: 0,
      total_time: 0,
      average_time: 0,
      facts_processed: 0,
      strategy_used: rule_analysis.strategy,
      complexity: rule_analysis.complexity
    })
  end

  defp validate_rule(rule) do
    required_fields = [:id, :conditions, :action]

    cond do
      not is_map(rule) ->
        {:error, :rule_must_be_map}

      not Enum.all?(required_fields, &Map.has_key?(rule, &1)) ->
        {:error, :missing_required_fields}

      not is_atom(rule.id) ->
        {:error, :id_must_be_atom}

      not is_list(rule.conditions) ->
        {:error, :conditions_must_be_list}

      not is_function(rule.action) ->
        {:error, :action_must_be_function}

      true ->
        :ok
    end
  end

  defp process_valid_rule(rule, state) do
    # Phase 1: Standard rule analysis
    rule_analysis = RuleAnalyzer.analyze_rule(rule)

    # Phase 2: Compile-time optimization (NEW)
    compiled_rule =
      if state.compile_time_config.enabled do
        [compiled] = CompileTimeOptimizer.compile_rules([rule], state.compile_time_config)
        compiled
      else
        %{
          id: rule.id,
          original_rule: rule,
          compiled_patterns: [],
          optimized_guards: [],
          execution_plan: %{strategy: :rete_network},
          shared_nodes: [],
          estimated_performance: %{}
        }
      end

    # Phase 3: Create network with compile-time optimizations
    {network_nodes, new_state} = create_rule_network(rule, rule_analysis, compiled_rule, state)

    updated_rules = Map.put(new_state.rules, rule.id, rule)
    updated_networks = Map.put(new_state.rule_networks, rule.id, network_nodes)
    updated_analyses = Map.put(new_state.rule_analyses, rule.id, rule_analysis)
    updated_compiled_rules = Map.put(new_state.compiled_rules, rule.id, compiled_rule)
    updated_fast_path_rules = update_fast_path_rules(rule, rule_analysis, new_state)
    updated_statistics = create_rule_statistics(rule, rule_analysis, new_state)

    final_state = %{
      new_state
      | rules: updated_rules,
        rule_networks: updated_networks,
        rule_analyses: updated_analyses,
        compiled_rules: updated_compiled_rules,
        fast_path_rules: updated_fast_path_rules,
        rule_statistics: updated_statistics,
        engine_statistics: Map.update!(new_state.engine_statistics, :total_rules, &(&1 + 1))
    }

    {:reply, :ok, final_state}
  end

  defp compile_rule_to_network_optimized(rule, compiled_rule, state) do
    # Use compile-time optimizations if available
    if state.compile_time_config.enabled and length(compiled_rule.compiled_patterns) > 0 do
      create_optimized_network(rule, compiled_rule, state)
    else
      # Fallback to standard compilation
      compile_rule_to_network(rule, state)
    end
  end

  defp create_optimized_network(_rule, compiled_rule, state) do
    # Create alpha nodes using compiled patterns
    {alpha_nodes, new_state} =
      create_optimized_alpha_nodes(compiled_rule.compiled_patterns, state)

    # Create beta nodes using execution plan
    {beta_nodes, final_state} =
      create_optimized_beta_nodes(
        compiled_rule.execution_plan,
        alpha_nodes,
        new_state
      )

    network_nodes = %{
      alpha_nodes: alpha_nodes,
      beta_nodes: beta_nodes,
      compiled_patterns: compiled_rule.compiled_patterns,
      optimized_guards: compiled_rule.optimized_guards,
      execution_plan: compiled_rule.execution_plan
    }

    {:ok, network_nodes, final_state}
  rescue
    error ->
      {:error, {:optimized_compilation_failed, error}}
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

  defp create_optimized_alpha_nodes(compiled_patterns, state) do
    # Create alpha nodes using compiled pattern matchers
    Enum.reduce(compiled_patterns, {[], state}, fn compiled_pattern, {acc_nodes, acc_state} ->
      # Create alpha node with optimized pattern matcher
      {:ok, node_id} =
        AlphaNetwork.create_optimized_alpha_node(
          acc_state.alpha_network,
          compiled_pattern.original_pattern,
          compiled_pattern.compiled_matcher
        )

      {[node_id | acc_nodes], acc_state}
    end)
  rescue
    # Fallback to standard alpha node creation if optimized version fails
    _error ->
      patterns = Enum.map(compiled_patterns, & &1.original_pattern)
      create_alpha_nodes_for_conditions(patterns, state)
  end

  defp create_optimized_beta_nodes(execution_plan, alpha_nodes, state) do
    # Create beta nodes using execution plan optimizations
    case execution_plan.strategy do
      :fast_path ->
        # No beta nodes needed for fast path
        {[], state}

      :rete_network ->
        # Standard beta node creation with potential optimizations
        create_beta_nodes_for_rule(alpha_nodes, state)

      :hybrid ->
        # Hybrid approach - some optimizations
        create_beta_nodes_for_rule(alpha_nodes, state)

      _ ->
        # Default to standard creation
        create_beta_nodes_for_rule(alpha_nodes, state)
    end
  end

  defp create_alpha_nodes_for_conditions(conditions, state) do
    # Extract pattern-based conditions and create alpha nodes
    {pattern_conditions, _test_conditions} = separate_conditions(conditions)

    Enum.reduce(pattern_conditions, {[], state}, fn condition, {acc_nodes, acc_state} ->
      {:ok, node_id} = AlphaNetwork.create_alpha_node(acc_state.alpha_network, condition)
      {[node_id | acc_nodes], acc_state}
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
      BetaNetwork.create_beta_node(state.beta_network, {:join, node1, node2, join_key})

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
      BetaNetwork.create_beta_node(
        state.beta_network,
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
      BetaNetwork.create_beta_node(
        state.beta_network,
        {:join, current_beta, next_alpha, join_key}
      )

    chain_remaining_nodes(new_beta, rest, [new_beta | acc_betas], state)
  end

  # Helper functions to determine join keys based on alpha node patterns
  defp determine_join_key(alpha_node1, alpha_node2, state) do
    # Get alpha node info to determine their patterns
    node1_info = AlphaNetwork.get_alpha_node_info(state.alpha_network, alpha_node1)
    node2_info = AlphaNetwork.get_alpha_node_info(state.alpha_network, alpha_node2)

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
    node_info = AlphaNetwork.get_alpha_node_info(state.alpha_network, alpha_node)

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
    case pattern do
      {fact_type, field1} ->
        relevant_tests = find_relevant_tests([field1], tests)
        {fact_type, field1, relevant_tests}

      {fact_type, field1, field2} ->
        relevant_tests = find_relevant_tests([field1, field2], tests)
        {fact_type, field1, field2, relevant_tests}

      {fact_type, field1, field2, field3} ->
        relevant_tests = find_relevant_tests([field1, field2, field3], tests)
        {fact_type, field1, field2, field3, relevant_tests}
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
    Enum.each(network_nodes.alpha_nodes, fn node_id ->
      AlphaNetwork.remove_alpha_node(state.alpha_network, node_id)
    end)

    # Remove beta nodes
    Enum.each(network_nodes.beta_nodes, fn node_id ->
      BetaNetwork.remove_beta_node(state.beta_network, node_id)
    end)

    state
  end

  defp execute_rules(state, concurrent) do
    # NEW: Use optimized execution strategy
    if state.optimization_config.enable_fast_path do
      execute_rules_optimized(state, concurrent)
    else
      execute_rules_traditional(state, concurrent)
    end
  end

  # NEW: Optimized execution with fast-path and batching
  defp execute_rules_optimized(state, concurrent) do
    # Process changes through beta network for RETE rules only
    BetaNetwork.process_alpha_changes(state.beta_network)

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
    all_rules = fast_path_rules ++ rete_rules
    final_state = collect_rule_statistics(updated_state, all_rules)

    {List.flatten(all_results), final_state}
  end

  # Traditional execution without optimizations
  defp execute_rules_traditional(state, concurrent) do
    # Process changes through beta network
    BetaNetwork.process_alpha_changes(state.beta_network)

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
    updated_state = %{state | last_execution_order: execution_order}

    # Collect updated rule statistics from process dictionary
    final_state = collect_rule_statistics(updated_state, sorted_rules)

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
          # Assert fact into working memory
          WorkingMemory.assert_fact(acc_state.working_memory, fact)

          # Update engine statistics
          %{
            acc_state
            | engine_statistics:
                Map.update!(acc_state.engine_statistics, :total_facts, &(&1 + 1)),
              facts_since_incremental: [fact | acc_state.facts_since_incremental]
          }
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

  defp execute_single_rule(rule_id, rule, state, :no_error_handling) do
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
    update_rule_statistics(rule_id, time, length(facts), state)

    results
  end

  defp execute_single_rule(rule_id, rule, state, :with_error_handling) do
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
    update_rule_statistics(rule_id, time, length(facts), state)

    results
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
        BetaNetwork.get_beta_memory(state.beta_network, final_beta)

      [] ->
        get_matches_from_alpha_nodes(network_nodes.alpha_nodes, state)
    end
  end

  defp get_matches_from_alpha_nodes([alpha_node | _], state) do
    AlphaNetwork.get_alpha_memory(state.alpha_network, alpha_node)
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
    case BetaNetwork.get_beta_node_info(state.beta_network, other_id) do
      %{left_input: ^node_id} -> true
      %{right_input: ^node_id} -> true
      _ -> false
    end
  end

  defp node_uses_as_input?(_other_id, _node_id, _state), do: false

  defp update_execution_statistics(state, execution_time, result_count) do
    new_engine_stats =
      state.engine_statistics
      |> Map.put(:last_execution_time, execution_time)
      |> Map.update!(:total_rule_firings, &(&1 + result_count))

    %{state | engine_statistics: new_engine_stats}
  end

  defp update_rule_statistics(rule_id, execution_time, facts_processed, state) do
    # Update statistics for this specific rule (execution_time is in microseconds)
    current_stats =
      Map.get(state.rule_statistics, rule_id, %{
        executions: 0,
        total_time: 0,
        average_time: 0,
        facts_processed: 0
      })

    new_executions = current_stats.executions + 1
    new_total_time = current_stats.total_time + execution_time
    new_average_time = if new_executions > 0, do: div(new_total_time, new_executions), else: 0
    new_facts_processed = current_stats.facts_processed + facts_processed

    updated_stats = %{
      executions: new_executions,
      total_time: new_total_time,
      average_time: new_average_time,
      facts_processed: new_facts_processed
    }

    # Note: This doesn't update the state directly since execute_single_rule
    # is called during rule execution and we don't want to return a modified state.
    # The statistics will be updated in the GenServer state after rule execution.
    # For now, we'll store it in the process dictionary as a workaround.
    Process.put({:rule_stats, rule_id}, updated_stats)

    :ok
  end

  defp collect_rule_statistics(state, sorted_rules) do
    # Collect updated statistics from process dictionary
    updated_rule_stats =
      Enum.reduce(sorted_rules, state.rule_statistics, fn {rule_id, _rule}, acc_stats ->
        case Process.get({:rule_stats, rule_id}) do
          # No update for this rule
          nil ->
            acc_stats

          updated_stats ->
            # Clean up process dictionary
            Process.delete({:rule_stats, rule_id})
            Map.put(acc_stats, rule_id, updated_stats)
        end
      end)

    %{state | rule_statistics: updated_rule_stats}
  end

  defp filter_incremental_results(all_results, new_facts, _state) do
    # For simple implementation, check if results involve any new facts
    # This is a basic heuristic - a full implementation would track fact lineage

    # Extract names from new facts that could be involved in results
    new_fact_identifiers = extract_fact_identifiers(new_facts)

    # Filter results that involve any of the new fact identifiers
    Enum.filter(all_results, &result_involves_identifiers?(&1, new_fact_identifiers))
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
    state.optimization_config.enable_fast_path and
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
    if state.optimization_config.enable_rule_batching do
      execute_fast_path_rules_batched(fast_path_rules, state)
    else
      execute_fast_path_rules_individually(fast_path_rules, state)
    end
  end

  defp execute_fast_path_rules_batched(fast_path_rules, state) do
    # Group rules by fact type for batched execution
    rules_by_fact_type =
      fast_path_rules
      |> Enum.group_by(fn {_rule_id, rule} -> extract_fact_type_from_rule(rule) end)

    # Execute each group as a batch
    Enum.flat_map(rules_by_fact_type, fn {_fact_type, rules} ->
      rule_list = Enum.map(rules, fn {_rule_id, rule} -> rule end)

      case FastPathExecutor.execute_batch_fast_path(rule_list, state.working_memory) do
        {:ok, results} -> results
        {:error, _reason} -> []
      end
    end)
  end

  defp execute_fast_path_rules_individually(fast_path_rules, state) do
    Enum.flat_map(fast_path_rules, fn {_rule_id, rule} ->
      case FastPathExecutor.execute_fast_path(rule, state.working_memory) do
        {:ok, results} -> results
        {:error, _reason} -> []
      end
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
end

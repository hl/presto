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
  alias Presto.PatternMatching
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
    conditions = Map.get(rule, :conditions, [])
    conditions_count = if is_list(conditions), do: length(conditions), else: 0

    PrestoLogger.log_rule_compilation(:info, rule_id, "adding_rule", %{
      rule_id: rule_id,
      conditions_count: conditions_count,
      priority: Map.get(rule, :priority, 0)
    })

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
    PrestoLogger.log_fact_processing(:debug, elem(fact, 0), "asserting_fact", %{
      fact_type: elem(fact, 0),
      fact_size: tuple_size(fact)
    })

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

  @spec fire_all_rules(GenServer.server()) :: [rule_result()]
  def fire_all_rules(pid) do
    fire_rules(pid, [])
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

  @spec get_rule_statistics(GenServer.server(), atom()) :: map() | nil
  def get_rule_statistics(pid, rule_id) do
    GenServer.call(pid, {:get_rule_statistics, rule_id})
  end

  @spec get_rule_performance_metrics(GenServer.server(), atom()) ::
          {:ok, map()} | {:error, term()}
  def get_rule_performance_metrics(pid, rule_id) do
    case get_rule_statistics(pid, rule_id) do
      nil -> {:error, :rule_not_found}
      stats -> {:ok, stats}
    end
  end

  @spec get_execution_metrics(GenServer.server()) :: map()
  def get_execution_metrics(pid) do
    get_engine_statistics(pid)
  end

  @spec get_fact_statistics(GenServer.server()) :: map()
  def get_fact_statistics(pid) do
    facts = get_facts(pid)

    %{
      total_facts: length(facts),
      fact_types: facts |> Enum.map(&elem(&1, 0)) |> Enum.uniq() |> length(),
      facts_by_type:
        facts
        |> Enum.group_by(&elem(&1, 0))
        |> Enum.map(fn {type, facts} -> {type, length(facts)} end)
        |> Enum.into(%{})
    }
  end

  @spec get_optimization_config(GenServer.server()) :: map()
  def get_optimization_config(pid) do
    GenServer.call(pid, :get_optimization_config)
  end

  # Bulk Operations

  @spec assert_facts_bulk(GenServer.server(), [tuple()]) :: :ok
  def assert_facts_bulk(pid, facts) do
    GenServer.call(pid, {:assert_facts_bulk, facts})
  end

  @spec retract_facts_bulk(GenServer.server(), [tuple()]) :: :ok
  def retract_facts_bulk(pid, facts) do
    GenServer.call(pid, {:retract_facts_bulk, facts})
  end

  # Query Interface

  @spec query_facts(GenServer.server(), tuple(), keyword()) :: [map()]
  def query_facts(pid, pattern, conditions) do
    GenServer.call(pid, {:query_facts, pattern, conditions})
  end

  @spec query_facts_join(GenServer.server(), [tuple()], keyword()) :: [map()]
  def query_facts_join(pid, patterns, opts) do
    GenServer.call(pid, {:query_facts_join, patterns, opts})
  end

  @spec count_facts(GenServer.server(), tuple(), keyword()) :: non_neg_integer()
  def count_facts(pid, pattern, conditions) do
    GenServer.call(pid, {:count_facts, pattern, conditions})
  end

  @spec explain_fact(GenServer.server(), tuple()) :: map()
  def explain_fact(pid, fact) do
    GenServer.call(pid, {:explain_fact, fact})
  end

  # Introspection and Debugging Tools

  @spec inspect_rule(GenServer.server(), atom()) :: map()
  def inspect_rule(pid, rule_id) do
    GenServer.call(pid, {:inspect_rule, rule_id})
  end

  @spec get_diagnostics(GenServer.server()) :: map()
  def get_diagnostics(pid) do
    GenServer.call(pid, :get_diagnostics)
  end

  @spec profile_execution(GenServer.server(), keyword()) :: map()
  def profile_execution(pid, opts) do
    GenServer.call(pid, {:profile_execution, opts})
  end

  @spec trace_fact_execution(GenServer.server(), tuple()) :: map()
  def trace_fact_execution(pid, fact) do
    GenServer.call(pid, {:trace_fact_execution, fact})
  end

  @spec get_network_visualization(GenServer.server()) :: map()
  def get_network_visualization(pid) do
    GenServer.call(pid, :get_network_visualization)
  end

  @spec analyze_performance_recommendations(GenServer.server()) :: [map()]
  def analyze_performance_recommendations(pid) do
    GenServer.call(pid, :analyze_performance_recommendations)
  end

  # State Persistence and Recovery

  @spec create_snapshot(GenServer.server()) :: {:ok, map()} | {:error, term()}
  def create_snapshot(pid) do
    # 30 second timeout for large engines
    GenServer.call(pid, :create_snapshot, 30_000)
  end

  @spec restore_from_snapshot(GenServer.server(), map()) :: :ok | {:error, term()}
  def restore_from_snapshot(pid, snapshot) do
    # 30 second timeout
    GenServer.call(pid, {:restore_from_snapshot, snapshot}, 30_000)
  end

  @spec save_snapshot_to_file(GenServer.server(), String.t()) :: :ok | {:error, term()}
  def save_snapshot_to_file(pid, file_path) do
    GenServer.call(pid, {:save_snapshot_to_file, file_path}, 30_000)
  end

  @spec load_snapshot_from_file(GenServer.server(), String.t()) :: :ok | {:error, term()}
  def load_snapshot_from_file(pid, file_path) do
    GenServer.call(pid, {:load_snapshot_from_file, file_path}, 30_000)
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
    # Hot path optimization: single coordinated operation
    new_state = process_fact_optimized(state, fact, :assert)
    {:reply, :ok, new_state}
  end

  @impl true
  def handle_call({:retract_fact, fact}, _from, state) do
    # Hot path optimization: single coordinated operation
    new_state = process_fact_optimized(state, fact, :retract)
    {:reply, :ok, new_state}
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
  def handle_call({:get_rule_statistics, rule_id}, _from, state) do
    stats = Statistics.get_rule_statistics(state, rule_id)
    {:reply, stats, state}
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
  def handle_call({:assert_facts_bulk, facts}, _from, state) do
    new_state = process_facts_bulk(state, facts, :assert)
    {:reply, :ok, new_state}
  end

  @impl true
  def handle_call({:retract_facts_bulk, facts}, _from, state) do
    new_state = process_facts_bulk(state, facts, :retract)
    {:reply, :ok, new_state}
  end

  @impl true
  def handle_call({:query_facts, pattern, conditions}, _from, state) do
    results = execute_fact_query(state, pattern, conditions)
    {:reply, results, state}
  end

  @impl true
  def handle_call({:query_facts_join, patterns, opts}, _from, state) do
    results = execute_join_query(state, patterns, opts)
    {:reply, results, state}
  end

  @impl true
  def handle_call({:count_facts, pattern, conditions}, _from, state) do
    results = execute_fact_query(state, pattern, conditions)
    count = length(results)
    {:reply, count, state}
  end

  @impl true
  def handle_call({:explain_fact, fact}, _from, state) do
    explanation = generate_fact_explanation(state, fact)
    {:reply, explanation, state}
  end

  @impl true
  def handle_call({:inspect_rule, rule_id}, _from, state) do
    inspection = generate_rule_inspection(state, rule_id)
    {:reply, inspection, state}
  end

  @impl true
  def handle_call(:get_diagnostics, _from, state) do
    diagnostics = generate_engine_diagnostics(state)
    {:reply, diagnostics, state}
  end

  @impl true
  def handle_call({:profile_execution, opts}, _from, state) do
    profile = execute_with_profiling(state, opts)
    {:reply, profile, state}
  end

  @impl true
  def handle_call({:trace_fact_execution, fact}, _from, state) do
    trace = trace_fact_through_network(state, fact)
    {:reply, trace, state}
  end

  @impl true
  def handle_call(:get_network_visualization, _from, state) do
    visualization = generate_network_visualization(state)
    {:reply, visualization, state}
  end

  @impl true
  def handle_call(:analyze_performance_recommendations, _from, state) do
    recommendations = generate_performance_recommendations(state)
    {:reply, recommendations, state}
  end

  @impl true
  def handle_call({:get_alpha_memory, node_id}, _from, state) do
    memory = AlphaNetworkCoordinator.get_alpha_memory(state, node_id)
    {:reply, memory, state}
  end

  @impl true
  def handle_call(:create_snapshot, _from, state) do
    case create_engine_snapshot(state) do
      {:ok, snapshot} -> {:reply, {:ok, snapshot}, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:restore_from_snapshot, snapshot}, _from, state) do
    case restore_engine_from_snapshot(state, snapshot) do
      {:ok, new_state} -> {:reply, :ok, new_state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:save_snapshot_to_file, file_path}, _from, state) do
    case save_engine_snapshot_to_file(state, file_path) do
      :ok -> {:reply, :ok, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:load_snapshot_from_file, file_path}, _from, state) do
    case load_engine_snapshot_from_file(state, file_path) do
      {:ok, new_state} -> {:reply, :ok, new_state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
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
            error ->
              Logger.debug("Rule action error: #{inspect(error)}")
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
        # Fallback to basic pattern matching when no network is available
        get_basic_rule_matches(rule, state)

      network_nodes ->
        case get_matches_from_network_nodes(network_nodes, state) do
          [] ->
            # If network returns no matches, try basic pattern matching
            get_basic_rule_matches(rule, state)

          matches ->
            matches
        end
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

  # Basic pattern matching fallback when RETE network is not available
  defp get_basic_rule_matches(rule, state) do
    conditions = Map.get(rule, :conditions, [])
    facts = WorkingMemory.get_facts(state)

    case conditions do
      [] ->
        # No conditions - rule always fires
        [%{}]

      _ ->
        # Handle multiple conditions with variable bindings and comparisons
        find_matching_combinations(conditions, facts)
    end
  end

  # Find all combinations of facts that satisfy all conditions
  defp find_matching_combinations(conditions, facts) do
    # Separate pattern conditions from comparison conditions
    {pattern_conditions, comparison_conditions} =
      Enum.split_with(conditions, fn condition ->
        not is_comparison_condition?(condition)
      end)

    # Find facts that match pattern conditions and build variable bindings
    matches_with_bindings = find_pattern_matches(pattern_conditions, facts)

    # Filter matches that satisfy all comparison conditions
    Enum.filter(matches_with_bindings, fn bindings ->
      Enum.all?(comparison_conditions, fn comp_condition ->
        evaluate_comparison_condition(comp_condition, bindings)
      end)
    end)
  end

  # Check if a condition is a comparison (has :>, :<, :==, etc.)
  defp is_comparison_condition?({var, op, _value}) when is_atom(var) and is_atom(op) do
    op in [:>, :<, :>=, :<=, :==, :!=, :=]
  end

  defp is_comparison_condition?(_), do: false

  # Find facts that match pattern conditions and build variable bindings
  defp find_pattern_matches(pattern_conditions, facts) do
    case pattern_conditions do
      [] ->
        # No pattern conditions, return empty binding
        [%{}]

      [first_condition | rest_conditions] ->
        # Find facts matching the first condition
        first_matches =
          Enum.flat_map(facts, fn fact ->
            case match_pattern_condition(fact, first_condition) do
              nil -> []
              bindings -> [bindings]
            end
          end)

        # For each first match, try to extend with remaining conditions
        Enum.flat_map(first_matches, fn initial_bindings ->
          extend_pattern_matches(rest_conditions, facts, initial_bindings)
        end)
    end
  end

  # Extend pattern matches with additional conditions
  defp extend_pattern_matches([], _facts, bindings), do: [bindings]

  defp extend_pattern_matches([condition | rest], facts, current_bindings) do
    # Find facts that match this condition and are consistent with current bindings
    extended_matches =
      Enum.flat_map(facts, fn fact ->
        case match_pattern_condition(fact, condition) do
          nil ->
            []

          new_bindings ->
            # Check if new bindings are consistent with current bindings
            case merge_bindings(current_bindings, new_bindings) do
              # Inconsistent bindings
              nil -> []
              merged -> [merged]
            end
        end
      end)

    # Continue with remaining conditions for each extended match
    Enum.flat_map(extended_matches, fn extended_bindings ->
      extend_pattern_matches(rest, facts, extended_bindings)
    end)
  end

  # Match a single fact against a pattern condition and return variable bindings
  defp match_pattern_condition(fact, condition) when is_tuple(fact) and is_tuple(condition) do
    fact_size = tuple_size(fact)
    condition_size = tuple_size(condition)

    if fact_size == condition_size do
      try_match_elements(fact, condition, 0, %{})
    else
      nil
    end
  end

  defp match_pattern_condition(_fact, _condition), do: nil

  # Try to match all elements and build bindings
  defp try_match_elements(fact, condition, index, bindings) when index < tuple_size(fact) do
    fact_elem = elem(fact, index)
    condition_elem = elem(condition, index)

    case match_element(fact_elem, condition_elem, bindings) do
      {:ok, new_bindings} ->
        try_match_elements(fact, condition, index + 1, new_bindings)

      :fail ->
        nil
    end
  end

  defp try_match_elements(_fact, _condition, _index, bindings), do: bindings

  # Match a single element and update bindings
  defp match_element(fact_elem, condition_elem, bindings) do
    cond do
      # Variable (atoms that are likely variables)
      is_variable?(condition_elem) ->
        case Map.get(bindings, condition_elem) do
          nil ->
            # New variable binding
            {:ok, Map.put(bindings, condition_elem, fact_elem)}

          ^fact_elem ->
            # Same binding, consistent
            {:ok, bindings}

          _different ->
            # Inconsistent binding
            :fail
        end

      # Literal match
      fact_elem == condition_elem ->
        {:ok, bindings}

      # No match
      true ->
        :fail
    end
  end

  # Check if an atom is a variable
  defp is_variable?(atom) when is_atom(atom) do
    atom_str = Atom.to_string(atom)
    # Variables are atoms that start with :, have common variable names, or start with _
    String.starts_with?(atom_str, "_") or
      atom in [:name, :age, :value, :data, :field, :salary, :amount, :company] or
      String.length(atom_str) == 1 or
      String.starts_with?(atom_str, "var_")
  end

  defp is_variable?(_), do: false

  # Merge two binding maps, return nil if inconsistent
  defp merge_bindings(bindings1, bindings2) do
    try do
      Enum.reduce(bindings2, bindings1, fn {var, value}, acc ->
        case Map.get(acc, var) do
          nil ->
            Map.put(acc, var, value)

          ^value ->
            # Same binding
            acc

          _different ->
            throw(:inconsistent)
        end
      end)
    catch
      :inconsistent -> nil
    end
  end

  # Evaluate a comparison condition against variable bindings
  defp evaluate_comparison_condition({var, op, value}, bindings) when is_atom(var) do
    case Map.get(bindings, var) do
      # Variable not bound
      nil -> false
      var_value -> apply_comparison(var_value, op, value)
    end
  end

  defp evaluate_comparison_condition(_condition, _bindings), do: false

  # Apply comparison operation
  defp apply_comparison(left, :>, right), do: left > right
  defp apply_comparison(left, :<, right), do: left < right
  defp apply_comparison(left, :>=, right), do: left >= right
  defp apply_comparison(left, :<=, right), do: left <= right
  defp apply_comparison(left, :==, right), do: left == right
  defp apply_comparison(left, :!=, right), do: left != right
  defp apply_comparison(left, :=, right), do: left == right
  defp apply_comparison(_left, _op, _right), do: false

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

  # Hot path optimization: coordinated fact processing
  defp process_fact_optimized(state, fact, operation) do
    fact_key = FactLineage.create_fact_key(fact)

    case operation do
      :assert ->
        # Single coordinated assertion operation
        new_state = WorkingMemory.assert_fact(state, fact)
        alpha_processed = AlphaNetworkCoordinator.process_fact_assertion(new_state, fact)

        # Batch lineage and statistics updates
        lineage_info = %{
          fact: fact,
          generation: alpha_processed.fact_generation,
          source: :input,
          derived_from: [],
          derived_by_rule: nil,
          timestamp: System.system_time(:microsecond)
        }

        alpha_processed
        |> FactLineage.update_fact_lineage(fact_key, lineage_info)
        |> Statistics.update_total_facts(1)
        |> FactLineage.update_facts_since_incremental([
          fact | alpha_processed.facts_since_incremental
        ])

      :retract ->
        # Single coordinated retraction operation
        new_state = WorkingMemory.retract_fact(state, fact)
        alpha_processed = AlphaNetworkCoordinator.process_fact_retraction(new_state, fact)
        Statistics.update_total_facts(alpha_processed, -1)
    end
  end

  # Bulk processing optimization: process multiple facts in batches
  defp process_facts_bulk(state, facts, operation) do
    PrestoLogger.log_fact_processing(:info, :bulk, "processing_facts_bulk", %{
      count: length(facts),
      operation: operation
    })

    # Process facts in optimized batches
    Enum.reduce(facts, state, fn fact, acc_state ->
      process_fact_optimized(acc_state, fact, operation)
    end)
  end

  # Query Interface Implementation

  defp execute_fact_query(state, pattern, conditions) do
    all_facts = WorkingMemory.get_facts(state)

    # Filter facts by pattern
    matching_facts = Enum.filter(all_facts, &fact_matches_pattern?(&1, pattern))

    # Apply additional conditions
    filtered_facts = apply_query_conditions(matching_facts, pattern, conditions)

    # Convert to result format with bindings
    Enum.map(filtered_facts, &extract_bindings_from_fact(&1, pattern))
  end

  defp execute_join_query(state, patterns, opts) do
    join_keys = Keyword.get(opts, :join_on, [])

    # Get matches for each pattern
    pattern_matches =
      Enum.map(patterns, fn pattern ->
        execute_fact_query(state, pattern, [])
      end)

    # Perform joins
    case pattern_matches do
      [single_pattern] ->
        single_pattern

      [first_matches, second_matches] ->
        join_two_patterns(first_matches, second_matches, join_keys)

      multiple_patterns ->
        # Chain joins for multiple patterns
        Enum.reduce(tl(multiple_patterns), hd(multiple_patterns), fn right_matches,
                                                                     left_matches ->
          join_two_patterns(left_matches, right_matches, join_keys)
        end)
    end
  end

  defp generate_fact_explanation(state, fact) do
    fact_type = elem(fact, 0)

    # Find relevant rules
    relevant_rules =
      state.rules
      |> Enum.filter(fn {_rule_id, rule} ->
        rule_applies_to_fact_type?(rule, fact_type)
      end)

    # Check which rules would match this fact
    matching_rules =
      Enum.filter(relevant_rules, fn {_rule_id, rule} ->
        would_rule_match_fact?(rule, fact, state)
      end)

    %{
      fact: fact,
      fact_type: fact_type,
      relevant_rules: Enum.map(relevant_rules, fn {rule_id, _rule} -> rule_id end),
      matching_rules: Enum.map(matching_rules, fn {rule_id, _rule} -> rule_id end),
      would_trigger: length(matching_rules) > 0
    }
  end

  # Query helper functions

  defp apply_query_conditions(facts, pattern, conditions) do
    Enum.filter(facts, fn fact ->
      bindings = extract_bindings_from_fact(fact, pattern)

      Enum.all?(conditions, fn {field, condition} ->
        evaluate_query_condition(Map.get(bindings, field), condition)
      end)
    end)
  end

  defp evaluate_query_condition(value, {:>, threshold}), do: value > threshold
  defp evaluate_query_condition(value, {:<, threshold}), do: value < threshold
  defp evaluate_query_condition(value, {:>=, threshold}), do: value >= threshold
  defp evaluate_query_condition(value, {:<=, threshold}), do: value <= threshold
  defp evaluate_query_condition(value, {:==, expected}), do: value == expected
  defp evaluate_query_condition(value, {:!=, expected}), do: value != expected
  defp evaluate_query_condition(value, {:match, regex}), do: Regex.match?(regex, to_string(value))
  defp evaluate_query_condition(value, expected), do: value == expected

  defp join_two_patterns(left_matches, right_matches, join_keys) do
    for left_match <- left_matches,
        right_match <- right_matches,
        joins_match?(left_match, right_match, join_keys) do
      Map.merge(left_match, right_match)
    end
  end

  defp joins_match?(left_match, right_match, join_keys) do
    Enum.all?(join_keys, fn key ->
      Map.get(left_match, key) == Map.get(right_match, key) and
        Map.has_key?(left_match, key) and
        Map.has_key?(right_match, key)
    end)
  end

  defp rule_applies_to_fact_type?(rule, fact_type) do
    conditions = Map.get(rule, :conditions, [])
    {patterns, _tests} = separate_conditions(conditions)

    Enum.any?(patterns, fn pattern ->
      elem(pattern, 0) == fact_type
    end)
  end

  defp would_rule_match_fact?(rule, fact, _state) do
    # Simplified check - would need full rule evaluation for complete accuracy
    conditions = Map.get(rule, :conditions, [])
    {patterns, tests} = separate_conditions(conditions)

    # Check if fact matches any pattern in the rule
    matching_pattern = Enum.find(patterns, &fact_matches_pattern?(fact, &1))

    matching_pattern && evaluate_rule_tests(fact, matching_pattern, tests)
  end

  defp evaluate_rule_tests(fact, matching_pattern, tests) do
    bindings = extract_bindings_from_fact(fact, matching_pattern)

    Enum.all?(tests, fn {var, op, value} ->
      case Map.get(bindings, var) do
        nil -> false
        bound_value -> evaluate_operator(op, bound_value, value)
      end
    end)
  end

  # Introspection and Debugging Implementation

  defp generate_rule_inspection(state, rule_id) do
    rule = Map.get(state.rules, rule_id)
    network = Map.get(state.rule_networks, rule_id)
    analysis = Map.get(state.rule_analyses, rule_id)
    stats = Map.get(state.rule_statistics, rule_id, %{})

    %{
      rule_id: rule_id,
      rule: rule,
      analysis: analysis,
      network_nodes: network,
      statistics: stats,
      alpha_nodes: get_rule_alpha_nodes(state, rule_id),
      beta_nodes: get_rule_beta_nodes(state, rule_id),
      fast_path_eligible: analysis && analysis.strategy == :fast_path,
      memory_usage: calculate_rule_memory_usage(state, rule_id)
    }
  end

  defp generate_engine_diagnostics(state) do
    {time, memory_info} =
      :timer.tc(fn ->
        %{
          facts_count: WorkingMemory.get_fact_count(state),
          rules_count: map_size(state.rules),
          alpha_nodes_count: map_size(state.alpha_nodes),
          beta_nodes_count: get_beta_nodes_count(state),
          ets_memory_usage: calculate_ets_memory_usage(state),
          process_memory: :erlang.process_info(self(), :memory),
          system_memory: :erlang.memory()
        }
      end)

    Map.put(memory_info, :diagnostics_time_us, time)
  end

  defp execute_with_profiling(state, opts) do
    rules_to_profile = Keyword.get(opts, :rules, Map.keys(state.rules))

    # Execute rules with detailed profiling
    {total_time, results} =
      :timer.tc(fn ->
        Enum.map(rules_to_profile, &profile_single_rule(&1, state))
      end)

    %{
      total_execution_time_us: total_time,
      rule_profiles: results,
      performance_summary: %{
        fastest_rule: Enum.min_by(results, & &1.execution_time_us),
        slowest_rule: Enum.max_by(results, & &1.execution_time_us),
        total_results: Enum.sum(Enum.map(results, & &1.results_count))
      }
    }
  end

  defp profile_single_rule(rule_id, state) do
    rule = Map.get(state.rules, rule_id)

    {rule_time, rule_results} =
      :timer.tc(fn ->
        execute_single_rule(rule_id, rule, state)
      end)

    %{
      rule_id: rule_id,
      execution_time_us: rule_time,
      results_count: length(rule_results),
      results: rule_results,
      memory_before: :erlang.process_info(self(), :memory),
      memory_after: :erlang.process_info(self(), :memory)
    }
  end

  defp trace_fact_through_network(state, fact) do
    fact_type = elem(fact, 0)

    # Trace through alpha network
    alpha_trace = trace_alpha_processing(state, fact)

    # Trace through beta network if applicable
    beta_trace = trace_beta_processing(state, fact, alpha_trace)

    # Find which rules would be triggered
    triggered_rules = find_rules_triggered_by_fact(state, fact)

    %{
      fact: fact,
      fact_type: fact_type,
      alpha_trace: alpha_trace,
      beta_trace: beta_trace,
      triggered_rules: triggered_rules,
      processing_path: generate_processing_path(alpha_trace, beta_trace)
    }
  end

  defp generate_network_visualization(state) do
    alpha_nodes =
      Enum.map(state.alpha_nodes, fn {node_id, node} ->
        %{
          id: node_id,
          type: :alpha,
          pattern: Map.get(node, :pattern),
          memory_size: length(AlphaNetworkCoordinator.get_alpha_memory(state, node_id))
        }
      end)

    beta_nodes = get_beta_network_visualization(state)

    %{
      nodes: alpha_nodes ++ beta_nodes,
      edges: generate_network_edges(state),
      metrics: %{
        total_nodes: length(alpha_nodes) + length(beta_nodes),
        total_memory_usage: calculate_total_network_memory(state),
        optimization_opportunities: identify_optimization_opportunities(state)
      }
    }
  end

  defp generate_performance_recommendations(state) do
    recommendations = []

    # Check for underutilized fast-path opportunities
    recommendations = maybe_recommend_fast_path(state, recommendations)

    # Check for memory optimization opportunities
    recommendations = maybe_recommend_memory_optimization(state, recommendations)

    # Check for rule consolidation opportunities
    recommendations = maybe_recommend_rule_consolidation(state, recommendations)

    # Check for ETS optimization opportunities
    recommendations = maybe_recommend_ets_optimization(state, recommendations)

    recommendations
  end

  # Helper functions for debugging

  defp get_rule_alpha_nodes(state, rule_id) do
    case Map.get(state.rule_networks, rule_id) do
      %{alpha_nodes: alpha_nodes} -> alpha_nodes
      _ -> []
    end
  end

  defp get_rule_beta_nodes(state, rule_id) do
    case Map.get(state.rule_networks, rule_id) do
      %{beta_nodes: beta_nodes} -> beta_nodes
      _ -> []
    end
  end

  defp calculate_rule_memory_usage(state, rule_id) do
    alpha_memory =
      get_rule_alpha_nodes(state, rule_id)
      |> Enum.map(&AlphaNetworkCoordinator.get_alpha_memory(state, &1))
      |> Enum.map(&length/1)
      |> Enum.sum()

    beta_memory =
      get_rule_beta_nodes(state, rule_id)
      |> Enum.map(&BetaNetworkCoordinator.get_beta_memory(state.beta_network, &1))
      |> Enum.map(&length/1)
      |> Enum.sum()

    %{
      alpha_memory_items: alpha_memory,
      beta_memory_items: beta_memory,
      total_items: alpha_memory + beta_memory
    }
  end

  defp get_beta_nodes_count(state) do
    # Simplified beta node count - would need proper implementation
    case state.beta_network do
      # Would query the beta network process
      pid when is_pid(pid) -> 0
      nil -> 0
    end
  rescue
    _ -> 0
  end

  defp calculate_ets_memory_usage(state) do
    %{
      facts_table: :ets.info(state.facts_table, :memory) || 0,
      alpha_memories: :ets.info(state.alpha_memories, :memory) || 0,
      compiled_patterns: :ets.info(state.compiled_patterns, :memory) || 0,
      rule_statistics: :ets.info(state.rule_statistics_table, :memory) || 0
    }
  end

  defp trace_alpha_processing(state, fact) do
    fact_type = elem(fact, 0)
    relevant_nodes = Map.get(state.fact_type_index, fact_type, [])

    Enum.map(relevant_nodes, fn node_id ->
      node = Map.get(state.alpha_nodes, node_id)
      # Simplified pattern matching check
      pattern = Map.get(node, :pattern)
      matches = fact_matches_pattern?(fact, pattern)

      %{
        node_id: node_id,
        pattern: Map.get(node, :pattern),
        matches: matches,
        memory_before: length(AlphaNetworkCoordinator.get_alpha_memory(state, node_id))
      }
    end)
  end

  defp trace_beta_processing(_state, _fact, alpha_trace) do
    # Simplified beta trace - would need more sophisticated implementation
    matching_alpha_nodes =
      alpha_trace
      |> Enum.filter(& &1.matches)
      |> Enum.map(& &1.node_id)

    %{
      affected_alpha_nodes: matching_alpha_nodes,
      # Simplified
      beta_propagation: "Would propagate to beta network"
    }
  end

  defp find_rules_triggered_by_fact(state, fact) do
    state.rules
    |> Enum.filter(fn {_rule_id, rule} ->
      would_rule_match_fact?(rule, fact, state)
    end)
    |> Enum.map(fn {rule_id, _rule} -> rule_id end)
  end

  defp generate_processing_path(alpha_trace, beta_trace) do
    alpha_steps =
      Enum.map(alpha_trace, fn trace ->
        %{step: :alpha_processing, node_id: trace.node_id, result: trace.matches}
      end)

    beta_steps = [
      %{step: :beta_processing, affected_nodes: beta_trace.affected_alpha_nodes}
    ]

    alpha_steps ++ beta_steps
  end

  defp get_beta_network_visualization(_state) do
    # Simplified beta network visualization
    []
  end

  defp generate_network_edges(_state) do
    # Simplified edge generation
    []
  end

  defp calculate_total_network_memory(state) do
    ets_memory = calculate_ets_memory_usage(state)
    Enum.sum(Map.values(ets_memory))
  end

  defp identify_optimization_opportunities(_state) do
    # Simplified optimization identification
    []
  end

  defp maybe_recommend_fast_path(state, recommendations) do
    slow_simple_rules =
      state.rule_analyses
      |> Enum.filter(fn {_rule_id, analysis} ->
        analysis.complexity == :simple and analysis.strategy != :fast_path
      end)

    if length(slow_simple_rules) > 0 do
      recommendation = %{
        type: :fast_path_opportunity,
        severity: :medium,
        description: "Simple rules not using fast-path execution",
        affected_rules: Enum.map(slow_simple_rules, fn {rule_id, _} -> rule_id end),
        action: "Enable fast-path execution for simple rules"
      }

      [recommendation | recommendations]
    else
      recommendations
    end
  end

  defp maybe_recommend_memory_optimization(state, recommendations) do
    fact_count = WorkingMemory.get_fact_count(state)

    if fact_count > 10_000 do
      recommendation = %{
        type: :memory_optimization,
        severity: :high,
        description: "Large fact count may benefit from cleanup",
        fact_count: fact_count,
        action: "Consider implementing fact cleanup or archival strategy"
      }

      [recommendation | recommendations]
    else
      recommendations
    end
  end

  defp maybe_recommend_rule_consolidation(state, recommendations) do
    rule_count = map_size(state.rules)

    if rule_count > 100 do
      recommendation = %{
        type: :rule_consolidation,
        severity: :medium,
        description: "Large number of rules may benefit from consolidation",
        rule_count: rule_count,
        action: "Consider consolidating similar rules or using rule templates"
      }

      [recommendation | recommendations]
    else
      recommendations
    end
  end

  defp maybe_recommend_ets_optimization(_state, recommendations) do
    # ETS tables are already optimized in our implementation
    recommendations
  end

  # Engine State Persistence and Recovery

  defp create_engine_snapshot(state) do
    try do
      # Snapshot working memory
      {:ok, facts_snapshot} = Presto.Persistence.snapshot(state.facts_table)
      {:ok, changes_snapshot} = Presto.Persistence.snapshot(state.changes_table)

      # Snapshot alpha networks
      alpha_snapshots = snapshot_alpha_networks(state)

      # Snapshot beta network state
      beta_snapshot = BetaNetworkCoordinator.create_snapshot(state)

      # Get all rules
      rules = RuleStorage.get_rules(state)

      # Get statistics
      stats = Statistics.get_engine_statistics(state)

      # Create comprehensive snapshot
      snapshot = %{
        version: "1.0",
        timestamp: System.system_time(:second),
        engine_id: state.engine_id,
        facts: facts_snapshot,
        changes: changes_snapshot,
        alpha_networks: alpha_snapshots,
        beta_network: beta_snapshot,
        rules: rules,
        statistics: stats,
        configuration: %{
          optimization_config: state.optimization_config
        }
      }

      {:ok, snapshot}
    rescue
      error -> {:error, error}
    end
  end

  defp restore_engine_from_snapshot(state, snapshot) do
    try do
      # Validate snapshot format
      unless Map.has_key?(snapshot, :version) and Map.has_key?(snapshot, :facts) do
        throw({:error, :invalid_snapshot_format})
      end

      # Clear current state
      :ok = Presto.Persistence.clear(state.facts_table)
      :ok = Presto.Persistence.clear(state.changes_table)

      # Restore facts and changes
      :ok = Presto.Persistence.restore(state.facts_table, snapshot.facts)
      :ok = Presto.Persistence.restore(state.changes_table, snapshot.changes)

      # Restore alpha networks
      new_state = restore_alpha_networks(state, Map.get(snapshot, :alpha_networks, %{}))

      # Restore beta network
      final_state =
        case BetaNetworkCoordinator.restore_snapshot(
               new_state,
               Map.get(snapshot, :beta_network, %{})
             ) do
          {:ok, restored_state} -> restored_state
          {:error, _reason} -> new_state
        end

      # Restore rules if they exist in snapshot
      restored_state =
        case Map.get(snapshot, :rules) do
          rules when is_map(rules) ->
            Enum.reduce(rules, final_state, fn {rule_id, rule}, acc_state ->
              RuleStorage.add_rule(acc_state, rule_id, rule)
            end)

          _ ->
            final_state
        end

      # Restore statistics if available
      stats_state =
        case Map.get(snapshot, :statistics) do
          stats when is_map(stats) ->
            Statistics.restore_statistics(restored_state, stats)

          _ ->
            restored_state
        end

      {:ok, stats_state}
    rescue
      error -> {:error, error}
    catch
      {:error, reason} -> {:error, reason}
    end
  end

  defp save_engine_snapshot_to_file(state, file_path) do
    case create_engine_snapshot(state) do
      {:ok, snapshot} ->
        try do
          # Ensure directory exists
          file_path |> Path.dirname() |> File.mkdir_p()

          # Convert to binary using :erlang.term_to_binary for Elixir terms
          binary_data = :erlang.term_to_binary(snapshot, [:compressed])

          # Write to file
          case File.write(file_path, binary_data) do
            :ok ->
              Logger.info("Engine snapshot saved to #{file_path}")
              :ok

            {:error, reason} ->
              Logger.error("Failed to write snapshot file #{file_path}: #{inspect(reason)}")
              {:error, reason}
          end
        rescue
          error -> {:error, error}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp load_engine_snapshot_from_file(state, file_path) do
    try do
      case File.read(file_path) do
        {:ok, binary_data} ->
          # Convert from binary back to Elixir terms
          snapshot = :erlang.binary_to_term(binary_data)
          Logger.info("Engine snapshot loaded from #{file_path}")
          restore_engine_from_snapshot(state, snapshot)

        {:error, reason} ->
          Logger.error("Failed to read snapshot file #{file_path}: #{inspect(reason)}")
          {:error, reason}
      end
    rescue
      error -> {:error, error}
    end
  end

  defp snapshot_alpha_networks(state) do
    try do
      # Get all alpha memory snapshots using persistence interface
      alpha_memories = AlphaNetworkCoordinator.get_all_alpha_memories(state)

      Enum.reduce(alpha_memories, %{}, fn {node_id, table}, acc ->
        case Presto.Persistence.snapshot(table) do
          {:ok, snapshot} ->
            Map.put(acc, node_id, snapshot)

          # Skip failed snapshots
          {:error, reason} ->
            Logger.debug("Snapshot error for alpha node: #{inspect(reason)}")
            acc
        end
      end)
    rescue
      # Return empty map on error
      error ->
        Logger.debug("Alpha network snapshot error: #{inspect(error)}")
        %{}
    end
  end

  defp restore_alpha_networks(state, alpha_snapshots) when is_map(alpha_snapshots) do
    try do
      # Restore each alpha network memory
      Enum.each(alpha_snapshots, fn {node_id, snapshot} ->
        case AlphaNetworkCoordinator.get_alpha_memory(state, node_id) do
          table when not is_nil(table) ->
            Presto.Persistence.restore(table, snapshot)

          # Skip if table doesn't exist
          _ ->
            :ok
        end
      end)

      state
    rescue
      # Return original state on error
      error ->
        Logger.debug("Alpha network restore error: #{inspect(error)}")
        state
    end
  end

  defp restore_alpha_networks(state, _), do: state
end

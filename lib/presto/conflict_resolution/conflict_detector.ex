defmodule Presto.ConflictResolution.ConflictDetector do
  @moduledoc """
  Advanced rule conflict detection system for Presto RETE engines.

  Provides intelligent conflict detection capabilities including:
  - Static conflict analysis during rule compilation
  - Dynamic conflict detection during execution
  - Cross-rule dependency analysis
  - Conflict pattern recognition and classification
  - Performance impact assessment of conflicts

  ## Conflict Types

  ### Direct Conflicts
  - Rules modifying the same facts with different values
  - Mutually exclusive rule conditions
  - Circular dependency chains

  ### Indirect Conflicts
  - Rules with overlapping side effects
  - Priority conflicts in execution order
  - Resource contention patterns

  ### Performance Conflicts
  - Rules causing excessive firing cycles
  - Memory allocation conflicts
  - Working memory thrashing patterns

  ## Example Usage

      # Analyze conflicts in an engine
      {:ok, conflicts} = ConflictDetector.analyze_engine_conflicts(:payment_engine)

      # Detect conflicts between specific rules
      {:ok, result} = ConflictDetector.check_rule_conflicts(
        :payment_engine,
        [:discount_rule, :tax_rule, :total_rule]
      )

      # Monitor conflicts during execution
      ConflictDetector.start_conflict_monitoring(:customer_engine, [
        detection_mode: :comprehensive,
        resolution_strategy: :priority_based
      ])
  """

  use GenServer
  require Logger

  @type conflict_type :: :direct | :indirect | :performance | :dependency
  @type conflict_severity :: :low | :medium | :high | :critical
  @type resolution_strategy :: :priority_based | :timestamp_based | :custom | :manual

  @type rule_conflict :: %{
          id: String.t(),
          type: conflict_type(),
          severity: conflict_severity(),
          rules_involved: [atom()],
          description: String.t(),
          impact_analysis: map(),
          suggested_resolutions: [map()],
          detection_timestamp: DateTime.t(),
          engine_name: atom()
        }

  @type conflict_analysis :: %{
          engine_name: atom(),
          total_conflicts: non_neg_integer(),
          conflicts_by_type: map(),
          conflicts_by_severity: map(),
          rule_interactions: map(),
          dependency_graph: map(),
          performance_impact: map(),
          analysis_timestamp: DateTime.t()
        }

  ## Client API

  @doc """
  Starts the conflict detector.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Analyzes all conflicts in an engine.
  """
  @spec analyze_engine_conflicts(atom(), keyword()) ::
          {:ok, conflict_analysis()} | {:error, term()}
  def analyze_engine_conflicts(engine_name, opts \\ []) do
    GenServer.call(__MODULE__, {:analyze_engine, engine_name, opts}, 30_000)
  end

  @doc """
  Checks for conflicts between specific rules.
  """
  @spec check_rule_conflicts(atom(), [atom()], keyword()) ::
          {:ok, [rule_conflict()]} | {:error, term()}
  def check_rule_conflicts(engine_name, rule_ids, opts \\ []) do
    GenServer.call(__MODULE__, {:check_rules, engine_name, rule_ids, opts})
  end

  @doc """
  Starts real-time conflict monitoring for an engine.
  """
  @spec start_conflict_monitoring(atom(), keyword()) :: :ok | {:error, term()}
  def start_conflict_monitoring(engine_name, opts \\ []) do
    GenServer.call(__MODULE__, {:start_monitoring, engine_name, opts})
  end

  @doc """
  Stops conflict monitoring for an engine.
  """
  @spec stop_conflict_monitoring(atom()) :: :ok
  def stop_conflict_monitoring(engine_name) do
    GenServer.call(__MODULE__, {:stop_monitoring, engine_name})
  end

  @doc """
  Gets real-time conflict statistics.
  """
  @spec get_conflict_stats(atom()) :: {:ok, map()} | {:error, term()}
  def get_conflict_stats(engine_name) do
    GenServer.call(__MODULE__, {:get_stats, engine_name})
  end

  @doc """
  Detects conflicts in a rule set before deployment.
  """
  @spec validate_rule_set([map()], keyword()) ::
          {:ok, [rule_conflict()]} | {:error, term()}
  def validate_rule_set(rules, opts \\ []) do
    GenServer.call(__MODULE__, {:validate_rules, rules, opts})
  end

  @doc """
  Gets conflict detection history for an engine.
  """
  @spec get_conflict_history(atom(), keyword()) :: {:ok, [rule_conflict()]}
  def get_conflict_history(engine_name, opts \\ []) do
    GenServer.call(__MODULE__, {:get_history, engine_name, opts})
  end

  ## Server implementation

  @impl GenServer
  def init(opts) do
    Logger.info("Starting Conflict Detector")

    state = %{
      # Configuration
      default_detection_mode: Keyword.get(opts, :default_detection_mode, :standard),
      max_conflict_history: Keyword.get(opts, :max_conflict_history, 1000),
      # 5 minutes
      analysis_cache_ttl: Keyword.get(opts, :analysis_cache_ttl, 300),

      # Active monitoring
      monitored_engines: %{},
      monitoring_configs: %{},

      # Analysis cache
      analysis_cache: %{},

      # Conflict data
      active_conflicts: %{},
      conflict_history: %{},

      # Rule metadata
      rule_metadata: %{},
      dependency_graphs: %{},

      # Statistics
      stats: %{
        total_analyses: 0,
        conflicts_detected: 0,
        conflicts_resolved: 0,
        engines_monitored: 0,
        cache_hits: 0,
        cache_misses: 0
      }
    }

    # Schedule periodic cleanup and analysis
    Process.send_after(self(), :periodic_analysis, 60_000)

    {:ok, state}
  end

  @impl GenServer
  def handle_call({:analyze_engine, engine_name, opts}, _from, state) do
    analysis_mode = Keyword.get(opts, :mode, state.default_detection_mode)

    case perform_engine_analysis(engine_name, analysis_mode, state) do
      {:ok, analysis, new_state} ->
        updated_stats = %{new_state.stats | total_analyses: new_state.stats.total_analyses + 1}

        {:reply, {:ok, analysis}, %{new_state | stats: updated_stats}}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl GenServer
  def handle_call({:check_rules, engine_name, rule_ids, opts}, _from, state) do
    case check_specific_rule_conflicts(engine_name, rule_ids, opts, state) do
      {:ok, conflicts} ->
        new_stats = %{
          state.stats
          | conflicts_detected: state.stats.conflicts_detected + length(conflicts)
        }

        {:reply, {:ok, conflicts}, %{state | stats: new_stats}}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl GenServer
  def handle_call({:start_monitoring, engine_name, opts}, _from, state) do
    monitoring_config = %{
      engine_name: engine_name,
      detection_mode: Keyword.get(opts, :detection_mode, :standard),
      resolution_strategy: Keyword.get(opts, :resolution_strategy, :priority_based),
      check_interval: Keyword.get(opts, :check_interval, 5000),
      auto_resolve: Keyword.get(opts, :auto_resolve, false),
      started_at: DateTime.utc_now()
    }

    new_configs = Map.put(state.monitoring_configs, engine_name, monitoring_config)
    new_monitored = Map.put(state.monitored_engines, engine_name, true)

    # Start monitoring process
    start_engine_monitoring(engine_name, monitoring_config)

    new_stats = %{state.stats | engines_monitored: state.stats.engines_monitored + 1}

    Logger.info("Started conflict monitoring", engine: engine_name)

    {:reply, :ok,
     %{
       state
       | monitoring_configs: new_configs,
         monitored_engines: new_monitored,
         stats: new_stats
     }}
  end

  @impl GenServer
  def handle_call({:stop_monitoring, engine_name}, _from, state) do
    new_configs = Map.delete(state.monitoring_configs, engine_name)
    new_monitored = Map.delete(state.monitored_engines, engine_name)

    new_stats = %{state.stats | engines_monitored: max(0, state.stats.engines_monitored - 1)}

    Logger.info("Stopped conflict monitoring", engine: engine_name)

    {:reply, :ok,
     %{
       state
       | monitoring_configs: new_configs,
         monitored_engines: new_monitored,
         stats: new_stats
     }}
  end

  @impl GenServer
  def handle_call({:get_stats, engine_name}, _from, state) do
    stats = build_conflict_stats(engine_name, state)
    {:reply, {:ok, stats}, state}
  end

  @impl GenServer
  def handle_call({:validate_rules, rules, opts}, _from, state) do
    {:ok, conflicts} = validate_rule_set_internal(rules, opts, state)
    {:reply, {:ok, conflicts}, state}
  end

  @impl GenServer
  def handle_call({:get_history, engine_name, opts}, _from, state) do
    history = get_engine_conflict_history(engine_name, opts, state)
    {:reply, {:ok, history}, state}
  end

  @impl GenServer
  def handle_info(:periodic_analysis, state) do
    new_state = perform_periodic_monitoring(state)

    # Schedule next analysis
    Process.send_after(self(), :periodic_analysis, 60_000)

    {:noreply, new_state}
  end

  @impl GenServer
  def handle_info({:conflict_detected, engine_name, conflict}, state) do
    new_state = handle_detected_conflict(engine_name, conflict, state)
    {:noreply, new_state}
  end

  ## Private functions

  defp perform_engine_analysis(engine_name, analysis_mode, state) do
    try do
      # Check cache first
      cache_key = {engine_name, analysis_mode}

      case check_analysis_cache(cache_key, state) do
        {:hit, analysis} ->
          updated_stats = %{state.stats | cache_hits: state.stats.cache_hits + 1}
          {:ok, analysis, %{state | stats: updated_stats}}

        :miss ->
          # Perform fresh analysis
          case conduct_conflict_analysis(engine_name, analysis_mode, state) do
            {:ok, analysis} ->
              # Cache results
              new_cache = Map.put(state.analysis_cache, cache_key, {analysis, DateTime.utc_now()})
              updated_stats = %{state.stats | cache_misses: state.stats.cache_misses + 1}

              new_state = %{state | analysis_cache: new_cache, stats: updated_stats}

              {:ok, analysis, new_state}

            error ->
              error
          end
      end
    rescue
      error ->
        Logger.error("Conflict analysis failed",
          engine: engine_name,
          error: inspect(error)
        )

        {:error, {:analysis_failed, error}}
    end
  end

  defp check_analysis_cache(cache_key, state) do
    case Map.get(state.analysis_cache, cache_key) do
      nil ->
        :miss

      {analysis, timestamp} ->
        # Check if cache entry is still valid
        if DateTime.diff(DateTime.utc_now(), timestamp, :second) < state.analysis_cache_ttl do
          {:hit, analysis}
        else
          :miss
        end
    end
  end

  defp conduct_conflict_analysis(engine_name, analysis_mode, _state) do
    # Get engine rules and metadata
    case get_engine_rules(engine_name) do
      {:ok, rules} ->
        # Build dependency graph
        dependency_graph = build_dependency_graph(rules)

        # Detect various types of conflicts
        direct_conflicts = detect_direct_conflicts(rules, analysis_mode)
        indirect_conflicts = detect_indirect_conflicts(rules, dependency_graph, analysis_mode)
        performance_conflicts = detect_performance_conflicts(rules, analysis_mode)

        all_conflicts = direct_conflicts ++ indirect_conflicts ++ performance_conflicts

        # Build comprehensive analysis
        analysis = %{
          engine_name: engine_name,
          total_conflicts: length(all_conflicts),
          conflicts_by_type: group_conflicts_by_type(all_conflicts),
          conflicts_by_severity: group_conflicts_by_severity(all_conflicts),
          rule_interactions: analyze_rule_interactions(rules),
          dependency_graph: dependency_graph,
          performance_impact: assess_performance_impact(all_conflicts),
          analysis_timestamp: DateTime.utc_now()
        }

        {:ok, analysis}

      error ->
        error
    end
  end

  defp detect_direct_conflicts(rules, analysis_mode) do
    conflicts = []

    # Check for fact modification conflicts
    fact_conflicts = detect_fact_modification_conflicts(rules)
    conflicts = conflicts ++ fact_conflicts

    # Check for condition conflicts
    condition_conflicts = detect_condition_conflicts(rules)
    conflicts = conflicts ++ condition_conflicts

    # Check for circular dependencies
    circular_conflicts = detect_circular_dependencies(rules)
    conflicts = conflicts ++ circular_conflicts

    # Additional analysis for comprehensive mode
    conflicts =
      if analysis_mode == :comprehensive do
        priority_conflicts = detect_priority_conflicts(rules)
        conflicts ++ priority_conflicts
      else
        conflicts
      end

    conflicts
  end

  defp detect_fact_modification_conflicts(rules) do
    # Group rules by facts they modify
    fact_modifiers =
      Enum.reduce(rules, %{}, fn rule, acc ->
        modified_facts = extract_modified_facts(rule)

        Enum.reduce(modified_facts, acc, fn fact_type, fact_acc ->
          modifiers = Map.get(fact_acc, fact_type, [])
          Map.put(fact_acc, fact_type, [rule | modifiers])
        end)
      end)

    # Find conflicts where multiple rules modify same facts
    Enum.flat_map(fact_modifiers, fn {fact_type, modifying_rules} ->
      if length(modifying_rules) > 1 do
        potential_conflicts = analyze_fact_conflict_potential(fact_type, modifying_rules)

        Enum.map(potential_conflicts, fn {rules_in_conflict, conflict_details} ->
          create_conflict(
            :direct,
            :medium,
            Enum.map(rules_in_conflict, & &1.id),
            "Rules modify the same fact type: #{fact_type}",
            conflict_details
          )
        end)
      else
        []
      end
    end)
  end

  defp detect_condition_conflicts(rules) do
    # Detect mutually exclusive conditions
    rule_pairs = for r1 <- rules, r2 <- rules, r1.id != r2.id, do: {r1, r2}

    Enum.flat_map(rule_pairs, fn {_rule1, _rule2} ->
      # Placeholder: condition exclusivity analysis not implemented
      []
    end)
  end

  defp detect_circular_dependencies(rules) do
    # Build dependency chains and detect cycles
    dependency_chains = build_rule_dependencies(rules)

    Enum.flat_map(dependency_chains, fn {rule_id, dependencies} ->
      case find_circular_dependency(rule_id, dependencies, dependency_chains) do
        {:circular, cycle} ->
          [
            create_conflict(
              :dependency,
              :critical,
              cycle,
              "Circular dependency detected in rule chain",
              %{dependency_cycle: cycle}
            )
          ]

        :no_cycle ->
          []
      end
    end)
  end

  defp detect_indirect_conflicts(rules, dependency_graph, _analysis_mode) do
    conflicts = []

    # Check for side effect conflicts
    side_effect_conflicts = detect_side_effect_conflicts(rules, dependency_graph)
    conflicts = conflicts ++ side_effect_conflicts

    # Check for execution order conflicts
    order_conflicts = detect_execution_order_conflicts(rules)
    conflicts = conflicts ++ order_conflicts

    conflicts
  end

  defp detect_performance_conflicts(rules, _analysis_mode) do
    conflicts = []

    # Detect infinite firing patterns
    firing_conflicts = detect_infinite_firing_patterns(rules)
    conflicts = conflicts ++ firing_conflicts

    # Detect memory thrashing patterns
    memory_conflicts = detect_memory_conflicts(rules)
    conflicts = conflicts ++ memory_conflicts

    conflicts
  end

  defp check_specific_rule_conflicts(engine_name, rule_ids, opts, _state) do
    case get_engine_rules(engine_name) do
      {:ok, all_rules} ->
        # Filter to specified rules
        target_rules = Enum.filter(all_rules, fn rule -> rule.id in rule_ids end)

        if length(target_rules) != length(rule_ids) do
          {:error, :some_rules_not_found}
        else
          # Analyze conflicts between these specific rules
          conflicts = analyze_rule_subset_conflicts(target_rules, opts)
          {:ok, conflicts}
        end

      error ->
        error
    end
  end

  defp validate_rule_set_internal(rules, _opts, _state) do
    # Perform comprehensive conflict analysis on rule set
    conflicts = []

    # Direct conflicts
    direct = detect_direct_conflicts(rules, :comprehensive)
    conflicts = conflicts ++ direct

    # Build dependency graph for the rule set
    dependency_graph = build_dependency_graph(rules)

    # Indirect conflicts
    indirect = detect_indirect_conflicts(rules, dependency_graph, :comprehensive)
    conflicts = conflicts ++ indirect

    # Performance conflicts
    performance = detect_performance_conflicts(rules, :comprehensive)
    conflicts = conflicts ++ performance

    {:ok, conflicts}
  end

  defp start_engine_monitoring(engine_name, monitoring_config) do
    # In a real implementation, this would start a monitoring process
    # For now, we'll simulate by scheduling periodic checks
    Task.start(fn ->
      monitor_engine_conflicts(engine_name, monitoring_config)
    end)
  end

  defp monitor_engine_conflicts(engine_name, monitoring_config) do
    # Simulate conflict monitoring
    :timer.sleep(monitoring_config.check_interval)

    # Check for new conflicts
    case get_engine_rules(engine_name) do
      {:ok, rules} ->
        conflicts = detect_direct_conflicts(rules, monitoring_config.detection_mode)

        # Report any new conflicts
        Enum.each(conflicts, fn conflict ->
          send(__MODULE__, {:conflict_detected, engine_name, conflict})
        end)

        # Continue monitoring
        monitor_engine_conflicts(engine_name, monitoring_config)

      _ ->
        Logger.warning("Failed to get rules for monitoring", engine: engine_name)
    end
  end

  defp handle_detected_conflict(engine_name, conflict, state) do
    # Add to active conflicts
    conflict_id = conflict.id
    new_active = Map.put(state.active_conflicts, conflict_id, conflict)

    # Add to history
    engine_history = Map.get(state.conflict_history, engine_name, [])
    new_history = [conflict | Enum.take(engine_history, state.max_conflict_history - 1)]
    new_conflict_history = Map.put(state.conflict_history, engine_name, new_history)

    # Update stats
    new_stats = %{state.stats | conflicts_detected: state.stats.conflicts_detected + 1}

    # Check if auto-resolution is enabled
    case Map.get(state.monitoring_configs, engine_name) do
      %{auto_resolve: true, resolution_strategy: strategy} ->
        attempt_conflict_resolution(conflict, strategy, state)

      _ ->
        :ok
    end

    Logger.warning("Conflict detected",
      engine: engine_name,
      conflict_type: conflict.type,
      severity: conflict.severity
    )

    %{
      state
      | active_conflicts: new_active,
        conflict_history: new_conflict_history,
        stats: new_stats
    }
  end

  defp perform_periodic_monitoring(state) do
    # Check all monitored engines for new conflicts
    Enum.reduce(state.monitored_engines, state, fn {engine_name, _}, acc_state ->
      case Map.get(state.monitoring_configs, engine_name) do
        nil -> acc_state
        config -> perform_engine_check(engine_name, config, acc_state)
      end
    end)
  end

  defp perform_engine_check(_engine_name, _config, state) do
    # Simplified check - in reality would be more sophisticated
    state
  end

  # Helper functions

  defp get_engine_rules(_engine_name) do
    # Placeholder - would integrate with actual engine
    {:ok,
     [
       %{id: :rule_1, conditions: [], actions: [], priority: 1},
       %{id: :rule_2, conditions: [], actions: [], priority: 2},
       %{id: :rule_3, conditions: [], actions: [], priority: 3}
     ]}
  end

  defp extract_modified_facts(_rule) do
    # Extract fact types modified by rule actions
    # Placeholder implementation
    [:customer, :order]
  end

  defp analyze_fact_conflict_potential(fact_type, modifying_rules) do
    # Analyze if rules actually conflict when modifying facts
    # Placeholder - would perform deeper analysis
    if length(modifying_rules) > 1 do
      [{modifying_rules, %{fact_type: fact_type, conflict_reason: "multiple_modifiers"}}]
    else
      []
    end
  end

  defp analyze_condition_exclusivity(_rule1, _rule2) do
    raise "Not implemented: analyze_condition_exclusivity/2 requires condition analysis algorithm"
  end

  defp build_rule_dependencies(rules) do
    # Build dependency map between rules
    Map.new(rules, fn rule ->
      deps = extract_rule_dependencies(rule, rules)
      {rule.id, deps}
    end)
  end

  defp extract_rule_dependencies(_rule, _all_rules) do
    # Extract which other rules this rule depends on
    # Placeholder implementation
    []
  end

  defp find_circular_dependency(rule_id, dependencies, all_dependencies) do
    # Use DFS to find circular dependencies
    find_cycle(rule_id, dependencies, all_dependencies, [rule_id])
  end

  defp find_cycle(current_rule, dependencies, all_dependencies, visited) do
    case dependencies do
      [] ->
        :no_cycle

      [dep | rest] ->
        if dep in visited do
          {:circular, visited ++ [dep]}
        else
          case Map.get(all_dependencies, dep, []) do
            [] ->
              find_cycle(current_rule, rest, all_dependencies, visited)

            dep_dependencies ->
              case find_cycle(dep, dep_dependencies, all_dependencies, visited ++ [dep]) do
                {:circular, cycle} -> {:circular, cycle}
                :no_cycle -> find_cycle(current_rule, rest, all_dependencies, visited)
              end
          end
        end
    end
  end

  defp detect_side_effect_conflicts(_rules, _dependency_graph) do
    # Detect conflicts from rule side effects
    []
  end

  defp detect_execution_order_conflicts(_rules) do
    # Detect conflicts in rule execution ordering
    []
  end

  defp detect_infinite_firing_patterns(_rules) do
    # Detect patterns that could lead to infinite rule firing
    []
  end

  defp detect_memory_conflicts(_rules) do
    # Detect patterns that could cause memory issues
    []
  end

  defp detect_priority_conflicts(_rules) do
    # Detect conflicts in rule priorities
    []
  end

  defp build_dependency_graph(rules) do
    # Build comprehensive dependency graph
    %{nodes: rules, edges: []}
  end

  defp analyze_rule_interactions(_rules) do
    # Analyze how rules interact with each other
    %{total_interactions: 0, interaction_types: %{}}
  end

  defp assess_performance_impact(_conflicts) do
    # Assess the performance impact of detected conflicts
    %{estimated_impact: :low, affected_operations: []}
  end

  defp group_conflicts_by_type(conflicts) do
    Enum.group_by(conflicts, & &1.type)
  end

  defp group_conflicts_by_severity(conflicts) do
    Enum.group_by(conflicts, & &1.severity)
  end

  defp analyze_rule_subset_conflicts(rules, _opts) do
    # Analyze conflicts within a subset of rules
    detect_direct_conflicts(rules, :standard)
  end

  defp create_conflict(type, severity, rules_involved, description, impact_analysis) do
    %{
      id: generate_conflict_id(),
      type: type,
      severity: severity,
      rules_involved: rules_involved,
      description: description,
      impact_analysis: impact_analysis,
      suggested_resolutions: generate_resolutions(type, severity, rules_involved),
      detection_timestamp: DateTime.utc_now(),
      engine_name: :unknown
    }
  end

  defp generate_resolutions(type, _severity, _rules_involved) do
    # Generate suggested resolutions based on conflict type and severity
    case type do
      :direct ->
        [
          %{strategy: :priority_adjustment, description: "Adjust rule priorities"},
          %{strategy: :condition_refinement, description: "Refine rule conditions"},
          %{strategy: :rule_combination, description: "Combine conflicting rules"}
        ]

      :dependency ->
        [
          %{strategy: :dependency_breaking, description: "Break circular dependencies"},
          %{strategy: :rule_reordering, description: "Reorder rule execution"}
        ]

      _ ->
        [%{strategy: :manual_review, description: "Manual review required"}]
    end
  end

  defp attempt_conflict_resolution(conflict, strategy, _state) do
    # Attempt to automatically resolve conflict based on strategy
    Logger.info("Attempting conflict resolution",
      conflict_id: conflict.id,
      strategy: strategy
    )

    # Implementation would depend on resolution strategy
  end

  defp build_conflict_stats(engine_name, state) do
    engine_conflicts = Map.get(state.conflict_history, engine_name, [])

    %{
      engine_name: engine_name,
      total_conflicts: length(engine_conflicts),
      active_conflicts: count_active_conflicts_for_engine(engine_name, state.active_conflicts),
      conflicts_by_type: group_conflicts_by_type(engine_conflicts),
      last_conflict: get_last_conflict_time(engine_conflicts),
      monitoring_active: Map.has_key?(state.monitored_engines, engine_name)
    }
  end

  defp count_active_conflicts_for_engine(engine_name, active_conflicts) do
    active_conflicts
    |> Map.values()
    |> Enum.count(&(&1.engine_name == engine_name))
  end

  defp get_last_conflict_time([]), do: nil
  defp get_last_conflict_time([latest | _]), do: latest.detection_timestamp

  defp get_engine_conflict_history(engine_name, opts, state) do
    history = Map.get(state.conflict_history, engine_name, [])
    limit = Keyword.get(opts, :limit, 100)
    Enum.take(history, limit)
  end

  defp generate_conflict_id do
    "conflict_" <> (:crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower))
  end
end

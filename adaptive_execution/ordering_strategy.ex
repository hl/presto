defmodule Presto.AdaptiveExecution.OrderingStrategy do
  @moduledoc """
  Strategy definitions and implementations for rule execution ordering.

  Provides a collection of proven ordering strategies including:
  - RETE-specific optimization strategies
  - General rule engine optimization patterns
  - Domain-specific ordering heuristics
  - Hybrid and custom strategy frameworks

  ## Built-in Strategies

  ### Classical Strategies
  - Priority-based ordering (explicit rule priorities)
  - Specificity ordering (most specific conditions first)
  - Complexity ordering (simple rules before complex ones)
  - Frequency ordering (most frequently firing rules first)

  ### Advanced Strategies
  - Selectivity-based ordering (highest selectivity first)
  - Cost-benefit optimization
  - Dependency-aware ordering
  - Resource-aware scheduling

  ### Adaptive Strategies
  - Performance-driven reordering
  - Load-aware optimization
  - Context-sensitive ordering
  - Learning-based strategies
  """

  require Logger

  @type strategy_name :: atom()
  @type rule_metrics :: map()
  @type ordering_context :: map()

  @doc """
  Lists all available ordering strategies.
  """
  @spec list_strategies() :: [strategy_name()]
  def list_strategies do
    [
      :priority_based,
      :specificity_based,
      :complexity_based,
      :frequency_based,
      :selectivity_based,
      :cost_benefit,
      :dependency_aware,
      :resource_aware,
      :performance_driven,
      :load_aware,
      :context_sensitive,
      :learning_based
    ]
  end

  @doc """
  Gets strategy information.
  """
  @spec get_strategy_info(strategy_name()) :: {:ok, map()} | {:error, term()}
  def get_strategy_info(strategy_name) do
    case strategy_name do
      :priority_based ->
        {:ok, %{
          name: :priority_based,
          description: "Orders rules by explicit priority values (lower number = higher priority)",
          complexity: :low,
          best_for: [:deterministic_execution, :user_controlled_ordering],
          metrics_required: [:priority],
          performance_characteristics: %{
            ordering_time: :constant,
            adaptation_speed: :none,
            memory_overhead: :minimal
          }
        }}
      
      :specificity_based ->
        {:ok, %{
          name: :specificity_based,
          description: "Orders rules by condition specificity (more specific conditions first)",
          complexity: :medium,
          best_for: [:conflict_resolution, :efficient_matching],
          metrics_required: [:condition_count, :constraint_specificity],
          performance_characteristics: %{
            ordering_time: :linear,
            adaptation_speed: :slow,
            memory_overhead: :low
          }
        }}
      
      :selectivity_based ->
        {:ok, %{
          name: :selectivity_based,
          description: "Orders rules by selectivity (rules that match fewer facts first)",
          complexity: :medium,
          best_for: [:performance_optimization, :early_pruning],
          metrics_required: [:selectivity, :match_ratio],
          performance_characteristics: %{
            ordering_time: :linear,
            adaptation_speed: :fast,
            memory_overhead: :low
          }
        }}
      
      :performance_driven ->
        {:ok, %{
          name: :performance_driven,
          description: "Orders rules based on execution performance metrics",
          complexity: :high,
          best_for: [:runtime_optimization, :adaptive_systems],
          metrics_required: [:execution_time, :success_rate, :resource_usage],
          performance_characteristics: %{
            ordering_time: :linear_log,
            adaptation_speed: :fast,
            memory_overhead: :medium
          }
        }}
      
      _ ->
        {:error, :unknown_strategy}
    end
  end

  @doc """
  Applies an ordering strategy to a list of rules.
  """
  @spec apply_strategy(strategy_name(), [map()], ordering_context()) :: 
    {:ok, [atom()]} | {:error, term()}
  def apply_strategy(strategy_name, rules_with_metrics, context \\ %{}) do
    case strategy_name do
      :priority_based ->
        apply_priority_based_strategy(rules_with_metrics, context)
      
      :specificity_based ->
        apply_specificity_based_strategy(rules_with_metrics, context)
      
      :complexity_based ->
        apply_complexity_based_strategy(rules_with_metrics, context)
      
      :frequency_based ->
        apply_frequency_based_strategy(rules_with_metrics, context)
      
      :selectivity_based ->
        apply_selectivity_based_strategy(rules_with_metrics, context)
      
      :cost_benefit ->
        apply_cost_benefit_strategy(rules_with_metrics, context)
      
      :dependency_aware ->
        apply_dependency_aware_strategy(rules_with_metrics, context)
      
      :resource_aware ->
        apply_resource_aware_strategy(rules_with_metrics, context)
      
      :performance_driven ->
        apply_performance_driven_strategy(rules_with_metrics, context)
      
      :load_aware ->
        apply_load_aware_strategy(rules_with_metrics, context)
      
      :context_sensitive ->
        apply_context_sensitive_strategy(rules_with_metrics, context)
      
      :learning_based ->
        apply_learning_based_strategy(rules_with_metrics, context)
      
      _ ->
        {:error, {:unknown_strategy, strategy_name}}
    end
  end

  ## Strategy Implementations

  defp apply_priority_based_strategy(rules_with_metrics, _context) do
    ordered_rules = rules_with_metrics
    |> Enum.sort_by(fn rule -> Map.get(rule, :priority, 999) end)
    |> Enum.map(& &1.id)
    
    {:ok, ordered_rules}
  end

  defp apply_specificity_based_strategy(rules_with_metrics, context) do
    # Order by specificity (most specific first)
    ordered_rules = rules_with_metrics
    |> Enum.map(fn rule ->
      specificity = calculate_rule_specificity(rule, context)
      {rule, specificity}
    end)
    |> Enum.sort_by(fn {_rule, specificity} -> specificity end, :desc)
    |> Enum.map(fn {rule, _specificity} -> rule.id end)
    
    {:ok, ordered_rules}
  end

  defp apply_complexity_based_strategy(rules_with_metrics, _context) do
    # Order by complexity (simple rules first)
    ordered_rules = rules_with_metrics
    |> Enum.sort_by(fn rule -> 
      Map.get(rule, :complexity, calculate_rule_complexity(rule))
    end)
    |> Enum.map(& &1.id)
    
    {:ok, ordered_rules}
  end

  defp apply_frequency_based_strategy(rules_with_metrics, _context) do
    # Order by firing frequency (most frequent first)
    ordered_rules = rules_with_metrics
    |> Enum.sort_by(fn rule ->
      get_in(rule, [:metrics, :firing_frequency]) || 0
    end, :desc)
    |> Enum.map(& &1.id)
    
    {:ok, ordered_rules}
  end

  defp apply_selectivity_based_strategy(rules_with_metrics, _context) do
    # Order by selectivity (most selective first)
    ordered_rules = rules_with_metrics
    |> Enum.sort_by(fn rule ->
      get_in(rule, [:metrics, :selectivity]) || 0.5
    end, :desc)
    |> Enum.map(& &1.id)
    
    {:ok, ordered_rules}
  end

  defp apply_cost_benefit_strategy(rules_with_metrics, context) do
    # Order by cost-benefit ratio
    ordered_rules = rules_with_metrics
    |> Enum.map(fn rule ->
      cost_benefit_ratio = calculate_cost_benefit_ratio(rule, context)
      {rule, cost_benefit_ratio}
    end)
    |> Enum.sort_by(fn {_rule, ratio} -> ratio end, :desc)
    |> Enum.map(fn {rule, _ratio} -> rule.id end)
    
    {:ok, ordered_rules}
  end

  defp apply_dependency_aware_strategy(rules_with_metrics, context) do
    # Order based on rule dependencies (topological sort)
    case build_dependency_graph(rules_with_metrics, context) do
      {:ok, dependency_order} ->
        {:ok, dependency_order}
      
      {:error, :circular_dependency} ->
        # Fallback to priority-based if circular dependencies exist
        Logger.warning("Circular dependencies detected, falling back to priority-based ordering")
        apply_priority_based_strategy(rules_with_metrics, context)
      
      error ->
        error
    end
  end

  defp apply_resource_aware_strategy(rules_with_metrics, context) do
    # Order based on resource usage and availability
    system_resources = Map.get(context, :system_resources, %{})
    
    ordered_rules = rules_with_metrics
    |> Enum.map(fn rule ->
      resource_score = calculate_resource_efficiency_score(rule, system_resources)
      {rule, resource_score}
    end)
    |> Enum.sort_by(fn {_rule, score} -> score end, :desc)
    |> Enum.map(fn {rule, _score} -> rule.id end)
    
    {:ok, ordered_rules}
  end

  defp apply_performance_driven_strategy(rules_with_metrics, context) do
    # Order based on comprehensive performance metrics
    weights = Map.get(context, :performance_weights, %{
      execution_time: 0.3,
      success_rate: 0.3,
      selectivity: 0.2,
      resource_usage: 0.2
    })
    
    ordered_rules = rules_with_metrics
    |> Enum.map(fn rule ->
      performance_score = calculate_weighted_performance_score(rule, weights)
      {rule, performance_score}
    end)
    |> Enum.sort_by(fn {_rule, score} -> score end, :desc)
    |> Enum.map(fn {rule, _score} -> rule.id end)
    
    {:ok, ordered_rules}
  end

  defp apply_load_aware_strategy(rules_with_metrics, context) do
    # Adapt ordering based on current system load
    system_load = Map.get(context, :system_load, %{cpu: 0.5, memory: 0.5})
    
    # Adjust strategy based on load
    adjusted_strategy = cond do
      system_load.cpu > 0.8 ->
        # High CPU load - prioritize fast-executing rules
        apply_execution_time_strategy(rules_with_metrics, :ascending)
      
      system_load.memory > 0.8 ->
        # High memory load - prioritize low-memory rules
        apply_memory_usage_strategy(rules_with_metrics, :ascending)
      
      true ->
        # Normal load - use balanced approach
        apply_performance_driven_strategy(rules_with_metrics, context)
    end
    
    adjusted_strategy
  end

  defp apply_context_sensitive_strategy(rules_with_metrics, context) do
    # Adapt ordering based on execution context
    execution_mode = Map.get(context, :execution_mode, :normal)
    domain_hints = Map.get(context, :domain_hints, [])
    
    strategy = determine_context_strategy(execution_mode, domain_hints)
    apply_strategy(strategy, rules_with_metrics, context)
  end

  defp apply_learning_based_strategy(rules_with_metrics, context) do
    # Use machine learning model to determine optimal ordering
    ml_model = Map.get(context, :ml_model)
    
    case ml_model do
      nil ->
        # Fallback to performance-driven if no ML model
        apply_performance_driven_strategy(rules_with_metrics, context)
      
      model ->
        case predict_optimal_ordering(model, rules_with_metrics, context) do
          {:ok, ordering} -> {:ok, ordering}
          {:error, _reason} ->
            # Fallback on ML failure
            apply_performance_driven_strategy(rules_with_metrics, context)
        end
    end
  end

  ## Helper Functions

  defp calculate_rule_specificity(rule, context) do
    # Calculate rule specificity based on conditions
    conditions = Map.get(rule, :conditions, [])
    base_specificity = length(conditions)
    
    # Add bonus for constraint types
    constraint_bonus = Enum.reduce(conditions, 0, fn condition, acc ->
      acc + calculate_condition_specificity(condition)
    end)
    
    # Consider domain-specific specificity hints
    domain_bonus = calculate_domain_specificity_bonus(rule, context)
    
    base_specificity + constraint_bonus + domain_bonus
  end

  defp calculate_condition_specificity(condition) do
    # Calculate specificity of individual condition
    cond do
      String.contains?(condition, "=") -> 2.0  # Equality is most specific
      String.contains?(condition, ">") or String.contains?(condition, "<") -> 1.5  # Range conditions
      String.contains?(condition, "in") -> 1.0  # Set membership
      true -> 0.5  # General conditions
    end
  end

  defp calculate_domain_specificity_bonus(rule, context) do
    # Calculate domain-specific specificity bonus
    domain_weights = Map.get(context, :domain_specificity_weights, %{})
    rule_domain = Map.get(rule, :domain, :general)
    Map.get(domain_weights, rule_domain, 0.0)
  end

  defp calculate_rule_complexity(rule) do
    # Calculate rule complexity score
    conditions = Map.get(rule, :conditions, [])
    actions = Map.get(rule, :actions, [])
    
    condition_complexity = length(conditions) * 2
    action_complexity = length(actions) * 1.5
    
    # Add complexity for nested conditions or complex actions
    nested_complexity = calculate_nested_complexity(conditions ++ actions)
    
    condition_complexity + action_complexity + nested_complexity
  end

  defp calculate_nested_complexity(elements) do
    # Calculate complexity from nested elements
    Enum.reduce(elements, 0, fn element, acc ->
      cond do
        String.contains?(element, "and") or String.contains?(element, "or") -> acc + 1
        String.contains?(element, "(") -> acc + 0.5
        true -> acc
      end
    end)
  end

  defp calculate_cost_benefit_ratio(rule, context) do
    # Calculate cost-benefit ratio for rule
    metrics = Map.get(rule, :metrics, %{})
    
    # Cost components
    execution_time = Map.get(metrics, :avg_execution_time, 50)
    resource_usage = Map.get(metrics, :resource_usage, 25)
    complexity = calculate_rule_complexity(rule)
    
    total_cost = execution_time * 0.4 + resource_usage * 0.3 + complexity * 0.3
    
    # Benefit components
    success_rate = Map.get(metrics, :success_rate, 0.8)
    selectivity = Map.get(metrics, :selectivity, 0.5)
    business_value = Map.get(rule, :business_value, 1.0)
    
    total_benefit = success_rate * 0.4 + selectivity * 0.3 + business_value * 0.3
    
    if total_cost > 0 do
      total_benefit / total_cost
    else
      total_benefit
    end
  end

  defp build_dependency_graph(rules_with_metrics, context) do
    # Build dependency graph and return topological ordering
    dependencies = extract_rule_dependencies(rules_with_metrics, context)
    
    case topological_sort(rules_with_metrics, dependencies) do
      {:ok, ordered_rules} -> {:ok, Enum.map(ordered_rules, & &1.id)}
      error -> error
    end
  end

  defp extract_rule_dependencies(rules_with_metrics, _context) do
    # Extract dependencies between rules
    # Simplified implementation - in reality would analyze rule conditions and actions
    rule_pairs = for r1 <- rules_with_metrics, r2 <- rules_with_metrics, r1.id != r2.id, do: {r1, r2}
    
    Enum.reduce(rule_pairs, [], fn {rule1, rule2}, acc ->
      if has_dependency?(rule1, rule2) do
        [{rule1.id, rule2.id} | acc]
      else
        acc
      end
    end)
  end

  defp has_dependency?(rule1, rule2) do
    # Check if rule1 depends on rule2
    # Simplified check based on rule priorities
    Map.get(rule1, :priority, 999) > Map.get(rule2, :priority, 999)
  end

  defp topological_sort(rules, dependencies) do
    # Perform topological sort
    rule_ids = Enum.map(rules, & &1.id)
    
    # Build adjacency list
    adjacency = build_adjacency_list(rule_ids, dependencies)
    
    # Calculate in-degrees
    in_degrees = calculate_in_degrees(rule_ids, dependencies)
    
    # Kahn's algorithm
    kahn_sort(rules, adjacency, in_degrees)
  end

  defp build_adjacency_list(rule_ids, dependencies) do
    base_list = Map.new(rule_ids, fn rule_id -> {rule_id, []} end)
    
    Enum.reduce(dependencies, base_list, fn {from, to}, acc ->
      Map.update(acc, from, [to], fn existing -> [to | existing] end)
    end)
  end

  defp calculate_in_degrees(rule_ids, dependencies) do
    base_degrees = Map.new(rule_ids, fn rule_id -> {rule_id, 0} end)
    
    Enum.reduce(dependencies, base_degrees, fn {_from, to}, acc ->
      Map.update(acc, to, 1, &(&1 + 1))
    end)
  end

  defp kahn_sort(rules, adjacency, in_degrees) do
    # Kahn's topological sort algorithm
    zero_in_degree = Enum.filter(Map.keys(in_degrees), fn rule_id ->
      Map.get(in_degrees, rule_id) == 0
    end)
    
    kahn_sort_helper(rules, adjacency, in_degrees, zero_in_degree, [])
  end

  defp kahn_sort_helper(rules, _adjacency, in_degrees, [], result) do
    # Check for cycles
    remaining_nodes = Enum.filter(Map.keys(in_degrees), fn rule_id ->
      Map.get(in_degrees, rule_id) > 0
    end)
    
    if length(remaining_nodes) > 0 do
      {:error, :circular_dependency}
    else
      # Return rules in sorted order
      rule_map = Map.new(rules, fn rule -> {rule.id, rule} end)
      ordered_rules = Enum.map(Enum.reverse(result), fn rule_id ->
        Map.get(rule_map, rule_id)
      end)
      {:ok, ordered_rules}
    end
  end

  defp kahn_sort_helper(rules, adjacency, in_degrees, [current | rest], result) do
    # Add current to result
    new_result = [current | result]
    
    # Get neighbors of current node
    neighbors = Map.get(adjacency, current, [])
    
    # Reduce in-degree of neighbors
    new_in_degrees = Enum.reduce(neighbors, in_degrees, fn neighbor, acc ->
      Map.update(acc, neighbor, 0, &(max(0, &1 - 1)))
    end)
    
    # Find new zero in-degree nodes
    new_zero_nodes = Enum.filter(neighbors, fn neighbor ->
      Map.get(new_in_degrees, neighbor) == 0 and neighbor not in new_result and neighbor not in rest
    end)
    
    kahn_sort_helper(rules, adjacency, new_in_degrees, rest ++ new_zero_nodes, new_result)
  end

  defp calculate_resource_efficiency_score(rule, system_resources) do
    # Calculate how efficiently rule uses available resources
    metrics = Map.get(rule, :metrics, %{})
    
    rule_cpu_usage = Map.get(metrics, :cpu_usage, 10)
    rule_memory_usage = Map.get(metrics, :memory_usage, 10)
    
    available_cpu = Map.get(system_resources, :available_cpu, 100)
    available_memory = Map.get(system_resources, :available_memory, 100)
    
    cpu_efficiency = if available_cpu > 0, do: 1.0 - (rule_cpu_usage / available_cpu), else: 0.0
    memory_efficiency = if available_memory > 0, do: 1.0 - (rule_memory_usage / available_memory), else: 0.0
    
    (cpu_efficiency + memory_efficiency) / 2.0
  end

  defp calculate_weighted_performance_score(rule, weights) do
    # Calculate weighted performance score
    metrics = Map.get(rule, :metrics, %{})
    
    execution_score = normalize_metric(Map.get(metrics, :avg_execution_time, 50), 0, 200, :lower_better)
    success_score = Map.get(metrics, :success_rate, 0.8)
    selectivity_score = Map.get(metrics, :selectivity, 0.5)
    resource_score = normalize_metric(Map.get(metrics, :resource_usage, 25), 0, 100, :lower_better)
    
    weights.execution_time * execution_score +
    weights.success_rate * success_score +
    weights.selectivity * selectivity_score +
    weights.resource_usage * resource_score
  end

  defp normalize_metric(value, min_val, max_val, direction) do
    # Normalize metric to 0-1 scale
    normalized = (value - min_val) / (max_val - min_val)
    clamped = max(0.0, min(1.0, normalized))
    
    case direction do
      :lower_better -> 1.0 - clamped
      :higher_better -> clamped
    end
  end

  defp apply_execution_time_strategy(rules_with_metrics, order) do
    # Order by execution time
    ordered_rules = rules_with_metrics
    |> Enum.sort_by(fn rule ->
      get_in(rule, [:metrics, :avg_execution_time]) || 50
    end, if(order == :ascending, do: :asc, else: :desc))
    |> Enum.map(& &1.id)
    
    {:ok, ordered_rules}
  end

  defp apply_memory_usage_strategy(rules_with_metrics, order) do
    # Order by memory usage
    ordered_rules = rules_with_metrics
    |> Enum.sort_by(fn rule ->
      get_in(rule, [:metrics, :memory_usage]) || 25
    end, if(order == :ascending, do: :asc, else: :desc))
    |> Enum.map(& &1.id)
    
    {:ok, ordered_rules}
  end

  defp determine_context_strategy(execution_mode, domain_hints) do
    # Determine best strategy based on context
    cond do
      execution_mode == :batch -> :frequency_based
      execution_mode == :realtime -> :performance_driven
      execution_mode == :debug -> :complexity_based
      :financial in domain_hints -> :dependency_aware
      :ml in domain_hints -> :learning_based
      true -> :performance_driven
    end
  end

  defp predict_optimal_ordering(ml_model, rules_with_metrics, context) do
    # Use ML model to predict optimal ordering
    # Placeholder implementation
    features = extract_features_for_ml(rules_with_metrics, context)
    
    case apply_ml_model(ml_model, features) do
      {:ok, predictions} ->
        ordered_rules = Enum.zip(rules_with_metrics, predictions)
        |> Enum.sort_by(fn {_rule, prediction} -> prediction end, :desc)
        |> Enum.map(fn {rule, _prediction} -> rule.id end)
        
        {:ok, ordered_rules}
      
      error ->
        error
    end
  end

  defp extract_features_for_ml(rules_with_metrics, context) do
    # Extract features for ML model
    Enum.map(rules_with_metrics, fn rule ->
      metrics = Map.get(rule, :metrics, %{})
      
      [
        Map.get(metrics, :avg_execution_time, 50) / 100.0,
        Map.get(metrics, :selectivity, 0.5),
        Map.get(metrics, :success_rate, 0.8),
        Map.get(metrics, :resource_usage, 25) / 100.0,
        Map.get(metrics, :firing_frequency, 100) / 1000.0,
        calculate_rule_complexity(rule) / 10.0,
        Map.get(rule, :priority, 5) / 10.0
      ]
    end)
  end

  defp apply_ml_model(ml_model, features) do
    # Apply ML model to features
    # Placeholder implementation - would use actual ML library
    predictions = Enum.map(features, fn feature_vector ->
      # Simple linear combination as placeholder
      Enum.sum(feature_vector) / length(feature_vector)
    end)
    
    {:ok, predictions}
  end
end
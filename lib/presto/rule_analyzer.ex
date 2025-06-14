defmodule Presto.RuleAnalyzer do
  @moduledoc """
  Rule complexity analysis and optimization classification for the RETE rule engine.

  Analyzes rules to determine optimal execution strategies:
  - Simple rules: Direct pattern matching without RETE overhead
  - Complex rules: Full RETE network processing
  - Shared patterns: Opportunities for alpha node sharing
  """

  alias Presto.Utils

  @type rule_complexity :: :simple | :moderate | :complex
  @type execution_strategy :: :fast_path | :rete_network | :hybrid
  @type rule_analysis :: %{
          complexity: rule_complexity(),
          strategy: execution_strategy(),
          pattern_count: non_neg_integer(),
          test_count: non_neg_integer(),
          join_count: non_neg_integer(),
          shareable_patterns: [tuple()],
          estimated_selectivity: float(),
          optimization_opportunities: [atom()]
        }

  @doc """
  Analyzes a rule and determines its complexity and optimal execution strategy.
  """
  @spec analyze_rule(map()) :: rule_analysis()
  def analyze_rule(rule) do
    conditions = Map.get(rule, :conditions, [])

    # Separate patterns from test conditions
    {patterns, tests} = separate_conditions(conditions)

    # Count different types of conditions
    pattern_count = length(patterns)
    test_count = length(tests)
    join_count = calculate_join_count(patterns)

    # Analyze patterns for sharing opportunities
    shareable_patterns = identify_shareable_patterns(patterns)

    # Estimate selectivity (how much the rule filters data)
    estimated_selectivity = estimate_rule_selectivity(patterns, tests)

    # Determine complexity based on metrics
    complexity = determine_complexity(pattern_count, test_count, join_count)

    # Choose execution strategy
    strategy = choose_execution_strategy(complexity, pattern_count, join_count)

    # Identify optimization opportunities
    opportunities = identify_optimization_opportunities(patterns, tests, complexity)

    %{
      complexity: complexity,
      strategy: strategy,
      pattern_count: pattern_count,
      test_count: test_count,
      join_count: join_count,
      shareable_patterns: shareable_patterns,
      estimated_selectivity: estimated_selectivity,
      optimization_opportunities: opportunities
    }
  end

  @doc """
  Analyzes multiple rules to find common patterns for alpha node sharing.
  """
  @spec analyze_rule_set([map()]) :: %{
          shared_patterns: %{tuple() => [atom()]},
          sharing_opportunities: non_neg_integer(),
          total_nodes_without_sharing: non_neg_integer(),
          total_nodes_with_sharing: non_neg_integer()
        }
  def analyze_rule_set(rules) when is_list(rules) do
    # Extract all patterns from all rules
    all_patterns = extract_all_patterns(rules)

    # Find patterns used by multiple rules
    shared_patterns = find_shared_patterns(all_patterns)

    # Calculate sharing benefits
    total_without_sharing = length(all_patterns)
    total_with_sharing = total_without_sharing - calculate_sharing_savings(shared_patterns)
    sharing_opportunities = map_size(shared_patterns)

    %{
      shared_patterns: shared_patterns,
      sharing_opportunities: sharing_opportunities,
      total_nodes_without_sharing: total_without_sharing,
      total_nodes_with_sharing: total_with_sharing
    }
  end

  def analyze_rule_set(_),
    do: %{
      shared_patterns: %{},
      sharing_opportunities: 0,
      total_nodes_without_sharing: 0,
      total_nodes_with_sharing: 0
    }

  @doc """
  Determines if a rule can use fast-path execution (bypassing RETE network).
  """
  @spec can_use_fast_path?(map()) :: boolean()
  def can_use_fast_path?(rule) do
    analysis = analyze_rule(rule)
    analysis.strategy == :fast_path
  end

  # Private functions

  defp separate_conditions(conditions) do
    Enum.split_with(conditions, &fact_pattern?/1)
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

  defp calculate_join_count(patterns) when length(patterns) <= 1, do: 0

  defp calculate_join_count(patterns) do
    # Count potential joins based on shared variables
    patterns
    |> Enum.map(&extract_variables_from_pattern/1)
    |> count_variable_intersections()
  end

  defp extract_variables_from_pattern(pattern) do
    pattern
    |> Tuple.to_list()
    # Skip fact type
    |> Enum.drop(1)
    |> Enum.filter(&Utils.variable?/1)
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

  defp identify_shareable_patterns(patterns) do
    # Patterns that use common fact types or structures
    patterns
    |> Enum.group_by(&extract_pattern_signature/1)
    |> Enum.filter(fn {_signature, pattern_list} -> length(pattern_list) > 1 end)
    |> Enum.flat_map(fn {_signature, pattern_list} -> pattern_list end)
    |> Enum.uniq()
  end

  defp extract_pattern_signature(pattern) do
    case pattern do
      {fact_type, field1} ->
        {fact_type, normalize_field(field1)}

      {fact_type, field1, field2} ->
        {fact_type, normalize_field(field1), normalize_field(field2)}

      {fact_type, field1, field2, field3} ->
        {fact_type, normalize_field(field1), normalize_field(field2), normalize_field(field3)}

      _ ->
        pattern
    end
  end

  defp normalize_field(field) do
    # Normalize variables to detect similar patterns
    if Utils.variable?(field), do: :variable, else: field
  end

  defp estimate_rule_selectivity(patterns, tests) do
    # Simple heuristic: more tests = higher selectivity (filters more)
    # Assume 10% base selectivity
    base_selectivity = 0.1
    # Each test reduces selectivity
    test_factor = length(tests) * 0.1
    # More patterns = slightly more selective
    pattern_factor = length(patterns) * 0.05

    selectivity = base_selectivity + test_factor + pattern_factor
    # Cap at 90% selectivity
    min(selectivity, 0.9)
  end

  defp determine_complexity(pattern_count, test_count, join_count) do
    total_complexity = pattern_count + test_count + join_count * 2

    cond do
      total_complexity <= 2 and join_count == 0 -> :simple
      total_complexity <= 5 and join_count <= 1 -> :moderate
      true -> :complex
    end
  end

  defp choose_execution_strategy(:simple, 1, 0) do
    # Single pattern, no joins, no complex tests - use fast path
    :fast_path
  end

  defp choose_execution_strategy(:moderate, pattern_count, join_count)
       when pattern_count <= 2 and join_count <= 1 do
    # Could benefit from hybrid approach
    :hybrid
  end

  defp choose_execution_strategy(_complexity, _pattern_count, _join_count) do
    # Use full RETE network for complex rules
    :rete_network
  end

  defp identify_optimization_opportunities(patterns, tests, complexity) do
    opportunities = []

    opportunities =
      if complexity == :simple do
        [:fast_path_eligible | opportunities]
      else
        opportunities
      end

    opportunities =
      if length(patterns) > 1 and has_common_variables?(patterns) do
        [:join_optimization | opportunities]
      else
        opportunities
      end

    opportunities =
      if length(tests) > 2 do
        [:test_compilation | opportunities]
      else
        opportunities
      end

    opportunities =
      if has_repeated_patterns?(patterns) do
        [:pattern_sharing | opportunities]
      else
        opportunities
      end

    opportunities
  end

  defp has_common_variables?(patterns) do
    variable_sets = Enum.map(patterns, &extract_variables_from_pattern/1)

    case variable_sets do
      [vars1, vars2 | _] ->
        not Enum.empty?(Enum.filter(vars1, &(&1 in vars2)))

      _ ->
        false
    end
  end

  defp has_repeated_patterns?(patterns) do
    signatures = Enum.map(patterns, &extract_pattern_signature/1)
    length(signatures) != length(Enum.uniq(signatures))
  end

  defp extract_all_patterns(rules) do
    rules
    |> Enum.flat_map(fn rule ->
      {patterns, _tests} = separate_conditions(Map.get(rule, :conditions, []))
      Enum.map(patterns, fn pattern -> {Map.get(rule, :id), pattern} end)
    end)
  end

  defp find_shared_patterns(all_patterns) do
    all_patterns
    |> Enum.group_by(fn {_rule_id, pattern} -> extract_pattern_signature(pattern) end)
    |> Enum.filter(fn {_signature, pattern_list} -> length(pattern_list) > 1 end)
    |> Enum.into(%{}, fn {signature, pattern_list} ->
      rule_ids = Enum.map(pattern_list, fn {rule_id, _pattern} -> rule_id end)
      {signature, Enum.uniq(rule_ids)}
    end)
  end

  defp calculate_sharing_savings(shared_patterns) do
    shared_patterns
    |> Enum.map(fn {_pattern, rule_ids} -> length(rule_ids) - 1 end)
    |> Enum.sum()
  end
end

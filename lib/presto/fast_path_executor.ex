defmodule Presto.FastPathExecutor do
  @moduledoc """
  Fast-path execution for simple rules that don't require full RETE network processing.

  This module provides optimized execution for rules with:
  - Single fact patterns
  - Simple test conditions
  - No complex joins

  By bypassing the RETE network for these simple cases, we can achieve
  significant performance improvements for common rule patterns.
  """

  alias Presto.RuleEngine
  alias Presto.Utils

  @type execution_result :: {:ok, [tuple()]} | {:error, term()}
  @type compiled_rule :: %{
          id: atom(),
          pattern: tuple(),
          test_function: function(),
          action: function(),
          fact_type: atom()
        }

  @doc """
  Executes a simple rule using fast-path optimization.

  This bypasses the full RETE network and directly matches facts against
  the rule pattern, then applies test conditions and executes the action.
  """
  @spec execute_fast_path(map(), GenServer.server()) :: execution_result()
  def execute_fast_path(rule, rule_engine) do
    # Compile the rule for fast execution
    compiled_rule = compile_rule_for_fast_path(rule)

    # Get all facts from rule engine (consolidated working memory)
    all_facts = RuleEngine.get_facts(rule_engine)

    # Filter facts by type for efficiency
    relevant_facts = filter_facts_by_type(all_facts, compiled_rule.fact_type)

    # Match facts against pattern and apply tests
    matching_facts = find_matching_facts(relevant_facts, compiled_rule)

    # Execute action for each matching fact
    results = execute_action_for_matches(matching_facts, compiled_rule)

    {:ok, List.flatten(results)}
  rescue
    error ->
      {:error, {:fast_path_execution_failed, error}}
  end

  @doc """
  Compiles a rule for fast-path execution by creating optimized pattern matching
  and test functions.
  """
  @spec compile_rule_for_fast_path(map()) :: compiled_rule()
  def compile_rule_for_fast_path(rule) do
    conditions = Map.get(rule, :conditions, [])
    {patterns, tests} = separate_conditions(conditions)

    # Fast path assumes single pattern
    pattern = hd(patterns)
    fact_type = elem(pattern, 0)

    # Compile test conditions into a single function
    test_function = compile_tests_for_fast_path(tests)

    %{
      id: Map.get(rule, :id),
      pattern: pattern,
      test_function: test_function,
      action: Map.get(rule, :action),
      fact_type: fact_type
    }
  end

  @doc """
  Determines if multiple rules can be batched together for fast-path execution.
  """
  @spec can_batch_rules?([map()]) :: boolean()
  def can_batch_rules?(rules) do
    # Rules can be batched if they all use the same fact type and are fast-path eligible
    fact_types =
      rules
      |> Enum.map(&extract_fact_type_from_rule/1)
      |> Enum.uniq()

    length(fact_types) == 1
  end

  @doc """
  Executes multiple fast-path rules as a batch for better performance.
  """
  @spec execute_batch_fast_path([map()], GenServer.server()) :: execution_result()
  def execute_batch_fast_path(rules, rule_engine) do
    # Compile all rules
    compiled_rules = Enum.map(rules, &compile_rule_for_fast_path/1)

    # Get the common fact type
    fact_type = hd(compiled_rules).fact_type

    # Get relevant facts once
    all_facts = RuleEngine.get_facts(rule_engine)
    relevant_facts = filter_facts_by_type(all_facts, fact_type)

    # Execute all rules against the same fact set
    all_results =
      compiled_rules
      |> Enum.flat_map(fn compiled_rule ->
        matching_facts = find_matching_facts(relevant_facts, compiled_rule)
        execute_action_for_matches(matching_facts, compiled_rule)
      end)

    {:ok, List.flatten(all_results)}
  rescue
    error ->
      {:error, {:batch_fast_path_execution_failed, error}}
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

  defp compile_tests_for_fast_path([]) do
    # No tests, always passes
    fn _bindings -> true end
  end

  defp compile_tests_for_fast_path(tests) do
    # Compile all tests into a single function for efficiency
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

  defp filter_facts_by_type(facts, fact_type) do
    Enum.filter(facts, fn fact ->
      elem(fact, 0) == fact_type
    end)
  end

  defp find_matching_facts(facts, compiled_rule) do
    facts
    |> Enum.filter(&fact_matches_pattern?(&1, compiled_rule.pattern))
    |> Enum.map(&extract_bindings_from_fact(&1, compiled_rule.pattern))
    |> Enum.filter(compiled_rule.test_function)
  end

  defp fact_matches_pattern?(fact, pattern) when tuple_size(fact) != tuple_size(pattern) do
    false
  end

  defp fact_matches_pattern?(fact, pattern) do
    fact_size = tuple_size(fact)
    match_elements?(fact, pattern, fact_size, 0)
  end

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

  defp execute_action_for_matches(matching_facts, compiled_rule) do
    Enum.map(matching_facts, fn fact_bindings ->
      try do
        compiled_rule.action.(fact_bindings)
      rescue
        _error ->
          []
      end
    end)
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

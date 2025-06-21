defmodule Presto.RuleEngine.PatternMatcher do
  @moduledoc """
  Pattern matching logic for the Presto Rule Engine.

  This module has a single responsibility: evaluating whether facts match
  patterns and test conditions. It handles all pattern matching logic
  including variable binding, test condition evaluation, and operators.

  Single Responsibility: Pattern matching and test evaluation
  """

  alias Presto.PatternMatching

  @doc """
  Checks if a fact matches an alpha node's pattern and conditions.
  """
  @spec matches?(map(), tuple()) :: boolean()
  def matches?(alpha_node, fact) do
    case Map.get(alpha_node, :test_function) do
      nil -> pattern_match_with_tests(alpha_node, fact)
      test_fn -> test_fn.(fact)
    end
  end

  @doc """
  Extracts variable bindings from a fact given a pattern.
  """
  @spec extract_bindings(tuple(), tuple()) :: map()
  def extract_bindings(pattern, fact) do
    pattern_list = Tuple.to_list(pattern)
    fact_list = Tuple.to_list(fact)

    Enum.zip(pattern_list, fact_list)
    |> Enum.with_index()
    |> Enum.reduce(%{}, fn {{pattern_elem, fact_elem}, _index}, bindings ->
      if PatternMatching.variable?(pattern_elem) do
        Map.put(bindings, pattern_elem, fact_elem)
      else
        bindings
      end
    end)
  end

  @doc """
  Performs basic pattern matching with variable binding extraction.
  """
  @spec basic_pattern_match_with_bindings(tuple(), tuple()) :: {:ok, map()} | false
  def basic_pattern_match_with_bindings(pattern, fact)
      when tuple_size(pattern) != tuple_size(fact) do
    false
  end

  def basic_pattern_match_with_bindings(pattern, fact) do
    pattern_list = Tuple.to_list(pattern)
    fact_list = Tuple.to_list(fact)

    Enum.zip(pattern_list, fact_list)
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, %{}}, &match_pattern_element/2)
    |> case do
      {:ok, final_bindings} -> {:ok, final_bindings}
      false -> false
    end
  end

  @doc """
  Evaluates test conditions with variable bindings.
  """
  @spec evaluate_test_conditions([tuple()], map()) :: boolean()
  def evaluate_test_conditions(conditions, bindings) do
    test_conditions = extract_test_conditions_from_alpha_conditions(conditions)

    Enum.all?(test_conditions, fn test_condition ->
      evaluate_single_test_condition(test_condition, bindings)
    end)
  end

  # Private functions

  defp pattern_match_with_tests(alpha_node, fact) do
    case basic_pattern_match_with_bindings(alpha_node.pattern, fact) do
      {:ok, bindings} ->
        evaluate_test_conditions(alpha_node.conditions, bindings)

      false ->
        false
    end
  end

  defp match_pattern_element({{pattern_elem, fact_elem}, 0}, {:ok, acc_bindings}) do
    # First element (fact type) must match exactly
    if pattern_elem == fact_elem do
      {:cont, {:ok, acc_bindings}}
    else
      {:halt, false}
    end
  end

  defp match_pattern_element({{pattern_elem, fact_elem}, _index}, {:ok, acc_bindings}) do
    match_non_type_element(pattern_elem, fact_elem, acc_bindings)
  end

  defp match_non_type_element(:_, _fact_elem, acc_bindings) do
    {:cont, {:ok, acc_bindings}}
  end

  defp match_non_type_element(pattern_elem, fact_elem, acc_bindings)
       when pattern_elem == fact_elem do
    {:cont, {:ok, acc_bindings}}
  end

  defp match_non_type_element(pattern_elem, fact_elem, acc_bindings) do
    if PatternMatching.variable?(pattern_elem) do
      new_bindings = Map.put(acc_bindings, pattern_elem, fact_elem)
      {:cont, {:ok, new_bindings}}
    else
      {:halt, false}
    end
  end

  defp extract_test_conditions_from_alpha_conditions(conditions) do
    Enum.flat_map(conditions, &extract_tests_from_condition/1)
  end

  defp extract_tests_from_condition({_pattern, tests}) when is_list(tests) do
    tests
  end

  defp extract_tests_from_condition(condition_tuple) when is_tuple(condition_tuple) do
    extract_tests_from_tuple(condition_tuple)
  end

  defp extract_tests_from_condition(_condition) do
    []
  end

  defp extract_tests_from_tuple(condition_tuple) do
    case Tuple.to_list(condition_tuple) do
      list when length(list) > 2 ->
        extract_tests_from_last_element(List.last(list))

      _ ->
        []
    end
  end

  defp extract_tests_from_last_element(last_elem) when is_list(last_elem) do
    last_elem
  end

  defp extract_tests_from_last_element(_last_elem) do
    []
  end

  defp evaluate_single_test_condition({variable, operator, value}, bindings) do
    case Map.get(bindings, variable) do
      nil -> false
      bound_value -> evaluate_operator(operator, bound_value, value)
    end
  end

  defp evaluate_single_test_condition(_, _), do: false

  defp evaluate_operator(:>, bound_value, value), do: bound_value > value
  defp evaluate_operator(:<, bound_value, value), do: bound_value < value
  defp evaluate_operator(:>=, bound_value, value), do: bound_value >= value
  defp evaluate_operator(:<=, bound_value, value), do: bound_value <= value
  defp evaluate_operator(:==, bound_value, value), do: bound_value == value
  defp evaluate_operator(:!=, bound_value, value), do: bound_value != value
  defp evaluate_operator(_, _bound_value, _value), do: false
end

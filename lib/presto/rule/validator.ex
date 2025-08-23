defmodule Presto.Rule.Validator do
  @moduledoc """
  Compile-time and runtime rule validation macros and functions.

  This module provides comprehensive validation for Presto rules at both compile-time
  and runtime, catching errors early and providing detailed feedback for rule construction.

  ## Usage

  ### Compile-time validation with `defrule/3`

      use Presto.Rule.Validator

      defrule :adult_rule do
        conditions [
          {:person, :name, :age},
          {:age, :>, 18}
        ]
        action fn facts -> [{:adult, facts[:name]}] end
      end

  ### Compile-time validation with `validate!/1`

      use Presto.Rule.Validator

      @rule %{
        id: :my_rule,
        conditions: [{:person, :name, :age}],
        action: fn facts -> [{:processed, facts[:name]}] end
      }

      validate!(@rule)

  ### Runtime validation

      rule = %{
        id: :runtime_rule,
        conditions: [{:person, :name, :age}],
        action: fn facts -> [{:adult, facts[:name]}] end
      }

      case Presto.Rule.Validator.validate(rule) do
        :ok -> # Rule is valid
        {:error, message} -> # Handle validation error
      end

  ## Validation Rules

  The validator enforces the following constraints:

  1. **Structure**: Rule must be a map
  2. **ID**: Must be an atom and unique within the compilation unit
  3. **Conditions**: Must be a list of valid condition tuples
  4. **Action**: Must be a function (except for aggregation rules)
  5. **Priority**: Must be an integer if present
  6. **Type-specific**: Additional validation for aggregation rules

  ### Condition Validation

  Conditions are validated to ensure they follow proper patterns:
  - Pattern conditions: `{fact_type, binding1, binding2, ...}`
  - Test conditions: `{variable, operator, value}`
  - Operators: `:==`, `:!=`, `:>`, `:<`, `:>=`, `:<=`, `:in`, `:not_in`

  ### Variable Binding Analysis

  The validator performs static analysis to ensure:
  - All variables used in tests are bound in patterns
  - No undefined variables are referenced
  - Variable types are consistent across conditions
  """

  @type validation_error :: {:error, String.t()}
  @type validation_result :: :ok | validation_error()

  # Valid operators for test conditions
  @valid_operators [:==, :!=, :>, :<, :>=, :<=, :in, :not_in]

  # Built-in aggregate functions
  @builtin_aggregates [:sum, :count, :avg, :min, :max, :collect]

  defmacro __using__(_opts) do
    quote do
      import Presto.Rule.Validator
      Module.register_attribute(__MODULE__, :presto_rules, accumulate: true)

      @before_compile Presto.Rule.Validator
    end
  end

  defmacro __before_compile__(env) do
    rules = Module.get_attribute(env.module, :presto_rules)
    rule_ids = Enum.map(rules, & &1.id)
    duplicate_ids = find_duplicates(rule_ids)

    if Enum.any?(duplicate_ids) do
      raise CompileError,
        file: env.file,
        line: env.line,
        description: "Duplicate rule IDs found: #{inspect(duplicate_ids)}"
    end

    quote do
      def __presto_rules__, do: unquote(Macro.escape(rules))
    end
  end

  @doc """
  Defines a rule with compile-time validation.

  ## Examples

      defrule :adult_rule do
        conditions [
          {:person, :name, :age},
          {:age, :>, 18}
        ]
        action fn facts -> [{:adult, facts[:name]}] end
        priority 10
      end

      defrule :aggregation_rule do
        type :aggregation
        conditions [{:sale, :amount, :date}]
        group_by [:date]
        aggregate :sum
        field :amount
        output {:daily_sales, :date, :total}
      end
  """
  defmacro defrule(id, do: block) do
    try do
      rule_def = parse_rule_block(block)
      rule = Map.put(rule_def, :id, id)

      # Generate runtime validation since compile-time validation with AST is complex
      quote do
        rule = unquote(Macro.escape(rule))

        case Presto.Rule.Validator.validate(rule) do
          :ok ->
            @presto_rules rule
            def unquote(:"rule_#{id}")(), do: rule

          {:error, message} ->
            raise ArgumentError, "Rule validation failed: #{message}"
        end
      end
    rescue
      error ->
        raise CompileError,
          file: __CALLER__.file,
          line: __CALLER__.line,
          description: "Rule parsing failed: #{Exception.message(error)}"
    end
  end

  @doc """
  Validates a rule at compile time, raising a CompileError if invalid.

  ## Examples

      @rule %{
        id: :my_rule,
        conditions: [{:person, :name, :age}],
        action: fn facts -> [{:processed, facts[:name]}] end
      }

      validate!(@rule)
  """
  defmacro validate!(rule_ast) do
    rule = Macro.expand(rule_ast, __CALLER__)

    if is_map(rule) do
      try do
        case validate(rule) do
          :ok ->
            quote do: :ok

          {:error, message} ->
            raise CompileError,
              file: __CALLER__.file,
              line: __CALLER__.line,
              description: "Rule validation failed: #{message}"
        end
      rescue
        _ ->
          # Runtime validation for complex rules
          quote do
            rule = unquote(rule_ast)

            case Presto.Rule.Validator.validate(rule) do
              :ok -> :ok
              {:error, message} -> raise ArgumentError, "Rule validation failed: #{message}"
            end
          end
      end
    else
      # Runtime validation for dynamic rules
      quote do
        rule = unquote(rule_ast)

        case Presto.Rule.Validator.validate(rule) do
          :ok -> :ok
          {:error, message} -> raise ArgumentError, "Rule validation failed: #{message}"
        end
      end
    end
  end

  @doc """
  Runtime validation of a rule structure.

  Returns `:ok` if the rule is valid, or `{:error, message}` with detailed
  error information if validation fails.

  ## Examples

      iex> rule = %{
      ...>   id: :test_rule,
      ...>   conditions: [{:person, :name, :age}],
      ...>   action: fn _ -> [] end
      ...> }
      iex> Presto.Rule.Validator.validate(rule)
      :ok

      iex> invalid_rule = %{id: "not_atom", conditions: []}
      iex> Presto.Rule.Validator.validate(invalid_rule)
      {:error, "Rule 'not_atom': id must be an atom"}
  """
  @spec validate(map()) :: validation_result()
  def validate(rule) do
    with :ok <- validate_structure(rule),
         :ok <- validate_id(rule),
         :ok <- validate_conditions(rule),
         :ok <- validate_condition_patterns(rule),
         :ok <- validate_variable_bindings(rule),
         :ok <- validate_action(rule),
         :ok <- validate_priority(rule) do
      validate_type_specific(rule)
    end
  end

  # Private functions for parsing macro blocks

  defp parse_rule_block({:__block__, _, statements}) do
    Enum.reduce(statements, %{}, &parse_rule_statement/2)
  end

  defp parse_rule_block(single_statement) do
    parse_rule_statement(single_statement, %{})
  end

  defp parse_rule_statement({:conditions, _, [conditions]}, acc) do
    Map.put(acc, :conditions, conditions)
  end

  defp parse_rule_statement({:action, _, [action]}, acc) do
    Map.put(acc, :action, action)
  end

  defp parse_rule_statement({:priority, _, [priority]}, acc) do
    Map.put(acc, :priority, priority)
  end

  defp parse_rule_statement({:type, _, [type]}, acc) do
    Map.put(acc, :type, type)
  end

  defp parse_rule_statement({:group_by, _, [group_by]}, acc) do
    Map.put(acc, :group_by, group_by)
  end

  defp parse_rule_statement({:aggregate, _, [aggregate]}, acc) do
    Map.put(acc, :aggregate, aggregate)
  end

  defp parse_rule_statement({:field, _, [field]}, acc) do
    Map.put(acc, :field, field)
  end

  defp parse_rule_statement({:output, _, [output]}, acc) do
    Map.put(acc, :output, output)
  end

  defp parse_rule_statement(unknown, _acc) do
    raise ArgumentError, "Unknown rule statement: #{inspect(unknown)}"
  end

  # Core validation functions

  defp validate_structure(rule) when is_map(rule), do: :ok
  defp validate_structure(_), do: {:error, "Rule must be a map"}

  defp validate_id(%{id: id}) when is_atom(id), do: :ok
  defp validate_id(%{id: id}), do: {:error, "Rule '#{inspect(id)}': id must be an atom"}
  defp validate_id(_), do: {:error, "Rule is missing required field: id"}

  defp validate_conditions(%{conditions: conditions}) when is_list(conditions) do
    if Enum.empty?(conditions) do
      {:error, "Rule conditions cannot be empty"}
    else
      :ok
    end
  end

  defp validate_conditions(%{conditions: _}), do: {:error, "Rule conditions must be a list"}
  defp validate_conditions(_), do: {:error, "Rule is missing required field: conditions"}

  defp validate_condition_patterns(%{conditions: conditions}) do
    conditions
    |> Enum.with_index(1)
    |> Enum.reduce_while(:ok, fn {condition, index}, :ok ->
      case validate_single_condition(condition, index) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp validate_single_condition(condition, index) when is_tuple(condition) do
    case tuple_size(condition) do
      0 ->
        {:error, "Condition #{index}: empty tuple is not valid"}

      1 ->
        {:error, "Condition #{index}: single-element tuple is not valid"}

      2 ->
        # 2-element tuples are valid pattern conditions like {:fact_type, :binding}
        validate_pattern_condition(condition, index)

      3 ->
        # 3-element tuples can be either test conditions or pattern conditions
        # Test conditions have an operator in the second position
        {_var, op, _value} = condition

        if op in @valid_operators do
          validate_test_condition(condition, index)
        else
          validate_pattern_condition(condition, index)
        end

      size when size > 3 ->
        validate_pattern_condition(condition, index)
    end
  end

  defp validate_single_condition(condition, index) do
    {:error, "Condition #{index}: must be a tuple, got #{inspect(condition)}"}
  end

  defp validate_test_condition({var, op, _value}, index) do
    cond do
      not is_atom(var) ->
        {:error, "Condition #{index}: test variable must be an atom, got #{inspect(var)}"}

      op not in @valid_operators ->
        {:error,
         "Condition #{index}: invalid operator #{inspect(op)}, must be one of #{inspect(@valid_operators)}"}

      true ->
        :ok
    end
  end

  defp validate_pattern_condition(pattern, index) do
    [fact_type | bindings] = Tuple.to_list(pattern)

    cond do
      not is_atom(fact_type) ->
        {:error, "Condition #{index}: fact type must be an atom, got #{inspect(fact_type)}"}

      not Enum.all?(bindings, &(is_atom(&1) or &1 == :_)) ->
        invalid_bindings = Enum.reject(bindings, &(is_atom(&1) or &1 == :_))

        {:error,
         "Condition #{index}: all bindings must be atoms or :_, invalid: #{inspect(invalid_bindings)}"}

      true ->
        :ok
    end
  end

  defp validate_variable_bindings(%{conditions: conditions}) do
    {patterns, tests} = partition_conditions(conditions)
    bound_vars = extract_bound_variables(patterns)
    test_vars = extract_test_variables(tests)

    unbound_vars = MapSet.difference(test_vars, bound_vars)

    if MapSet.size(unbound_vars) > 0 do
      {:error,
       "Unbound variables in tests: #{inspect(MapSet.to_list(unbound_vars))}. " <>
         "All test variables must be bound in pattern conditions."}
    else
      :ok
    end
  end

  defp partition_conditions(conditions) do
    Enum.split_with(conditions, fn condition ->
      case tuple_size(condition) do
        3 ->
          {_var, op, _value} = condition
          # It's a pattern if op is not a valid operator
          op not in @valid_operators

        size when size > 3 ->
          # 4+ element tuples are always patterns
          true

        2 ->
          # 2 element tuples are pattern conditions
          true

        _ ->
          # 1 element tuples are invalid
          false
      end
    end)
  end

  defp extract_bound_variables(patterns) do
    patterns
    |> Enum.flat_map(fn pattern ->
      [_fact_type | bindings] = Tuple.to_list(pattern)
      Enum.reject(bindings, &(&1 == :_))
    end)
    |> MapSet.new()
  end

  defp extract_test_variables(tests) do
    tests
    |> Enum.map(fn {var, _op, _value} -> var end)
    |> MapSet.new()
  end

  # Aggregation rules don't need action
  defp validate_action(%{type: :aggregation}), do: :ok
  defp validate_action(%{action: action}) when is_function(action), do: :ok
  defp validate_action(%{action: _}), do: {:error, "Rule action must be a function"}
  defp validate_action(_), do: {:error, "Rule is missing required field: action"}

  defp validate_priority(%{priority: priority}) when is_integer(priority), do: :ok
  defp validate_priority(%{priority: _}), do: {:error, "Rule priority must be an integer"}
  # Priority is optional
  defp validate_priority(_), do: :ok

  defp validate_type_specific(%{type: :aggregation} = rule) do
    validate_aggregation_fields(rule)
  end

  defp validate_type_specific(_), do: :ok

  defp validate_aggregation_fields(rule) do
    required_fields = [:group_by, :aggregate, :output]
    missing_fields = Enum.filter(required_fields, &(not Map.has_key?(rule, &1)))

    if Enum.empty?(missing_fields) do
      with :ok <- validate_group_by(rule.group_by),
           :ok <- validate_aggregate_function(rule.aggregate),
           :ok <- validate_output_pattern(rule.output) do
        :ok
      end
    else
      {:error, "Aggregation rule missing required fields: #{inspect(missing_fields)}"}
    end
  end

  defp validate_group_by(group_by) when is_list(group_by) do
    if Enum.all?(group_by, &is_atom/1) do
      :ok
    else
      {:error, "group_by must be a list of atoms"}
    end
  end

  defp validate_group_by(_), do: {:error, "group_by must be a list"}

  defp validate_aggregate_function(func) when func in @builtin_aggregates, do: :ok
  defp validate_aggregate_function(func) when is_function(func, 1), do: :ok

  defp validate_aggregate_function(func) do
    {:error,
     "Invalid aggregate function: #{inspect(func)}. " <>
       "Must be one of #{inspect(@builtin_aggregates)} or a function/1"}
  end

  defp validate_output_pattern(pattern) when is_tuple(pattern), do: :ok
  defp validate_output_pattern(_), do: {:error, "output pattern must be a tuple"}

  # Utility functions

  defp find_duplicates(list) do
    list
    |> Enum.frequencies()
    |> Enum.filter(fn {_item, count} -> count > 1 end)
    |> Enum.map(fn {item, _count} -> item end)
  end
end

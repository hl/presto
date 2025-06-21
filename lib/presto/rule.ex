defmodule Presto.Rule do
  @moduledoc """
  Rule construction helpers and validation for Presto.

  This module provides explicit Elixir functions to construct and validate rules,
  making it easier to create well-formed rules without verbose map structures.

  ## Example

      rule = Presto.Rule.new(
        :adult_rule,
        [
          {:person, :name, :age},
          {:age, :>, 18}
        ],
        fn facts -> [{:adult, facts[:name]}] end
      )
      
      # With options
      rule = Presto.Rule.new(
        :high_priority_rule,
        conditions,
        action_fn,
        priority: 100
      )
  """

  @type rule :: %{
          id: atom(),
          conditions: [condition()],
          action: function(),
          priority: integer()
        }

  @type condition :: tuple()
  @type pattern :: tuple()
  @type test :: {atom(), atom(), any()}

  @doc """
  Creates a new rule with the given id, conditions, and action function.

  ## Options

    * `:priority` - Rule execution priority (higher numbers execute first). Default: 0
    
  ## Examples

      rule = Presto.Rule.new(
        :adult_rule,
        [
          {:person, :name, :age},
          {:age, :>, 18}
        ],
        fn facts -> [{:adult, facts[:name]}] end
      )
  """
  @spec new(atom(), [condition()], function(), keyword()) :: rule()
  def new(id, conditions, action_fn, opts \\ []) do
    %{
      id: id,
      conditions: conditions,
      action: action_fn,
      priority: Keyword.get(opts, :priority, 0)
    }
  end

  @doc """
  Creates an aggregation rule for computing aggregates over groups of facts.

  This creates a special rule type that the RETE engine processes differently,
  computing aggregates incrementally as facts are added/removed.

  ## Options

    * `:priority` - Rule execution priority. Default: 0
    * `:output` - Output fact pattern. Default: `{:aggregate_result, {group_fields...}, value}`
    
  ## Examples

      # Sum hours by employee and week
      rule = Presto.Rule.aggregation(
        :weekly_hours,
        [{:shift_segment, :id, :data}],
        [:employee_id, :week],
        :sum,
        :hours
      )
      
      # Count shifts by department
      rule = Presto.Rule.aggregation(
        :department_shift_count,
        [{:shift, :id, :data}],
        [:department],
        :count,
        nil,
        output: {:dept_shifts, :department, :count}
      )
  """
  @spec aggregation(atom(), [pattern()], [atom()], atom(), atom() | nil, keyword()) :: rule()
  def aggregation(id, conditions, group_by, aggregate_fn, field, opts \\ []) do
    output_pattern = Keyword.get(opts, :output, default_output_pattern(group_by))

    %{
      id: id,
      type: :aggregation,
      conditions: conditions,
      group_by: group_by,
      aggregate: aggregate_fn,
      field: field,
      output: output_pattern,
      priority: Keyword.get(opts, :priority, 0)
    }
  end

  @doc """
  Creates a pattern condition that matches facts of a specific type.

  ## Examples

      # Match person facts with name and age bindings
      pattern(:person, [:name, :age])
      # => {:person, :name, :age}
      
      # Match any person fact
      pattern(:person, [:_, :_])
      # => {:person, :_, :_}
  """
  @spec pattern(atom(), [atom()]) :: pattern()
  def pattern(fact_type, bindings) do
    List.to_tuple([fact_type | bindings])
  end

  @doc """
  Creates a test condition that compares a bound variable to a value.

  ## Examples

      test(:age, :>, 18)
      # => {:age, :>, 18}
      
      test(:status, :==, :active)
      # => {:status, :==, :active}
  """
  @spec test(atom(), atom(), any()) :: test()
  def test(variable, operator, value) do
    {variable, operator, value}
  end

  @doc """
  Validates a rule structure and returns :ok or an error with details.

  ## Examples

      iex> Presto.Rule.validate(%{id: :test, conditions: [], action: fn _ -> [] end})
      :ok
      
      iex> Presto.Rule.validate(%{id: "not_atom", conditions: [], action: fn _ -> [] end})
      {:error, "Rule 'not_atom': id must be an atom"}
  """
  @spec validate(map()) :: :ok | {:error, String.t()}
  def validate(rule) do
    with :ok <- validate_structure(rule),
         :ok <- validate_id(rule),
         :ok <- validate_conditions(rule),
         :ok <- validate_action(rule),
         :ok <- validate_priority(rule),
         :ok <- validate_type_specific(rule) do
      :ok
    end
  end

  # Validation functions

  defp validate_structure(rule) when is_map(rule), do: :ok
  defp validate_structure(_), do: {:error, "Rule must be a map"}

  defp validate_id(%{id: id}) when is_atom(id), do: :ok
  defp validate_id(%{id: id}), do: {:error, "Rule '#{inspect(id)}': id must be an atom"}
  defp validate_id(_), do: {:error, "Rule is missing required field: id"}

  defp validate_conditions(%{conditions: conditions}) when is_list(conditions), do: :ok
  defp validate_conditions(%{conditions: _}), do: {:error, "Rule conditions must be a list"}
  defp validate_conditions(_), do: {:error, "Rule is missing required field: conditions"}

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
    with :ok <- validate_aggregation_fields(rule) do
      :ok
    end
  end

  defp validate_type_specific(_), do: :ok

  defp validate_aggregation_fields(rule) do
    required_fields = [:group_by, :aggregate, :output]

    missing_fields = Enum.filter(required_fields, &(not Map.has_key?(rule, &1)))

    if Enum.empty?(missing_fields) do
      validate_aggregate_function(rule.aggregate)
    else
      {:error, "Aggregation rule missing required fields: #{inspect(missing_fields)}"}
    end
  end

  defp validate_aggregate_function(func) when func in [:sum, :count, :avg, :min, :max, :collect],
    do: :ok

  defp validate_aggregate_function(func),
    do: {:error, "Unknown aggregate function: #{inspect(func)}"}

  defp default_output_pattern(group_by) do
    group_key = List.to_tuple(group_by)
    {:aggregate_result, group_key, :value}
  end
end

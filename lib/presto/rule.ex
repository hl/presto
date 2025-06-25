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

  The rule is validated at creation time to ensure it follows proper structure.

  ## Options

    * `:priority` - Rule execution priority (higher numbers execute first). Default: 0
    * `:validate` - Whether to validate the rule structure. Default: true
    
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
    rule = %{
      id: id,
      conditions: conditions,
      action: action_fn,
      priority: Keyword.get(opts, :priority, 0)
    }

    if Keyword.get(opts, :validate, true) do
      case Presto.Rule.Validator.validate(rule) do
        :ok -> rule
        {:error, message} -> raise ArgumentError, "Invalid rule: #{message}"
      end
    else
      rule
    end
  end

  @doc """
  Creates an aggregation rule for computing aggregates over groups of facts.

  This creates a special rule type that the RETE engine processes differently,
  computing aggregates incrementally as facts are added/removed.

  ## Options

    * `:priority` - Rule execution priority. Default: 0
    * `:output` - Output fact pattern. Default: `{:aggregate_result, {group_fields...}, value}`
    * `:window_size` - For windowed aggregations (time-based). Default: nil
    * `:incremental` - Enable incremental updates. Default: true
    
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

      # Custom aggregation function
      rule = Presto.Rule.aggregation(
        :custom_metric,
        conditions,
        [:group_field],
        fn values -> Enum.max(values) - Enum.min(values) end,
        :value_field
      )

      # Windowed aggregation (moving average)
      rule = Presto.Rule.aggregation(
        :moving_average,
        conditions,
        [:sensor_id],
        :avg,
        :reading,
        window_size: 100  # Last 100 readings
      )
  """
  def aggregation(id, conditions, group_by, aggregate_fn, field, opts \\ []) do
    output_pattern = Keyword.get(opts, :output, default_output_pattern(group_by))

    rule = %{
      id: id,
      type: :aggregation,
      conditions: conditions,
      group_by: group_by,
      aggregate: aggregate_fn,
      field: field,
      output: output_pattern,
      priority: Keyword.get(opts, :priority, 0),
      window_size: Keyword.get(opts, :window_size),
      incremental: Keyword.get(opts, :incremental, true)
    }

    if Keyword.get(opts, :validate, true) do
      case Presto.Rule.Validator.validate(rule) do
        :ok -> rule
        {:error, message} -> raise ArgumentError, "Invalid aggregation rule: #{message}"
      end
    else
      rule
    end
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

  Delegates to the comprehensive Presto.Rule.Validator module which provides
  enhanced validation including variable binding analysis and compile-time checks.

  ## Examples

      iex> Presto.Rule.validate(%{id: :test, conditions: [{:person, :name}], action: fn _ -> [] end})
      :ok
      
      iex> Presto.Rule.validate(%{id: "not_atom", conditions: []})
      {:error, "Rule 'not_atom': id must be an atom"}
  """
  @spec validate(map()) :: :ok | {:error, String.t()}
  def validate(rule) do
    Presto.Rule.Validator.validate(rule)
  end

  # Private helper functions

  defp default_output_pattern(group_by) do
    group_key = List.to_tuple(group_by)
    {:aggregate_result, group_key, :value}
  end
end

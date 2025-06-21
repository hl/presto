defmodule Presto.RuleEngine.ExecutionTracker do
  @moduledoc """
  Execution tracking for the Presto Rule Engine.

  This module has a single responsibility: tracking and managing rule execution
  order, execution history, and maintaining the sequence of rule firings.

  Single Responsibility: Rule execution order tracking and history management
  """

  require Logger
  alias Presto.RuleEngine.State
  alias Presto.Logger, as: PrestoLogger

  @doc """
  Records rule execution in the state.
  """
  @spec record_execution(State.t(), atom()) :: State.t()
  def record_execution(%State{} = state, rule_id) do
    PrestoLogger.log_rule_execution(:debug, rule_id, "recording_execution", %{
      rule_id: rule_id,
      current_order_length: length(state.last_execution_order)
    })

    new_execution_order = [rule_id | state.last_execution_order]
    %{state | last_execution_order: new_execution_order}
  end

  @doc """
  Gets the last execution order.
  """
  @spec get_last_execution_order(State.t()) :: [atom()]
  def get_last_execution_order(%State{} = state) do
    Enum.reverse(state.last_execution_order)
  end

  @doc """
  Clears the execution order history.
  """
  @spec clear_execution_order(State.t()) :: State.t()
  def clear_execution_order(%State{} = state) do
    PrestoLogger.log_rule_execution(:debug, :system, "clearing_execution_order", %{
      previous_length: length(state.last_execution_order)
    })

    %{state | last_execution_order: []}
  end

  @doc """
  Gets execution statistics.
  """
  @spec get_execution_stats(State.t()) :: map()
  def get_execution_stats(%State{} = state) do
    execution_order = state.last_execution_order
    total_executions = length(execution_order)

    rule_execution_counts =
      execution_order
      |> Enum.frequencies()

    most_executed_rule =
      if total_executions > 0 do
        rule_execution_counts
        |> Enum.max_by(fn {_rule, count} -> count end)
        |> elem(0)
      else
        nil
      end

    %{
      total_executions: total_executions,
      unique_rules_executed: map_size(rule_execution_counts),
      rule_execution_counts: rule_execution_counts,
      most_executed_rule: most_executed_rule,
      last_executed_rule: if(total_executions > 0, do: hd(execution_order), else: nil)
    }
  end

  @doc """
  Determines if a rule was executed in the last firing cycle.
  """
  @spec was_executed?(State.t(), atom()) :: boolean()
  def was_executed?(%State{} = state, rule_id) do
    rule_id in state.last_execution_order
  end

  @doc """
  Gets the execution position of a rule (1-based index).
  """
  @spec get_execution_position(State.t(), atom()) :: integer() | nil
  def get_execution_position(%State{} = state, rule_id) do
    state.last_execution_order
    |> Enum.reverse()
    |> Enum.find_index(&(&1 == rule_id))
    |> case do
      nil -> nil
      index -> index + 1
    end
  end

  @doc """
  Gets rules executed before a specific rule in the last cycle.
  """
  @spec get_rules_executed_before(State.t(), atom()) :: [atom()]
  def get_rules_executed_before(%State{} = state, rule_id) do
    execution_order = Enum.reverse(state.last_execution_order)

    case Enum.find_index(execution_order, &(&1 == rule_id)) do
      nil -> []
      index -> Enum.take(execution_order, index)
    end
  end

  @doc """
  Gets rules executed after a specific rule in the last cycle.
  """
  @spec get_rules_executed_after(State.t(), atom()) :: [atom()]
  def get_rules_executed_after(%State{} = state, rule_id) do
    execution_order = Enum.reverse(state.last_execution_order)

    case Enum.find_index(execution_order, &(&1 == rule_id)) do
      nil -> []
      index -> Enum.drop(execution_order, index + 1)
    end
  end

  @doc """
  Validates execution order consistency.
  """
  @spec validate_execution_consistency(State.t()) :: {:ok, :consistent} | {:error, [String.t()]}
  def validate_execution_consistency(%State{} = state) do
    issues = []
    execution_order = state.last_execution_order

    # Check that all executed rules exist in the rules map
    invalid_rules =
      execution_order
      |> Enum.uniq()
      |> Enum.reject(&Map.has_key?(state.rules, &1))

    issues =
      if Enum.empty?(invalid_rules) do
        issues
      else
        ["Executed rules that don't exist: #{inspect(invalid_rules)}" | issues]
      end

    if Enum.empty?(issues) do
      {:ok, :consistent}
    else
      {:error, Enum.reverse(issues)}
    end
  end
end

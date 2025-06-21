defmodule Presto.RuleEngine.RuleStorage do
  @moduledoc """
  Rule storage for the Presto Rule Engine.

  This module has a single responsibility: managing the basic CRUD operations
  for rules including adding, removing, retrieving, and querying rules.

  Single Responsibility: Rule storage and retrieval operations
  """

  require Logger
  alias Presto.Logger, as: PrestoLogger
  alias Presto.RuleEngine.State

  @doc """
  Adds a rule to the state.
  """
  @spec add_rule(State.t(), atom(), map()) :: State.t()
  def add_rule(%State{} = state, rule_id, rule) do
    PrestoLogger.log_rule_compilation(:info, rule_id, "adding_rule_to_storage", %{
      rule_type: Map.get(rule, :type, :standard),
      conditions_count: length(Map.get(rule, :conditions, []))
    })

    updated_rules = Map.put(state.rules, rule_id, rule)
    updated_statistics = Map.update!(state.engine_statistics, :total_rules, &(&1 + 1))

    new_state = %{state | rules: updated_rules, engine_statistics: updated_statistics}

    PrestoLogger.log_rule_compilation(:info, rule_id, "rule_added_to_storage", %{
      total_rules: updated_statistics.total_rules
    })

    new_state
  end

  @doc """
  Removes a rule from the state.
  """
  @spec remove_rule(State.t(), atom()) :: State.t()
  def remove_rule(%State{} = state, rule_id) do
    PrestoLogger.log_rule_compilation(:info, rule_id, "removing_rule_from_storage", %{})

    updated_rules = Map.delete(state.rules, rule_id)
    updated_engine_statistics = Map.update!(state.engine_statistics, :total_rules, &(&1 - 1))

    new_state = %{
      state
      | rules: updated_rules,
        engine_statistics: updated_engine_statistics
    }

    PrestoLogger.log_rule_compilation(:info, rule_id, "rule_removed_from_storage", %{
      total_rules: updated_engine_statistics.total_rules
    })

    new_state
  end

  @doc """
  Gets all rules from the state.
  """
  @spec get_rules(State.t()) :: %{atom() => map()}
  def get_rules(%State{} = state), do: state.rules

  @doc """
  Gets a specific rule by ID.
  """
  @spec get_rule(State.t(), atom()) :: map() | nil
  def get_rule(%State{} = state, rule_id) do
    Map.get(state.rules, rule_id)
  end

  @doc """
  Checks if a rule exists in the state.
  """
  @spec rule_exists?(State.t(), atom()) :: boolean()
  def rule_exists?(%State{} = state, rule_id) do
    Map.has_key?(state.rules, rule_id)
  end

  @doc """
  Gets the total number of rules in the state.
  """
  @spec get_rule_count(State.t()) :: non_neg_integer()
  def get_rule_count(%State{} = state) do
    map_size(state.rules)
  end

  @doc """
  Gets rules by type.
  """
  @spec get_rules_by_type(State.t(), atom()) :: %{atom() => map()}
  def get_rules_by_type(%State{} = state, rule_type) do
    state.rules
    |> Enum.filter(fn {_rule_id, rule} ->
      Map.get(rule, :type, :standard) == rule_type
    end)
    |> Map.new()
  end

  @doc """
  Gets all rule IDs.
  """
  @spec get_rule_ids(State.t()) :: [atom()]
  def get_rule_ids(%State{} = state) do
    Map.keys(state.rules)
  end

  @doc """
  Gets rules by priority range.
  """
  @spec get_rules_by_priority_range(State.t(), integer(), integer()) :: %{atom() => map()}
  def get_rules_by_priority_range(%State{} = state, min_priority, max_priority) do
    state.rules
    |> Enum.filter(fn {_rule_id, rule} ->
      priority = Map.get(rule, :priority, 0)
      priority >= min_priority and priority <= max_priority
    end)
    |> Map.new()
  end

  @doc """
  Gets basic rule storage statistics.
  """
  @spec get_storage_stats(State.t()) :: map()
  def get_storage_stats(%State{} = state) do
    rules = state.rules
    total_rules = map_size(rules)

    rule_types =
      rules
      |> Map.values()
      |> Enum.group_by(fn rule -> Map.get(rule, :type, :standard) end)
      |> Map.new(fn {type, rules_list} -> {type, length(rules_list)} end)

    %{
      total_rules: total_rules,
      rule_types: rule_types,
      avg_conditions_per_rule:
        if total_rules > 0 do
          total_conditions =
            rules
            |> Map.values()
            |> Enum.map(fn rule -> length(Map.get(rule, :conditions, [])) end)
            |> Enum.sum()

          total_conditions / total_rules
        else
          0
        end
    }
  end
end

defmodule Presto.RuleEngine.RuleManager do
  @moduledoc """
  Rule management for the Presto Rule Engine.

  This module handles all rule-related state management operations, including:
  - Adding and removing rules from state
  - Managing rule networks and analyses
  - Fast path rule management
  - Rule execution order tracking
  - Rule metadata and relationship management

  The module operates on the State struct and provides a clean interface
  for rule lifecycle management within the RETE engine.
  """

  require Logger
  alias Presto.RuleEngine.State
  alias Presto.Logger, as: PrestoLogger

  @doc """
  Adds a rule to the state.

  This function updates the rules map and increments the total rules counter
  in the engine statistics.
  """
  @spec add_rule(State.t(), atom(), map()) :: State.t()
  def add_rule(%State{} = state, rule_id, rule) do
    PrestoLogger.log_rule_compilation(:info, rule_id, "adding_rule_to_state", %{
      rule_type: Map.get(rule, :type, :standard),
      conditions_count: length(Map.get(rule, :conditions, []))
    })

    updated_rules = Map.put(state.rules, rule_id, rule)
    updated_statistics = Map.update!(state.engine_statistics, :total_rules, &(&1 + 1))

    new_state = %{state | rules: updated_rules, engine_statistics: updated_statistics}

    PrestoLogger.log_rule_compilation(:info, rule_id, "rule_added_to_state", %{
      total_rules: updated_statistics.total_rules
    })

    new_state
  end

  @doc """
  Removes a rule from the state.

  This function removes the rule from all related state maps and decrements
  the total rules counter.
  """
  @spec remove_rule(State.t(), atom()) :: State.t()
  def remove_rule(%State{} = state, rule_id) do
    PrestoLogger.log_rule_compilation(:info, rule_id, "removing_rule_from_state", %{})

    updated_rules = Map.delete(state.rules, rule_id)
    updated_networks = Map.delete(state.rule_networks, rule_id)
    updated_analyses = Map.delete(state.rule_analyses, rule_id)
    updated_rule_statistics = Map.delete(state.rule_statistics, rule_id)
    updated_engine_statistics = Map.update!(state.engine_statistics, :total_rules, &(&1 - 1))

    new_state = %{
      state
      | rules: updated_rules,
        rule_networks: updated_networks,
        rule_analyses: updated_analyses,
        rule_statistics: updated_rule_statistics,
        engine_statistics: updated_engine_statistics
    }

    PrestoLogger.log_rule_compilation(:info, rule_id, "rule_removed_from_state", %{
      total_rules: updated_engine_statistics.total_rules
    })

    new_state
  end

  @doc """
  Updates rule networks in the state.

  This function stores the network nodes created for a specific rule.
  """
  @spec update_rule_networks(State.t(), atom(), map()) :: State.t()
  def update_rule_networks(%State{} = state, rule_id, network_nodes) do
    PrestoLogger.log_rule_compilation(:debug, rule_id, "updating_rule_networks", %{
      alpha_nodes_count: length(Map.get(network_nodes, :alpha_nodes, [])),
      beta_nodes_count: length(Map.get(network_nodes, :beta_nodes, []))
    })

    updated_networks = Map.put(state.rule_networks, rule_id, network_nodes)
    %{state | rule_networks: updated_networks}
  end

  @doc """
  Updates rule analyses in the state.

  This function stores the analysis results for a specific rule.
  """
  @spec update_rule_analyses(State.t(), atom(), map()) :: State.t()
  def update_rule_analyses(%State{} = state, rule_id, analysis) do
    PrestoLogger.log_rule_compilation(:debug, rule_id, "updating_rule_analysis", %{
      complexity: Map.get(analysis, :complexity),
      strategy: Map.get(analysis, :strategy),
      pattern_count: Map.get(analysis, :pattern_count)
    })

    updated_analyses = Map.put(state.rule_analyses, rule_id, analysis)
    %{state | rule_analyses: updated_analyses}
  end

  @doc """
  Updates fast path rules in the state.

  This function manages the mapping of fact types to rules that can use
  the fast path execution strategy.
  """
  @spec update_fast_path_rules(State.t(), atom(), [atom()]) :: State.t()
  def update_fast_path_rules(%State{} = state, fact_type, rule_ids) do
    PrestoLogger.log_rule_compilation(:debug, fact_type, "updating_fast_path_rules", %{
      fact_type: fact_type,
      rule_ids: rule_ids,
      rule_count: length(rule_ids)
    })

    updated_fast_path = Map.put(state.fast_path_rules, fact_type, rule_ids)
    %{state | fast_path_rules: updated_fast_path}
  end

  @doc """
  Updates the last execution order.

  This function records the order in which rules were executed in the
  most recent rule firing.
  """
  @spec update_execution_order(State.t(), [atom()]) :: State.t()
  def update_execution_order(%State{} = state, execution_order) do
    %{state | last_execution_order: execution_order}
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
  Gets rule networks for a specific rule.
  """
  @spec get_rule_networks(State.t(), atom()) :: map() | nil
  def get_rule_networks(%State{} = state, rule_id) do
    Map.get(state.rule_networks, rule_id)
  end

  @doc """
  Gets rule analysis for a specific rule.
  """
  @spec get_rule_analysis(State.t(), atom()) :: map() | nil
  def get_rule_analysis(%State{} = state, rule_id) do
    Map.get(state.rule_analyses, rule_id)
  end

  @doc """
  Gets fast path rules for a specific fact type.
  """
  @spec get_fast_path_rules(State.t(), atom()) :: [atom()]
  def get_fast_path_rules(%State{} = state, fact_type) do
    Map.get(state.fast_path_rules, fact_type, [])
  end

  @doc """
  Gets the last execution order.
  """
  @spec get_last_execution_order(State.t()) :: [atom()]
  def get_last_execution_order(%State{} = state), do: state.last_execution_order

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
  Gets rules by complexity level.
  """
  @spec get_rules_by_complexity(State.t(), atom()) :: [atom()]
  def get_rules_by_complexity(%State{} = state, complexity) do
    state.rule_analyses
    |> Enum.filter(fn {_rule_id, analysis} ->
      Map.get(analysis, :complexity) == complexity
    end)
    |> Enum.map(fn {rule_id, _analysis} -> rule_id end)
  end

  @doc """
  Gets rules by execution strategy.
  """
  @spec get_rules_by_strategy(State.t(), atom()) :: [atom()]
  def get_rules_by_strategy(%State{} = state, strategy) do
    state.rule_analyses
    |> Enum.filter(fn {_rule_id, analysis} ->
      Map.get(analysis, :strategy) == strategy
    end)
    |> Enum.map(fn {rule_id, _analysis} -> rule_id end)
  end

  @doc """
  Gets rule management statistics.
  """
  @spec get_rule_management_stats(State.t()) :: map()
  def get_rule_management_stats(%State{} = state) do
    total_rules = map_size(state.rules)
    rules_with_networks = map_size(state.rule_networks)
    rules_with_analyses = map_size(state.rule_analyses)
    fast_path_fact_types = map_size(state.fast_path_rules)

    fast_path_rules_count =
      state.fast_path_rules
      |> Map.values()
      |> List.flatten()
      |> length()

    %{
      total_rules: total_rules,
      rules_with_networks: rules_with_networks,
      rules_with_analyses: rules_with_analyses,
      fast_path_fact_types: fast_path_fact_types,
      fast_path_rules_count: fast_path_rules_count,
      network_coverage: if(total_rules > 0, do: rules_with_networks / total_rules, else: 0),
      analysis_coverage: if(total_rules > 0, do: rules_with_analyses / total_rules, else: 0)
    }
  end

  @doc """
  Validates rule consistency in the state.

  This function checks for inconsistencies between the various rule-related
  state maps and returns any issues found.
  """
  @spec validate_rule_consistency(State.t()) :: {:ok, :consistent} | {:error, [String.t()]}
  def validate_rule_consistency(%State{} = state) do
    issues = []

    # Check that all rules in rule_networks exist in rules
    network_orphans =
      state.rule_networks
      |> Map.keys()
      |> Enum.reject(&Map.has_key?(state.rules, &1))

    issues =
      if Enum.empty?(network_orphans) do
        issues
      else
        ["Rules with networks but no rule definition: #{inspect(network_orphans)}" | issues]
      end

    # Check that all rules in rule_analyses exist in rules
    analysis_orphans =
      state.rule_analyses
      |> Map.keys()
      |> Enum.reject(&Map.has_key?(state.rules, &1))

    issues =
      if Enum.empty?(analysis_orphans) do
        issues
      else
        ["Rules with analyses but no rule definition: #{inspect(analysis_orphans)}" | issues]
      end

    # Check that engine statistics match actual rule count
    actual_count = map_size(state.rules)
    stats_count = state.engine_statistics.total_rules

    issues =
      if actual_count == stats_count do
        issues
      else
        [
          "Rule count mismatch: actual=#{actual_count}, stats=#{stats_count}"
          | issues
        ]
      end

    if Enum.empty?(issues) do
      {:ok, :consistent}
    else
      {:error, Enum.reverse(issues)}
    end
  end
end

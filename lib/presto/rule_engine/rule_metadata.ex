defmodule Presto.RuleEngine.RuleMetadata do
  @moduledoc """
  Rule metadata management for the Presto Rule Engine.

  This module has a single responsibility: managing rule metadata including
  networks, analyses, fast path optimization mappings, and rule relationships.

  Single Responsibility: Rule metadata and optimization data management
  """

  require Logger
  alias Presto.RuleEngine.State
  alias Presto.Logger, as: PrestoLogger

  @doc """
  Updates rule networks in the state.
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
  Updates fast path rules mapping.
  """
  @spec update_fast_path_rules(State.t(), atom(), [atom()]) :: State.t()
  def update_fast_path_rules(%State{} = state, fact_type, rule_ids) do
    fact_type_str = if is_atom(fact_type), do: fact_type, else: inspect(fact_type)

    PrestoLogger.log_rule_compilation(:debug, fact_type_str, "updating_fast_path_rules", %{
      fact_type: fact_type,
      rule_ids: rule_ids,
      rule_count: length(rule_ids)
    })

    updated_fast_path = Map.put(state.fast_path_rules, fact_type, rule_ids)
    %{state | fast_path_rules: updated_fast_path}
  end

  @doc """
  Removes all metadata for a rule.
  """
  @spec remove_rule_metadata(State.t(), atom()) :: State.t()
  def remove_rule_metadata(%State{} = state, rule_id) do
    updated_networks = Map.delete(state.rule_networks, rule_id)
    updated_analyses = Map.delete(state.rule_analyses, rule_id)

    %{
      state
      | rule_networks: updated_networks,
        rule_analyses: updated_analyses
    }
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
  Gets all rules that have fast path optimization.
  """
  @spec get_all_fast_path_rules(State.t()) :: [atom()]
  def get_all_fast_path_rules(%State{} = state) do
    state.fast_path_rules
    |> Map.values()
    |> List.flatten()
    |> Enum.uniq()
  end

  @doc """
  Gets metadata statistics.
  """
  @spec get_metadata_stats(State.t()) :: map()
  def get_metadata_stats(%State{} = state) do
    rules_with_networks = map_size(state.rule_networks)
    rules_with_analyses = map_size(state.rule_analyses)
    fast_path_fact_types = map_size(state.fast_path_rules)
    total_rules = map_size(state.rules)

    fast_path_rules_count =
      state.fast_path_rules
      |> Map.values()
      |> List.flatten()
      |> length()

    %{
      rules_with_networks: rules_with_networks,
      rules_with_analyses: rules_with_analyses,
      fast_path_fact_types: fast_path_fact_types,
      fast_path_rules_count: fast_path_rules_count,
      network_coverage: if(total_rules > 0, do: rules_with_networks / total_rules, else: 0),
      analysis_coverage: if(total_rules > 0, do: rules_with_analyses / total_rules, else: 0)
    }
  end

  @doc """
  Validates metadata consistency.
  """
  @spec validate_metadata_consistency(State.t()) :: {:ok, :consistent} | {:error, [String.t()]}
  def validate_metadata_consistency(%State{} = state) do
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

    if Enum.empty?(issues) do
      {:ok, :consistent}
    else
      {:error, Enum.reverse(issues)}
    end
  end
end

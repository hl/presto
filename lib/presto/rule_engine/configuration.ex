defmodule Presto.RuleEngine.Configuration do
  @moduledoc """
  Configuration management for the Presto Rule Engine.

  This module handles optimization settings and runtime configuration
  for the rule engine, including fast path settings, thresholds,
  and performance tuning parameters.
  """

  alias Presto.RuleEngine.State

  @doc """
  Updates optimization configuration.
  """
  @spec update_optimization_config(State.t(), map()) :: State.t()
  def update_optimization_config(%State{} = state, new_config) do
    merged_config = Map.merge(state.optimization_config, new_config)
    %{state | optimization_config: merged_config}
  end

  @doc """
  Gets the current optimization configuration.
  """
  @spec get_optimization_config(State.t()) :: map()
  def get_optimization_config(%State{} = state), do: state.optimization_config

  @doc """
  Checks if fast path is enabled.
  """
  @spec fast_path_enabled?(State.t()) :: boolean()
  def fast_path_enabled?(%State{} = state) do
    state.optimization_config.enable_fast_path
  end

  @doc """
  Checks if alpha sharing is enabled.
  """
  @spec alpha_sharing_enabled?(State.t()) :: boolean()
  def alpha_sharing_enabled?(%State{} = state) do
    state.optimization_config.enable_alpha_sharing
  end

  @doc """
  Checks if rule batching is enabled.
  """
  @spec rule_batching_enabled?(State.t()) :: boolean()
  def rule_batching_enabled?(%State{} = state) do
    state.optimization_config.enable_rule_batching
  end
end

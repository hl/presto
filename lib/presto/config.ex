defmodule Presto.Config do
  @moduledoc """
  Simple configuration utilities for the Presto rules engine.

  Provides basic configuration access with sensible defaults.
  Uses standard Application.get_env/3 for simplicity.
  """

  @doc """
  Gets engine configuration with defaults applied.
  """
  @spec get_engine_config() :: map()
  def get_engine_config do
    Application.get_env(:presto, :engine, %{})
    |> apply_engine_defaults()
  end

  @doc """
  Gets rule registry configuration with defaults applied.
  """
  @spec get_rule_registry_config() :: map()
  def get_rule_registry_config do
    Application.get_env(:presto, :rule_registry, %{})
    |> normalize_config()
    |> apply_rule_registry_defaults()
  end

  @doc """
  Gets performance configuration with defaults applied.
  """
  @spec get_performance_config() :: map()
  def get_performance_config do
    Application.get_env(:presto, :performance, %{})
    |> apply_performance_defaults()
  end

  # Private functions - apply sensible defaults

  defp normalize_config(config) when is_list(config), do: Enum.into(config, %{})
  defp normalize_config(config) when is_map(config), do: config
  defp normalize_config(_), do: %{}

  defp apply_engine_defaults(config) do
    Map.merge(
      %{
        max_rules: 1000,
        rule_timeout_ms: 5000,
        enable_concurrent_execution: false,
        max_concurrent_rules: 10,
        working_memory_limit: 100_000
      },
      config
    )
  end

  defp apply_rule_registry_defaults(config) do
    Map.merge(
      %{
        default_rules: [],
        rule_cache_size: 1000,
        enable_rule_hot_reload: false
      },
      config
    )
  end

  defp apply_performance_defaults(config) do
    Map.merge(
      %{
        enable_metrics: true,
        metrics_interval_ms: 60_000,
        enable_profiling: false
      },
      config
    )
  end
end

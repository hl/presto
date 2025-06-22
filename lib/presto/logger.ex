defmodule Presto.Logger do
  @moduledoc """
  Structured logging utilities for the Presto rules engine.

  Provides consistent logging patterns with structured metadata
  for better observability and debugging.

  ## Configuration

  Presto logging can be controlled by the host application:

      config :presto, :logging,
        enabled: true,    # Enable/disable all Presto logging
        level: :info      # Minimum log level when enabled

  By default, Presto logging is disabled to avoid interfering with
  host application logging.
  """

  require Logger

  @doc """
  Checks if Presto logging is enabled and at the appropriate level.

  Configuration priority:
  1. Environment variables (PRESTO_LOGGING_ENABLED, PRESTO_LOGGING_LEVEL)
  2. Application config (:presto, :logging)
  3. Default: disabled
  """
  @spec logging_enabled?(atom()) :: boolean()
  def logging_enabled?(level) do
    {enabled, min_level} = get_logging_config()
    enabled and Logger.compare_levels(level, min_level) != :lt
  end

  # Extract configuration logic to reduce cyclomatic complexity
  defp get_logging_config do
    case get_env_config() do
      {enabled, min_level} when not is_nil(enabled) ->
        {enabled, min_level}

      _ ->
        get_app_config()
    end
  end

  defp get_env_config do
    env_enabled = System.get_env("PRESTO_LOGGING_ENABLED")
    env_level = System.get_env("PRESTO_LOGGING_LEVEL")

    case {env_enabled, env_level} do
      {enabled_str, level_str} when is_binary(enabled_str) ->
        enabled = enabled_str in ["true", "1", "yes"]
        level = parse_log_level(level_str)
        {enabled, level}

      _ ->
        {nil, nil}
    end
  end

  defp get_app_config do
    case Application.get_env(:presto, :logging, enabled: false) do
      config when is_list(config) ->
        enabled = Keyword.get(config, :enabled, false)
        min_level = Keyword.get(config, :level, :info)
        {enabled, min_level}

      # Backward compatibility
      true ->
        {true, :info}

      false ->
        {false, :info}
    end
  end

  defp parse_log_level(level_str) do
    case level_str do
      "debug" -> :debug
      "info" -> :info
      "warn" -> :warn
      "error" -> :error
      _ -> :info
    end
  end

  @doc """
  Logs rule execution events with structured metadata.
  """
  def log_rule_execution(level, rule_id, event, metadata \\ %{}) do
    if logging_enabled?(level) do
      Logger.log(
        level,
        fn ->
          "Rule execution: #{event}"
        end,
        [
          rule_id: rule_id,
          event: event,
          component: :rule_engine,
          timestamp: DateTime.utc_now()
        ] ++ Map.to_list(metadata)
      )
    end
  end

  @doc """
  Logs fact processing events.
  """
  def log_fact_processing(level, fact_type, event, metadata \\ %{}) do
    if logging_enabled?(level) do
      Logger.log(
        level,
        fn ->
          "Fact processing: #{event}"
        end,
        [
          fact_type: fact_type,
          event: event,
          component: :working_memory,
          timestamp: DateTime.utc_now()
        ] ++ Map.to_list(metadata)
      )
    end
  end

  @doc """
  Logs network operations (alpha/beta network changes).
  """
  def log_network_operation(level, network_type, operation, metadata \\ %{}) do
    if logging_enabled?(level) do
      Logger.log(
        level,
        fn ->
          "Network operation: #{operation} on #{network_type}"
        end,
        [
          network_type: network_type,
          operation: operation,
          component: :rete_network,
          timestamp: DateTime.utc_now()
        ] ++ Map.to_list(metadata)
      )
    end
  end

  @doc """
  Logs performance metrics.
  """
  def log_performance(level, operation, duration_ms, metadata \\ %{}) do
    if logging_enabled?(level) do
      Logger.log(
        level,
        fn ->
          "Performance: #{operation} completed in #{duration_ms}ms"
        end,
        [
          operation: operation,
          duration_ms: duration_ms,
          component: :performance,
          timestamp: DateTime.utc_now()
        ] ++ Map.to_list(metadata)
      )
    end
  end

  @doc """
  Logs engine lifecycle events.
  """
  def log_engine_lifecycle(level, engine_id, event, metadata \\ %{}) do
    if logging_enabled?(level) do
      Logger.log(
        level,
        fn ->
          "Engine lifecycle: #{event}"
        end,
        [
          engine_id: engine_id,
          event: event,
          component: :engine_lifecycle,
          timestamp: DateTime.utc_now()
        ] ++ Map.to_list(metadata)
      )
    end
  end

  @doc """
  Logs configuration events.
  """
  def log_configuration(level, config_key, event, metadata \\ %{}) do
    if logging_enabled?(level) do
      Logger.log(
        level,
        fn ->
          "Configuration: #{event} for #{config_key}"
        end,
        [
          config_key: config_key,
          event: event,
          component: :configuration,
          timestamp: DateTime.utc_now()
        ] ++ Map.to_list(metadata)
      )
    end
  end

  @doc """
  Logs errors with structured context.
  """
  def log_error(error, context \\ %{}, stacktrace \\ nil) do
    level = :error

    if logging_enabled?(level) do
      Logger.log(
        level,
        fn ->
          "Error occurred: #{Exception.message(error)}"
        end,
        error_type: error.__struct__,
        error_message: Exception.message(error),
        context: context,
        component: :error_handler,
        timestamp: DateTime.utc_now(),
        stacktrace:
          if(stacktrace, do: Exception.format_stacktrace(stacktrace), else: "not_available")
      )
    end
  end

  @doc """
  Logs debug information with trace ID for correlation.
  """
  def log_debug(message, trace_id, metadata \\ %{}) do
    level = :debug

    if logging_enabled?(level) do
      Logger.log(
        level,
        fn ->
          "[#{trace_id}] #{message}"
        end,
        [
          trace_id: trace_id,
          component: :debug,
          timestamp: DateTime.utc_now()
        ] ++ Map.to_list(metadata)
      )
    end
  end

  @doc """
  Creates a trace ID for correlating related log entries.
  """
  def generate_trace_id do
    :crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower)
  end

  @doc """
  Logs with timing information.
  """
  def log_with_timing(level, operation, fun, metadata \\ %{}) do
    start_time = System.monotonic_time(:millisecond)

    try do
      result = fun.()
      end_time = System.monotonic_time(:millisecond)
      duration = end_time - start_time

      log_performance(level, operation, duration, Map.put(metadata, :status, :success))

      result
    rescue
      error ->
        end_time = System.monotonic_time(:millisecond)
        duration = end_time - start_time

        log_performance(
          :error,
          operation,
          duration,
          Map.merge(metadata, %{status: :error, error: Exception.message(error)})
        )

        reraise error, __STACKTRACE__
    end
  end

  @doc """
  Logs rule compilation events.
  """
  def log_rule_compilation(level, rule_id, stage, metadata \\ %{}) do
    if logging_enabled?(level) do
      Logger.log(
        level,
        fn ->
          "Rule compilation: #{stage} for rule #{rule_id}"
        end,
        [
          rule_id: rule_id,
          compilation_stage: stage,
          component: :rule_compiler,
          timestamp: DateTime.utc_now()
        ] ++ Map.to_list(metadata)
      )
    end
  end

  @doc """
  Logs working memory statistics.
  """
  def log_memory_stats(level, stats, metadata \\ %{}) do
    if logging_enabled?(level) do
      Logger.log(
        level,
        fn ->
          "Working memory stats: #{stats.fact_count} facts, #{stats.memory_usage_bytes} bytes"
        end,
        [
          fact_count: stats.fact_count,
          memory_usage_bytes: stats.memory_usage_bytes,
          component: :working_memory,
          timestamp: DateTime.utc_now()
        ] ++ Map.to_list(metadata)
      )
    end
  end

  @doc """
  Logs distributed system events with structured metadata.
  """
  def log_distributed(level, node_id, event, metadata \\ %{}) do
    if logging_enabled?(level) do
      Logger.log(
        level,
        fn ->
          "Distributed system: #{event} on node #{node_id}"
        end,
        [
          node_id: node_id,
          event: event,
          component: :distributed_system,
          timestamp: DateTime.utc_now()
        ] ++ Map.to_list(metadata)
      )
    end
  end
end

defmodule Presto.Logger do
  @moduledoc """
  Structured logging utilities for the Presto rules engine.

  Provides consistent logging patterns with structured metadata
  for better observability and debugging.
  """

  require Logger

  @doc """
  Logs rule execution events with structured metadata.
  """
  def log_rule_execution(level, rule_id, event, metadata \\ %{}) do
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

  @doc """
  Logs fact processing events.
  """
  def log_fact_processing(level, fact_type, event, metadata \\ %{}) do
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

  @doc """
  Logs network operations (alpha/beta network changes).
  """
  def log_network_operation(level, network_type, operation, metadata \\ %{}) do
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

  @doc """
  Logs performance metrics.
  """
  def log_performance(level, operation, duration_ms, metadata \\ %{}) do
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

  @doc """
  Logs engine lifecycle events.
  """
  def log_engine_lifecycle(level, engine_id, event, metadata \\ %{}) do
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

  @doc """
  Logs configuration events.
  """
  def log_configuration(level, config_key, event, metadata \\ %{}) do
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

  @doc """
  Logs errors with structured context.
  """
  def log_error(error, context \\ %{}, stacktrace \\ nil) do
    Logger.error(
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

  @doc """
  Logs debug information with trace ID for correlation.
  """
  def log_debug(message, trace_id, metadata \\ %{}) do
    Logger.debug(
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

  @doc """
  Logs working memory statistics.
  """
  def log_memory_stats(level, stats, metadata \\ %{}) do
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

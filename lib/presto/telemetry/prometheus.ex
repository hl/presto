defmodule Presto.Telemetry.Prometheus do
  @moduledoc """
  Prometheus metrics integration for Presto telemetry.

  This module provides Prometheus metrics collection for Presto rule engines,
  enabling integration with monitoring systems like Grafana.

  ## Setup

      # Add to your application supervision tree
      children = [
        # ... other children
        Presto.Telemetry.Prometheus
      ]

      # Or setup manually
      Presto.Telemetry.Prometheus.setup()

  ## Metrics

  ### Counters
  - `presto_facts_asserted_total` - Total facts asserted
  - `presto_facts_retracted_total` - Total facts retracted
  - `presto_rules_executed_total` - Total rule executions
  - `presto_rules_added_total` - Total rules added
  - `presto_rules_removed_total` - Total rules removed
  - `presto_engines_started_total` - Total engines started
  - `presto_engines_stopped_total` - Total engines stopped
  - `presto_errors_total` - Total errors

  ### Histograms
  - `presto_fact_assertion_duration_seconds` - Fact assertion duration
  - `presto_rule_execution_duration_seconds` - Rule execution duration
  - `presto_bulk_operation_duration_seconds` - Bulk operation duration

  ### Gauges
  - `presto_engines_active` - Currently active engines
  - `presto_memory_usage_bytes` - Memory usage by engine
  - `presto_facts_count` - Current fact count per engine
  - `presto_rules_count` - Current rule count per engine

  ## Usage with Grafana

  Example queries for dashboards:

      # Engine throughput
      rate(presto_facts_asserted_total[5m])

      # Rule execution latency
      histogram_quantile(0.95, presto_rule_execution_duration_seconds_bucket)

      # Error rate
      rate(presto_errors_total[5m])
  """

  use GenServer
  require Logger

  @metrics [
    # Counters
    {:presto_facts_asserted_total, :counter, "Total facts asserted", [:engine_id, :fact_type]},
    {:presto_facts_retracted_total, :counter, "Total facts retracted", [:engine_id, :fact_type]},
    {:presto_rules_executed_total, :counter, "Total rule executions", [:engine_id, :rule_id]},
    {:presto_rules_added_total, :counter, "Total rules added",
     [:engine_id, :complexity, :strategy]},
    {:presto_rules_removed_total, :counter, "Total rules removed", [:engine_id]},
    {:presto_engines_started_total, :counter, "Total engines started", []},
    {:presto_engines_stopped_total, :counter, "Total engines stopped", [:reason]},
    {:presto_errors_total, :counter, "Total errors", [:engine_id, :error_type]},
    {:presto_aggregations_updated_total, :counter, "Total aggregation updates",
     [:engine_id, :aggregation_type]},

    # Histograms
    {:presto_fact_assertion_duration_seconds, :histogram, "Fact assertion duration",
     [:engine_id, :fact_type]},
    {:presto_rule_execution_duration_seconds, :histogram, "Rule execution duration",
     [:engine_id, :rule_id]},
    {:presto_bulk_operation_duration_seconds, :histogram, "Bulk operation duration",
     [:engine_id, :operation]},
    {:presto_aggregation_duration_seconds, :histogram, "Aggregation computation duration",
     [:engine_id, :aggregation_type]},

    # Gauges
    {:presto_engines_active, :gauge, "Currently active engines", []},
    {:presto_memory_usage_bytes, :gauge, "Memory usage by engine", [:engine_id, :memory_type]},
    {:presto_facts_count, :gauge, "Current fact count per engine", [:engine_id]},
    {:presto_rules_count, :gauge, "Current rule count per engine", [:engine_id]}
  ]

  @buckets [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 2.5, 5.0, 10.0]

  ## Client API

  @doc """
  Starts the Prometheus metrics collection.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Sets up Prometheus metrics and telemetry handlers.
  """
  @spec setup() :: :ok
  def setup do
    # Ensure prometheus is available
    if prometheus_available?() do
      # Setup metrics
      setup_metrics()

      # Attach telemetry handlers
      :telemetry.attach_many(
        "presto-prometheus",
        Presto.Telemetry.events(),
        &handle_telemetry_event/4,
        %{}
      )

      Logger.info("Presto Prometheus metrics setup complete")
    else
      require Logger
      Logger.warning("prometheus_ex dependency not available, metrics collection disabled")
    end

    :ok
  end

  @doc """
  Removes Prometheus telemetry handlers.
  """
  @spec teardown() :: :ok
  def teardown do
    :telemetry.detach("presto-prometheus")
    Logger.info("Presto Prometheus metrics removed")
    :ok
  end

  @doc """
  Gets current metric values for debugging.
  """
  @spec get_metrics() :: map()
  def get_metrics do
    @metrics
    |> Enum.map(fn {name, type, _help, _labels} ->
      case type do
        :counter ->
          {name, :prometheus_counter.value(name)}

        :gauge ->
          {name, :prometheus_gauge.value(name)}

        :histogram ->
          {name, :prometheus_histogram.value(name)}
      end
    end)
    |> Map.new()
  rescue
    _ -> %{error: "Prometheus not available"}
  end

  ## Server implementation

  @impl GenServer
  def init(opts) do
    Logger.info("Starting Presto Prometheus metrics collector", opts: inspect(opts))

    # Setup metrics if not already done
    if Keyword.get(opts, :auto_setup, true) do
      setup()
    end

    # Schedule periodic metric collection
    interval = Keyword.get(opts, :collection_interval_ms, 30_000)
    :timer.send_interval(interval, :collect_system_metrics)

    {:ok, %{collection_interval: interval}}
  end

  @impl GenServer
  def handle_info(:collect_system_metrics, state) do
    collect_system_metrics()
    {:noreply, state}
  end

  ## Telemetry event handlers

  @doc """
  Handles telemetry events and updates Prometheus metrics.
  """
  @spec handle_telemetry_event([atom()], map(), map(), term()) :: :ok
  def handle_telemetry_event([:presto, :engine, :start], _measurements, metadata, _config) do
    :prometheus_counter.inc(:presto_engines_started_total)
    :prometheus_gauge.inc(:presto_engines_active)

    # Initialize engine-specific gauges
    :prometheus_gauge.set(:presto_facts_count, [metadata.engine_id], 0)
    :prometheus_gauge.set(:presto_rules_count, [metadata.engine_id], 0)
  end

  def handle_telemetry_event([:presto, :engine, :stop], _measurements, metadata, _config) do
    reason = to_string(metadata.reason)
    :prometheus_counter.inc(:presto_engines_stopped_total, [reason])
    :prometheus_gauge.dec(:presto_engines_active)

    # Clean up engine-specific gauges
    :prometheus_gauge.remove(:presto_facts_count, [metadata.engine_id])
    :prometheus_gauge.remove(:presto_rules_count, [metadata.engine_id])
  end

  def handle_telemetry_event([:presto, :fact, :assert], measurements, metadata, _config) do
    :prometheus_counter.inc(:presto_facts_asserted_total, [metadata.engine_id, metadata.fact_type])

    if measurements.duration > 0 do
      duration_seconds = System.convert_time_unit(measurements.duration, :native, :second)

      :prometheus_histogram.observe(
        :presto_fact_assertion_duration_seconds,
        [metadata.engine_id, metadata.fact_type],
        duration_seconds
      )
    end
  end

  def handle_telemetry_event([:presto, :fact, :retract], measurements, metadata, _config) do
    :prometheus_counter.inc(:presto_facts_retracted_total, [
      metadata.engine_id,
      metadata.fact_type
    ])

    if measurements.duration > 0 do
      duration_seconds = System.convert_time_unit(measurements.duration, :native, :second)

      :prometheus_histogram.observe(
        :presto_fact_assertion_duration_seconds,
        [metadata.engine_id, metadata.fact_type],
        duration_seconds
      )
    end
  end

  def handle_telemetry_event([:presto, :fact, :bulk_assert], measurements, metadata, _config) do
    # Update counters for each fact type
    Enum.each(metadata.fact_types, fn {fact_type, count} ->
      :prometheus_counter.inc(
        :presto_facts_asserted_total,
        [metadata.engine_id, fact_type],
        count
      )
    end)

    duration_seconds = System.convert_time_unit(measurements.duration, :native, :second)

    :prometheus_histogram.observe(
      :presto_bulk_operation_duration_seconds,
      [metadata.engine_id, "assert"],
      duration_seconds
    )
  end

  def handle_telemetry_event([:presto, :fact, :bulk_retract], measurements, metadata, _config) do
    # Update counters for each fact type
    Enum.each(metadata.fact_types, fn {fact_type, count} ->
      :prometheus_counter.inc(
        :presto_facts_retracted_total,
        [metadata.engine_id, fact_type],
        count
      )
    end)

    duration_seconds = System.convert_time_unit(measurements.duration, :native, :second)

    :prometheus_histogram.observe(
      :presto_bulk_operation_duration_seconds,
      [metadata.engine_id, "retract"],
      duration_seconds
    )
  end

  def handle_telemetry_event([:presto, :rule, :add], _measurements, metadata, _config) do
    :prometheus_counter.inc(
      :presto_rules_added_total,
      [metadata.engine_id, metadata.complexity, metadata.strategy]
    )

    # Update rules count gauge
    update_rules_count_gauge(metadata.engine_id, 1)
  end

  def handle_telemetry_event([:presto, :rule, :remove], _measurements, metadata, _config) do
    :prometheus_counter.inc(:presto_rules_removed_total, [metadata.engine_id])

    # Update rules count gauge
    update_rules_count_gauge(metadata.engine_id, -1)
  end

  def handle_telemetry_event([:presto, :rule, :execute], measurements, metadata, _config) do
    :prometheus_counter.inc(:presto_rules_executed_total, [metadata.engine_id, metadata.rule_id])

    duration_seconds = System.convert_time_unit(measurements.duration, :native, :second)

    :prometheus_histogram.observe(
      :presto_rule_execution_duration_seconds,
      [metadata.engine_id, metadata.rule_id],
      duration_seconds
    )
  end

  def handle_telemetry_event([:presto, :error, :occurred], _measurements, metadata, _config) do
    :prometheus_counter.inc(:presto_errors_total, [metadata.engine_id, metadata.error_type])
  end

  def handle_telemetry_event([:presto, :aggregation, :update], measurements, metadata, _config) do
    :prometheus_counter.inc(
      :presto_aggregations_updated_total,
      [metadata.engine_id, metadata.aggregation_type]
    )

    duration_seconds = System.convert_time_unit(measurements.duration, :native, :second)

    :prometheus_histogram.observe(
      :presto_aggregation_duration_seconds,
      [metadata.engine_id, metadata.aggregation_type],
      duration_seconds
    )
  end

  def handle_telemetry_event([:presto, :memory, :pressure], measurements, metadata, _config) do
    :prometheus_gauge.set(
      :presto_memory_usage_bytes,
      [metadata.engine_id, "working_memory"],
      measurements.memory_bytes
    )
  end

  # Default handler for unhandled events
  def handle_telemetry_event(_event, _measurements, _metadata, _config) do
    :ok
  end

  ## Private functions

  defp setup_metrics do
    Enum.each(@metrics, fn {name, type, help, labels} ->
      case type do
        :counter ->
          :prometheus_counter.new(
            name: name,
            help: help,
            labels: labels
          )

        :gauge ->
          :prometheus_gauge.new(
            name: name,
            help: help,
            labels: labels
          )

        :histogram ->
          :prometheus_histogram.new(
            name: name,
            help: help,
            labels: labels,
            buckets: @buckets
          )
      end
    end)
  end

  defp collect_system_metrics do
    try do
      # Collect engine statistics for all registered engines
      case Presto.EngineRegistry.list_engines() do
        engines when is_list(engines) ->
          Enum.each(engines, &update_engine_metrics/1)

        _ ->
          :ok
      end
    rescue
      error ->
        Logger.warning("Failed to collect system metrics", error: inspect(error))
    end
  end

  defp update_engine_metrics(engine_info) do
    engine_id = engine_info.engine_id

    # Update fact count if available
    if Map.has_key?(engine_info, :total_facts) do
      :prometheus_gauge.set(:presto_facts_count, [engine_id], engine_info.total_facts)
    end

    # Update rule count if available
    if Map.has_key?(engine_info, :total_rules) do
      :prometheus_gauge.set(:presto_rules_count, [engine_id], engine_info.total_rules)
    end

    # Update memory usage if available
    if Map.has_key?(engine_info, :memory_usage) do
      :prometheus_gauge.set(
        :presto_memory_usage_bytes,
        [engine_id, "process"],
        engine_info.memory_usage
      )
    end
  end

  defp update_rules_count_gauge(engine_id, delta) do
    try do
      current = :prometheus_gauge.value(:presto_rules_count, [engine_id])
      new_value = max(0, current + delta)
      :prometheus_gauge.set(:presto_rules_count, [engine_id], new_value)
    rescue
      # Ignore errors in gauge updates
      _ -> :ok
    end
  end

  defp prometheus_available? do
    Code.ensure_loaded?(:prometheus_counter) &&
      Code.ensure_loaded?(:prometheus_gauge) &&
      Code.ensure_loaded?(:prometheus_histogram)
  end
end

defmodule Presto.Telemetry do
  @moduledoc """
  Telemetry integration for Presto rule engines.

  This module provides comprehensive telemetry instrumentation for monitoring
  and observability of Presto rule engines in production environments.

  ## Events

  Presto emits the following telemetry events:

  ### Engine Lifecycle
  - `[:presto, :engine, :start]` - Engine started
  - `[:presto, :engine, :stop]` - Engine stopped
  - `[:presto, :engine, :registered]` - Engine registered with name
  - `[:presto, :engine, :unregistered]` - Engine unregistered

  ### Fact Operations
  - `[:presto, :fact, :assert]` - Fact asserted
  - `[:presto, :fact, :retract]` - Fact retracted
  - `[:presto, :fact, :bulk_assert]` - Bulk fact assertion
  - `[:presto, :fact, :bulk_retract]` - Bulk fact retraction

  ### Rule Operations
  - `[:presto, :rule, :add]` - Rule added
  - `[:presto, :rule, :remove]` - Rule removed
  - `[:presto, :rule, :execute]` - Rule execution (span)
  - `[:presto, :rule, :fire_all]` - All rules fired (span)

  ### Performance & Health
  - `[:presto, :memory, :pressure]` - Memory pressure detected
  - `[:presto, :performance, :slow_query]` - Slow rule execution detected
  - `[:presto, :error, :occurred]` - Error occurred

  ### Aggregations
  - `[:presto, :aggregation, :update]` - Aggregation updated
  - `[:presto, :aggregation, :compute]` - Aggregation computed

  ## Usage

      # Setup default handlers
      Presto.Telemetry.setup_default_handlers()

      # Setup custom handlers
      :telemetry.attach_many(
        "my-app-presto",
        Presto.Telemetry.events(),
        &MyApp.PrestoTelemetryHandler.handle_event/4,
        %{}
      )

  ## Metrics Integration

  For Prometheus integration:

      Presto.Telemetry.Prometheus.setup()
  """

  require Logger

  @events [
    # Engine lifecycle
    [:presto, :engine, :start],
    [:presto, :engine, :stop],
    [:presto, :engine, :registered],
    [:presto, :engine, :unregistered],

    # Fact operations
    [:presto, :fact, :assert],
    [:presto, :fact, :retract],
    [:presto, :fact, :bulk_assert],
    [:presto, :fact, :bulk_retract],

    # Rule operations
    [:presto, :rule, :add],
    [:presto, :rule, :remove],
    [:presto, :rule, :execute],
    [:presto, :rule, :fire_all],

    # Performance & health
    [:presto, :memory, :pressure],
    [:presto, :performance, :slow_query],
    [:presto, :error, :occurred],

    # Aggregations
    [:presto, :aggregation, :update],
    [:presto, :aggregation, :compute]
  ]

  @doc """
  Returns the list of all telemetry events emitted by Presto.
  """
  @spec events() :: [[atom()]]
  def events, do: @events

  @doc """
  Sets up default telemetry handlers for logging.
  """
  @spec setup_default_handlers() :: :ok
  def setup_default_handlers do
    if telemetry_available?() do
      :telemetry.attach_many(
        "presto-default-handlers",
        events(),
        &handle_event/4,
        %{}
      )
    else
      :ok
    end
  end

  @doc """
  Removes default telemetry handlers.
  """
  @spec remove_default_handlers() :: :ok
  def remove_default_handlers do
    if telemetry_available?() do
      :telemetry.detach("presto-default-handlers")
    else
      :ok
    end
  end

  ## Event emission functions

  @doc "Emits engine start event"
  @spec emit_engine_start(String.t(), pid()) :: :ok
  def emit_engine_start(engine_id, pid) do
    safe_telemetry_execute(
      [:presto, :engine, :start],
      %{timestamp: System.system_time()},
      %{engine_id: engine_id, pid: pid}
    )
  end

  @doc "Emits engine stop event"
  @spec emit_engine_stop(String.t(), pid(), term()) :: :ok
  def emit_engine_stop(engine_id, pid, reason \\ :normal) do
    safe_telemetry_execute(
      [:presto, :engine, :stop],
      %{timestamp: System.system_time()},
      %{engine_id: engine_id, pid: pid, reason: reason}
    )
  end

  @doc "Emits engine registration event"
  @spec emit_engine_registered(atom(), pid(), String.t()) :: :ok
  def emit_engine_registered(name, pid, engine_id) do
    safe_telemetry_execute(
      [:presto, :engine, :registered],
      %{timestamp: System.system_time()},
      %{name: name, pid: pid, engine_id: engine_id}
    )
  end

  @doc "Emits engine unregistration event"
  @spec emit_engine_unregistered(atom(), pid(), String.t(), term()) :: :ok
  def emit_engine_unregistered(name, pid, engine_id, reason) do
    safe_telemetry_execute(
      [:presto, :engine, :unregistered],
      %{timestamp: System.system_time()},
      %{name: name, pid: pid, engine_id: engine_id, reason: reason}
    )
  end

  @doc "Emits fact assertion event"
  @spec emit_fact_assert(String.t(), tuple(), non_neg_integer()) :: :ok
  def emit_fact_assert(engine_id, fact, duration_native \\ 0) do
    safe_telemetry_execute(
      [:presto, :fact, :assert],
      %{
        duration: duration_native,
        timestamp: System.system_time()
      },
      %{
        engine_id: engine_id,
        fact_type: elem(fact, 0),
        fact_arity: tuple_size(fact)
      }
    )
  end

  @doc "Emits fact retraction event"
  @spec emit_fact_retract(String.t(), tuple(), non_neg_integer()) :: :ok
  def emit_fact_retract(engine_id, fact, duration_native \\ 0) do
    safe_telemetry_execute(
      [:presto, :fact, :retract],
      %{
        duration: duration_native,
        timestamp: System.system_time()
      },
      %{
        engine_id: engine_id,
        fact_type: elem(fact, 0),
        fact_arity: tuple_size(fact)
      }
    )
  end

  @doc "Emits bulk fact assertion event"
  @spec emit_bulk_fact_assert(String.t(), [tuple()], non_neg_integer()) :: :ok
  def emit_bulk_fact_assert(engine_id, facts, duration_native) do
    fact_types = facts |> Enum.map(&elem(&1, 0)) |> Enum.frequencies()

    safe_telemetry_execute(
      [:presto, :fact, :bulk_assert],
      %{
        duration: duration_native,
        fact_count: length(facts),
        timestamp: System.system_time()
      },
      %{
        engine_id: engine_id,
        fact_types: fact_types
      }
    )
  end

  @doc "Emits bulk fact retraction event"
  @spec emit_bulk_fact_retract(String.t(), [tuple()], non_neg_integer()) :: :ok
  def emit_bulk_fact_retract(engine_id, facts, duration_native) do
    fact_types = facts |> Enum.map(&elem(&1, 0)) |> Enum.frequencies()

    safe_telemetry_execute(
      [:presto, :fact, :bulk_retract],
      %{
        duration: duration_native,
        fact_count: length(facts),
        timestamp: System.system_time()
      },
      %{
        engine_id: engine_id,
        fact_types: fact_types
      }
    )
  end

  @doc "Emits rule addition event"
  @spec emit_rule_add(String.t(), atom(), map()) :: :ok
  def emit_rule_add(engine_id, rule_id, rule_analysis) do
    safe_telemetry_execute(
      [:presto, :rule, :add],
      %{timestamp: System.system_time()},
      %{
        engine_id: engine_id,
        rule_id: rule_id,
        complexity: rule_analysis.complexity,
        strategy: rule_analysis.strategy,
        condition_count: rule_analysis.pattern_count + rule_analysis.test_count
      }
    )
  end

  @doc "Emits rule removal event"
  @spec emit_rule_remove(String.t(), atom()) :: :ok
  def emit_rule_remove(engine_id, rule_id) do
    safe_telemetry_execute(
      [:presto, :rule, :remove],
      %{timestamp: System.system_time()},
      %{engine_id: engine_id, rule_id: rule_id}
    )
  end

  @doc "Emits rule execution span"
  @spec emit_rule_execute_span(String.t(), atom(), function()) :: term()
  def emit_rule_execute_span(engine_id, rule_id, fun) do
    safe_telemetry_span(
      [:presto, :rule, :execute],
      %{engine_id: engine_id, rule_id: rule_id},
      fun
    )
  end

  @doc "Emits rule firing span"
  @spec emit_rule_fire_all_span(String.t(), non_neg_integer(), keyword(), function()) :: term()
  def emit_rule_fire_all_span(engine_id, rule_count, opts, fun) do
    safe_telemetry_span(
      [:presto, :rule, :fire_all],
      %{
        engine_id: engine_id,
        rule_count: rule_count,
        concurrent: Keyword.get(opts, :concurrent, false),
        auto_chain: Keyword.get(opts, :auto_chain, false)
      },
      fun
    )
  end

  @doc "Emits memory pressure event"
  @spec emit_memory_pressure(String.t(), non_neg_integer(), float()) :: :ok
  def emit_memory_pressure(engine_id, memory_bytes, usage_percent) do
    safe_telemetry_execute(
      [:presto, :memory, :pressure],
      %{
        memory_bytes: memory_bytes,
        usage_percent: usage_percent,
        timestamp: System.system_time()
      },
      %{engine_id: engine_id}
    )
  end

  @doc "Emits slow query event"
  @spec emit_slow_query(String.t(), atom(), non_neg_integer(), non_neg_integer()) :: :ok
  def emit_slow_query(engine_id, rule_id, duration_ms, threshold_ms) do
    safe_telemetry_execute(
      [:presto, :performance, :slow_query],
      %{
        duration_ms: duration_ms,
        threshold_ms: threshold_ms,
        timestamp: System.system_time()
      },
      %{engine_id: engine_id, rule_id: rule_id}
    )
  end

  @doc "Emits error event"
  @spec emit_error(String.t(), atom(), Exception.t(), map()) :: :ok
  def emit_error(engine_id, error_type, error, metadata \\ %{}) do
    safe_telemetry_execute(
      [:presto, :error, :occurred],
      %{timestamp: System.system_time()},
      %{
        engine_id: engine_id,
        error_type: error_type,
        error_message: Exception.message(error),
        error_module: error.__struct__,
        metadata: metadata
      }
    )
  end

  @doc "Emits aggregation update event"
  @spec emit_aggregation_update(String.t(), atom(), map(), non_neg_integer()) :: :ok
  def emit_aggregation_update(engine_id, rule_id, aggregation_result, duration_native) do
    group_keys = Map.keys(aggregation_result) -- [:count, :sum, :avg, :min, :max, :collect]

    safe_telemetry_execute(
      [:presto, :aggregation, :update],
      %{
        duration: duration_native,
        group_count: length(group_keys),
        timestamp: System.system_time()
      },
      %{
        engine_id: engine_id,
        rule_id: rule_id,
        aggregation_type: detect_aggregation_type(aggregation_result)
      }
    )
  end

  ## Event handlers

  @doc """
  Default event handler for Presto telemetry events.

  This handler provides structured logging for all events.
  """
  @spec handle_event([atom()], map(), map(), term()) :: :ok
  def handle_event([:presto, :engine, :start], _measurements, metadata, _config) do
    Logger.info("Engine started",
      engine_id: metadata.engine_id,
      pid: inspect(metadata.pid)
    )
  end

  def handle_event([:presto, :engine, :stop], _measurements, metadata, _config) do
    Logger.info("Engine stopped",
      engine_id: metadata.engine_id,
      pid: inspect(metadata.pid),
      reason: inspect(metadata.reason)
    )
  end

  def handle_event([:presto, :engine, :registered], _measurements, metadata, _config) do
    Logger.info("Engine registered",
      name: metadata.name,
      engine_id: metadata.engine_id,
      pid: inspect(metadata.pid)
    )
  end

  def handle_event([:presto, :engine, :unregistered], _measurements, metadata, _config) do
    Logger.info("Engine unregistered",
      name: metadata.name,
      engine_id: metadata.engine_id,
      pid: inspect(metadata.pid),
      reason: inspect(metadata.reason)
    )
  end

  def handle_event([:presto, :fact, :assert], measurements, metadata, _config) do
    if measurements.duration > 0 do
      duration_ms = System.convert_time_unit(measurements.duration, :native, :millisecond)

      Logger.debug("Fact asserted",
        engine_id: metadata.engine_id,
        fact_type: metadata.fact_type,
        duration_ms: duration_ms
      )
    end
  end

  def handle_event([:presto, :fact, :bulk_assert], measurements, metadata, _config) do
    duration_ms = System.convert_time_unit(measurements.duration, :native, :millisecond)

    Logger.info("Bulk facts asserted",
      engine_id: metadata.engine_id,
      fact_count: measurements.fact_count,
      duration_ms: duration_ms,
      throughput: round(measurements.fact_count / (duration_ms / 1000))
    )
  end

  def handle_event([:presto, :rule, :add], _measurements, metadata, _config) do
    Logger.info("Rule added",
      engine_id: metadata.engine_id,
      rule_id: metadata.rule_id,
      complexity: metadata.complexity,
      strategy: metadata.strategy
    )
  end

  def handle_event([:presto, :rule, :execute], measurements, metadata, _config) do
    duration_ms = System.convert_time_unit(measurements.duration, :native, :millisecond)

    # Log slow rule executions
    if duration_ms > 100 do
      Logger.warning("Slow rule execution",
        engine_id: metadata.engine_id,
        rule_id: metadata.rule_id,
        duration_ms: duration_ms,
        result_count: Map.get(measurements, :result_count, 0)
      )
    else
      Logger.debug("Rule executed",
        engine_id: metadata.engine_id,
        rule_id: metadata.rule_id,
        duration_ms: duration_ms
      )
    end
  end

  def handle_event([:presto, :rule, :fire_all], measurements, metadata, _config) do
    duration_ms = System.convert_time_unit(measurements.duration, :native, :millisecond)

    Logger.info("Rules fired",
      engine_id: metadata.engine_id,
      rule_count: metadata.rule_count,
      result_count: Map.get(measurements, :result_count, 0),
      duration_ms: duration_ms,
      concurrent: metadata.concurrent
    )
  end

  def handle_event([:presto, :memory, :pressure], measurements, metadata, _config) do
    Logger.warning("Memory pressure detected",
      engine_id: metadata.engine_id,
      memory_mb: round(measurements.memory_bytes / 1024 / 1024),
      usage_percent: Float.round(measurements.usage_percent, 2)
    )
  end

  def handle_event([:presto, :performance, :slow_query], measurements, metadata, _config) do
    Logger.warning("Slow query detected",
      engine_id: metadata.engine_id,
      rule_id: metadata.rule_id,
      duration_ms: measurements.duration_ms,
      threshold_ms: measurements.threshold_ms
    )
  end

  def handle_event([:presto, :error, :occurred], _measurements, metadata, _config) do
    Logger.error("Engine error occurred",
      engine_id: metadata.engine_id,
      error_type: metadata.error_type,
      error_message: metadata.error_message,
      error_module: metadata.error_module,
      metadata: inspect(metadata.metadata)
    )
  end

  def handle_event([:presto, :aggregation, :update], measurements, metadata, _config) do
    duration_ms = System.convert_time_unit(measurements.duration, :native, :millisecond)

    Logger.debug("Aggregation updated",
      engine_id: metadata.engine_id,
      rule_id: metadata.rule_id,
      aggregation_type: metadata.aggregation_type,
      group_count: measurements.group_count,
      duration_ms: duration_ms
    )
  end

  # Default handler for unknown events
  def handle_event(event, measurements, metadata, _config) do
    Logger.debug("Presto telemetry event",
      event: event,
      measurements: measurements,
      metadata: metadata
    )
  end

  ## Private functions

  defp detect_aggregation_type(result) do
    cond do
      Map.has_key?(result, :count) -> :count
      Map.has_key?(result, :sum) -> :sum
      Map.has_key?(result, :avg) -> :avg
      Map.has_key?(result, :min) -> :min
      Map.has_key?(result, :max) -> :max
      Map.has_key?(result, :collect) -> :collect
      true -> :custom
    end
  end

  ## Helper functions

  defp telemetry_available? do
    Code.ensure_loaded?(:telemetry)
  end

  defp safe_telemetry_execute(event, measurements, metadata) do
    if telemetry_available?() do
      safe_telemetry_execute(event, measurements, metadata)
    else
      :ok
    end
  end

  defp safe_telemetry_span(event, metadata, fun) do
    if telemetry_available?() do
      safe_telemetry_span(event, metadata, fun)
    else
      fun.()
    end
  end
end

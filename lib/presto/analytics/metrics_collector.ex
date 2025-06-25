defmodule Presto.Analytics.MetricsCollector do
  @moduledoc """
  Advanced metrics collection system for Presto RETE engines.

  Provides comprehensive analytics capabilities including:
  - Rule execution performance metrics
  - Memory usage patterns and optimization opportunities
  - Fact insertion/retraction patterns
  - Network and cluster performance metrics
  - User behavior and usage analytics
  - Real-time and historical metrics aggregation

  ## Features

  ### Performance Metrics
  - Rule firing frequency and execution time
  - Alpha/Beta network traversal statistics
  - Memory consumption by rule and fact type
  - Working memory efficiency metrics

  ### Analytical Insights
  - Rule effectiveness scoring
  - Conflict resolution strategy performance
  - Fact pattern analysis and recommendations
  - Resource utilisation optimisation suggestions

  ### Real-time Monitoring
  - Live dashboards with streaming metrics
  - Alerting for performance degradation
  - Capacity planning recommendations
  - Anomaly detection and analysis

  ## Example Usage

      # Start metrics collection for an engine
      MetricsCollector.start_collection(:customer_engine, [
        metrics: [:all],
        collection_interval: 1000,
        retention_period: :day
      ])

      # Get performance insights
      {:ok, insights} = MetricsCollector.get_performance_insights(:customer_engine)

      # Configure alerting
      MetricsCollector.set_alert_threshold(:customer_engine, :execution_time, 
        warning: 100, critical: 500
      )

      # Generate analytics report
      {:ok, report} = MetricsCollector.generate_analytics_report(:customer_engine,
        period: :last_hour,
        format: :detailed
      )
  """

  use GenServer
  require Logger

  alias Presto.Analytics.{MetricStorage, AlertManager, PerformanceAnalyzer}

  @type metric_type ::
          :rule_execution
          | :fact_operations
          | :memory_usage
          | :network_stats
          | :user_actions
          | :system_health
          | :conflict_resolution
          | :pattern_matching

  @type collection_opts :: [
          metrics: [metric_type()] | :all,
          collection_interval: pos_integer(),
          retention_period: :hour | :day | :week | :month | :permanent,
          sampling_rate: float(),
          aggregation_window: pos_integer()
        ]

  @type performance_insight :: %{
          metric_type: metric_type(),
          engine_name: atom(),
          insight_type: :optimization | :warning | :recommendation | :anomaly,
          severity: :low | :medium | :high | :critical,
          description: String.t(),
          suggested_actions: [String.t()],
          confidence: float(),
          timestamp: DateTime.t()
        }

  @type analytics_report :: %{
          engine_name: atom(),
          period: {DateTime.t(), DateTime.t()},
          summary: map(),
          detailed_metrics: map(),
          insights: [performance_insight()],
          recommendations: [String.t()],
          generated_at: DateTime.t()
        }

  ## Client API

  @doc """
  Starts the metrics collector.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Starts metrics collection for a specific engine.
  """
  @spec start_collection(atom(), collection_opts()) :: :ok | {:error, term()}
  def start_collection(engine_name, opts \\ []) do
    GenServer.call(__MODULE__, {:start_collection, engine_name, opts})
  end

  @doc """
  Stops metrics collection for an engine.
  """
  @spec stop_collection(atom()) :: :ok
  def stop_collection(engine_name) do
    GenServer.call(__MODULE__, {:stop_collection, engine_name})
  end

  @doc """
  Records a metric data point.
  """
  @spec record_metric(atom(), metric_type(), map()) :: :ok
  def record_metric(engine_name, metric_type, data) do
    GenServer.cast(__MODULE__, {:record_metric, engine_name, metric_type, data})
  end

  @doc """
  Gets current performance metrics for an engine.
  """
  @spec get_current_metrics(atom()) :: {:ok, map()} | {:error, :not_found}
  def get_current_metrics(engine_name) do
    GenServer.call(__MODULE__, {:get_current_metrics, engine_name})
  end

  @doc """
  Gets performance insights for an engine.
  """
  @spec get_performance_insights(atom()) :: {:ok, [performance_insight()]} | {:error, term()}
  def get_performance_insights(engine_name) do
    GenServer.call(__MODULE__, {:get_performance_insights, engine_name})
  end

  @doc """
  Sets alert thresholds for specific metrics.
  """
  @spec set_alert_threshold(atom(), metric_type(), keyword()) :: :ok | {:error, term()}
  def set_alert_threshold(engine_name, metric_type, thresholds) do
    GenServer.call(__MODULE__, {:set_alert_threshold, engine_name, metric_type, thresholds})
  end

  @doc """
  Generates a comprehensive analytics report.
  """
  @spec generate_analytics_report(atom(), keyword()) ::
          {:ok, analytics_report()} | {:error, term()}
  def generate_analytics_report(engine_name, opts \\ []) do
    GenServer.call(__MODULE__, {:generate_report, engine_name, opts}, 30_000)
  end

  @doc """
  Gets aggregated metrics for a time period.
  """
  @spec get_aggregated_metrics(atom(), DateTime.t(), DateTime.t()) ::
          {:ok, map()} | {:error, term()}
  def get_aggregated_metrics(engine_name, start_time, end_time) do
    GenServer.call(__MODULE__, {:get_aggregated_metrics, engine_name, start_time, end_time})
  end

  @doc """
  Lists all engines currently being monitored.
  """
  @spec list_monitored_engines() :: [atom()]
  def list_monitored_engines do
    GenServer.call(__MODULE__, :list_monitored_engines)
  end

  @doc """
  Gets collection statistics for the metrics collector.
  """
  @spec get_collector_stats() :: map()
  def get_collector_stats do
    GenServer.call(__MODULE__, :get_collector_stats)
  end

  ## Server implementation

  @impl GenServer
  def init(opts) do
    Logger.info("Starting Advanced Metrics Collector")

    # Start metric storage backend
    {:ok, storage_pid} = MetricStorage.start_link([])

    # Start alert manager
    {:ok, alert_pid} = AlertManager.start_link([])

    # Start performance analyzer
    {:ok, analyzer_pid} = PerformanceAnalyzer.start_link([])

    state = %{
      # Configuration
      default_collection_interval: Keyword.get(opts, :default_collection_interval, 5_000),
      default_retention_period: Keyword.get(opts, :default_retention_period, :day),
      max_concurrent_collections: Keyword.get(opts, :max_concurrent_collections, 50),

      # Runtime state
      active_collections: %{},
      collection_refs: %{},
      metric_buffers: %{},

      # Component PIDs
      storage_pid: storage_pid,
      alert_pid: alert_pid,
      analyzer_pid: analyzer_pid,

      # Statistics
      stats: %{
        total_metrics_collected: 0,
        active_collections: 0,
        alerts_triggered: 0,
        reports_generated: 0,
        collection_errors: 0
      }
    }

    # Schedule periodic maintenance
    Process.send_after(self(), :maintenance_cycle, 60_000)

    {:ok, state}
  end

  @impl GenServer
  def handle_call({:start_collection, engine_name, opts}, _from, state) do
    case start_engine_collection(engine_name, opts, state) do
      {:ok, new_state} ->
        Logger.info("Started metrics collection", engine: engine_name)
        {:reply, :ok, new_state}

      {:error, reason} ->
        Logger.error("Failed to start metrics collection",
          engine: engine_name,
          reason: inspect(reason)
        )

        {:reply, {:error, reason}, state}
    end
  end

  @impl GenServer
  def handle_call({:stop_collection, engine_name}, _from, state) do
    new_state = stop_engine_collection(engine_name, state)
    Logger.info("Stopped metrics collection", engine: engine_name)
    {:reply, :ok, new_state}
  end

  @impl GenServer
  def handle_call({:get_current_metrics, engine_name}, _from, state) do
    case Map.get(state.active_collections, engine_name) do
      nil ->
        {:reply, {:error, :not_found}, state}

      _collection_info ->
        case MetricStorage.get_latest_metrics(state.storage_pid, engine_name) do
          {:ok, metrics} ->
            {:reply, {:ok, metrics}, state}

          error ->
            {:reply, error, state}
        end
    end
  end

  @impl GenServer
  def handle_call({:get_performance_insights, engine_name}, _from, state) do
    case PerformanceAnalyzer.analyze_engine_performance(state.analyzer_pid, engine_name) do
      {:ok, insights} ->
        {:reply, {:ok, insights}, state}

      error ->
        {:reply, error, state}
    end
  end

  @impl GenServer
  def handle_call({:set_alert_threshold, engine_name, metric_type, thresholds}, _from, state) do
    case AlertManager.set_threshold(state.alert_pid, engine_name, metric_type, thresholds) do
      :ok ->
        {:reply, :ok, state}

      error ->
        {:reply, error, state}
    end
  end

  @impl GenServer
  def handle_call({:generate_report, engine_name, opts}, _from, state) do
    case generate_comprehensive_report(engine_name, opts, state) do
      {:ok, report} ->
        new_stats = %{state.stats | reports_generated: state.stats.reports_generated + 1}
        {:reply, {:ok, report}, %{state | stats: new_stats}}

      error ->
        {:reply, error, state}
    end
  end

  @impl GenServer
  def handle_call({:get_aggregated_metrics, engine_name, start_time, end_time}, _from, state) do
    case MetricStorage.get_aggregated_metrics(
           state.storage_pid,
           engine_name,
           start_time,
           end_time
         ) do
      {:ok, metrics} ->
        {:reply, {:ok, metrics}, state}

      error ->
        {:reply, error, state}
    end
  end

  @impl GenServer
  def handle_call(:list_monitored_engines, _from, state) do
    engines = Map.keys(state.active_collections)
    {:reply, engines, state}
  end

  @impl GenServer
  def handle_call(:get_collector_stats, _from, state) do
    stats =
      Map.merge(state.stats, %{
        active_collections: map_size(state.active_collections),
        buffer_sizes: calculate_buffer_sizes(state.metric_buffers)
      })

    {:reply, stats, state}
  end

  @impl GenServer
  def handle_cast({:record_metric, engine_name, metric_type, data}, state) do
    case Map.get(state.active_collections, engine_name) do
      nil ->
        # Engine not being monitored, ignore
        {:noreply, state}

      collection_info ->
        new_state = process_metric_record(engine_name, metric_type, data, collection_info, state)
        {:noreply, new_state}
    end
  end

  @impl GenServer
  def handle_info({:collect_metrics, engine_name}, state) do
    case Map.get(state.active_collections, engine_name) do
      nil ->
        # Collection stopped, ignore
        {:noreply, state}

      collection_info ->
        new_state = perform_metric_collection(engine_name, collection_info, state)

        # Schedule next collection
        collection_interval = collection_info.collection_interval

        timer_ref =
          Process.send_after(self(), {:collect_metrics, engine_name}, collection_interval)

        updated_refs = Map.put(state.collection_refs, engine_name, timer_ref)
        {:noreply, %{new_state | collection_refs: updated_refs}}
    end
  end

  @impl GenServer
  def handle_info(:maintenance_cycle, state) do
    new_state = perform_maintenance(state)

    # Schedule next maintenance
    Process.send_after(self(), :maintenance_cycle, 60_000)

    {:noreply, new_state}
  end

  @impl GenServer
  def handle_info({:alert_triggered, engine_name, alert_info}, state) do
    Logger.warning("Performance alert triggered",
      engine: engine_name,
      alert: alert_info
    )

    new_stats = %{state.stats | alerts_triggered: state.stats.alerts_triggered + 1}
    {:noreply, %{state | stats: new_stats}}
  end

  ## Private functions

  defp start_engine_collection(engine_name, opts, state) do
    if map_size(state.active_collections) >= state.max_concurrent_collections do
      {:error, :max_collections_exceeded}
    else
      collection_info = %{
        engine_name: engine_name,
        metrics: normalize_metric_types(Keyword.get(opts, :metrics, :all)),
        collection_interval:
          Keyword.get(opts, :collection_interval, state.default_collection_interval),
        retention_period: Keyword.get(opts, :retention_period, state.default_retention_period),
        sampling_rate: Keyword.get(opts, :sampling_rate, 1.0),
        aggregation_window: Keyword.get(opts, :aggregation_window, 60_000),
        started_at: DateTime.utc_now(),
        last_collection: nil,
        collection_count: 0
      }

      # Start initial collection
      timer_ref = Process.send_after(self(), {:collect_metrics, engine_name}, 1000)

      new_active = Map.put(state.active_collections, engine_name, collection_info)
      new_refs = Map.put(state.collection_refs, engine_name, timer_ref)
      new_buffers = Map.put(state.metric_buffers, engine_name, [])

      new_stats = %{state.stats | active_collections: state.stats.active_collections + 1}

      new_state = %{
        state
        | active_collections: new_active,
          collection_refs: new_refs,
          metric_buffers: new_buffers,
          stats: new_stats
      }

      {:ok, new_state}
    end
  end

  defp stop_engine_collection(engine_name, state) do
    # Cancel collection timer
    case Map.get(state.collection_refs, engine_name) do
      nil -> :ok
      timer_ref -> Process.cancel_timer(timer_ref)
    end

    # Flush any remaining metrics
    flush_metric_buffer(engine_name, state)

    # Clean up state
    new_active = Map.delete(state.active_collections, engine_name)
    new_refs = Map.delete(state.collection_refs, engine_name)
    new_buffers = Map.delete(state.metric_buffers, engine_name)

    new_stats = %{state.stats | active_collections: state.stats.active_collections - 1}

    %{
      state
      | active_collections: new_active,
        collection_refs: new_refs,
        metric_buffers: new_buffers,
        stats: new_stats
    }
  end

  defp normalize_metric_types(:all) do
    [
      :rule_execution,
      :fact_operations,
      :memory_usage,
      :network_stats,
      :user_actions,
      :system_health,
      :conflict_resolution,
      :pattern_matching
    ]
  end

  defp normalize_metric_types(metrics) when is_list(metrics), do: metrics

  defp process_metric_record(engine_name, metric_type, data, collection_info, state) do
    # Apply sampling rate
    if should_sample?(collection_info.sampling_rate) do
      # Add to metric buffer
      timestamped_data =
        Map.merge(data, %{
          timestamp: DateTime.utc_now(),
          metric_type: metric_type,
          engine_name: engine_name
        })

      current_buffer = Map.get(state.metric_buffers, engine_name, [])
      new_buffer = [timestamped_data | current_buffer]

      # Check if buffer should be flushed
      new_buffers =
        if length(new_buffer) >= 100 do
          flush_buffer_to_storage(engine_name, new_buffer, state)
          Map.put(state.metric_buffers, engine_name, [])
        else
          Map.put(state.metric_buffers, engine_name, new_buffer)
        end

      new_stats = %{
        state.stats
        | total_metrics_collected: state.stats.total_metrics_collected + 1
      }

      %{state | metric_buffers: new_buffers, stats: new_stats}
    else
      state
    end
  end

  defp should_sample?(sampling_rate) when sampling_rate >= 1.0, do: true
  defp should_sample?(sampling_rate), do: :rand.uniform() <= sampling_rate

  defp perform_metric_collection(engine_name, collection_info, state) do
    try do
      # Collect metrics based on configured types
      collected_metrics =
        Enum.reduce(collection_info.metrics, %{}, fn metric_type, acc ->
          case collect_metric_type(engine_name, metric_type) do
            {:ok, metrics} ->
              Map.put(acc, metric_type, metrics)

            {:error, reason} ->
              Logger.warning("Failed to collect metric",
                engine: engine_name,
                metric_type: metric_type,
                reason: inspect(reason)
              )

              acc
          end
        end)

      # Store collected metrics
      MetricStorage.store_metrics(state.storage_pid, engine_name, collected_metrics)

      # Check for alerts
      AlertManager.check_thresholds(state.alert_pid, engine_name, collected_metrics)

      # Update collection info
      updated_info = %{
        collection_info
        | last_collection: DateTime.utc_now(),
          collection_count: collection_info.collection_count + 1
      }

      new_active = Map.put(state.active_collections, engine_name, updated_info)
      %{state | active_collections: new_active}
    rescue
      error ->
        Logger.error("Metrics collection failed",
          engine: engine_name,
          error: inspect(error)
        )

        new_stats = %{state.stats | collection_errors: state.stats.collection_errors + 1}
        %{state | stats: new_stats}
    end
  end

  defp collect_metric_type(engine_name, :rule_execution) do
    case Presto.EngineRegistry.lookup_engine(engine_name) do
      {:ok, engine_pid} ->
        # Get rule execution metrics from engine
        try do
          metrics = Presto.RuleEngine.get_execution_metrics(engine_pid)
          {:ok, metrics}
        rescue
          _ -> {:error, :engine_not_responsive}
        end

      _ ->
        {:error, :engine_not_found}
    end
  end

  defp collect_metric_type(engine_name, :memory_usage) do
    case Presto.EngineRegistry.lookup_engine(engine_name) do
      {:ok, engine_pid} ->
        try do
          process_info = Process.info(engine_pid, [:memory, :heap_size, :stack_size])

          {:ok,
           %{
             memory: process_info[:memory],
             heap_size: process_info[:heap_size],
             stack_size: process_info[:stack_size],
             timestamp: DateTime.utc_now()
           }}
        rescue
          _ -> {:error, :process_info_failed}
        end

      _ ->
        {:error, :engine_not_found}
    end
  end

  defp collect_metric_type(engine_name, :fact_operations) do
    case Presto.EngineRegistry.lookup_engine(engine_name) do
      {:ok, engine_pid} ->
        try do
          # Get fact operation statistics
          stats = Presto.RuleEngine.get_fact_statistics(engine_pid)
          {:ok, stats}
        rescue
          _ -> {:error, :engine_not_responsive}
        end

      _ ->
        {:error, :engine_not_found}
    end
  end

  defp collect_metric_type(_engine_name, metric_type) do
    # Placeholder for other metric types
    {:ok,
     %{
       metric_type: metric_type,
       placeholder: true,
       timestamp: DateTime.utc_now()
     }}
  end

  defp flush_buffer_to_storage(engine_name, buffer, state) do
    MetricStorage.store_metric_batch(state.storage_pid, engine_name, buffer)
  end

  defp flush_metric_buffer(engine_name, state) do
    case Map.get(state.metric_buffers, engine_name) do
      nil -> :ok
      [] -> :ok
      buffer -> flush_buffer_to_storage(engine_name, buffer, state)
    end
  end

  defp generate_comprehensive_report(engine_name, opts, state) do
    period = determine_report_period(opts)
    format = Keyword.get(opts, :format, :summary)

    try do
      # Get metrics for the period
      {start_time, end_time} = period

      {:ok, metrics} =
        MetricStorage.get_aggregated_metrics(
          state.storage_pid,
          engine_name,
          start_time,
          end_time
        )

      # Get performance insights
      {:ok, insights} =
        PerformanceAnalyzer.analyze_engine_performance(
          state.analyzer_pid,
          engine_name
        )

      # Generate recommendations
      recommendations = generate_recommendations(metrics, insights)

      report = %{
        engine_name: engine_name,
        period: {start_time, end_time},
        summary: generate_summary(metrics),
        detailed_metrics: if(format == :detailed, do: metrics, else: %{}),
        insights: insights,
        recommendations: recommendations,
        generated_at: DateTime.utc_now()
      }

      {:ok, report}
    rescue
      error ->
        {:error, {:report_generation_failed, error}}
    end
  end

  defp determine_report_period(opts) do
    case Keyword.get(opts, :period, :last_hour) do
      :last_hour ->
        end_time = DateTime.utc_now()
        start_time = DateTime.add(end_time, -3600, :second)
        {start_time, end_time}

      :last_day ->
        end_time = DateTime.utc_now()
        start_time = DateTime.add(end_time, -86400, :second)
        {start_time, end_time}

      {start_time, end_time} ->
        {start_time, end_time}
    end
  end

  defp generate_summary(metrics) do
    %{
      total_rule_executions: get_metric_value(metrics, [:rule_execution, :total_executions], 0),
      avg_execution_time: get_metric_value(metrics, [:rule_execution, :avg_execution_time], 0),
      memory_usage_mb: get_metric_value(metrics, [:memory_usage, :memory], 0) / (1024 * 1024),
      fact_operations: get_metric_value(metrics, [:fact_operations, :total_operations], 0),
      health_score: calculate_health_score(metrics)
    }
  end

  defp get_metric_value(metrics, path, default) do
    Enum.reduce(path, metrics, fn key, acc ->
      case acc do
        map when is_map(map) -> Map.get(map, key, default)
        _ -> default
      end
    end)
  end

  defp calculate_health_score(metrics) do
    # Simple health scoring algorithm
    # In a real implementation, this would be more sophisticated
    base_score = 100

    # Deduct points for high execution times
    avg_time = get_metric_value(metrics, [:rule_execution, :avg_execution_time], 0)
    # Max 30 point penalty
    time_penalty = min(avg_time / 10, 30)

    # Deduct points for high memory usage
    memory_mb = get_metric_value(metrics, [:memory_usage, :memory], 0) / (1024 * 1024)
    # Max 20 point penalty
    memory_penalty = min(memory_mb / 100, 20)

    max(base_score - time_penalty - memory_penalty, 0)
  end

  defp generate_recommendations(metrics, insights) do
    recommendations = []

    # High execution time recommendation
    avg_time = get_metric_value(metrics, [:rule_execution, :avg_execution_time], 0)

    recommendations =
      if avg_time > 100 do
        ["Consider optimising rule conditions for better performance" | recommendations]
      else
        recommendations
      end

    # High memory usage recommendation
    memory_mb = get_metric_value(metrics, [:memory_usage, :memory], 0) / (1024 * 1024)

    recommendations =
      if memory_mb > 500 do
        ["Review fact retention policies to reduce memory usage" | recommendations]
      else
        recommendations
      end

    # Add insight-based recommendations
    insight_recommendations =
      insights
      |> Enum.filter(fn insight -> insight.insight_type == :recommendation end)
      |> Enum.map(fn insight -> insight.description end)

    recommendations ++ insight_recommendations
  end

  defp perform_maintenance(state) do
    # Clean up old metrics
    MetricStorage.cleanup_old_metrics(state.storage_pid)

    # Flush any pending metric buffers
    Enum.each(state.metric_buffers, fn {engine_name, buffer} ->
      if length(buffer) > 0 do
        flush_buffer_to_storage(engine_name, buffer, state)
      end
    end)

    # Clear buffers
    new_buffers = Map.new(state.metric_buffers, fn {k, _v} -> {k, []} end)

    %{state | metric_buffers: new_buffers}
  end

  defp calculate_buffer_sizes(metric_buffers) do
    Map.new(metric_buffers, fn {engine_name, buffer} ->
      {engine_name, length(buffer)}
    end)
  end
end

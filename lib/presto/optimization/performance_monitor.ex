defmodule Presto.Optimization.PerformanceMonitor do
  @moduledoc """
  Performance monitoring for RETE optimization features.

  This module tracks the effectiveness of optimization features including
  shared memory, join ordering, and advanced indexing. It provides metrics,
  alerts, and recommendations for optimization tuning.
  """

  use GenServer

  @type optimization_metrics :: %{
          shared_memory: shared_memory_metrics(),
          join_ordering: join_ordering_metrics(),
          advanced_indexing: indexing_metrics(),
          overall: overall_metrics()
        }

  @type shared_memory_metrics :: %{
          total_shared_memories: non_neg_integer(),
          memory_savings_bytes: non_neg_integer(),
          memory_savings_percentage: float(),
          cache_hit_rate: float(),
          average_reuse_count: float()
        }

  @type join_ordering_metrics :: %{
          optimizations_applied: non_neg_integer(),
          average_improvement: float(),
          best_improvement: float(),
          worst_improvement: float(),
          selectivity_accuracy: float()
        }

  @type indexing_metrics :: %{
          active_indexes: non_neg_integer(),
          index_hit_rate: float(),
          average_lookup_time: float(),
          index_rebuilds: non_neg_integer(),
          memory_overhead: non_neg_integer()
        }

  @type overall_metrics :: %{
          total_join_operations: non_neg_integer(),
          average_join_time: float(),
          optimization_effectiveness: float(),
          memory_efficiency: float(),
          performance_trend: :improving | :stable | :degrading
        }

  @type performance_alert :: %{
          type: :memory_pressure | :performance_degradation | :optimization_failure,
          severity: :low | :medium | :high | :critical,
          message: String.t(),
          timestamp: integer(),
          metrics: map()
        }

  @type active_optimizations :: MapSet.t(atom())
  @type alert_set :: MapSet.t(String.t())

  # Client API

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @spec record_shared_memory_operation(atom(), map()) :: :ok
  def record_shared_memory_operation(operation, metrics) do
    GenServer.cast(__MODULE__, {:record_operation, :shared_memory, operation, metrics})
  end

  @spec record_join_ordering_operation(map()) :: :ok
  def record_join_ordering_operation(metrics) do
    GenServer.cast(__MODULE__, {:record_operation, :join_ordering, :optimization, metrics})
  end

  @spec record_indexing_operation(atom(), map()) :: :ok
  def record_indexing_operation(operation, metrics) do
    GenServer.cast(__MODULE__, {:record_operation, :advanced_indexing, operation, metrics})
  end

  @spec record_join_performance(atom(), integer(), integer()) :: :ok
  def record_join_performance(join_type, execution_time_us, result_count) do
    metrics = %{
      join_type: join_type,
      execution_time_us: execution_time_us,
      result_count: result_count,
      timestamp: System.monotonic_time(:microsecond)
    }

    GenServer.cast(__MODULE__, {:record_operation, :overall, :join_performance, metrics})
  end

  @spec get_current_metrics() :: optimization_metrics()
  def get_current_metrics do
    GenServer.call(__MODULE__, :get_current_metrics)
  end

  @spec get_performance_alerts() :: [performance_alert()]
  def get_performance_alerts do
    GenServer.call(__MODULE__, :get_performance_alerts)
  end

  @spec get_optimization_recommendations() :: [String.t()]
  def get_optimization_recommendations do
    GenServer.call(__MODULE__, :get_optimization_recommendations)
  end

  @spec reset_metrics() :: :ok
  def reset_metrics do
    GenServer.call(__MODULE__, :reset_metrics)
  end

  @spec generate_performance_report() :: %{
          summary: map(),
          detailed_metrics: optimization_metrics(),
          alerts: [performance_alert()],
          recommendations: [String.t()],
          trends: map()
        }
  def generate_performance_report do
    GenServer.call(__MODULE__, :generate_performance_report)
  end

  # Server implementation

  @impl true
  def init(opts) do
    # 30 seconds
    monitoring_interval = Keyword.get(opts, :monitoring_interval, 30_000)
    alert_threshold = Keyword.get(opts, :alert_threshold, %{})

    state = %{
      # Metrics storage
      metrics: init_metrics(),
      # Historical data for trend analysis
      historical_data: :ets.new(:perf_history, [:ordered_set, :private]),
      # Active alerts (using MapSet for efficient operations)
      alerts: MapSet.new(),
      # Alert history for deduplication
      alert_history: MapSet.new(),
      # Active optimization types (using MapSet for efficient membership testing)
      active_optimizations: MapSet.new(),
      # Configuration
      config: %{
        monitoring_interval: monitoring_interval,
        alert_thresholds: Map.merge(default_alert_thresholds(), alert_threshold),
        max_history_entries: 1000,
        # Limit number of active alerts
        max_alerts: 50
      },
      # Runtime state
      last_analysis: System.monotonic_time(:millisecond),
      monitoring_timer: nil
    }

    # Schedule periodic monitoring
    timer = Process.send_after(self(), :monitor_performance, monitoring_interval)
    state = %{state | monitoring_timer: timer}

    {:ok, state}
  end

  @impl true
  def handle_call(:get_current_metrics, _from, state) do
    {:reply, state.metrics, state}
  end

  @impl true
  def handle_call(:get_performance_alerts, _from, state) do
    # Convert MapSet to list for API compatibility
    alert_list = MapSet.to_list(state.alerts)
    {:reply, alert_list, state}
  end

  @impl true
  def handle_call(:get_optimization_recommendations, _from, state) do
    recommendations = generate_recommendations(state.metrics, state.alerts)
    {:reply, recommendations, state}
  end

  @impl true
  def handle_call(:reset_metrics, _from, state) do
    new_state = %{
      state
      | metrics: init_metrics(),
        alerts: MapSet.new(),
        alert_history: MapSet.new(),
        active_optimizations: MapSet.new()
    }

    :ets.delete_all_objects(state.historical_data)
    {:reply, :ok, new_state}
  end

  @impl true
  def handle_call(:generate_performance_report, _from, state) do
    report = %{
      summary: generate_summary(state.metrics, state.active_optimizations),
      detailed_metrics: state.metrics,
      alerts: MapSet.to_list(state.alerts),
      recommendations: generate_recommendations(state.metrics, state.alerts),
      trends: analyze_trends(state.historical_data),
      active_optimizations: MapSet.to_list(state.active_optimizations)
    }

    {:reply, report, state}
  end

  @impl true
  def handle_cast({:record_operation, optimization_type, operation, metrics}, state) do
    updated_metrics = update_metrics(state.metrics, optimization_type, operation, metrics)

    # Track active optimization types using MapSet for efficient operations
    updated_optimizations = MapSet.put(state.active_optimizations, optimization_type)

    new_state = %{state | metrics: updated_metrics, active_optimizations: updated_optimizations}
    {:noreply, new_state}
  end

  @impl true
  def handle_info(:monitor_performance, state) do
    # Perform periodic monitoring tasks
    new_state = perform_monitoring_analysis(state)

    # Schedule next monitoring cycle
    timer = Process.send_after(self(), :monitor_performance, state.config.monitoring_interval)
    new_state = %{new_state | monitoring_timer: timer}

    {:noreply, new_state}
  end

  @impl true
  def terminate(_reason, state) do
    if state.monitoring_timer do
      Process.cancel_timer(state.monitoring_timer)
    end

    :ets.delete(state.historical_data)
    :ok
  end

  # Private functions

  defp init_metrics do
    %{
      shared_memory: %{
        total_shared_memories: 0,
        memory_savings_bytes: 0,
        memory_savings_percentage: 0.0,
        cache_hit_rate: 0.0,
        average_reuse_count: 0.0
      },
      join_ordering: %{
        optimizations_applied: 0,
        average_improvement: 0.0,
        best_improvement: 0.0,
        worst_improvement: 0.0,
        selectivity_accuracy: 0.0
      },
      advanced_indexing: %{
        active_indexes: 0,
        index_hit_rate: 0.0,
        average_lookup_time: 0.0,
        index_rebuilds: 0,
        memory_overhead: 0
      },
      overall: %{
        total_join_operations: 0,
        average_join_time: 0.0,
        optimization_effectiveness: 0.0,
        memory_efficiency: 0.0,
        performance_trend: :stable
      }
    }
  end

  defp default_alert_thresholds do
    %{
      # Minimum % memory savings expected
      memory_savings_threshold: 20.0,
      # Minimum % cache hit rate
      cache_hit_rate_threshold: 70.0,
      # Maximum microseconds per join
      join_time_threshold: 10_000,
      # Minimum % improvement
      optimization_effectiveness_threshold: 50.0,
      # Maximum % memory usage
      memory_pressure_threshold: 80.0
    }
  end

  defp update_metrics(current_metrics, optimization_type, operation, operation_metrics) do
    case optimization_type do
      :shared_memory ->
        update_shared_memory_metrics(current_metrics, operation, operation_metrics)

      :join_ordering ->
        update_join_ordering_metrics(current_metrics, operation_metrics)

      :advanced_indexing ->
        update_indexing_metrics(current_metrics, operation, operation_metrics)

      :overall ->
        update_overall_metrics(current_metrics, operation, operation_metrics)
    end
  end

  defp update_shared_memory_metrics(metrics, operation, op_metrics) do
    shared_memory = metrics.shared_memory

    updated_shared_memory =
      case operation do
        :cache_hit ->
          update_cache_hit_rate(shared_memory, true)

        :cache_miss ->
          update_cache_hit_rate(shared_memory, false)

        :memory_saved ->
          %{
            shared_memory
            | memory_savings_bytes:
                shared_memory.memory_savings_bytes + Map.get(op_metrics, :bytes_saved, 0),
              memory_savings_percentage:
                calculate_memory_savings_percentage(shared_memory, op_metrics)
          }

        :shared_memory_created ->
          %{shared_memory | total_shared_memories: shared_memory.total_shared_memories + 1}

        _ ->
          shared_memory
      end

    %{metrics | shared_memory: updated_shared_memory}
  end

  defp update_join_ordering_metrics(metrics, op_metrics) do
    join_ordering = metrics.join_ordering
    improvement = Map.get(op_metrics, :improvement_percentage, 0.0)

    updated_join_ordering = %{
      join_ordering
      | optimizations_applied: join_ordering.optimizations_applied + 1,
        average_improvement:
          calculate_running_average(
            join_ordering.average_improvement,
            improvement,
            join_ordering.optimizations_applied
          ),
        best_improvement: max(join_ordering.best_improvement, improvement),
        worst_improvement:
          if(join_ordering.worst_improvement == 0.0,
            do: improvement,
            else: min(join_ordering.worst_improvement, improvement)
          )
    }

    %{metrics | join_ordering: updated_join_ordering}
  end

  defp update_indexing_metrics(metrics, operation, op_metrics) do
    indexing = metrics.advanced_indexing

    updated_indexing =
      case operation do
        :index_lookup ->
          lookup_time = Map.get(op_metrics, :lookup_time_us, 0.0)
          hit = Map.get(op_metrics, :hit, false)

          %{
            indexing
            | index_hit_rate: update_hit_rate(indexing.index_hit_rate, hit),
              average_lookup_time:
                calculate_running_average(
                  indexing.average_lookup_time,
                  lookup_time,
                  Map.get(op_metrics, :total_lookups, 1)
                )
          }

        :index_created ->
          %{
            indexing
            | active_indexes: indexing.active_indexes + 1,
              memory_overhead: indexing.memory_overhead + Map.get(op_metrics, :memory_size, 0)
          }

        :index_rebuilt ->
          %{indexing | index_rebuilds: indexing.index_rebuilds + 1}

        _ ->
          indexing
      end

    %{metrics | advanced_indexing: updated_indexing}
  end

  defp update_overall_metrics(metrics, operation, op_metrics) do
    overall = metrics.overall

    updated_overall =
      case operation do
        :join_performance ->
          join_time = Map.get(op_metrics, :execution_time_us, 0.0)

          %{
            overall
            | total_join_operations: overall.total_join_operations + 1,
              average_join_time:
                calculate_running_average(
                  overall.average_join_time,
                  join_time,
                  overall.total_join_operations
                )
          }

        _ ->
          overall
      end

    %{metrics | overall: updated_overall}
  end

  defp calculate_running_average(current_avg, new_value, count) when count > 0 do
    (current_avg * (count - 1) + new_value) / count
  end

  defp calculate_running_average(_current_avg, new_value, _count), do: new_value

  defp update_cache_hit_rate(shared_memory, hit) do
    # Simple running average for cache hit rate
    # In production, might want to use a more sophisticated method
    current_rate = shared_memory.cache_hit_rate
    new_rate = if hit, do: current_rate + 0.1, else: current_rate - 0.1
    %{shared_memory | cache_hit_rate: max(0.0, min(100.0, new_rate))}
  end

  defp update_hit_rate(current_rate, hit) do
    # Simple hit rate update
    adjustment = if hit, do: 0.1, else: -0.1
    max(0.0, min(100.0, current_rate + adjustment))
  end

  defp calculate_memory_savings_percentage(shared_memory, op_metrics) do
    total_memory = Map.get(op_metrics, :total_memory_used, 1)

    if total_memory > 0 do
      shared_memory.memory_savings_bytes / total_memory * 100.0
    else
      shared_memory.memory_savings_percentage
    end
  end

  defp perform_monitoring_analysis(state) do
    # Store current metrics in historical data
    timestamp = System.monotonic_time(:millisecond)
    :ets.insert(state.historical_data, {timestamp, state.metrics})

    # Clean up old historical data
    cleanup_old_history(state.historical_data, state.config.max_history_entries)

    # Check for performance alerts
    new_alerts = check_for_alerts(state.metrics, state.config.alert_thresholds)

    # Update performance trends
    updated_metrics = update_performance_trends(state.metrics, state.historical_data)

    # Merge new alerts with existing ones using MapSet for efficient deduplication
    updated_alerts =
      add_new_alerts(state.alerts, state.alert_history, new_alerts, state.config.max_alerts)

    updated_alert_history = update_alert_history(state.alert_history, new_alerts)

    %{
      state
      | metrics: updated_metrics,
        alerts: updated_alerts,
        alert_history: updated_alert_history,
        last_analysis: timestamp
    }
  end

  defp cleanup_old_history(history_table, max_entries) do
    current_size = :ets.info(history_table, :size)

    if current_size > max_entries do
      # Remove oldest entries
      excess = current_size - max_entries

      oldest_keys =
        :ets.select(history_table, [{{:"$1", :"$2"}, [], [:"$1"]}])
        |> Enum.sort()
        |> Enum.take(excess)

      Enum.each(oldest_keys, &:ets.delete(history_table, &1))
    end
  end

  defp check_for_alerts(metrics, thresholds) do
    alerts = []

    # Check memory savings alert
    alerts =
      if metrics.shared_memory.memory_savings_percentage < thresholds.memory_savings_threshold do
        [
          create_alert(
            :memory_pressure,
            :medium,
            "Memory savings below threshold: #{metrics.shared_memory.memory_savings_percentage}%",
            metrics.shared_memory
          )
          | alerts
        ]
      else
        alerts
      end

    # Check cache hit rate alert
    alerts =
      if metrics.shared_memory.cache_hit_rate < thresholds.cache_hit_rate_threshold do
        [
          create_alert(
            :performance_degradation,
            :medium,
            "Cache hit rate below threshold: #{metrics.shared_memory.cache_hit_rate}%",
            metrics.shared_memory
          )
          | alerts
        ]
      else
        alerts
      end

    # Check join time alert
    alerts =
      if metrics.overall.average_join_time > thresholds.join_time_threshold do
        [
          create_alert(
            :performance_degradation,
            :high,
            "Average join time above threshold: #{metrics.overall.average_join_time}μs",
            metrics.overall
          )
          | alerts
        ]
      else
        alerts
      end

    alerts
  end

  defp add_new_alerts(current_alerts, alert_history, new_alerts, max_alerts) do
    # Create alert signatures for deduplication
    new_alert_set =
      Enum.reduce(new_alerts, MapSet.new(), fn alert, acc ->
        alert_signature = create_alert_signature(alert)

        if MapSet.member?(alert_history, alert_signature) do
          acc
        else
          MapSet.put(acc, alert)
        end
      end)

    # Merge with current alerts and limit size
    updated_alerts = MapSet.union(current_alerts, new_alert_set)

    # If we exceed max alerts, keep only the most recent/severe ones
    if MapSet.size(updated_alerts) > max_alerts do
      updated_alerts
      |> MapSet.to_list()
      |> Enum.sort_by(fn alert -> {alert.severity, alert.timestamp} end, :desc)
      |> Enum.take(max_alerts)
      |> MapSet.new()
    else
      updated_alerts
    end
  end

  defp update_alert_history(alert_history, new_alerts) do
    Enum.reduce(new_alerts, alert_history, fn alert, acc ->
      alert_signature = create_alert_signature(alert)
      MapSet.put(acc, alert_signature)
    end)
  end

  defp create_alert_signature(alert) do
    # Create a unique signature based on alert type and message core
    # This prevents duplicate alerts for the same issue
    {alert.type, alert.message}
  end

  defp create_alert(type, severity, message, metrics) do
    %{
      type: type,
      severity: severity,
      message: message,
      timestamp: System.monotonic_time(:millisecond),
      metrics: metrics
    }
  end

  defp update_performance_trends(metrics, history_table) do
    # Analyze recent performance trend
    recent_entries =
      :ets.select(history_table, [{{:"$1", :"$2"}, [], [:"$2"]}])
      # Last 10 entries
      |> Enum.take(-10)

    trend =
      if length(recent_entries) >= 3 do
        analyze_performance_trend(recent_entries)
      else
        :stable
      end

    put_in(metrics, [:overall, :performance_trend], trend)
  end

  defp analyze_performance_trend(recent_metrics) do
    # Simple trend analysis based on average join time
    join_times = Enum.map(recent_metrics, & &1.overall.average_join_time)

    if length(join_times) < 3 do
      :stable
    else
      # Calculate trend direction
      first_half_avg = Enum.take(join_times, div(length(join_times), 2)) |> average()
      second_half_avg = Enum.drop(join_times, div(length(join_times), 2)) |> average()

      trend_ratio = second_half_avg / max(first_half_avg, 1.0)

      cond do
        trend_ratio < 0.95 -> :improving
        trend_ratio > 1.05 -> :degrading
        true -> :stable
      end
    end
  end

  defp average([]), do: 0.0
  defp average(list), do: Enum.sum(list) / length(list)

  defp generate_summary(metrics, active_optimizations) do
    %{
      total_optimizations:
        metrics.shared_memory.total_shared_memories +
          metrics.join_ordering.optimizations_applied +
          metrics.advanced_indexing.active_indexes,
      overall_effectiveness: calculate_overall_effectiveness(metrics),
      memory_efficiency: metrics.shared_memory.memory_savings_percentage,
      performance_status: metrics.overall.performance_trend,
      active_optimization_count: MapSet.size(active_optimizations),
      optimization_types_used: MapSet.to_list(active_optimizations)
    }
  end

  defp calculate_overall_effectiveness(metrics) do
    # Weighted average of different optimization effectiveness metrics
    shared_memory_weight = 0.3
    join_ordering_weight = 0.4
    indexing_weight = 0.3

    shared_memory_effectiveness = metrics.shared_memory.cache_hit_rate
    join_ordering_effectiveness = metrics.join_ordering.average_improvement
    indexing_effectiveness = metrics.advanced_indexing.index_hit_rate

    shared_memory_effectiveness * shared_memory_weight +
      join_ordering_effectiveness * join_ordering_weight +
      indexing_effectiveness * indexing_weight
  end

  defp generate_recommendations(metrics, alerts) do
    recommendations = []

    # Memory optimization recommendations
    recommendations =
      if metrics.shared_memory.memory_savings_percentage < 20.0 do
        [
          "Consider increasing shared memory thresholds to improve memory efficiency"
          | recommendations
        ]
      else
        recommendations
      end

    # Join ordering recommendations
    recommendations =
      if metrics.join_ordering.average_improvement < 10.0 do
        [
          "Review join ordering configuration - improvements are below expected levels"
          | recommendations
        ]
      else
        recommendations
      end

    # Indexing recommendations
    recommendations =
      if metrics.advanced_indexing.index_hit_rate < 70.0 do
        ["Consider adjusting indexing strategies to improve hit rates" | recommendations]
      else
        recommendations
      end

    # Alert-based recommendations
    alert_recommendations =
      Enum.map(alerts, fn alert ->
        case alert.type do
          :memory_pressure -> "Increase memory allocation or optimize memory usage patterns"
          :performance_degradation -> "Review optimization configurations and data patterns"
          :optimization_failure -> "Check optimization implementation and error logs"
        end
      end)

    recommendations ++ alert_recommendations
  end

  defp analyze_trends(history_table) do
    # Comprehensive trend analysis
    all_entries =
      :ets.select(history_table, [{{:"$1", :"$2"}, [], [:"$_"]}])
      |> Enum.sort_by(fn {timestamp, _} -> timestamp end)

    # Keep using length for ETS results (list)
    entry_count = length(all_entries)

    if entry_count < 5 do
      %{insufficient_data: true}
    else
      %{
        memory_trend:
          analyze_metric_trend(all_entries, [:shared_memory, :memory_savings_percentage]),
        performance_trend: analyze_metric_trend(all_entries, [:overall, :average_join_time]),
        optimization_trend:
          analyze_metric_trend(all_entries, [:join_ordering, :average_improvement])
      }
    end
  end

  defp analyze_metric_trend(entries, metric_path) do
    values =
      Enum.map(entries, fn {_timestamp, metrics} ->
        get_in(metrics, metric_path) || 0.0
      end)

    if length(values) < 3 do
      :insufficient_data
    else
      first_third = Enum.take(values, div(length(values), 3)) |> average()
      last_third = Enum.drop(values, -div(length(values), 3)) |> average()

      trend_ratio = last_third / max(first_third, 0.1)

      cond do
        trend_ratio < 0.9 -> :declining
        trend_ratio > 1.1 -> :improving
        true -> :stable
      end
    end
  end
end

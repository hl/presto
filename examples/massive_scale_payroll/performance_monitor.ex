defmodule Examples.MassiveScalePayroll.PerformanceMonitor do
  @moduledoc """
  Real-time performance monitoring and metrics tracking for massive scale payroll processing.

  This module provides comprehensive performance tracking:
  - Processing rates and throughput metrics
  - Memory usage monitoring  
  - Rule execution statistics
  - Bottleneck identification
  - Resource utilization tracking

  Works independently of Presto to monitor the overall system performance
  while Presto handles the rule processing.
  """

  use GenServer

  @type performance_metrics :: %{
          employees_per_minute: float(),
          segments_per_minute: float(),
          rules_executed_per_second: float(),
          average_processing_time_ms: float(),
          memory_usage_mb: float(),
          cpu_utilization_percent: float()
        }

  @type bottleneck_analysis :: %{
          bottleneck_type: :cpu | :memory | :io | :rule_engine | :network,
          severity: :low | :medium | :high | :critical,
          description: String.t(),
          recommended_action: String.t()
        }

  @type monitoring_state :: %{
          run_id: String.t(),
          start_time: DateTime.t(),
          metrics_history: [performance_metrics()],
          bottlenecks: [bottleneck_analysis()],
          employee_processing_times: [float()],
          rule_execution_stats: map(),
          memory_samples: [float()],
          last_metrics_update: DateTime.t()
        }

  # Monitoring configuration
  # Collect metrics every 5 seconds
  @metrics_interval_ms 5_000
  # Keep 60 minutes of history
  @history_retention_minutes 60
  # Warning at 3GB
  @memory_warning_threshold_mb 3_000
  # Critical at 4.5GB
  @memory_critical_threshold_mb 4_500

  # Client API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Starts monitoring for a payroll run.
  """
  @spec start_monitoring(pid(), String.t(), map()) :: :ok
  def start_monitoring(monitor_pid, run_id, run_config) do
    GenServer.call(monitor_pid, {:start_monitoring, run_id, run_config})
  end

  @doc """
  Records employee processing completion.
  """
  @spec record_employee_processed(String.t(), float(), non_neg_integer()) :: :ok
  def record_employee_processed(run_id, processing_time_ms, segments_count \\ 0) do
    GenServer.cast(__MODULE__, {:employee_processed, run_id, processing_time_ms, segments_count})
  end

  @doc """
  Records rule execution statistics.
  """
  @spec record_rule_execution(String.t(), String.t(), float(), non_neg_integer()) :: :ok
  def record_rule_execution(run_id, rule_name, execution_time_ms, facts_matched) do
    GenServer.cast(
      __MODULE__,
      {:rule_executed, run_id, rule_name, execution_time_ms, facts_matched}
    )
  end

  @doc """
  Gets current performance metrics.
  """
  @spec get_current_metrics(pid(), String.t()) ::
          {:ok, performance_metrics()} | {:error, :not_found}
  def get_current_metrics(monitor_pid, run_id) do
    GenServer.call(monitor_pid, {:get_current_metrics, run_id})
  end

  @doc """
  Gets bottleneck analysis.
  """
  @spec get_bottleneck_analysis(pid(), String.t()) ::
          {:ok, [bottleneck_analysis()]} | {:error, :not_found}
  def get_bottleneck_analysis(monitor_pid, run_id) do
    GenServer.call(monitor_pid, {:get_bottleneck_analysis, run_id})
  end

  @doc """
  Finalizes monitoring and generates performance report.
  """
  @spec finalize_monitoring(pid(), String.t()) :: {:ok, map()} | {:error, term()}
  def finalize_monitoring(monitor_pid, run_id) do
    GenServer.call(monitor_pid, {:finalize_monitoring, run_id})
  end

  # Server implementation

  @impl true
  def init(_opts) do
    # Start periodic metrics collection
    schedule_metrics_collection()

    state = %{
      active_monitoring: %{},
      completed_monitoring: %{},
      system_metrics: %{
        cpu_samples: [],
        memory_samples: [],
        last_system_check: DateTime.utc_now()
      }
    }

    {:ok, state}
  end

  @impl true
  def handle_call({:start_monitoring, run_id, run_config}, _from, state) do
    if Map.has_key?(state.active_monitoring, run_id) do
      {:reply, {:error, :already_monitoring}, state}
    else
      monitoring_state = %{
        run_id: run_id,
        run_config: run_config,
        start_time: DateTime.utc_now(),
        metrics_history: [],
        bottlenecks: [],
        employee_processing_times: [],
        rule_execution_stats: %{},
        memory_samples: [],
        last_metrics_update: DateTime.utc_now(),
        employees_processed: 0,
        total_segments_processed: 0,
        total_rules_executed: 0
      }

      new_state = put_in(state, [:active_monitoring, run_id], monitoring_state)
      {:reply, :ok, new_state}
    end
  end

  @impl true
  def handle_call({:get_current_metrics, run_id}, _from, state) do
    case Map.get(state.active_monitoring, run_id) do
      nil ->
        {:reply, {:error, :not_found}, state}

      monitoring_state ->
        metrics = calculate_current_metrics(monitoring_state)
        {:reply, {:ok, metrics}, state}
    end
  end

  @impl true
  def handle_call({:get_bottleneck_analysis, run_id}, _from, state) do
    case Map.get(state.active_monitoring, run_id) do
      nil ->
        {:reply, {:error, :not_found}, state}

      monitoring_state ->
        bottlenecks = analyze_bottlenecks(monitoring_state, state.system_metrics)
        {:reply, {:ok, bottlenecks}, state}
    end
  end

  @impl true
  def handle_call({:finalize_monitoring, run_id}, _from, state) do
    case Map.get(state.active_monitoring, run_id) do
      nil ->
        {:reply, {:error, :not_found}, state}

      monitoring_state ->
        # Generate final performance report
        performance_report = generate_performance_report(monitoring_state, state.system_metrics)

        # Move to completed monitoring
        completed_state = Map.put(monitoring_state, :status, :completed)

        new_state =
          state
          |> Map.update!(:active_monitoring, &Map.delete(&1, run_id))
          |> Map.update!(:completed_monitoring, &Map.put(&1, run_id, completed_state))

        {:reply, {:ok, performance_report}, new_state}
    end
  end

  @impl true
  def handle_cast({:employee_processed, run_id, processing_time_ms, segments_count}, state) do
    case Map.get(state.active_monitoring, run_id) do
      nil ->
        {:noreply, state}

      monitoring_state ->
        updated_state =
          monitoring_state
          |> Map.update!(:employee_processing_times, &[processing_time_ms | &1])
          |> Map.update!(:employees_processed, &(&1 + 1))
          |> Map.update!(:total_segments_processed, &(&1 + segments_count))

        new_state = put_in(state, [:active_monitoring, run_id], updated_state)
        {:noreply, new_state}
    end
  end

  @impl true
  def handle_cast({:rule_executed, run_id, rule_name, execution_time_ms, facts_matched}, state) do
    case Map.get(state.active_monitoring, run_id) do
      nil ->
        {:noreply, state}

      monitoring_state ->
        # Update rule execution statistics
        rule_stats =
          Map.get(monitoring_state.rule_execution_stats, rule_name, %{
            execution_count: 0,
            total_execution_time_ms: 0.0,
            total_facts_matched: 0,
            average_execution_time_ms: 0.0
          })

        updated_rule_stats = %{
          execution_count: rule_stats.execution_count + 1,
          total_execution_time_ms: rule_stats.total_execution_time_ms + execution_time_ms,
          total_facts_matched: rule_stats.total_facts_matched + facts_matched,
          average_execution_time_ms:
            (rule_stats.total_execution_time_ms + execution_time_ms) /
              (rule_stats.execution_count + 1)
        }

        updated_monitoring_state =
          monitoring_state
          |> Map.update!(:rule_execution_stats, &Map.put(&1, rule_name, updated_rule_stats))
          |> Map.update!(:total_rules_executed, &(&1 + 1))

        new_state = put_in(state, [:active_monitoring, run_id], updated_monitoring_state)
        {:noreply, new_state}
    end
  end

  @impl true
  def handle_info(:collect_system_metrics, state) do
    # Collect system-wide metrics
    system_metrics = collect_system_metrics(state.system_metrics)

    # Update all active monitoring with current system metrics
    updated_active_monitoring =
      Enum.reduce(state.active_monitoring, %{}, fn {run_id, monitoring_state}, acc ->
        updated_state = update_monitoring_with_system_metrics(monitoring_state, system_metrics)
        Map.put(acc, run_id, updated_state)
      end)

    new_state = %{
      state
      | active_monitoring: updated_active_monitoring,
        system_metrics: system_metrics
    }

    # Schedule next collection
    schedule_metrics_collection()

    {:noreply, new_state}
  end

  # Private helper functions

  defp schedule_metrics_collection do
    Process.send_after(self(), :collect_system_metrics, @metrics_interval_ms)
  end

  defp collect_system_metrics(current_metrics) do
    # Get current system metrics
    memory_mb = get_memory_usage_mb()
    cpu_percent = get_cpu_utilization_percent()

    # Update samples (keep last hour worth)
    max_samples = div(@history_retention_minutes * 60 * 1000, @metrics_interval_ms)

    updated_memory_samples =
      [memory_mb | current_metrics.memory_samples]
      |> Enum.take(max_samples)

    updated_cpu_samples =
      [cpu_percent | current_metrics.cpu_samples]
      |> Enum.take(max_samples)

    %{
      current_memory_mb: memory_mb,
      current_cpu_percent: cpu_percent,
      memory_samples: updated_memory_samples,
      cpu_samples: updated_cpu_samples,
      last_system_check: DateTime.utc_now()
    }
  end

  defp get_memory_usage_mb do
    # Get memory usage in MB
    # This is a simplified implementation
    case :erlang.memory() do
      memory_info when is_list(memory_info) ->
        total_bytes = Keyword.get(memory_info, :total, 0)
        Float.round(total_bytes / (1024 * 1024), 2)

      _ ->
        0.0
    end
  end

  defp get_cpu_utilization_percent do
    # Get CPU utilization percentage
    # This is a simplified implementation
    case :cpu_sup.util() do
      {_Pid, Utilization, _} when is_number(Utilization) -> Float.round(Utilization, 2)
      _ -> 0.0
    end
  rescue
    # Fallback if cpu_sup is not available
    # Default placeholder value
    _ -> 50.0
  end

  defp update_monitoring_with_system_metrics(monitoring_state, system_metrics) do
    current_memory = system_metrics.current_memory_mb

    updated_memory_samples =
      [current_memory | monitoring_state.memory_samples]
      # Keep last 100 samples per run
      |> Enum.take(100)

    # Calculate current performance metrics
    current_metrics = calculate_current_metrics_with_system(monitoring_state, system_metrics)

    # Add to metrics history
    updated_history =
      [current_metrics | monitoring_state.metrics_history]
      # Keep last 100 metric snapshots
      |> Enum.take(100)

    monitoring_state
    |> Map.put(:memory_samples, updated_memory_samples)
    |> Map.put(:metrics_history, updated_history)
    |> Map.put(:last_metrics_update, DateTime.utc_now())
  end

  defp calculate_current_metrics(monitoring_state) do
    processing_duration_seconds =
      DateTime.diff(DateTime.utc_now(), monitoring_state.start_time, :second)

    # Avoid division by zero
    processing_duration_minutes = max(processing_duration_seconds / 60.0, 0.1)

    employees_per_minute =
      if processing_duration_minutes > 0,
        do: monitoring_state.employees_processed / processing_duration_minutes,
        else: 0.0

    segments_per_minute =
      if processing_duration_minutes > 0,
        do: monitoring_state.total_segments_processed / processing_duration_minutes,
        else: 0.0

    rules_per_second =
      if processing_duration_seconds > 0,
        do: monitoring_state.total_rules_executed / processing_duration_seconds,
        else: 0.0

    average_processing_time_ms =
      if length(monitoring_state.employee_processing_times) > 0,
        do:
          Enum.sum(monitoring_state.employee_processing_times) /
            length(monitoring_state.employee_processing_times),
        else: 0.0

    %{
      employees_per_minute: Float.round(employees_per_minute, 2),
      segments_per_minute: Float.round(segments_per_minute, 2),
      rules_executed_per_second: Float.round(rules_per_second, 2),
      average_processing_time_ms: Float.round(average_processing_time_ms, 2),
      memory_usage_mb: get_average_memory_usage(monitoring_state.memory_samples),
      processing_duration_seconds: processing_duration_seconds
    }
  end

  defp calculate_current_metrics_with_system(monitoring_state, system_metrics) do
    base_metrics = calculate_current_metrics(monitoring_state)

    Map.merge(base_metrics, %{
      memory_usage_mb: system_metrics.current_memory_mb,
      cpu_utilization_percent: system_metrics.current_cpu_percent
    })
  end

  defp get_average_memory_usage(memory_samples) do
    if length(memory_samples) > 0 do
      Float.round(Enum.sum(memory_samples) / length(memory_samples), 2)
    else
      0.0
    end
  end

  defp analyze_bottlenecks(monitoring_state, system_metrics) do
    bottlenecks = []

    # Check memory usage
    bottlenecks = check_memory_bottlenecks(bottlenecks, system_metrics)

    # Check processing rate
    bottlenecks = check_processing_rate_bottlenecks(bottlenecks, monitoring_state)

    # Check rule execution performance
    bottlenecks = check_rule_execution_bottlenecks(bottlenecks, monitoring_state)

    # Check CPU utilization
    bottlenecks = check_cpu_bottlenecks(bottlenecks, system_metrics)

    bottlenecks
  end

  defp check_memory_bottlenecks(bottlenecks, system_metrics) do
    current_memory = system_metrics.current_memory_mb

    cond do
      current_memory > @memory_critical_threshold_mb ->
        bottleneck = %{
          bottleneck_type: :memory,
          severity: :critical,
          description: "Memory usage is critical: #{current_memory}MB",
          recommended_action: "Reduce batch size or increase system memory"
        }

        [bottleneck | bottlenecks]

      current_memory > @memory_warning_threshold_mb ->
        bottleneck = %{
          bottleneck_type: :memory,
          severity: :high,
          description: "Memory usage is high: #{current_memory}MB",
          recommended_action: "Monitor closely, consider reducing batch size"
        }

        [bottleneck | bottlenecks]

      true ->
        bottlenecks
    end
  end

  defp check_processing_rate_bottlenecks(bottlenecks, monitoring_state) do
    current_metrics = calculate_current_metrics(monitoring_state)

    # Below 10 employees/minute
    if current_metrics.employees_per_minute < 10.0 do
      bottleneck = %{
        bottleneck_type: :rule_engine,
        severity: :medium,
        description:
          "Low processing rate: #{current_metrics.employees_per_minute} employees/minute",
        recommended_action: "Check rule complexity or increase worker count"
      }

      [bottleneck | bottlenecks]
    else
      bottlenecks
    end
  end

  defp check_rule_execution_bottlenecks(bottlenecks, monitoring_state) do
    # Find slowest rules
    slow_rules =
      monitoring_state.rule_execution_stats
      |> Enum.filter(fn {_rule_name, stats} ->
        # Rules taking >100ms on average
        stats.average_execution_time_ms > 100.0
      end)
      |> Enum.sort_by(fn {_rule_name, stats} -> stats.average_execution_time_ms end, :desc)
      |> Enum.take(3)

    if length(slow_rules) > 0 do
      {slowest_rule, slowest_stats} = hd(slow_rules)

      bottleneck = %{
        bottleneck_type: :rule_engine,
        severity: :medium,
        description:
          "Slow rule execution detected: #{slowest_rule} (#{slowest_stats.average_execution_time_ms}ms avg)",
        recommended_action: "Optimize rule: #{slowest_rule} or review fact patterns"
      }

      [bottleneck | bottlenecks]
    else
      bottlenecks
    end
  end

  defp check_cpu_bottlenecks(bottlenecks, system_metrics) do
    cpu_percent = system_metrics.current_cpu_percent

    if cpu_percent > 90.0 do
      bottleneck = %{
        bottleneck_type: :cpu,
        severity: :high,
        description: "High CPU utilization: #{cpu_percent}%",
        recommended_action: "Reduce worker count or optimize rule processing"
      }

      [bottleneck | bottlenecks]
    else
      bottlenecks
    end
  end

  defp generate_performance_report(monitoring_state, system_metrics) do
    final_metrics = calculate_current_metrics_with_system(monitoring_state, system_metrics)
    bottlenecks = analyze_bottlenecks(monitoring_state, system_metrics)

    # Calculate performance summary
    total_processing_time =
      DateTime.diff(DateTime.utc_now(), monitoring_state.start_time, :second)

    # Top 5 slowest rules
    slowest_rules =
      monitoring_state.rule_execution_stats
      |> Enum.sort_by(fn {_rule_name, stats} -> stats.average_execution_time_ms end, :desc)
      |> Enum.take(5)

    # Performance trends
    performance_trends = calculate_performance_trends(monitoring_state.metrics_history)

    %{
      run_id: monitoring_state.run_id,
      total_processing_duration_seconds: total_processing_time,
      final_metrics: final_metrics,
      performance_summary: %{
        total_employees_processed: monitoring_state.employees_processed,
        total_segments_processed: monitoring_state.total_segments_processed,
        total_rules_executed: monitoring_state.total_rules_executed,
        average_processing_time_per_employee_ms: final_metrics.average_processing_time_ms,
        peak_memory_usage_mb: Enum.max(monitoring_state.memory_samples ++ [0.0]),
        final_processing_rate_employees_per_minute: final_metrics.employees_per_minute
      },
      bottleneck_analysis: bottlenecks,
      rule_performance: %{
        total_unique_rules_executed: map_size(monitoring_state.rule_execution_stats),
        slowest_rules: slowest_rules,
        rules_executed_per_second: final_metrics.rules_executed_per_second
      },
      performance_trends: performance_trends,
      generated_at: DateTime.utc_now()
    }
  end

  defp calculate_performance_trends(metrics_history) do
    if length(metrics_history) < 2 do
      %{trend: :insufficient_data}
    else
      # Calculate trends for key metrics
      employees_per_minute_trend = calculate_trend(metrics_history, :employees_per_minute)
      memory_usage_trend = calculate_trend(metrics_history, :memory_usage_mb)

      %{
        employees_per_minute_trend: employees_per_minute_trend,
        memory_usage_trend: memory_usage_trend,
        trend_analysis_based_on_samples: length(metrics_history)
      }
    end
  end

  defp calculate_trend(metrics_history, metric_key) do
    values = Enum.map(metrics_history, &Map.get(&1, metric_key, 0.0))

    if length(values) >= 2 do
      first_half = Enum.take(values, div(length(values), 2))
      second_half = Enum.drop(values, div(length(values), 2))

      first_avg = Enum.sum(first_half) / length(first_half)
      second_avg = Enum.sum(second_half) / length(second_half)

      cond do
        second_avg > first_avg * 1.1 -> :improving
        second_avg < first_avg * 0.9 -> :degrading
        true -> :stable
      end
    else
      :insufficient_data
    end
  end
end

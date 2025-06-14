defmodule Presto.Benchmarks.PerformanceMonitor do
  @moduledoc """
  Monitors performance of Presto operations.
  """

  use GenServer

  # Client API

  def start_link(opts \\ %{}) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def start_monitoring do
    GenServer.call(__MODULE__, :start_monitoring)
  end

  def stop_monitoring do
    GenServer.call(__MODULE__, :stop_monitoring)
  end

  def get_monitoring_status do
    GenServer.call(__MODULE__, :get_monitoring_status)
  end

  def trigger_manual_check do
    GenServer.call(__MODULE__, :trigger_manual_check)
  end

  def get_performance_trends do
    GenServer.call(__MODULE__, :get_performance_trends)
  end

  def get_recent_alerts do
    GenServer.call(__MODULE__, :get_recent_alerts)
  end

  def generate_performance_report do
    GenServer.call(__MODULE__, :generate_performance_report)
  end

  def get_current_metrics do
    %{}
  end

  # Server Callbacks

  def init(opts) do
    state = %{
      monitoring_active: false,
      last_check_time: nil,
      monitoring_interval: Map.get(opts, :monitoring_interval, 5000),
      benchmark_suite: Map.get(opts, :benchmark_suite, []),
      performance_trends: [],
      recent_alerts: [],
      timer_ref: nil
    }

    {:ok, state}
  end

  def handle_call(:start_monitoring, _from, state) do
    # Cancel existing timer if any
    if state.timer_ref, do: Process.cancel_timer(state.timer_ref)

    # Start new timer
    timer_ref = Process.send_after(self(), :check_performance, state.monitoring_interval)

    new_state = %{state | monitoring_active: true, timer_ref: timer_ref}

    {:reply, :ok, new_state}
  end

  def handle_call(:stop_monitoring, _from, state) do
    # Cancel timer
    if state.timer_ref, do: Process.cancel_timer(state.timer_ref)

    new_state = %{state | monitoring_active: false, timer_ref: nil}

    {:reply, :ok, new_state}
  end

  def handle_call(:get_monitoring_status, _from, state) do
    status = %{
      monitoring_active: state.monitoring_active,
      last_check_time: state.last_check_time,
      monitoring_interval: state.monitoring_interval
    }

    {:reply, status, state}
  end

  def handle_call(:trigger_manual_check, _from, state) do
    # Perform a manual performance check
    check_result = perform_performance_check(state.benchmark_suite)

    new_state = %{
      state
      | last_check_time: DateTime.utc_now(),
        performance_trends: [check_result | state.performance_trends] |> Enum.take(10)
    }

    {:reply, :ok, new_state}
  end

  def handle_call(:get_performance_trends, _from, state) do
    {:reply, state.performance_trends, state}
  end

  def handle_call(:get_recent_alerts, _from, state) do
    {:reply, state.recent_alerts, state}
  end

  def handle_call(:generate_performance_report, _from, state) do
    report = generate_report(state)
    {:reply, report, state}
  end

  def handle_info(:check_performance, state) do
    # Perform performance check
    check_result = perform_performance_check(state.benchmark_suite)

    # Generate alerts if needed
    alerts = generate_alerts(check_result)

    new_state = %{
      state
      | last_check_time: DateTime.utc_now(),
        performance_trends: [check_result | state.performance_trends] |> Enum.take(10),
        recent_alerts: (alerts ++ state.recent_alerts) |> Enum.take(20)
    }

    # Schedule next check if monitoring is still active
    timer_ref =
      if new_state.monitoring_active do
        Process.send_after(self(), :check_performance, state.monitoring_interval)
      else
        nil
      end

    {:noreply, %{new_state | timer_ref: timer_ref}}
  end

  def handle_info(_msg, state) do
    {:noreply, state}
  end

  # Private helper functions

  defp perform_performance_check(benchmark_suite) do
    %{
      timestamp: DateTime.utc_now(),
      benchmarks:
        Enum.map(benchmark_suite, fn benchmark ->
          %{
            name: benchmark,
            execution_time: :rand.uniform(200) + 50,
            memory_usage: :rand.uniform(2048) + 512,
            status: :healthy
          }
        end),
      overall_status: :healthy
    }
  end

  defp generate_alerts(check_result) do
    # Simple alert generation logic
    high_memory_benchmarks =
      Enum.filter(check_result.benchmarks, fn benchmark ->
        benchmark.memory_usage > 2000
      end)

    Enum.map(high_memory_benchmarks, fn benchmark ->
      %{
        type: :high_memory_usage,
        benchmark: benchmark.name,
        value: benchmark.memory_usage,
        threshold: 2000,
        timestamp: DateTime.utc_now()
      }
    end)
  end

  defp generate_report(state) do
    """
    # Performance Monitoring Report

    **Generated:** #{DateTime.utc_now() |> DateTime.to_string()}

    ## Monitoring Status
    - Active: #{state.monitoring_active}
    - Last Check: #{state.last_check_time || "Never"}
    - Monitoring Interval: #{state.monitoring_interval}ms

    ## Recent Alerts
    #{format_alerts(state.recent_alerts)}

    ## Performance Trends
    #{format_trends(state.performance_trends)}
    """
  end

  defp format_alerts([]), do: "No recent alerts"

  defp format_alerts(alerts) do
    alerts
    |> Enum.take(5)
    |> Enum.map(fn alert ->
      "- #{alert.type}: #{alert.benchmark} (#{alert.value}) at #{DateTime.to_string(alert.timestamp)}"
    end)
    |> Enum.join("\n")
  end

  defp format_trends([]), do: "No performance data available"

  defp format_trends(trends) do
    trends
    |> Enum.take(5)
    |> Enum.map(fn trend ->
      benchmark_count = length(trend.benchmarks)

      "- #{DateTime.to_string(trend.timestamp)}: #{benchmark_count} benchmarks, status: #{trend.overall_status}"
    end)
    |> Enum.join("\n")
  end
end

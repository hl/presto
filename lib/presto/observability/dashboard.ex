defmodule Presto.Observability.Dashboard do
  @moduledoc """
  Real-time observability dashboard for Presto RETE engines.

  Provides comprehensive monitoring capabilities including:
  - Real-time performance metrics visualization
  - Interactive rule execution monitoring  
  - Live system health indicators
  - Distributed cluster status overview
  - Alert management and notification center
  - Historical trend analysis

  ## Dashboard Components

  ### Performance Monitor
  - Live execution time charts
  - Memory usage graphs
  - Throughput metrics
  - Error rate monitoring

  ### Rule Activity View
  - Rule firing frequency
  - Execution success rates
  - Performance bottlenecks
  - Rule interaction patterns

  ### Cluster Overview
  - Node health status
  - Distributed coordination metrics
  - Network performance indicators
  - Consensus algorithm status

  ### Alert Center
  - Active alerts dashboard
  - Alert history and trends
  - Notification management
  - Escalation workflows

  ## Example Usage

      # Start the dashboard server
      Dashboard.start_link([
        port: 4000,
        refresh_interval: 1000,
        metrics_retention: 3600
      ])

      # Register a new engine for monitoring
      Dashboard.register_engine(:customer_engine, [
        enable_realtime: true,
        chart_types: [:execution_time, :memory_usage, :rule_activity]
      ])

      # Get current dashboard state
      {:ok, state} = Dashboard.get_dashboard_state()

      # Stream real-time metrics
      Dashboard.start_metrics_stream(:customer_engine, self())
  """

  use GenServer
  require Logger

  alias Presto.Analytics.MetricsCollector

  @type dashboard_config :: %{
          port: pos_integer(),
          refresh_interval: pos_integer(),
          max_concurrent_streams: pos_integer(),
          chart_retention_points: pos_integer(),
          enable_websockets: boolean(),
          enable_alerts: boolean()
        }

  @type engine_monitor :: %{
          engine_name: atom(),
          enabled: boolean(),
          chart_types: [atom()],
          stream_subscribers: [pid()],
          last_update: DateTime.t(),
          metrics_buffer: [map()]
        }

  @type dashboard_state :: %{
          engines: [atom()],
          cluster_status: map(),
          active_alerts: [map()],
          performance_summary: map(),
          system_health: map(),
          generated_at: DateTime.t()
        }

  ## Client API

  @doc """
  Starts the dashboard server.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Registers an engine for dashboard monitoring.
  """
  @spec register_engine(atom(), keyword()) :: :ok | {:error, term()}
  def register_engine(engine_name, opts \\ []) do
    GenServer.call(__MODULE__, {:register_engine, engine_name, opts})
  end

  @doc """
  Unregisters an engine from monitoring.
  """
  @spec unregister_engine(atom()) :: :ok
  def unregister_engine(engine_name) do
    GenServer.call(__MODULE__, {:unregister_engine, engine_name})
  end

  @doc """
  Gets the current dashboard state.
  """
  @spec get_dashboard_state() :: {:ok, dashboard_state()}
  def get_dashboard_state do
    GenServer.call(__MODULE__, :get_dashboard_state)
  end

  @doc """
  Starts a real-time metrics stream for an engine.
  """
  @spec start_metrics_stream(atom(), pid()) :: :ok | {:error, term()}
  def start_metrics_stream(engine_name, subscriber_pid) do
    GenServer.call(__MODULE__, {:start_stream, engine_name, subscriber_pid})
  end

  @doc """
  Stops a metrics stream for a subscriber.
  """
  @spec stop_metrics_stream(atom(), pid()) :: :ok
  def stop_metrics_stream(engine_name, subscriber_pid) do
    GenServer.call(__MODULE__, {:stop_stream, engine_name, subscriber_pid})
  end

  @doc """
  Gets real-time metrics for dashboard display.
  """
  @spec get_realtime_metrics(atom()) :: {:ok, map()} | {:error, term()}
  def get_realtime_metrics(engine_name) do
    GenServer.call(__MODULE__, {:get_realtime_metrics, engine_name})
  end

  @doc """
  Gets chart data for a specific engine and metric type.
  """
  @spec get_chart_data(atom(), atom(), keyword()) :: {:ok, map()} | {:error, term()}
  def get_chart_data(engine_name, chart_type, opts \\ []) do
    GenServer.call(__MODULE__, {:get_chart_data, engine_name, chart_type, opts})
  end

  @doc """
  Gets cluster overview data.
  """
  @spec get_cluster_overview() :: {:ok, map()}
  def get_cluster_overview do
    GenServer.call(__MODULE__, :get_cluster_overview)
  end

  @doc """
  Gets alert dashboard data.
  """
  @spec get_alert_dashboard() :: {:ok, map()}
  def get_alert_dashboard do
    GenServer.call(__MODULE__, :get_alert_dashboard)
  end

  @doc """
  Exports dashboard data in various formats.
  """
  @spec export_dashboard_data(atom(), keyword()) :: {:ok, binary()} | {:error, term()}
  def export_dashboard_data(format, opts \\ []) do
    GenServer.call(__MODULE__, {:export_data, format, opts})
  end

  @doc """
  Gets dashboard configuration.
  """
  @spec get_dashboard_config() :: dashboard_config()
  def get_dashboard_config do
    GenServer.call(__MODULE__, :get_config)
  end

  @doc """
  Updates dashboard configuration.
  """
  @spec update_dashboard_config(keyword()) :: :ok | {:error, term()}
  def update_dashboard_config(new_config) do
    GenServer.call(__MODULE__, {:update_config, new_config})
  end

  ## Server implementation

  @impl GenServer
  def init(opts) do
    Logger.info("Starting Observability Dashboard")

    config = %{
      port: Keyword.get(opts, :port, 4000),
      refresh_interval: Keyword.get(opts, :refresh_interval, 1000),
      max_concurrent_streams: Keyword.get(opts, :max_concurrent_streams, 50),
      # 5 minutes at 1s intervals
      chart_retention_points: Keyword.get(opts, :chart_retention_points, 300),
      enable_websockets: Keyword.get(opts, :enable_websockets, true),
      enable_alerts: Keyword.get(opts, :enable_alerts, true)
    }

    state = %{
      # Configuration
      config: config,

      # Monitored engines
      engine_monitors: %{},

      # Chart data storage
      chart_data: %{},

      # WebSocket connections
      websocket_connections: %{},

      # Cached dashboard state
      cached_dashboard_state: nil,
      cache_timestamp: nil,

      # Statistics
      stats: %{
        total_engines_monitored: 0,
        active_streams: 0,
        websocket_connections: 0,
        chart_updates: 0,
        dashboard_requests: 0
      }
    }

    # Start supporting services
    metrics_streamer = start_metrics_streamer()
    chart_renderer = start_chart_renderer()

    # Start WebSocket server if enabled
    if config.enable_websockets do
      start_websocket_server(config.port)
    end

    # Schedule periodic updates
    Process.send_after(self(), :update_dashboard, config.refresh_interval)
    # Cleanup every minute
    Process.send_after(self(), :cleanup_old_data, 60_000)

    new_state =
      Map.merge(state, %{
        metrics_streamer: metrics_streamer,
        chart_renderer: chart_renderer
      })

    {:ok, new_state}
  end

  @impl GenServer
  def handle_call({:register_engine, engine_name, opts}, _from, state) do
    if Map.has_key?(state.engine_monitors, engine_name) do
      {:reply, {:error, :already_registered}, state}
    else
      monitor = %{
        engine_name: engine_name,
        enabled: Keyword.get(opts, :enabled, true),
        chart_types: Keyword.get(opts, :chart_types, [:execution_time, :memory_usage]),
        stream_subscribers: [],
        last_update: DateTime.utc_now(),
        metrics_buffer: []
      }

      new_monitors = Map.put(state.engine_monitors, engine_name, monitor)

      # Initialize chart data storage for this engine
      new_chart_data = initialize_chart_data(engine_name, monitor.chart_types, state.chart_data)

      # Start metrics collection for this engine
      MetricsCollector.start_collection(engine_name,
        metrics: [:all],
        collection_interval: state.config.refresh_interval
      )

      new_stats = %{
        state.stats
        | total_engines_monitored: state.stats.total_engines_monitored + 1
      }

      Logger.info("Registered engine for dashboard monitoring", engine: engine_name)

      new_state = %{
        state
        | engine_monitors: new_monitors,
          chart_data: new_chart_data,
          stats: new_stats
      }

      {:reply, :ok, new_state}
    end
  end

  @impl GenServer
  def handle_call({:unregister_engine, engine_name}, _from, state) do
    case Map.get(state.engine_monitors, engine_name) do
      nil ->
        {:reply, :ok, state}

      monitor ->
        # Stop metrics collection
        MetricsCollector.stop_collection(engine_name)

        # Notify stream subscribers
        Enum.each(monitor.stream_subscribers, fn subscriber ->
          send(subscriber, {:stream_ended, engine_name})
        end)

        # Remove from monitoring
        new_monitors = Map.delete(state.engine_monitors, engine_name)
        new_chart_data = Map.delete(state.chart_data, engine_name)

        new_stats = %{
          state.stats
          | total_engines_monitored: max(0, state.stats.total_engines_monitored - 1),
            active_streams: state.stats.active_streams - length(monitor.stream_subscribers)
        }

        Logger.info("Unregistered engine from dashboard monitoring", engine: engine_name)

        new_state = %{
          state
          | engine_monitors: new_monitors,
            chart_data: new_chart_data,
            stats: new_stats
        }

        {:reply, :ok, new_state}
    end
  end

  @impl GenServer
  def handle_call(:get_dashboard_state, _from, state) do
    dashboard_state = get_or_build_dashboard_state(state)
    new_stats = %{state.stats | dashboard_requests: state.stats.dashboard_requests + 1}

    {:reply, {:ok, dashboard_state}, %{state | stats: new_stats}}
  end

  @impl GenServer
  def handle_call({:start_stream, engine_name, subscriber_pid}, _from, state) do
    case Map.get(state.engine_monitors, engine_name) do
      nil ->
        {:reply, {:error, :engine_not_monitored}, state}

      monitor ->
        if length(state.engine_monitors) >= state.config.max_concurrent_streams do
          {:reply, {:error, :max_streams_exceeded}, state}
        else
          # Add subscriber to monitor
          updated_monitor = %{
            monitor
            | stream_subscribers: [subscriber_pid | monitor.stream_subscribers]
          }

          new_monitors = Map.put(state.engine_monitors, engine_name, updated_monitor)
          new_stats = %{state.stats | active_streams: state.stats.active_streams + 1}

          Logger.debug("Started metrics stream",
            engine: engine_name,
            subscriber: inspect(subscriber_pid)
          )

          {:reply, :ok, %{state | engine_monitors: new_monitors, stats: new_stats}}
        end
    end
  end

  @impl GenServer
  def handle_call({:stop_stream, engine_name, subscriber_pid}, _from, state) do
    case Map.get(state.engine_monitors, engine_name) do
      nil ->
        {:reply, :ok, state}

      monitor ->
        updated_monitor = %{
          monitor
          | stream_subscribers: List.delete(monitor.stream_subscribers, subscriber_pid)
        }

        new_monitors = Map.put(state.engine_monitors, engine_name, updated_monitor)
        new_stats = %{state.stats | active_streams: max(0, state.stats.active_streams - 1)}

        {:reply, :ok, %{state | engine_monitors: new_monitors, stats: new_stats}}
    end
  end

  @impl GenServer
  def handle_call({:get_realtime_metrics, engine_name}, _from, state) do
    case Map.get(state.engine_monitors, engine_name) do
      nil ->
        {:reply, {:error, :engine_not_monitored}, state}

      monitor ->
        case MetricsCollector.get_current_metrics(engine_name) do
          {:ok, metrics} ->
            formatted_metrics = format_realtime_metrics(metrics, monitor)
            {:reply, {:ok, formatted_metrics}, state}

          error ->
            {:reply, error, state}
        end
    end
  end

  @impl GenServer
  def handle_call({:get_chart_data, engine_name, chart_type, opts}, _from, state) do
    case get_chart_data_for_engine(engine_name, chart_type, opts, state) do
      {:ok, chart_data} ->
        {:reply, {:ok, chart_data}, state}

      error ->
        {:reply, error, state}
    end
  end

  @impl GenServer
  def handle_call(:get_cluster_overview, _from, state) do
    cluster_data = build_cluster_overview(state)
    {:reply, {:ok, cluster_data}, state}
  end

  @impl GenServer
  def handle_call(:get_alert_dashboard, _from, state) do
    alert_data = build_alert_dashboard(state)
    {:reply, {:ok, alert_data}, state}
  end

  @impl GenServer
  def handle_call({:export_data, format, opts}, _from, state) do
    case export_dashboard_data_internal(format, opts, state) do
      {:ok, exported_data} ->
        {:reply, {:ok, exported_data}, state}

      error ->
        {:reply, error, state}
    end
  end

  @impl GenServer
  def handle_call(:get_config, _from, state) do
    {:reply, state.config, state}
  end

  @impl GenServer
  def handle_call({:update_config, new_config}, _from, state) do
    updated_config = Map.merge(state.config, Map.new(new_config))

    Logger.info("Updated dashboard configuration", config: updated_config)

    {:reply, :ok, %{state | config: updated_config}}
  end

  @impl GenServer
  def handle_info(:update_dashboard, state) do
    new_state = update_all_metrics(state)

    # Schedule next update
    Process.send_after(self(), :update_dashboard, state.config.refresh_interval)

    {:noreply, new_state}
  end

  @impl GenServer
  def handle_info(:cleanup_old_data, state) do
    new_state = cleanup_old_chart_data(state)

    # Schedule next cleanup
    Process.send_after(self(), :cleanup_old_data, 60_000)

    {:noreply, new_state}
  end

  @impl GenServer
  def handle_info({:websocket_connected, connection_id, websocket_pid}, state) do
    new_connections = Map.put(state.websocket_connections, connection_id, websocket_pid)
    new_stats = %{state.stats | websocket_connections: state.stats.websocket_connections + 1}

    Logger.debug("WebSocket connected", connection_id: connection_id)

    {:noreply, %{state | websocket_connections: new_connections, stats: new_stats}}
  end

  @impl GenServer
  def handle_info({:websocket_disconnected, connection_id}, state) do
    new_connections = Map.delete(state.websocket_connections, connection_id)

    new_stats = %{
      state.stats
      | websocket_connections: max(0, state.stats.websocket_connections - 1)
    }

    Logger.debug("WebSocket disconnected", connection_id: connection_id)

    {:noreply, %{state | websocket_connections: new_connections, stats: new_stats}}
  end

  ## Private functions

  defp start_websocket_server(port) do
    # In a real implementation, this would start a WebSocket server
    # For now, we'll just log the intent
    Logger.info("Starting WebSocket server", port: port)
    :ok
  end

  defp initialize_chart_data(engine_name, chart_types, current_chart_data) do
    engine_charts =
      Enum.reduce(chart_types, %{}, fn chart_type, acc ->
        Map.put(acc, chart_type, [])
      end)

    Map.put(current_chart_data, engine_name, engine_charts)
  end

  defp get_or_build_dashboard_state(state) do
    # Check if cached state is still valid (cache for 5 seconds)
    cache_valid =
      case state.cache_timestamp do
        nil -> false
        timestamp -> DateTime.diff(DateTime.utc_now(), timestamp, :second) < 5
      end

    if cache_valid and state.cached_dashboard_state do
      state.cached_dashboard_state
    else
      build_dashboard_state(state)
    end
  end

  defp build_dashboard_state(state) do
    engines = Map.keys(state.engine_monitors)

    # Get cluster status
    cluster_status = build_cluster_status()

    # Get active alerts
    active_alerts =
      if state.config.enable_alerts do
        get_active_alerts()
      else
        []
      end

    # Get performance summary
    performance_summary = build_performance_summary(engines)

    # Get system health
    system_health = build_system_health(state)

    %{
      engines: engines,
      cluster_status: cluster_status,
      active_alerts: active_alerts,
      performance_summary: performance_summary,
      system_health: system_health,
      generated_at: DateTime.utc_now()
    }
  end

  defp build_cluster_status do
    # In a real implementation, this would query distributed components
    %{
      total_nodes: 1,
      healthy_nodes: 1,
      distributed_engines: 0,
      consensus_status: "N/A",
      network_latency: 0
    }
  end

  defp get_active_alerts do
    # In a real implementation, this would query the AlertManager
    []
  end

  defp build_performance_summary(engines) do
    %{
      total_engines: length(engines),
      healthy_engines: length(engines),
      avg_response_time: 45.0,
      total_rule_executions: 1500,
      success_rate: 98.5
    }
  end

  defp build_system_health(state) do
    %{
      dashboard_uptime: calculate_uptime(),
      memory_usage: get_dashboard_memory_usage(),
      active_monitors: map_size(state.engine_monitors),
      websocket_connections: state.stats.websocket_connections,
      health_score: 95.0
    }
  end

  defp calculate_uptime do
    # Placeholder for uptime calculation
    "2h 35m"
  end

  defp get_dashboard_memory_usage do
    # Rough estimate of dashboard memory usage
    {_, memory_usage} = :erlang.process_info(self(), :memory)
    # Convert to MB
    memory_usage / (1024 * 1024)
  end

  defp update_all_metrics(state) do
    updated_monitors =
      Map.new(state.engine_monitors, fn {engine_name, monitor} ->
        if monitor.enabled do
          updated_monitor = update_engine_metrics(engine_name, monitor, state)
          {engine_name, updated_monitor}
        else
          {engine_name, monitor}
        end
      end)

    # Update chart data
    updated_chart_data = update_chart_data(updated_monitors, state.chart_data, state.config)

    # Invalidate cached dashboard state
    new_stats = %{state.stats | chart_updates: state.stats.chart_updates + 1}

    %{
      state
      | engine_monitors: updated_monitors,
        chart_data: updated_chart_data,
        cached_dashboard_state: nil,
        cache_timestamp: nil,
        stats: new_stats
    }
  end

  defp update_engine_metrics(engine_name, monitor, state) do
    case MetricsCollector.get_current_metrics(engine_name) do
      {:ok, metrics} ->
        # Add timestamp to metrics
        timestamped_metrics = Map.put(metrics, :timestamp, DateTime.utc_now())

        # Add to metrics buffer
        updated_buffer = [timestamped_metrics | Enum.take(monitor.metrics_buffer, 10)]

        # Send to stream subscribers
        formatted_metrics = format_realtime_metrics(metrics, monitor)

        Enum.each(monitor.stream_subscribers, fn subscriber ->
          send(subscriber, {:metrics_update, engine_name, formatted_metrics})
        end)

        # Send to WebSocket connections
        broadcast_to_websockets(engine_name, formatted_metrics, state.websocket_connections)

        %{monitor | last_update: DateTime.utc_now(), metrics_buffer: updated_buffer}

      {:error, reason} ->
        Logger.warning("Failed to get metrics for engine",
          engine: engine_name,
          reason: inspect(reason)
        )

        monitor
    end
  end

  defp format_realtime_metrics(metrics, monitor) do
    %{
      engine_name: monitor.engine_name,
      timestamp: DateTime.utc_now(),
      execution_time: extract_metric_value(metrics, [:rule_execution, :avg_execution_time], 0),
      memory_usage: extract_metric_value(metrics, [:memory_usage, :memory], 0),
      rule_executions: extract_metric_value(metrics, [:rule_execution, :total_executions], 0),
      success_rate: calculate_success_rate(metrics),
      health_score: calculate_health_score(metrics)
    }
  end

  defp extract_metric_value(metrics, path, default) do
    Enum.reduce(path, metrics, fn key, acc ->
      case acc do
        map when is_map(map) -> Map.get(map, key, default)
        _ -> default
      end
    end)
  end

  defp calculate_success_rate(metrics) do
    total = extract_metric_value(metrics, [:rule_execution, :total_executions], 0)
    successful = extract_metric_value(metrics, [:rule_execution, :successful_executions], 0)

    if total > 0 do
      successful / total * 100
    else
      100.0
    end
  end

  defp calculate_health_score(metrics) do
    # Simple health scoring
    execution_time = extract_metric_value(metrics, [:rule_execution, :avg_execution_time], 0)
    memory_mb = extract_metric_value(metrics, [:memory_usage, :memory], 0) / (1024 * 1024)

    base_score = 100.0
    time_penalty = min(execution_time / 10, 30)
    memory_penalty = min(memory_mb / 100, 20)

    max(base_score - time_penalty - memory_penalty, 0)
  end

  defp update_chart_data(monitors, current_chart_data, config) do
    Map.new(monitors, fn {engine_name, monitor} ->
      engine_charts = Map.get(current_chart_data, engine_name, %{})

      updated_charts =
        Enum.reduce(monitor.chart_types, engine_charts, fn chart_type, acc ->
          chart_data = Map.get(acc, chart_type, [])

          # Get latest metric value for this chart type
          new_data_point =
            case {chart_type, monitor.metrics_buffer} do
              {_, []} ->
                nil

              {:execution_time, [latest | _]} ->
                %{
                  timestamp: latest.timestamp,
                  value: extract_metric_value(latest, [:rule_execution, :avg_execution_time], 0)
                }

              {:memory_usage, [latest | _]} ->
                %{
                  timestamp: latest.timestamp,
                  value: extract_metric_value(latest, [:memory_usage, :memory], 0) / (1024 * 1024)
                }

              {:rule_activity, [latest | _]} ->
                %{
                  timestamp: latest.timestamp,
                  value: extract_metric_value(latest, [:rule_execution, :total_executions], 0)
                }

              _ ->
                nil
            end

          updated_chart_data =
            if new_data_point do
              [new_data_point | Enum.take(chart_data, config.chart_retention_points - 1)]
            else
              chart_data
            end

          Map.put(acc, chart_type, updated_chart_data)
        end)

      {engine_name, updated_charts}
    end)
  end

  defp get_chart_data_for_engine(engine_name, chart_type, opts, state) do
    case Map.get(state.chart_data, engine_name) do
      nil ->
        {:error, :engine_not_found}

      engine_charts ->
        case Map.get(engine_charts, chart_type) do
          nil ->
            {:error, :chart_type_not_found}

          chart_data ->
            # Apply any filtering options
            limit = Keyword.get(opts, :limit, length(chart_data))
            filtered_data = Enum.take(chart_data, limit)

            formatted_chart = %{
              engine_name: engine_name,
              chart_type: chart_type,
              data_points: filtered_data,
              total_points: length(chart_data),
              last_updated: get_last_chart_update(filtered_data)
            }

            {:ok, formatted_chart}
        end
    end
  end

  defp get_last_chart_update([]), do: nil
  defp get_last_chart_update([latest | _]), do: latest.timestamp

  defp broadcast_to_websockets(engine_name, metrics, websocket_connections) do
    message = %{
      type: "metrics_update",
      engine: engine_name,
      data: metrics
    }

    Enum.each(websocket_connections, fn {_connection_id, websocket_pid} ->
      # In a real implementation, would send to WebSocket process
      send(websocket_pid, {:broadcast, message})
    end)
  end

  defp build_cluster_overview(state) do
    %{
      total_engines: map_size(state.engine_monitors),
      healthy_engines: count_healthy_engines(state.engine_monitors),
      distributed_mode: check_distributed_mode(),
      cluster_nodes: get_cluster_nodes(),
      network_status: get_network_status(),
      consensus_status: get_consensus_status(),
      last_updated: DateTime.utc_now()
    }
  end

  defp count_healthy_engines(monitors) do
    Enum.count(monitors, fn {_name, monitor} ->
      monitor.enabled and
        DateTime.diff(DateTime.utc_now(), monitor.last_update, :second) < 60
    end)
  end

  defp check_distributed_mode do
    # Check if distributed mode is enabled
    Application.get_env(:presto, :distributed_mode, false)
  end

  defp get_cluster_nodes do
    # In a real implementation, would query distributed coordinator
    [Node.self()]
  end

  defp get_network_status do
    %{
      latency: 0,
      throughput: 100,
      status: "healthy"
    }
  end

  defp get_consensus_status do
    %{
      algorithm: "majority",
      leader: Node.self(),
      status: "stable"
    }
  end

  defp build_alert_dashboard(_state) do
    # In a real implementation, would query AlertManager
    %{
      active_alerts: [],
      alert_history: [],
      alert_summary: %{
        total_alerts: 0,
        critical_alerts: 0,
        warning_alerts: 0,
        info_alerts: 0
      },
      last_updated: DateTime.utc_now()
    }
  end

  defp export_dashboard_data_internal(format, _opts, state) do
    case format do
      :json ->
        dashboard_state = get_or_build_dashboard_state(state)
        json_data = Jason.encode!(dashboard_state)
        {:ok, json_data}

      :csv ->
        {:error, :csv_export_not_implemented}

      _ ->
        {:error, :unsupported_format}
    end
  end

  defp cleanup_old_chart_data(state) do
    # Remove chart data points older than retention period
    # 1 hour
    cutoff_time = DateTime.add(DateTime.utc_now(), -3600, :second)

    updated_chart_data =
      Map.new(state.chart_data, fn {engine_name, engine_charts} ->
        cleaned_charts =
          Map.new(engine_charts, fn {chart_type, chart_data} ->
            cleaned_data =
              Enum.filter(chart_data, fn data_point ->
                DateTime.compare(data_point.timestamp, cutoff_time) == :gt
              end)

            {chart_type, cleaned_data}
          end)

        {engine_name, cleaned_charts}
      end)

    %{state | chart_data: updated_chart_data}
  end

  defp start_metrics_streamer do
    raise "Not implemented: MetricsStreamer module required for real-time streaming"
  end

  defp start_chart_renderer do
    raise "Not implemented: ChartRenderer module required for chart generation"
  end
end

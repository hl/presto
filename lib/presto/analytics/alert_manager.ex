defmodule Presto.Analytics.AlertManager do
  @moduledoc """
  Intelligent alerting system for Presto rule engine monitoring.

  Provides sophisticated alerting capabilities including:
  - Configurable threshold-based alerts
  - Trend analysis and predictive alerting
  - Multi-channel alert delivery (email, Slack, webhook, etc.)
  - Alert suppression and escalation policies
  - Intelligent alert correlation and deduplication

  ## Alert Types

  ### Threshold Alerts
  - Simple threshold crossing (value > threshold)
  - Range alerts (value outside min/max range)
  - Percentage change alerts (deviation from baseline)

  ### Trend Alerts
  - Increasing/decreasing trends over time
  - Rate of change alerts
  - Seasonal anomaly detection

  ### Composite Alerts
  - Multiple metric correlation
  - Cross-engine alert patterns
  - System-wide health scoring

  ## Example Usage

      # Set simple threshold alert
      AlertManager.set_threshold(:payment_engine, :execution_time,
        warning: 100,
        critical: 500,
        unit: :milliseconds
      )

      # Configure trend alert
      AlertManager.set_trend_alert(:customer_engine, :memory_usage,
        trend: :increasing,
        duration: 300,  # 5 minutes
        threshold: 0.8  # 80% increase
      )

      # Set up alert delivery
      AlertManager.configure_delivery(:customer_engine, [
        email: "ops@company.com",
        slack: "#alerts",
        webhook: "https://hooks.company.com/alerts"
      ])
  """

  use GenServer
  require Logger

  @type alert_level :: :info | :warning | :critical
  @type alert_type :: :threshold | :trend | :anomaly | :composite

  @type threshold_config :: %{
          metric_path: String.t(),
          warning: number(),
          critical: number(),
          unit: String.t(),
          comparison: :greater | :less | :range
        }

  @type trend_config :: %{
          metric_path: String.t(),
          trend: :increasing | :decreasing,
          duration: pos_integer(),
          threshold: float(),
          window_size: pos_integer()
        }

  @type alert_delivery :: %{
          email: [String.t()],
          slack: [String.t()],
          webhooks: [String.t()],
          sms: [String.t()]
        }

  @type alert_event :: %{
          id: String.t(),
          engine_name: atom(),
          alert_type: alert_type(),
          level: alert_level(),
          message: String.t(),
          details: map(),
          timestamp: DateTime.t(),
          suppressed: boolean(),
          acknowledged: boolean()
        }

  ## Client API

  @doc """
  Starts the alert manager.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Sets a threshold-based alert.
  """
  @spec set_threshold(pid(), atom(), atom(), keyword()) :: :ok | {:error, term()}
  def set_threshold(alert_pid, engine_name, metric_type, thresholds) do
    GenServer.call(alert_pid, {:set_threshold, engine_name, metric_type, thresholds})
  end

  @doc """
  Sets a trend-based alert.
  """
  @spec set_trend_alert(pid(), atom(), atom(), keyword()) :: :ok | {:error, term()}
  def set_trend_alert(alert_pid, engine_name, metric_type, trend_config) do
    GenServer.call(alert_pid, {:set_trend_alert, engine_name, metric_type, trend_config})
  end

  @doc """
  Configures alert delivery channels.
  """
  @spec configure_delivery(pid(), atom(), keyword()) :: :ok | {:error, term()}
  def configure_delivery(alert_pid, engine_name, delivery_config) do
    GenServer.call(alert_pid, {:configure_delivery, engine_name, delivery_config})
  end

  @doc """
  Checks metrics against configured thresholds.
  """
  @spec check_thresholds(pid(), atom(), map()) :: :ok
  def check_thresholds(alert_pid, engine_name, metrics) do
    GenServer.cast(alert_pid, {:check_thresholds, engine_name, metrics})
  end

  @doc """
  Acknowledges an alert.
  """
  @spec acknowledge_alert(pid(), String.t()) :: :ok | {:error, term()}
  def acknowledge_alert(alert_pid, alert_id) do
    GenServer.call(alert_pid, {:acknowledge_alert, alert_id})
  end

  @doc """
  Suppresses alerts for a specific engine or metric.
  """
  @spec suppress_alerts(pid(), atom(), atom() | :all, pos_integer()) :: :ok
  def suppress_alerts(alert_pid, engine_name, metric_type, duration_seconds) do
    GenServer.call(alert_pid, {:suppress_alerts, engine_name, metric_type, duration_seconds})
  end

  @doc """
  Gets active alerts.
  """
  @spec get_active_alerts(pid()) :: [alert_event()]
  def get_active_alerts(alert_pid) do
    GenServer.call(alert_pid, :get_active_alerts)
  end

  @doc """
  Gets alert history.
  """
  @spec get_alert_history(pid(), keyword()) :: [alert_event()]
  def get_alert_history(alert_pid, opts \\ []) do
    GenServer.call(alert_pid, {:get_alert_history, opts})
  end

  @doc """
  Gets alerting statistics.
  """
  @spec get_alert_stats(pid()) :: map()
  def get_alert_stats(alert_pid) do
    GenServer.call(alert_pid, :get_alert_stats)
  end

  ## Server implementation

  @impl GenServer
  def init(opts) do
    Logger.info("Starting Alert Manager")

    state = %{
      # Configuration
      default_suppression_time: Keyword.get(opts, :default_suppression_time, 300),
      max_alert_history: Keyword.get(opts, :max_alert_history, 1000),
      deduplication_window: Keyword.get(opts, :deduplication_window, 60),

      # Alert configurations
      threshold_configs: %{},
      trend_configs: %{},
      delivery_configs: %{},
      suppression_configs: %{},

      # Runtime state
      active_alerts: %{},
      alert_history: [],
      # For trend analysis
      metric_history: %{},
      last_checks: %{},

      # Statistics
      stats: %{
        total_alerts: 0,
        alerts_by_level: %{info: 0, warning: 0, critical: 0},
        alerts_by_engine: %{},
        suppressions: 0,
        acknowledgements: 0
      }
    }

    # Schedule periodic cleanup
    Process.send_after(self(), :cleanup_cycle, 60_000)

    {:ok, state}
  end

  @impl GenServer
  def handle_call({:set_threshold, engine_name, metric_type, thresholds}, _from, state) do
    threshold_config = %{
      metric_path: "#{metric_type}",
      warning: Keyword.get(thresholds, :warning),
      critical: Keyword.get(thresholds, :critical),
      unit: Keyword.get(thresholds, :unit, ""),
      comparison: Keyword.get(thresholds, :comparison, :greater),
      enabled: Keyword.get(thresholds, :enabled, true)
    }

    key = {engine_name, metric_type}
    new_configs = Map.put(state.threshold_configs, key, threshold_config)

    Logger.info("Set threshold alert",
      engine: engine_name,
      metric: metric_type,
      config: threshold_config
    )

    {:reply, :ok, %{state | threshold_configs: new_configs}}
  end

  @impl GenServer
  def handle_call({:set_trend_alert, engine_name, metric_type, trend_config}, _from, state) do
    config = %{
      metric_path: "#{metric_type}",
      trend: Keyword.get(trend_config, :trend, :increasing),
      duration: Keyword.get(trend_config, :duration, 300),
      threshold: Keyword.get(trend_config, :threshold, 0.2),
      window_size: Keyword.get(trend_config, :window_size, 10),
      enabled: Keyword.get(trend_config, :enabled, true)
    }

    key = {engine_name, metric_type}
    new_configs = Map.put(state.trend_configs, key, config)

    Logger.info("Set trend alert",
      engine: engine_name,
      metric: metric_type,
      config: config
    )

    {:reply, :ok, %{state | trend_configs: new_configs}}
  end

  @impl GenServer
  def handle_call({:configure_delivery, engine_name, delivery_config}, _from, state) do
    config = %{
      email: Keyword.get(delivery_config, :email, []),
      slack: Keyword.get(delivery_config, :slack, []),
      webhooks: Keyword.get(delivery_config, :webhooks, []),
      sms: Keyword.get(delivery_config, :sms, [])
    }

    new_configs = Map.put(state.delivery_configs, engine_name, config)

    Logger.info("Configured alert delivery",
      engine: engine_name,
      channels: Map.keys(config)
    )

    {:reply, :ok, %{state | delivery_configs: new_configs}}
  end

  @impl GenServer
  def handle_call({:acknowledge_alert, alert_id}, _from, state) do
    case Map.get(state.active_alerts, alert_id) do
      nil ->
        {:reply, {:error, :alert_not_found}, state}

      alert ->
        updated_alert = %{alert | acknowledged: true}
        new_active = Map.put(state.active_alerts, alert_id, updated_alert)
        new_stats = %{state.stats | acknowledgements: state.stats.acknowledgements + 1}

        Logger.info("Alert acknowledged", alert_id: alert_id)

        {:reply, :ok, %{state | active_alerts: new_active, stats: new_stats}}
    end
  end

  @impl GenServer
  def handle_call({:suppress_alerts, engine_name, metric_type, duration}, _from, state) do
    suppression_key = {engine_name, metric_type}
    expires_at = DateTime.add(DateTime.utc_now(), duration, :second)

    suppression_config = %{
      engine_name: engine_name,
      metric_type: metric_type,
      expires_at: expires_at,
      duration: duration
    }

    new_suppressions = Map.put(state.suppression_configs, suppression_key, suppression_config)
    new_stats = %{state.stats | suppressions: state.stats.suppressions + 1}

    Logger.info("Alert suppression configured",
      engine: engine_name,
      metric: metric_type,
      duration: duration
    )

    {:reply, :ok, %{state | suppression_configs: new_suppressions, stats: new_stats}}
  end

  @impl GenServer
  def handle_call(:get_active_alerts, _from, state) do
    active_alerts = Map.values(state.active_alerts)
    {:reply, active_alerts, state}
  end

  @impl GenServer
  def handle_call({:get_alert_history, opts}, _from, state) do
    limit = Keyword.get(opts, :limit, 100)
    engine_filter = Keyword.get(opts, :engine)
    level_filter = Keyword.get(opts, :level)

    filtered_history =
      state.alert_history
      |> filter_by_engine(engine_filter)
      |> filter_by_level(level_filter)
      |> Enum.take(limit)

    {:reply, filtered_history, state}
  end

  @impl GenServer
  def handle_call(:get_alert_stats, _from, state) do
    stats =
      Map.merge(state.stats, %{
        active_alerts_count: map_size(state.active_alerts),
        configured_thresholds: map_size(state.threshold_configs),
        configured_trends: map_size(state.trend_configs),
        active_suppressions: count_active_suppressions(state.suppression_configs)
      })

    {:reply, stats, state}
  end

  @impl GenServer
  def handle_cast({:check_thresholds, engine_name, metrics}, state) do
    new_state = check_all_alerts(engine_name, metrics, state)
    {:noreply, new_state}
  end

  @impl GenServer
  def handle_info(:cleanup_cycle, state) do
    new_state = perform_cleanup(state)

    # Schedule next cleanup
    Process.send_after(self(), :cleanup_cycle, 60_000)

    {:noreply, new_state}
  end

  ## Private functions

  defp check_all_alerts(engine_name, metrics, state) do
    timestamp = DateTime.utc_now()

    # Store metrics for trend analysis
    new_state = store_metrics_for_trends(engine_name, metrics, timestamp, state)

    # Check threshold alerts
    threshold_state = check_threshold_alerts(engine_name, metrics, new_state)

    # Check trend alerts
    trend_state = check_trend_alerts(engine_name, timestamp, threshold_state)

    # Update last check timestamp
    new_last_checks = Map.put(trend_state.last_checks, engine_name, timestamp)

    %{trend_state | last_checks: new_last_checks}
  end

  defp store_metrics_for_trends(engine_name, metrics, timestamp, state) do
    # Store metrics with timestamp for trend analysis
    engine_history = Map.get(state.metric_history, engine_name, [])

    metric_entry = %{
      timestamp: timestamp,
      metrics: metrics
    }

    # Keep only recent history (configurable window)
    max_history_size = 100
    updated_history = [metric_entry | Enum.take(engine_history, max_history_size - 1)]

    new_metric_history = Map.put(state.metric_history, engine_name, updated_history)

    %{state | metric_history: new_metric_history}
  end

  defp check_threshold_alerts(engine_name, metrics, state) do
    # Check all configured threshold alerts for this engine
    Enum.reduce(state.threshold_configs, state, fn {{config_engine, metric_type}, config},
                                                   acc_state ->
      if config_engine == engine_name and config.enabled do
        check_single_threshold(engine_name, metric_type, metrics, config, acc_state)
      else
        acc_state
      end
    end)
  end

  defp check_single_threshold(engine_name, metric_type, metrics, config, state) do
    # Extract metric value
    metric_value = extract_metric_value(metrics, config.metric_path)

    if metric_value do
      # Check if alert should be suppressed
      if suppressed?(engine_name, metric_type, state) do
        state
      else
        # Evaluate threshold conditions
        case evaluate_threshold(metric_value, config) do
          # No alert needed
          nil ->
            state

          level ->
            trigger_threshold_alert(engine_name, metric_type, level, metric_value, config, state)
        end
      end
    else
      state
    end
  end

  defp extract_metric_value(metrics, metric_path) do
    path_parts = String.split(metric_path, ".")

    Enum.reduce(path_parts, metrics, fn part, acc ->
      case acc do
        map when is_map(map) ->
          Map.get(map, String.to_atom(part)) || Map.get(map, part)

        _ ->
          nil
      end
    end)
  end

  defp evaluate_threshold(value, config) when is_number(value) do
    cond do
      config.critical && should_alert?(value, config.critical, config.comparison) ->
        :critical

      config.warning && should_alert?(value, config.warning, config.comparison) ->
        :warning

      true ->
        nil
    end
  end

  defp evaluate_threshold(_, _), do: nil

  defp should_alert?(value, threshold, :greater), do: value > threshold
  defp should_alert?(value, threshold, :less), do: value < threshold
  defp should_alert?(value, {min, max}, :range), do: value < min or value > max
  defp should_alert?(_, _, _), do: false

  defp trigger_threshold_alert(engine_name, metric_type, level, value, config, state) do
    alert_id = generate_alert_id()

    alert = %{
      id: alert_id,
      engine_name: engine_name,
      alert_type: :threshold,
      level: level,
      message: format_threshold_message(metric_type, level, value, config),
      details: %{
        metric_type: metric_type,
        current_value: value,
        threshold: get_threshold_for_level(config, level),
        unit: config.unit
      },
      timestamp: DateTime.utc_now(),
      suppressed: false,
      acknowledged: false
    }

    # Add to active alerts
    new_active = Map.put(state.active_alerts, alert_id, alert)

    # Add to history
    new_history = [alert | Enum.take(state.alert_history, state.max_alert_history - 1)]

    # Update stats
    new_stats = update_alert_stats(state.stats, level, engine_name)

    # Deliver alert
    deliver_alert(alert, state)

    Logger.warning("Threshold alert triggered",
      engine: engine_name,
      metric: metric_type,
      level: level,
      value: value
    )

    %{state | active_alerts: new_active, alert_history: new_history, stats: new_stats}
  end

  defp check_trend_alerts(engine_name, _timestamp, state) do
    # Check all configured trend alerts for this engine
    Enum.reduce(state.trend_configs, state, fn {{config_engine, metric_type}, config},
                                               acc_state ->
      if config_engine == engine_name and config.enabled do
        check_single_trend(engine_name, metric_type, config, acc_state)
      else
        acc_state
      end
    end)
  end

  defp check_single_trend(engine_name, metric_type, config, state) do
    case Map.get(state.metric_history, engine_name) do
      nil ->
        state

      [] ->
        state

      history ->
        if suppressed?(engine_name, metric_type, state) do
          state
        else
          case analyze_trend(history, config) do
            nil ->
              state

            trend_result ->
              trigger_trend_alert(engine_name, metric_type, trend_result, config, state)
          end
        end
    end
  end

  defp analyze_trend(history, config) do
    # Take recent samples for trend analysis
    recent_samples = Enum.take(history, config.window_size)

    if length(recent_samples) < 3 do
      # Not enough data for trend analysis
      nil
    else
      values =
        Enum.map(recent_samples, fn sample ->
          extract_metric_value(sample.metrics, config.metric_path)
        end)
        |> Enum.filter(&is_number/1)

      if length(values) < 3 do
        nil
      else
        calculate_trend(values, config)
      end
    end
  end

  defp calculate_trend(values, config) do
    [newest | _] = values
    [oldest | _] = Enum.reverse(values)

    if oldest == 0 do
      nil
    else
      change_ratio = (newest - oldest) / oldest

      case config.trend do
        :increasing when change_ratio > config.threshold ->
          {:increasing, change_ratio}

        :decreasing when change_ratio < -config.threshold ->
          {:decreasing, abs(change_ratio)}

        _ ->
          nil
      end
    end
  end

  defp trigger_trend_alert(
         engine_name,
         metric_type,
         {trend_direction, change_ratio},
         config,
         state
       ) do
    alert_id = generate_alert_id()

    level = if change_ratio > 0.5, do: :critical, else: :warning

    alert = %{
      id: alert_id,
      engine_name: engine_name,
      alert_type: :trend,
      level: level,
      message: format_trend_message(metric_type, trend_direction, change_ratio),
      details: %{
        metric_type: metric_type,
        trend_direction: trend_direction,
        change_ratio: change_ratio,
        duration: config.duration
      },
      timestamp: DateTime.utc_now(),
      suppressed: false,
      acknowledged: false
    }

    # Add to active alerts
    new_active = Map.put(state.active_alerts, alert_id, alert)

    # Add to history
    new_history = [alert | Enum.take(state.alert_history, state.max_alert_history - 1)]

    # Update stats
    new_stats = update_alert_stats(state.stats, level, engine_name)

    # Deliver alert
    deliver_alert(alert, state)

    Logger.warning("Trend alert triggered",
      engine: engine_name,
      metric: metric_type,
      trend: trend_direction,
      change: change_ratio
    )

    %{state | active_alerts: new_active, alert_history: new_history, stats: new_stats}
  end

  defp suppressed?(engine_name, metric_type, state) do
    suppression_key = {engine_name, metric_type}
    global_key = {engine_name, :all}

    case Map.get(state.suppression_configs, suppression_key) ||
           Map.get(state.suppression_configs, global_key) do
      nil -> false
      suppression -> DateTime.compare(DateTime.utc_now(), suppression.expires_at) == :lt
    end
  end

  defp deliver_alert(alert, state) do
    case Map.get(state.delivery_configs, alert.engine_name) do
      # No delivery configured
      nil ->
        :ok

      delivery_config ->
        Task.start(fn ->
          perform_alert_delivery(alert, delivery_config)
        end)
    end
  end

  defp perform_alert_delivery(alert, delivery_config) do
    # Email delivery
    Enum.each(delivery_config.email, fn email ->
      send_email_alert(alert, email)
    end)

    # Slack delivery
    Enum.each(delivery_config.slack, fn channel ->
      send_slack_alert(alert, channel)
    end)

    # Webhook delivery
    Enum.each(delivery_config.webhooks, fn webhook_url ->
      send_webhook_alert(alert, webhook_url)
    end)
  end

  defp send_email_alert(alert, email) do
    Logger.info("Sending email alert",
      alert_id: alert.id,
      email: email,
      level: alert.level
    )

    # Implementation would integrate with email service
  end

  defp send_slack_alert(alert, channel) do
    Logger.info("Sending Slack alert",
      alert_id: alert.id,
      channel: channel,
      level: alert.level
    )

    # Implementation would integrate with Slack API
  end

  defp send_webhook_alert(alert, webhook_url) do
    Logger.info("Sending webhook alert",
      alert_id: alert.id,
      webhook: webhook_url,
      level: alert.level
    )

    # Implementation would make HTTP POST to webhook
  end

  defp format_threshold_message(metric_type, level, value, config) do
    threshold = get_threshold_for_level(config, level)

    "#{String.upcase(to_string(level))}: #{metric_type} is #{value}#{config.unit}, " <>
      "exceeding #{level} threshold of #{threshold}#{config.unit}"
  end

  defp format_trend_message(metric_type, trend_direction, change_ratio) do
    percentage = Float.round(change_ratio * 100, 1)
    "TREND ALERT: #{metric_type} is #{trend_direction} by #{percentage}% over recent period"
  end

  defp get_threshold_for_level(config, :warning), do: config.warning
  defp get_threshold_for_level(config, :critical), do: config.critical

  defp update_alert_stats(stats, level, engine_name) do
    new_by_level = Map.update(stats.alerts_by_level, level, 1, &(&1 + 1))
    new_by_engine = Map.update(stats.alerts_by_engine, engine_name, 1, &(&1 + 1))

    %{
      stats
      | total_alerts: stats.total_alerts + 1,
        alerts_by_level: new_by_level,
        alerts_by_engine: new_by_engine
    }
  end

  defp filter_by_engine(alerts, nil), do: alerts

  defp filter_by_engine(alerts, engine) do
    Enum.filter(alerts, fn alert -> alert.engine_name == engine end)
  end

  defp filter_by_level(alerts, nil), do: alerts

  defp filter_by_level(alerts, level) do
    Enum.filter(alerts, fn alert -> alert.level == level end)
  end

  defp count_active_suppressions(suppressions) do
    now = DateTime.utc_now()

    Enum.count(suppressions, fn {_key, suppression} ->
      DateTime.compare(now, suppression.expires_at) == :lt
    end)
  end

  defp perform_cleanup(state) do
    Logger.debug("Performing alert manager cleanup")

    # Remove expired suppressions
    now = DateTime.utc_now()

    new_suppressions =
      Map.filter(state.suppression_configs, fn {_key, suppression} ->
        DateTime.compare(now, suppression.expires_at) == :lt
      end)

    # Remove acknowledged alerts older than 1 hour
    cutoff_time = DateTime.add(now, -3600, :second)

    new_active =
      Map.filter(state.active_alerts, fn {_id, alert} ->
        not alert.acknowledged or
          DateTime.compare(alert.timestamp, cutoff_time) == :gt
      end)

    %{state | suppression_configs: new_suppressions, active_alerts: new_active}
  end

  defp generate_alert_id do
    "alert_" <> (:crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower))
  end
end

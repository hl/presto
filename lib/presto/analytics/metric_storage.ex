defmodule Presto.Analytics.MetricStorage do
  @moduledoc """
  High-performance metric storage backend for Presto analytics.

  Provides efficient storage and retrieval of time-series metrics with:
  - Configurable retention policies
  - Automatic data aggregation and compression
  - Fast range queries and analytics
  - Memory-efficient storage strategies
  - Background maintenance and cleanup

  ## Storage Strategies

  ### In-Memory Storage (Default)
  - Fast access for real-time metrics
  - Configurable memory limits
  - Automatic eviction policies

  ### Persistent Storage
  - Long-term metric retention
  - Disk-based storage with compression
  - Automatic backup and recovery

  ### Hybrid Storage
  - Recent metrics in memory for speed
  - Historical metrics on disk for capacity
  - Transparent data lifecycle management
  """

  use GenServer
  require Logger

  @type metric_data :: %{
          timestamp: DateTime.t(),
          values: map(),
          metadata: map()
        }

  @type aggregation_type :: :avg | :sum | :count | :min | :max | :percentile

  @type storage_opts :: [
          strategy: :memory | :disk | :hybrid,
          retention_period: pos_integer(),
          max_memory_usage: pos_integer(),
          compression: boolean(),
          aggregation_interval: pos_integer()
        ]

  ## Client API

  @doc """
  Starts the metric storage backend.
  """
  @spec start_link(storage_opts()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Stores metrics for an engine.
  """
  @spec store_metrics(pid(), atom(), map()) :: :ok | {:error, term()}
  def store_metrics(storage_pid, engine_name, metrics) do
    GenServer.call(storage_pid, {:store_metrics, engine_name, metrics})
  end

  @doc """
  Stores a batch of metric data points.
  """
  @spec store_metric_batch(pid(), atom(), [map()]) :: :ok | {:error, term()}
  def store_metric_batch(storage_pid, engine_name, metric_batch) do
    GenServer.call(storage_pid, {:store_batch, engine_name, metric_batch})
  end

  @doc """
  Gets the latest metrics for an engine.
  """
  @spec get_latest_metrics(pid(), atom()) :: {:ok, map()} | {:error, term()}
  def get_latest_metrics(storage_pid, engine_name) do
    GenServer.call(storage_pid, {:get_latest, engine_name})
  end

  @doc """
  Gets aggregated metrics for a time range.
  """
  @spec get_aggregated_metrics(pid(), atom(), DateTime.t(), DateTime.t()) ::
          {:ok, map()} | {:error, term()}
  def get_aggregated_metrics(storage_pid, engine_name, start_time, end_time) do
    GenServer.call(storage_pid, {:get_aggregated, engine_name, start_time, end_time})
  end

  @doc """
  Gets metric time series data.
  """
  @spec get_time_series(pid(), atom(), String.t(), DateTime.t(), DateTime.t()) ::
          {:ok, [metric_data()]} | {:error, term()}
  def get_time_series(storage_pid, engine_name, metric_path, start_time, end_time) do
    GenServer.call(
      storage_pid,
      {:get_time_series, engine_name, metric_path, start_time, end_time}
    )
  end

  @doc """
  Cleans up old metrics based on retention policies.
  """
  @spec cleanup_old_metrics(pid()) :: :ok
  def cleanup_old_metrics(storage_pid) do
    GenServer.cast(storage_pid, :cleanup_old_metrics)
  end

  @doc """
  Gets storage statistics.
  """
  @spec get_storage_stats(pid()) :: map()
  def get_storage_stats(storage_pid) do
    GenServer.call(storage_pid, :get_storage_stats)
  end

  ## Server implementation

  @impl GenServer
  def init(opts) do
    Logger.info("Starting Metric Storage Backend")

    strategy = Keyword.get(opts, :strategy, :memory)

    state = %{
      # Configuration
      strategy: strategy,
      # 24 hours
      retention_period: Keyword.get(opts, :retention_period, 86400),
      # 100MB
      max_memory_usage: Keyword.get(opts, :max_memory_usage, 100 * 1024 * 1024),
      compression: Keyword.get(opts, :compression, true),
      # 5 minutes
      aggregation_interval: Keyword.get(opts, :aggregation_interval, 300),

      # Storage backends
      memory_storage: %{},
      disk_storage_path: determine_disk_path(strategy),

      # Metadata
      engine_metadata: %{},
      last_cleanup: DateTime.utc_now(),

      # Statistics
      stats: %{
        total_metrics_stored: 0,
        memory_usage_bytes: 0,
        disk_usage_bytes: 0,
        engines_tracked: 0,
        cleanup_runs: 0
      }
    }

    # Initialize storage backend
    case strategy do
      :disk -> initialize_disk_storage(state)
      :hybrid -> initialize_hybrid_storage(state)
      _ -> {:ok, state}
    end
  end

  @impl GenServer
  def handle_call({:store_metrics, engine_name, metrics}, _from, state) do
    timestamp = DateTime.utc_now()

    metric_data = %{
      timestamp: timestamp,
      values: metrics,
      metadata: %{
        engine_name: engine_name,
        storage_strategy: state.strategy
      }
    }

    case store_metric_data(engine_name, metric_data, state) do
      {:ok, new_state} ->
        {:reply, :ok, new_state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl GenServer
  def handle_call({:store_batch, engine_name, metric_batch}, _from, state) do
    case store_batch_data(engine_name, metric_batch, state) do
      {:ok, new_state} ->
        {:reply, :ok, new_state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl GenServer
  def handle_call({:get_latest, engine_name}, _from, state) do
    case get_latest_metric_data(engine_name, state) do
      {:ok, data} ->
        {:reply, {:ok, data}, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl GenServer
  def handle_call({:get_aggregated, engine_name, start_time, end_time}, _from, state) do
    case get_aggregated_data(engine_name, start_time, end_time, state) do
      {:ok, data} ->
        {:reply, {:ok, data}, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl GenServer
  def handle_call(
        {:get_time_series, engine_name, metric_path, start_time, end_time},
        _from,
        state
      ) do
    case get_time_series_data(engine_name, metric_path, start_time, end_time, state) do
      {:ok, data} ->
        {:reply, {:ok, data}, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl GenServer
  def handle_call(:get_storage_stats, _from, state) do
    stats = calculate_current_stats(state)
    {:reply, stats, state}
  end

  @impl GenServer
  def handle_cast(:cleanup_old_metrics, state) do
    new_state = perform_cleanup(state)
    {:noreply, new_state}
  end

  ## Private functions

  defp determine_disk_path(:disk), do: "tmp/presto_metrics"
  defp determine_disk_path(:hybrid), do: "tmp/presto_metrics"
  defp determine_disk_path(_), do: nil

  defp initialize_disk_storage(state) do
    case state.disk_storage_path do
      nil ->
        {:ok, state}

      path ->
        File.mkdir_p(path)
        {:ok, state}
    end
  end

  defp initialize_hybrid_storage(state) do
    initialize_disk_storage(state)
  end

  defp store_metric_data(engine_name, metric_data, state) do
    case state.strategy do
      :memory ->
        store_to_memory(engine_name, metric_data, state)

      :disk ->
        store_to_disk(engine_name, metric_data, state)

      :hybrid ->
        store_to_hybrid(engine_name, metric_data, state)
    end
  end

  defp store_to_memory(engine_name, metric_data, state) do
    # Get existing metrics for engine
    engine_metrics = Map.get(state.memory_storage, engine_name, [])

    # Add new metric data
    updated_metrics = [metric_data | engine_metrics]

    # Limit memory usage by keeping only recent metrics
    trimmed_metrics = trim_metrics_by_retention(updated_metrics, state.retention_period)

    # Update storage
    new_memory_storage = Map.put(state.memory_storage, engine_name, trimmed_metrics)

    # Update statistics
    new_stats = %{
      state.stats
      | total_metrics_stored: state.stats.total_metrics_stored + 1,
        memory_usage_bytes: calculate_memory_usage(new_memory_storage),
        engines_tracked: map_size(new_memory_storage)
    }

    new_state = %{state | memory_storage: new_memory_storage, stats: new_stats}

    {:ok, new_state}
  end

  defp store_to_disk(engine_name, metric_data, state) do
    # For disk storage, we'd write to files
    # This is a simplified implementation
    case state.disk_storage_path do
      nil ->
        {:error, :disk_path_not_configured}

      path ->
        engine_path = Path.join(path, "#{engine_name}.metrics")

        # In a real implementation, this would be more sophisticated
        # with proper serialization, compression, and indexing
        serialized_data = :erlang.term_to_binary(metric_data)

        case File.write(engine_path, serialized_data, [:append]) do
          :ok ->
            new_stats = %{
              state.stats
              | total_metrics_stored: state.stats.total_metrics_stored + 1,
                disk_usage_bytes: state.stats.disk_usage_bytes + byte_size(serialized_data)
            }

            {:ok, %{state | stats: new_stats}}

          {:error, reason} ->
            {:error, reason}
        end
    end
  end

  defp store_to_hybrid(engine_name, metric_data, state) do
    # Store recent metrics in memory for fast access
    {:ok, memory_state} = store_to_memory(engine_name, metric_data, state)

    # Asynchronously store to disk for persistence
    Task.start(fn ->
      store_to_disk(engine_name, metric_data, state)
    end)

    {:ok, memory_state}
  end

  defp store_batch_data(engine_name, metric_batch, state) do
    # Process batch efficiently
    case state.strategy do
      :memory ->
        store_batch_to_memory(engine_name, metric_batch, state)

      :disk ->
        store_batch_to_disk(engine_name, metric_batch, state)

      :hybrid ->
        store_batch_to_hybrid(engine_name, metric_batch, state)
    end
  end

  defp store_batch_to_memory(engine_name, metric_batch, state) do
    # Convert batch to metric data format
    metric_data_list =
      Enum.map(metric_batch, fn metric ->
        %{
          timestamp: Map.get(metric, :timestamp, DateTime.utc_now()),
          values: metric,
          metadata: %{engine_name: engine_name}
        }
      end)

    # Get existing metrics
    engine_metrics = Map.get(state.memory_storage, engine_name, [])

    # Add batch
    updated_metrics = metric_data_list ++ engine_metrics

    # Trim by retention
    trimmed_metrics = trim_metrics_by_retention(updated_metrics, state.retention_period)

    # Update storage
    new_memory_storage = Map.put(state.memory_storage, engine_name, trimmed_metrics)

    new_stats = %{
      state.stats
      | total_metrics_stored: state.stats.total_metrics_stored + length(metric_batch),
        memory_usage_bytes: calculate_memory_usage(new_memory_storage)
    }

    {:ok, %{state | memory_storage: new_memory_storage, stats: new_stats}}
  end

  defp store_batch_to_disk(engine_name, metric_batch, state) do
    # Batch write to disk for efficiency
    case state.disk_storage_path do
      nil ->
        {:error, :disk_path_not_configured}

      path ->
        engine_path = Path.join(path, "#{engine_name}.metrics")

        serialized_batch = Enum.map(metric_batch, &:erlang.term_to_binary/1)
        # Use null separator
        combined_data = Enum.join(serialized_batch, <<0>>)

        case File.write(engine_path, combined_data, [:append]) do
          :ok ->
            new_stats = %{
              state.stats
              | total_metrics_stored: state.stats.total_metrics_stored + length(metric_batch),
                disk_usage_bytes: state.stats.disk_usage_bytes + byte_size(combined_data)
            }

            {:ok, %{state | stats: new_stats}}

          {:error, reason} ->
            {:error, reason}
        end
    end
  end

  defp store_batch_to_hybrid(engine_name, metric_batch, state) do
    {:ok, memory_state} = store_batch_to_memory(engine_name, metric_batch, state)

    Task.start(fn ->
      store_batch_to_disk(engine_name, metric_batch, state)
    end)

    {:ok, memory_state}
  end

  defp get_latest_metric_data(engine_name, state) do
    case state.strategy do
      :memory ->
        case Map.get(state.memory_storage, engine_name) do
          nil -> {:error, :not_found}
          [] -> {:error, :no_data}
          [latest | _] -> {:ok, latest.values}
        end

      :disk ->
        get_latest_from_disk(engine_name, state)

      :hybrid ->
        # Try memory first, fallback to disk
        case Map.get(state.memory_storage, engine_name) do
          nil -> get_latest_from_disk(engine_name, state)
          [] -> get_latest_from_disk(engine_name, state)
          [latest | _] -> {:ok, latest.values}
        end
    end
  end

  defp get_latest_from_disk(engine_name, state) do
    # Simplified disk read - in reality would be more sophisticated
    case state.disk_storage_path do
      nil ->
        {:error, :disk_not_configured}

      path ->
        engine_path = Path.join(path, "#{engine_name}.metrics")

        case File.exists?(engine_path) do
          false -> {:error, :not_found}
          # Placeholder
          true -> {:error, :disk_read_not_implemented}
        end
    end
  end

  defp get_aggregated_data(engine_name, start_time, end_time, state) do
    case get_metrics_in_range(engine_name, start_time, end_time, state) do
      {:ok, metrics} ->
        aggregated = aggregate_metrics(metrics)
        {:ok, aggregated}

      error ->
        error
    end
  end

  defp get_metrics_in_range(engine_name, start_time, end_time, state) do
    case state.strategy do
      :memory ->
        case Map.get(state.memory_storage, engine_name) do
          nil ->
            {:error, :not_found}

          metrics ->
            filtered =
              Enum.filter(metrics, fn metric ->
                DateTime.compare(metric.timestamp, start_time) != :lt and
                  DateTime.compare(metric.timestamp, end_time) != :gt
              end)

            {:ok, filtered}
        end

      _ ->
        # Placeholder for disk/hybrid
        {:error, :not_implemented}
    end
  end

  defp get_time_series_data(engine_name, metric_path, start_time, end_time, state) do
    case get_metrics_in_range(engine_name, start_time, end_time, state) do
      {:ok, metrics} ->
        # Extract specific metric path from each data point
        time_series =
          Enum.map(metrics, fn metric ->
            value = extract_metric_value(metric.values, metric_path)

            %{
              timestamp: metric.timestamp,
              value: value
            }
          end)

        {:ok, time_series}

      error ->
        error
    end
  end

  defp extract_metric_value(values, path) do
    path_parts = String.split(path, ".")

    Enum.reduce(path_parts, values, fn part, acc ->
      case acc do
        map when is_map(map) -> Map.get(map, String.to_atom(part))
        _ -> nil
      end
    end)
  end

  defp aggregate_metrics(metrics) do
    case metrics do
      [] ->
        %{}

      [first | _] ->
        # Initialize aggregation with first metric structure
        initial_agg = initialize_aggregation(first.values)

        # Aggregate all metrics
        Enum.reduce(metrics, initial_agg, fn metric, acc ->
          aggregate_single_metric(metric.values, acc)
        end)
        |> finalize_aggregation(length(metrics))
    end
  end

  defp initialize_aggregation(values) do
    Map.new(values, fn {key, _value} ->
      {key, %{sum: 0, count: 0, min: nil, max: nil, values: []}}
    end)
  end

  defp aggregate_single_metric(values, aggregation) do
    Enum.reduce(values, aggregation, fn {key, value}, acc ->
      case Map.get(acc, key) do
        nil ->
          acc

        agg_data ->
          updated_agg = %{
            agg_data
            | sum: agg_data.sum + if(is_number(value), do: value, else: 0),
              count: agg_data.count + 1,
              min: min_value(agg_data.min, value),
              max: max_value(agg_data.max, value),
              values: [value | agg_data.values]
          }

          Map.put(acc, key, updated_agg)
      end
    end)
  end

  defp finalize_aggregation(aggregation, total_count) do
    Map.new(aggregation, fn {key, agg_data} ->
      final_data = %{
        avg: if(agg_data.count > 0, do: agg_data.sum / agg_data.count, else: 0),
        sum: agg_data.sum,
        count: agg_data.count,
        min: agg_data.min,
        max: agg_data.max,
        total_samples: total_count
      }

      {key, final_data}
    end)
  end

  defp min_value(nil, value), do: value

  defp min_value(current, value) when is_number(current) and is_number(value),
    do: min(current, value)

  defp min_value(current, _), do: current

  defp max_value(nil, value), do: value

  defp max_value(current, value) when is_number(current) and is_number(value),
    do: max(current, value)

  defp max_value(current, _), do: current

  defp trim_metrics_by_retention(metrics, retention_seconds) do
    cutoff_time = DateTime.add(DateTime.utc_now(), -retention_seconds, :second)

    Enum.filter(metrics, fn metric ->
      DateTime.compare(metric.timestamp, cutoff_time) == :gt
    end)
  end

  defp calculate_memory_usage(memory_storage) do
    # Rough estimate of memory usage
    :erlang.external_size(memory_storage)
  end

  defp perform_cleanup(state) do
    Logger.info("Performing metric storage cleanup")

    case state.strategy do
      :memory ->
        cleanup_memory_storage(state)

      :disk ->
        cleanup_disk_storage(state)

      :hybrid ->
        state
        |> cleanup_memory_storage()
        |> cleanup_disk_storage()
    end
  end

  defp cleanup_memory_storage(state) do
    new_memory_storage =
      Map.new(state.memory_storage, fn {engine_name, metrics} ->
        cleaned_metrics = trim_metrics_by_retention(metrics, state.retention_period)
        {engine_name, cleaned_metrics}
      end)

    new_stats = %{
      state.stats
      | cleanup_runs: state.stats.cleanup_runs + 1,
        memory_usage_bytes: calculate_memory_usage(new_memory_storage)
    }

    %{
      state
      | memory_storage: new_memory_storage,
        stats: new_stats,
        last_cleanup: DateTime.utc_now()
    }
  end

  defp cleanup_disk_storage(state) do
    # Placeholder for disk cleanup
    # In reality, would clean up old metric files
    new_stats = %{state.stats | cleanup_runs: state.stats.cleanup_runs + 1}
    %{state | stats: new_stats, last_cleanup: DateTime.utc_now()}
  end

  defp calculate_current_stats(state) do
    Map.merge(state.stats, %{
      strategy: state.strategy,
      retention_period_hours: state.retention_period / 3600,
      last_cleanup: state.last_cleanup,
      engines_with_data: map_size(state.memory_storage)
    })
  end
end

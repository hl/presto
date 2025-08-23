defmodule Presto.Debugging.WorkingMemoryInspector do
  @moduledoc """
  Working memory inspection system for debugging rule engines.

  Provides comprehensive working memory analysis including:
  - Real-time fact monitoring
  - Working memory snapshots
  - Fact lifecycle tracking
  - Memory usage analysis
  - Rule-fact relationship mapping
  """

  use GenServer
  require Logger

  @type fact_entry :: %{
          id: String.t(),
          fact: term(),
          fact_type: atom(),
          inserted_at: DateTime.t(),
          modified_at: DateTime.t(),
          source_rule: atom() | nil,
          reference_count: non_neg_integer(),
          metadata: map()
        }

  @type memory_snapshot :: %{
          id: String.t(),
          timestamp: DateTime.t(),
          engine_name: atom(),
          fact_count: non_neg_integer(),
          facts: [fact_entry()],
          memory_stats: map(),
          context: map()
        }

  @type inspector_state :: %{
          monitored_engines: MapSet.t(),
          fact_tracking: %{atom() => %{String.t() => fact_entry()}},
          snapshots: [memory_snapshot()],
          max_snapshots: pos_integer(),
          auto_snapshot_interval: pos_integer() | nil,
          snapshot_subscribers: [pid()],
          fact_change_subscribers: [pid()],
          stats: map()
        }

  ## Client API

  @doc """
  Starts the working memory inspector.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Starts monitoring an engine's working memory.
  """
  @spec start_monitoring(atom(), keyword()) :: :ok | {:error, term()}
  def start_monitoring(engine_name, opts \\ []) do
    GenServer.call(__MODULE__, {:start_monitoring, engine_name, opts})
  end

  @doc """
  Stops monitoring an engine's working memory.
  """
  @spec stop_monitoring(atom()) :: :ok
  def stop_monitoring(engine_name) do
    GenServer.call(__MODULE__, {:stop_monitoring, engine_name})
  end

  @doc """
  Takes a snapshot of an engine's working memory.
  """
  @spec take_snapshot(atom(), map()) :: {:ok, String.t()} | {:error, term()}
  def take_snapshot(engine_name, context \\ %{}) do
    GenServer.call(__MODULE__, {:take_snapshot, engine_name, context})
  end

  @doc """
  Gets all snapshots for an engine.
  """
  @spec get_snapshots(atom(), keyword()) :: [memory_snapshot()]
  def get_snapshots(engine_name \\ :all, opts \\ []) do
    GenServer.call(__MODULE__, {:get_snapshots, engine_name, opts})
  end

  @doc """
  Gets current working memory state for an engine.
  """
  @spec get_current_memory(atom()) :: {:ok, map()} | {:error, term()}
  def get_current_memory(engine_name) do
    GenServer.call(__MODULE__, {:get_current_memory, engine_name})
  end

  @doc """
  Tracks a fact insertion.
  """
  @spec track_fact_insert(atom(), term(), atom() | nil, map()) :: :ok
  def track_fact_insert(engine_name, fact, source_rule \\ nil, metadata \\ %{}) do
    GenServer.cast(__MODULE__, {:track_fact_insert, engine_name, fact, source_rule, metadata})
  end

  @doc """
  Tracks a fact modification.
  """
  @spec track_fact_modify(atom(), String.t(), term(), map()) :: :ok
  def track_fact_modify(engine_name, fact_id, new_fact, metadata \\ %{}) do
    GenServer.cast(__MODULE__, {:track_fact_modify, engine_name, fact_id, new_fact, metadata})
  end

  @doc """
  Tracks a fact retraction.
  """
  @spec track_fact_retract(atom(), String.t(), map()) :: :ok
  def track_fact_retract(engine_name, fact_id, metadata \\ %{}) do
    GenServer.cast(__MODULE__, {:track_fact_retract, engine_name, fact_id, metadata})
  end

  @doc """
  Searches facts by pattern or type.
  """
  @spec search_facts(atom(), term()) :: [fact_entry()]
  def search_facts(engine_name, pattern_or_type) do
    GenServer.call(__MODULE__, {:search_facts, engine_name, pattern_or_type})
  end

  @doc """
  Gets memory usage statistics.
  """
  @spec get_memory_stats(atom()) :: {:ok, map()} | {:error, term()}
  def get_memory_stats(engine_name) do
    GenServer.call(__MODULE__, {:get_memory_stats, engine_name})
  end

  @doc """
  Subscribes to snapshot notifications.
  """
  @spec subscribe_snapshots(pid()) :: :ok
  def subscribe_snapshots(subscriber_pid) do
    GenServer.call(__MODULE__, {:subscribe_snapshots, subscriber_pid})
  end

  @doc """
  Subscribes to fact change notifications.
  """
  @spec subscribe_fact_changes(pid()) :: :ok
  def subscribe_fact_changes(subscriber_pid) do
    GenServer.call(__MODULE__, {:subscribe_fact_changes, subscriber_pid})
  end

  @doc """
  Enables or disables auto-snapshots.
  """
  @spec set_auto_snapshot(pos_integer() | nil) :: :ok
  def set_auto_snapshot(interval_ms) do
    GenServer.call(__MODULE__, {:set_auto_snapshot, interval_ms})
  end

  ## Server implementation

  @impl GenServer
  def init(opts) do
    state = %{
      monitored_engines: MapSet.new(),
      fact_tracking: %{},
      snapshots: [],
      max_snapshots: Keyword.get(opts, :max_snapshots, 100),
      auto_snapshot_interval: Keyword.get(opts, :auto_snapshot_interval),
      snapshot_subscribers: [],
      fact_change_subscribers: [],
      stats: %{
        total_snapshots: 0,
        total_fact_operations: 0,
        monitored_engines: 0
      }
    }

    # Start auto-snapshot timer if configured
    if state.auto_snapshot_interval do
      Process.send_after(self(), :auto_snapshot, state.auto_snapshot_interval)
    end

    Logger.info("Starting Working Memory Inspector")
    {:ok, state}
  end

  @impl GenServer
  def handle_call({:start_monitoring, engine_name, opts}, _from, state) do
    if MapSet.member?(state.monitored_engines, engine_name) do
      {:reply, {:error, :already_monitoring}, state}
    else
      new_monitored = MapSet.put(state.monitored_engines, engine_name)
      new_fact_tracking = Map.put(state.fact_tracking, engine_name, %{})

      # Take initial snapshot if requested
      new_state = %{
        state
        | monitored_engines: new_monitored,
          fact_tracking: new_fact_tracking
      }

      final_state =
        if Keyword.get(opts, :initial_snapshot, true) do
          case take_snapshot_internal(engine_name, %{type: :initial}, new_state) do
            {:ok, _, updated_state} -> updated_state
            {:error, _} -> new_state
          end
        else
          new_state
        end

      new_stats = %{
        final_state.stats
        | monitored_engines: final_state.stats.monitored_engines + 1
      }

      Logger.info("Started monitoring working memory", engine: engine_name)
      {:reply, :ok, %{final_state | stats: new_stats}}
    end
  end

  @impl GenServer
  def handle_call({:stop_monitoring, engine_name}, _from, state) do
    new_monitored = MapSet.delete(state.monitored_engines, engine_name)
    new_fact_tracking = Map.delete(state.fact_tracking, engine_name)
    new_stats = %{state.stats | monitored_engines: max(0, state.stats.monitored_engines - 1)}

    Logger.info("Stopped monitoring working memory", engine: engine_name)

    {:reply, :ok,
     %{
       state
       | monitored_engines: new_monitored,
         fact_tracking: new_fact_tracking,
         stats: new_stats
     }}
  end

  @impl GenServer
  def handle_call({:take_snapshot, engine_name, context}, _from, state) do
    case take_snapshot_internal(engine_name, context, state) do
      {:ok, snapshot_id, new_state} ->
        {:reply, {:ok, snapshot_id}, new_state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl GenServer
  def handle_call({:get_snapshots, engine_filter, opts}, _from, state) do
    limit = Keyword.get(opts, :limit, length(state.snapshots))
    since = Keyword.get(opts, :since)

    filtered_snapshots =
      state.snapshots
      |> filter_snapshots_by_engine(engine_filter)
      |> filter_snapshots_by_time(since)
      |> Enum.take(limit)

    {:reply, filtered_snapshots, state}
  end

  @impl GenServer
  def handle_call({:get_current_memory, engine_name}, _from, state) do
    if MapSet.member?(state.monitored_engines, engine_name) do
      case Map.get(state.fact_tracking, engine_name) do
        nil ->
          {:reply, {:error, :engine_not_monitored}, state}

        fact_map ->
          current_memory = %{
            engine_name: engine_name,
            fact_count: map_size(fact_map),
            facts: Map.values(fact_map),
            memory_stats: calculate_memory_stats(fact_map),
            timestamp: DateTime.utc_now()
          }

          {:reply, {:ok, current_memory}, state}
      end
    else
      {:reply, {:error, :engine_not_monitored}, state}
    end
  end

  @impl GenServer
  def handle_call({:search_facts, engine_name, pattern_or_type}, _from, state) do
    case Map.get(state.fact_tracking, engine_name) do
      nil ->
        {:reply, [], state}

      fact_map ->
        matching_facts =
          fact_map
          |> Map.values()
          |> Enum.filter(&fact_matches_pattern?(&1, pattern_or_type))

        {:reply, matching_facts, state}
    end
  end

  @impl GenServer
  def handle_call({:get_memory_stats, engine_name}, _from, state) do
    case Map.get(state.fact_tracking, engine_name) do
      nil ->
        {:reply, {:error, :engine_not_monitored}, state}

      fact_map ->
        stats = calculate_detailed_memory_stats(fact_map, engine_name)
        {:reply, {:ok, stats}, state}
    end
  end

  @impl GenServer
  def handle_call({:subscribe_snapshots, subscriber_pid}, _from, state) do
    if subscriber_pid in state.snapshot_subscribers do
      {:reply, :ok, state}
    else
      Process.monitor(subscriber_pid)
      new_subscribers = [subscriber_pid | state.snapshot_subscribers]
      {:reply, :ok, %{state | snapshot_subscribers: new_subscribers}}
    end
  end

  @impl GenServer
  def handle_call({:subscribe_fact_changes, subscriber_pid}, _from, state) do
    if subscriber_pid in state.fact_change_subscribers do
      {:reply, :ok, state}
    else
      Process.monitor(subscriber_pid)
      new_subscribers = [subscriber_pid | state.fact_change_subscribers]
      {:reply, :ok, %{state | fact_change_subscribers: new_subscribers}}
    end
  end

  @impl GenServer
  def handle_call({:set_auto_snapshot, interval_ms}, _from, state) do
    # Cancel existing timer if any
    if state.auto_snapshot_interval do
      # Note: In a real implementation, we'd store and cancel the timer reference
    end

    # Start new timer if interval provided
    if interval_ms do
      Process.send_after(self(), :auto_snapshot, interval_ms)
    end

    {:reply, :ok, %{state | auto_snapshot_interval: interval_ms}}
  end

  @impl GenServer
  def handle_cast({:track_fact_insert, engine_name, fact, source_rule, metadata}, state) do
    if MapSet.member?(state.monitored_engines, engine_name) do
      fact_id = generate_fact_id()

      fact_entry = %{
        id: fact_id,
        fact: fact,
        fact_type: determine_fact_type(fact),
        inserted_at: DateTime.utc_now(),
        modified_at: DateTime.utc_now(),
        source_rule: source_rule,
        reference_count: 1,
        metadata: metadata
      }

      engine_facts = Map.get(state.fact_tracking, engine_name, %{})
      new_engine_facts = Map.put(engine_facts, fact_id, fact_entry)
      new_fact_tracking = Map.put(state.fact_tracking, engine_name, new_engine_facts)

      # Notify subscribers
      notify_fact_change_subscribers(
        :insert,
        engine_name,
        fact_entry,
        state.fact_change_subscribers
      )

      new_stats = %{state.stats | total_fact_operations: state.stats.total_fact_operations + 1}

      {:noreply, %{state | fact_tracking: new_fact_tracking, stats: new_stats}}
    else
      {:noreply, state}
    end
  end

  @impl GenServer
  def handle_cast({:track_fact_modify, engine_name, fact_id, new_fact, metadata}, state) do
    if MapSet.member?(state.monitored_engines, engine_name) do
      engine_facts = Map.get(state.fact_tracking, engine_name, %{})

      case Map.get(engine_facts, fact_id) do
        nil ->
          {:noreply, state}

        existing_fact ->
          updated_fact = %{
            existing_fact
            | fact: new_fact,
              modified_at: DateTime.utc_now(),
              metadata: Map.merge(existing_fact.metadata, metadata)
          }

          new_engine_facts = Map.put(engine_facts, fact_id, updated_fact)
          new_fact_tracking = Map.put(state.fact_tracking, engine_name, new_engine_facts)

          # Notify subscribers
          notify_fact_change_subscribers(
            :modify,
            engine_name,
            updated_fact,
            state.fact_change_subscribers
          )

          new_stats = %{
            state.stats
            | total_fact_operations: state.stats.total_fact_operations + 1
          }

          {:noreply, %{state | fact_tracking: new_fact_tracking, stats: new_stats}}
      end
    else
      {:noreply, state}
    end
  end

  @impl GenServer
  def handle_cast({:track_fact_retract, engine_name, fact_id, metadata}, state) do
    if MapSet.member?(state.monitored_engines, engine_name) do
      engine_facts = Map.get(state.fact_tracking, engine_name, %{})

      case Map.get(engine_facts, fact_id) do
        nil ->
          {:noreply, state}

        fact_entry ->
          new_engine_facts = Map.delete(engine_facts, fact_id)
          new_fact_tracking = Map.put(state.fact_tracking, engine_name, new_engine_facts)

          # Notify subscribers
          retracted_fact = %{fact_entry | metadata: Map.merge(fact_entry.metadata, metadata)}

          notify_fact_change_subscribers(
            :retract,
            engine_name,
            retracted_fact,
            state.fact_change_subscribers
          )

          new_stats = %{
            state.stats
            | total_fact_operations: state.stats.total_fact_operations + 1
          }

          {:noreply, %{state | fact_tracking: new_fact_tracking, stats: new_stats}}
      end
    else
      {:noreply, state}
    end
  end

  @impl GenServer
  def handle_info(:auto_snapshot, state) do
    # Take snapshots for all monitored engines
    new_state =
      Enum.reduce(state.monitored_engines, state, fn engine_name, acc_state ->
        case take_snapshot_internal(engine_name, %{type: :auto}, acc_state) do
          {:ok, _, updated_state} -> updated_state
          {:error, _} -> acc_state
        end
      end)

    # Schedule next auto-snapshot
    if new_state.auto_snapshot_interval do
      Process.send_after(self(), :auto_snapshot, new_state.auto_snapshot_interval)
    end

    {:noreply, new_state}
  end

  @impl GenServer
  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    # Remove dead subscribers
    new_snapshot_subscribers = List.delete(state.snapshot_subscribers, pid)
    new_fact_change_subscribers = List.delete(state.fact_change_subscribers, pid)

    {:noreply,
     %{
       state
       | snapshot_subscribers: new_snapshot_subscribers,
         fact_change_subscribers: new_fact_change_subscribers
     }}
  end

  ## Private functions

  defp take_snapshot_internal(engine_name, context, state) do
    if MapSet.member?(state.monitored_engines, engine_name) do
      case Map.get(state.fact_tracking, engine_name) do
        nil ->
          {:error, :engine_not_monitored}

        fact_map ->
          snapshot_id = generate_snapshot_id()

          snapshot = %{
            id: snapshot_id,
            timestamp: DateTime.utc_now(),
            engine_name: engine_name,
            fact_count: map_size(fact_map),
            facts: Map.values(fact_map),
            memory_stats: calculate_memory_stats(fact_map),
            context: context
          }

          new_snapshots = add_snapshot(snapshot, state.snapshots, state.max_snapshots)
          new_stats = %{state.stats | total_snapshots: state.stats.total_snapshots + 1}

          # Notify subscribers
          notify_snapshot_subscribers(snapshot, state.snapshot_subscribers)

          Logger.debug("Took working memory snapshot",
            engine: engine_name,
            snapshot_id: snapshot_id,
            fact_count: snapshot.fact_count
          )

          {:ok, snapshot_id, %{state | snapshots: new_snapshots, stats: new_stats}}
      end
    else
      {:error, :engine_not_monitored}
    end
  end

  defp add_snapshot(snapshot, snapshots, max_snapshots) do
    [snapshot | Enum.take(snapshots, max_snapshots - 1)]
  end

  defp filter_snapshots_by_engine(snapshots, :all), do: snapshots

  defp filter_snapshots_by_engine(snapshots, engine_name) do
    Enum.filter(snapshots, &(&1.engine_name == engine_name))
  end

  defp filter_snapshots_by_time(snapshots, nil), do: snapshots

  defp filter_snapshots_by_time(snapshots, since_datetime) do
    Enum.filter(snapshots, fn snapshot ->
      DateTime.compare(snapshot.timestamp, since_datetime) != :lt
    end)
  end

  defp fact_matches_pattern?(fact_entry, pattern) when is_atom(pattern) do
    fact_entry.fact_type == pattern
  end

  defp fact_matches_pattern?(fact_entry, pattern) do
    # Simple pattern matching - in production this would be more sophisticated
    inspect(fact_entry.fact) =~ inspect(pattern)
  end

  defp calculate_memory_stats(fact_map) do
    facts = Map.values(fact_map)

    %{
      total_facts: length(facts),
      fact_types: count_fact_types(facts),
      average_fact_size: calculate_average_fact_size(facts),
      oldest_fact: find_oldest_fact(facts),
      newest_fact: find_newest_fact(facts)
    }
  end

  defp calculate_detailed_memory_stats(fact_map, engine_name) do
    basic_stats = calculate_memory_stats(fact_map)

    Map.merge(basic_stats, %{
      engine_name: engine_name,
      memory_usage_bytes: estimate_memory_usage(fact_map),
      fact_lifecycle_stats: calculate_lifecycle_stats(fact_map),
      reference_count_distribution: calculate_reference_distribution(fact_map)
    })
  end

  defp count_fact_types(facts) do
    Enum.group_by(facts, & &1.fact_type)
    |> Map.new(fn {type, facts} -> {type, length(facts)} end)
  end

  defp calculate_average_fact_size([]), do: 0

  defp calculate_average_fact_size(facts) do
    total_size =
      facts
      |> Enum.map(&:erlang.external_size(&1.fact))
      |> Enum.sum()

    total_size / length(facts)
  end

  defp find_oldest_fact([]), do: nil

  defp find_oldest_fact(facts) do
    Enum.min_by(facts, & &1.inserted_at, DateTime)
  end

  defp find_newest_fact([]), do: nil

  defp find_newest_fact(facts) do
    Enum.max_by(facts, & &1.inserted_at, DateTime)
  end

  defp estimate_memory_usage(fact_map) do
    :erlang.external_size(fact_map)
  end

  defp calculate_lifecycle_stats(fact_map) do
    facts = Map.values(fact_map)
    now = DateTime.utc_now()

    lifetimes =
      Enum.map(facts, fn fact ->
        DateTime.diff(now, fact.inserted_at, :second)
      end)

    %{
      average_lifetime_seconds:
        if(length(lifetimes) > 0, do: Enum.sum(lifetimes) / length(lifetimes), else: 0),
      max_lifetime_seconds: if(length(lifetimes) > 0, do: Enum.max(lifetimes), else: 0),
      min_lifetime_seconds: if(length(lifetimes) > 0, do: Enum.min(lifetimes), else: 0)
    }
  end

  defp calculate_reference_distribution(fact_map) do
    facts = Map.values(fact_map)

    reference_counts = Enum.map(facts, & &1.reference_count)

    %{
      average_references:
        if(length(reference_counts) > 0,
          do: Enum.sum(reference_counts) / length(reference_counts),
          else: 0
        ),
      max_references: if(length(reference_counts) > 0, do: Enum.max(reference_counts), else: 0),
      unreferenced_facts: Enum.count(facts, &(&1.reference_count == 0))
    }
  end

  defp determine_fact_type(fact) when is_tuple(fact) and tuple_size(fact) > 0 do
    elem(fact, 0)
  end

  defp determine_fact_type(%{__struct__: module}), do: module
  defp determine_fact_type(fact) when is_map(fact), do: :map
  defp determine_fact_type(fact) when is_list(fact), do: :list
  defp determine_fact_type(fact) when is_atom(fact), do: :atom
  defp determine_fact_type(_), do: :unknown

  defp notify_snapshot_subscribers(snapshot, subscribers) do
    Enum.each(subscribers, fn subscriber ->
      send(subscriber, {:memory_snapshot, snapshot})
    end)
  end

  defp notify_fact_change_subscribers(operation, engine_name, fact_entry, subscribers) do
    notification = %{
      operation: operation,
      engine_name: engine_name,
      fact_entry: fact_entry,
      timestamp: DateTime.utc_now()
    }

    Enum.each(subscribers, fn subscriber ->
      send(subscriber, {:fact_change, notification})
    end)
  end

  defp generate_fact_id do
    "fact_" <> (:crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower))
  end

  defp generate_snapshot_id do
    "snapshot_" <> (:crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower))
  end
end

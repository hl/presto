defmodule Presto.Optimisation.SharedMemoryManager do
  @moduledoc """
  Enhanced memory sharing across beta nodes for RETE optimization.

  This module implements RETE-II/RETE-NT style memory sharing where identical
  intermediate results across different beta nodes are stored only once and
  referenced by multiple nodes. This reduces memory usage and can improve
  cache locality.
  """

  use GenServer

  @type shared_memory_key :: {{binary(), binary(), binary()}, [atom()]}
  @type memory_reference :: String.t()
  @type shared_entry :: %{
          key: shared_memory_key(),
          data: [map()],
          reference_count: non_neg_integer(),
          last_accessed: integer(),
          memory_size: non_neg_integer()
        }

  # Client API

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @spec get_or_create_shared_memory(shared_memory_key(), [map()]) ::
          {:ok, memory_reference()} | {:error, term()}
  def get_or_create_shared_memory(memory_key, data) do
    GenServer.call(__MODULE__, {:get_or_create_shared_memory, memory_key, data})
  end

  @spec get_shared_memory(memory_reference()) :: {:ok, [map()]} | {:error, :not_found}
  def get_shared_memory(memory_ref) do
    GenServer.call(__MODULE__, {:get_shared_memory, memory_ref})
  end

  @spec update_shared_memory(memory_reference(), [map()]) :: :ok | {:error, :not_found}
  def update_shared_memory(memory_ref, new_data) do
    GenServer.call(__MODULE__, {:update_shared_memory, memory_ref, new_data})
  end

  @spec release_shared_memory(memory_reference()) :: :ok
  def release_shared_memory(memory_ref) do
    GenServer.cast(__MODULE__, {:release_shared_memory, memory_ref})
  end

  @spec get_memory_statistics() :: %{
          total_shared_memories: non_neg_integer(),
          total_memory_usage: non_neg_integer(),
          memory_savings: float(),
          cache_hit_rate: float()
        }
  def get_memory_statistics do
    GenServer.call(__MODULE__, :get_memory_statistics)
  end

  @spec cleanup_unused_memories() :: :ok
  def cleanup_unused_memories do
    GenServer.cast(__MODULE__, :cleanup_unused_memories)
  end

  # Server implementation

  @impl true
  def init(opts) do
    # 1 minute
    cleanup_interval = Keyword.get(opts, :cleanup_interval, 60_000)
    # 100MB
    max_memory_size = Keyword.get(opts, :max_memory_size, 100 * 1024 * 1024)

    state = %{
      # ETS table for shared memory entries
      shared_memories: :ets.new(:shared_memories, [:set, :private]),
      # ETS table for key -> memory_ref mapping
      key_index: :ets.new(:shared_memory_keys, [:set, :private]),
      # Performance statistics
      statistics: %{
        total_lookups: 0,
        cache_hits: 0,
        total_shared_memories: 0,
        total_memory_usage: 0,
        memory_savings: 0.0
      },
      # Configuration
      config: %{
        cleanup_interval: cleanup_interval,
        max_memory_size: max_memory_size,
        # entries
        cleanup_threshold: 1000
      },
      # Runtime state
      next_ref_id: 1,
      cleanup_timer: nil
    }

    # Schedule periodic cleanup
    timer = Process.send_after(self(), :cleanup_cycle, cleanup_interval)
    state = %{state | cleanup_timer: timer}

    {:ok, state}
  end

  @impl true
  def handle_call({:get_or_create_shared_memory, memory_key, data}, _from, state) do
    # Update statistics
    new_stats = %{state.statistics | total_lookups: state.statistics.total_lookups + 1}
    state = %{state | statistics: new_stats}

    case :ets.lookup(state.key_index, memory_key) do
      [{^memory_key, memory_ref}] ->
        # Found existing shared memory
        increment_reference_count(memory_ref, state)

        # Update cache hit statistics
        new_stats = %{state.statistics | cache_hits: state.statistics.cache_hits + 1}
        state = %{state | statistics: new_stats}

        {:reply, {:ok, memory_ref}, state}

      [] ->
        # Create new shared memory
        {memory_ref, new_state} = create_shared_memory(memory_key, data, state)
        {:reply, {:ok, memory_ref}, new_state}
    end
  rescue
    error ->
      {:reply, {:error, {:shared_memory_error, error}}, state}
  end

  @impl true
  def handle_call({:get_shared_memory, memory_ref}, _from, state) do
    case :ets.lookup(state.shared_memories, memory_ref) do
      [{^memory_ref, entry}] ->
        # Update last accessed time
        updated_entry = %{entry | last_accessed: System.monotonic_time(:millisecond)}
        :ets.insert(state.shared_memories, {memory_ref, updated_entry})

        {:reply, {:ok, entry.data}, state}

      [] ->
        {:reply, {:error, :not_found}, state}
    end
  end

  @impl true
  def handle_call({:update_shared_memory, memory_ref, new_data}, _from, state) do
    case :ets.lookup(state.shared_memories, memory_ref) do
      [{^memory_ref, entry}] ->
        # Calculate new memory size
        new_memory_size = estimate_memory_size(new_data)
        memory_diff = new_memory_size - entry.memory_size

        updated_entry = %{
          entry
          | data: new_data,
            memory_size: new_memory_size,
            last_accessed: System.monotonic_time(:millisecond)
        }

        :ets.insert(state.shared_memories, {memory_ref, updated_entry})

        # Update statistics
        new_stats = %{
          state.statistics
          | total_memory_usage: state.statistics.total_memory_usage + memory_diff
        }

        state = %{state | statistics: new_stats}

        {:reply, :ok, state}

      [] ->
        {:reply, {:error, :not_found}, state}
    end
  end

  @impl true
  def handle_call(:get_memory_statistics, _from, state) do
    # Calculate memory savings based on reference counts
    total_savings = calculate_memory_savings(state)
    cache_hit_rate = calculate_cache_hit_rate(state.statistics)

    stats = %{
      total_shared_memories: state.statistics.total_shared_memories,
      total_memory_usage: state.statistics.total_memory_usage,
      memory_savings: total_savings,
      cache_hit_rate: cache_hit_rate
    }

    {:reply, stats, state}
  end

  @impl true
  def handle_cast({:release_shared_memory, memory_ref}, state) do
    new_state = decrement_reference_count(memory_ref, state)
    {:noreply, new_state}
  end

  @impl true
  def handle_cast(:cleanup_unused_memories, state) do
    new_state = perform_memory_cleanup(state)
    {:noreply, new_state}
  end

  @impl true
  def handle_info(:cleanup_cycle, state) do
    # Perform periodic cleanup
    new_state = perform_memory_cleanup(state)

    # Schedule next cleanup
    timer = Process.send_after(self(), :cleanup_cycle, state.config.cleanup_interval)
    new_state = %{new_state | cleanup_timer: timer}

    {:noreply, new_state}
  end

  @impl true
  def terminate(_reason, state) do
    if state.cleanup_timer do
      Process.cancel_timer(state.cleanup_timer)
    end

    :ets.delete(state.shared_memories)
    :ets.delete(state.key_index)
    :ok
  end

  # Private functions

  defp create_shared_memory(memory_key, data, state) do
    memory_ref = "shared_mem_#{state.next_ref_id}"

    # Safely estimate memory size with a fallback
    memory_size =
      try do
        estimate_memory_size(data)
      rescue
        # Rough fallback: 100 bytes per item
        _ -> length(data) * 100
      end

    entry = %{
      key: memory_key,
      data: data,
      reference_count: 1,
      last_accessed: System.monotonic_time(:millisecond),
      memory_size: memory_size
    }

    # Store in both tables
    :ets.insert(state.shared_memories, {memory_ref, entry})
    :ets.insert(state.key_index, {memory_key, memory_ref})

    # Update statistics
    new_stats = %{
      state.statistics
      | total_shared_memories: state.statistics.total_shared_memories + 1,
        total_memory_usage: state.statistics.total_memory_usage + memory_size
    }

    new_state = %{state | next_ref_id: state.next_ref_id + 1, statistics: new_stats}

    {memory_ref, new_state}
  end

  defp increment_reference_count(memory_ref, state) do
    case :ets.lookup(state.shared_memories, memory_ref) do
      [{^memory_ref, entry}] ->
        updated_entry = %{
          entry
          | reference_count: entry.reference_count + 1,
            last_accessed: System.monotonic_time(:millisecond)
        }

        :ets.insert(state.shared_memories, {memory_ref, updated_entry})

      [] ->
        # Memory reference not found - this shouldn't happen
        :ok
    end
  end

  defp decrement_reference_count(memory_ref, state) do
    case :ets.lookup(state.shared_memories, memory_ref) do
      [{^memory_ref, entry}] ->
        new_ref_count = entry.reference_count - 1

        if new_ref_count <= 0 do
          # Remove shared memory when no references remain
          :ets.delete(state.shared_memories, memory_ref)
          :ets.delete(state.key_index, entry.key)

          # Update statistics
          new_stats = %{
            state.statistics
            | total_shared_memories: state.statistics.total_shared_memories - 1,
              total_memory_usage: state.statistics.total_memory_usage - entry.memory_size
          }

          %{state | statistics: new_stats}
        else
          # Just decrement reference count
          updated_entry = %{entry | reference_count: new_ref_count}
          :ets.insert(state.shared_memories, {memory_ref, updated_entry})
          state
        end

      [] ->
        # Memory reference not found
        state
    end
  end

  defp perform_memory_cleanup(state) do
    current_time = System.monotonic_time(:millisecond)
    # 5 minutes
    cleanup_threshold = 5 * 60 * 1000

    # Find unused memories (reference count = 0 or very old)
    unused_memories =
      :ets.select(state.shared_memories, [
        {{:"$1", :"$2"},
         [
           {:orelse, {:"=<", {:map_get, :reference_count, :"$2"}, 0},
            {:<, {:map_get, :last_accessed, :"$2"}, current_time - cleanup_threshold}}
         ], [:"$_"]}
      ])

    # Remove unused memories
    {cleaned_count, memory_freed} =
      Enum.reduce(unused_memories, {0, 0}, fn {memory_ref, entry}, {count, freed} ->
        :ets.delete(state.shared_memories, memory_ref)
        :ets.delete(state.key_index, entry.key)
        {count + 1, freed + entry.memory_size}
      end)

    # Update statistics
    new_stats = %{
      state.statistics
      | total_shared_memories: max(0, state.statistics.total_shared_memories - cleaned_count),
        total_memory_usage: max(0, state.statistics.total_memory_usage - memory_freed)
    }

    %{state | statistics: new_stats}
  end

  defp estimate_memory_size(data) when is_list(data) do
    # For large datasets, sample the first few items to avoid performance issues
    sample_size = min(10, length(data))

    if sample_size == 0 do
      0
    else
      # Sample first few items and extrapolate
      sample = Enum.take(data, sample_size)
      sample_total = Enum.reduce(sample, 0, &(&2 + estimate_term_size(&1)))
      average_size = sample_total / sample_size

      # Total: list overhead + estimated content size
      length(data) * (8 + average_size)
    end
  end

  defp estimate_term_size(term) when is_struct(term) do
    # Structs: estimate as fixed size + fields
    32 + estimate_term_size(Map.from_struct(term))
  end

  defp estimate_term_size(term) when is_map(term) do
    # Rough estimation: 8 bytes per key-value pair + content
    map_size(term) * 16 +
      Enum.reduce(term, 0, fn {k, v}, acc ->
        acc + estimate_term_size(k) + estimate_term_size(v)
      end)
  end

  defp estimate_term_size(term) when is_binary(term), do: byte_size(term)
  defp estimate_term_size(term) when is_atom(term), do: 8
  defp estimate_term_size(term) when is_integer(term), do: 8
  defp estimate_term_size(term) when is_float(term), do: 8
  # fallback
  defp estimate_term_size(_), do: 16

  defp calculate_memory_savings(state) do
    # Calculate total memory that would be used without sharing
    total_without_sharing =
      :ets.foldl(
        fn {_ref, entry}, acc ->
          savings_for_entry = entry.memory_size * (entry.reference_count - 1)
          acc + savings_for_entry
        end,
        0,
        state.shared_memories
      )

    # Calculate savings percentage
    total_with_sharing = state.statistics.total_memory_usage

    if total_with_sharing > 0 do
      total_without_sharing / (total_with_sharing + total_without_sharing) * 100.0
    else
      0.0
    end
  end

  defp calculate_cache_hit_rate(statistics) do
    if statistics.total_lookups > 0 do
      statistics.cache_hits / statistics.total_lookups * 100.0
    else
      0.0
    end
  end
end

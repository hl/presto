defmodule Presto.Persistence.EtsAdapter do
  @moduledoc """
  ETS-based persistence adapter for Presto RETE engines.

  This adapter provides the default persistence implementation using Erlang Term Storage (ETS),
  maintaining full backward compatibility with existing Presto behavior whilst providing
  the foundation for pluggable persistence.

  ## Features

  - Full ETS table support with all standard table types
  - Optimized batch operations using ETS insert/2 with lists
  - Efficient snapshotting and recovery using ETS tab2list/1 and insert/2
  - Proper error handling and graceful degradation
  - Thread-safe operations with appropriate concurrency settings

  ## Usage

  This adapter is automatically used as the default when no other adapter is configured:

      # Automatic usage (default)
      table = Presto.Persistence.create_table(:facts, type: :set)
      
      # Explicit configuration
      config :presto, :persistence_adapter, Presto.Persistence.EtsAdapter

  ## Table Options

  Supports all standard ETS table options:
  - `:type` - Table type (:set, :ordered_set, :bag, :duplicate_bag)
  - `:access` - Access mode (:public, :protected, :private) 
  - `:read_concurrency` - Enable read concurrency optimizations
  - `:write_concurrency` - Enable write concurrency optimizations
  - `:compressed` - Enable table compression

  ## Performance Characteristics

  - Create: O(1) - ETS table creation is constant time
  - Insert: O(1) average for :set/:bag, O(log n) for :ordered_set
  - Lookup: O(1) average for :set/:bag, O(log n) for :ordered_set
  - Delete: O(1) average for :set/:bag, O(log n) for :ordered_set
  - Batch operations: Optimized using ETS native batch insert
  - Memory: In-memory only, no persistence across restarts
  """

  @behaviour Presto.Persistence.Adapter

  @type ets_table :: :ets.tid() | atom()

  @impl true
  def create_table(name, opts \\ []) do
    # Extract ETS-specific options
    table_type = Keyword.get(opts, :type, :set)
    access = Keyword.get(opts, :access, :public)
    read_concurrency = Keyword.get(opts, :read_concurrency, true)
    write_concurrency = Keyword.get(opts, :write_concurrency, true)
    compressed = Keyword.get(opts, :compressed, false)

    # Build ETS options list
    ets_opts = [
      table_type,
      :named_table,
      access,
      {:read_concurrency, read_concurrency},
      {:write_concurrency, write_concurrency}
    ]

    ets_opts = if compressed, do: [:compressed | ets_opts], else: ets_opts

    # Create the ETS table
    try do
      :ets.new(name, ets_opts)
    rescue
      ArgumentError ->
        # Table might already exist, try to reuse it
        case :ets.info(name) do
          :undefined ->
            reraise ArgumentError, "Failed to create ETS table: #{name}", __STACKTRACE__

          _info ->
            # Return existing table
            name
        end
    end
  end

  @impl true
  def insert(table, {key, value}) do
    try do
      :ets.insert(table, {key, value})
      :ok
    rescue
      ArgumentError -> {:error, :table_not_found}
      error -> {:error, error}
    end
  end

  @impl true
  def lookup(table, key) do
    try do
      case :ets.lookup(table, key) do
        [] -> []
        results -> Enum.map(results, fn {^key, value} -> value end)
      end
    rescue
      ArgumentError -> []
      _error -> []
    end
  end

  @impl true
  def delete(table, key) do
    try do
      :ets.delete(table, key)
      :ok
    rescue
      ArgumentError -> {:error, :table_not_found}
      error -> {:error, error}
    end
  end

  @impl true
  def delete_table(table) do
    try do
      :ets.delete(table)
      :ok
    rescue
      ArgumentError -> {:error, :table_not_found}
      error -> {:error, error}
    end
  end

  @impl true
  def list_all(table) do
    try do
      :ets.tab2list(table)
    rescue
      ArgumentError -> []
      _error -> []
    end
  end

  @impl true
  def clear(table) do
    try do
      :ets.delete_all_objects(table)
      :ok
    rescue
      ArgumentError -> {:error, :table_not_found}
      error -> {:error, error}
    end
  end

  @impl true
  def size(table) do
    try do
      :ets.info(table, :size) || 0
    rescue
      ArgumentError -> 0
      _error -> 0
    end
  end

  @impl true
  def table_info(table, key) do
    try do
      :ets.info(table, key)
    rescue
      ArgumentError -> nil
      _error -> nil
    end
  end

  @impl true
  def insert_batch(table, entries) when is_list(entries) do
    try do
      :ets.insert(table, entries)
      :ok
    rescue
      ArgumentError -> {:error, :table_not_found}
      error -> {:error, error}
    end
  end

  @impl true
  def snapshot(table) do
    try do
      data = :ets.tab2list(table)

      metadata = %{
        type: :ets.info(table, :type),
        size: :ets.info(table, :size),
        memory: :ets.info(table, :memory),
        name: :ets.info(table, :name)
      }

      {:ok, %{data: data, metadata: metadata}}
    rescue
      ArgumentError -> {:error, :table_not_found}
      error -> {:error, error}
    end
  end

  @impl true
  def restore(table, snapshot) do
    try do
      case snapshot do
        %{data: data} when is_list(data) ->
          # Clear existing data and insert snapshot
          :ets.delete_all_objects(table)
          :ets.insert(table, data)
          :ok

        data when is_list(data) ->
          # Handle simple list format for backward compatibility
          :ets.delete_all_objects(table)
          :ets.insert(table, data)
          :ok

        _ ->
          {:error, :invalid_snapshot_format}
      end
    rescue
      ArgumentError -> {:error, :table_not_found}
      error -> {:error, error}
    end
  end
end

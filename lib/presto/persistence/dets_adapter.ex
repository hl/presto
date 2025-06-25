defmodule Presto.Persistence.DetsAdapter do
  @moduledoc """
  DETS-based persistence adapter for Presto RETE engines.

  This adapter provides disk-based persistence using Disk Erlang Term Storage (DETS),
  enabling engine state to survive process restarts and system reboots. Tables are
  automatically synchronized to disk and can be recovered after system failures.

  ## Features

  - Persistent storage using DETS disk files
  - Automatic file creation and management
  - Configurable sync policies for durability vs performance
  - Automatic table recovery on restart
  - File-based snapshots and backups
  - Graceful error handling with file system issues

  ## Usage

  Configure the adapter and specify storage directory:

      config :presto, :persistence_adapter, Presto.Persistence.DetsAdapter
      config :presto, :dets_storage_dir, "/var/lib/presto"
      
  Or pass options directly:

      {:ok, engine} = Presto.RuleEngine.start_link([
        persistence_adapter: Presto.Persistence.DetsAdapter,
        adapter_opts: [storage_dir: "/tmp/presto", auto_sync: true]
      ])

  ## Configuration Options

  - `:storage_dir` - Directory for DETS files (default: "./presto_data")
  - `:auto_sync` - Automatic sync to disk (default: true)
  - `:sync_interval` - Sync interval in ms when auto_sync is true (default: 5000)
  - `:file_size_limit` - Maximum file size before error (default: 2GB)

  ## Performance Characteristics

  - Create: O(1) - File creation is constant time
  - Insert: O(log n) - DETS uses balanced trees internally
  - Lookup: O(log n) - Logarithmic search in disk structures
  - Delete: O(log n) - Logarithmic delete in disk structures
  - Batch operations: Optimized with transaction-like semantics
  - Memory: Fixed memory footprint, data stored on disk
  - Durability: Automatic sync ensures data survives restarts

  ## File Management

  Each table creates a separate .dets file:
  - Table `:facts` → `{storage_dir}/facts.dets`
  - Table `:rules` → `{storage_dir}/rules.dets`

  Files are automatically created if they don't exist and opened
  in read-write mode with appropriate locking.
  """

  @behaviour Presto.Persistence.Adapter

  require Logger

  @type dets_table :: :dets.tab_name()

  # Default configuration
  @default_storage_dir "./presto_data"
  @default_auto_sync true
  @default_sync_interval 5000
  # 2GB
  @default_file_size_limit 2 * 1024 * 1024 * 1024

  @impl true
  def create_table(name, opts \\ []) do
    # Get storage directory
    storage_dir = get_storage_dir(opts)
    auto_sync = Keyword.get(opts, :auto_sync, @default_auto_sync)
    sync_interval = Keyword.get(opts, :sync_interval, @default_sync_interval)
    file_size_limit = Keyword.get(opts, :file_size_limit, @default_file_size_limit)

    # Ensure storage directory exists
    :ok = File.mkdir_p(storage_dir)

    # Generate file path
    file_path = Path.join(storage_dir, "#{name}.dets")

    # DETS options
    dets_opts = [
      type: Keyword.get(opts, :type, :set),
      file: String.to_charlist(file_path),
      auto_save: if(auto_sync, do: sync_interval, else: :infinity),
      max_no_slots: :default,
      min_no_slots: :default,
      # Always use disk
      ram_file: false
    ]

    # Add file size limit if specified
    dets_opts =
      if file_size_limit > 0 do
        [{:estimated_no_objects, div(file_size_limit, 1000)} | dets_opts]
      else
        dets_opts
      end

    try do
      case :dets.open_file(name, dets_opts) do
        {:ok, table} ->
          Logger.debug("Opened DETS table #{name} at #{file_path}")
          table

        {:error, reason} ->
          Logger.error("Failed to open DETS table #{name}: #{inspect(reason)}")
          raise ArgumentError, "Failed to create DETS table #{name}: #{inspect(reason)}"
      end
    rescue
      error ->
        Logger.error("Exception opening DETS table #{name}: #{inspect(error)}")
        reraise error, __STACKTRACE__
    end
  end

  @impl true
  def insert(table, {key, value}) do
    try do
      case :dets.insert(table, {key, value}) do
        :ok -> :ok
        {:error, reason} -> {:error, reason}
      end
    rescue
      error -> {:error, error}
    end
  end

  @impl true
  def lookup(table, key) do
    try do
      case :dets.lookup(table, key) do
        [] -> []
        results -> Enum.map(results, fn {^key, value} -> value end)
      end
    rescue
      _error -> []
    end
  end

  @impl true
  def delete(table, key) do
    try do
      case :dets.delete(table, key) do
        :ok -> :ok
        {:error, reason} -> {:error, reason}
      end
    rescue
      error -> {:error, error}
    end
  end

  @impl true
  def delete_table(table) do
    try do
      # Get file info before closing
      file_path =
        case :dets.info(table, :filename) do
          :undefined -> nil
          filename when is_list(filename) -> List.to_string(filename)
          filename -> filename
        end

      # Close the table first
      :dets.close(table)

      # Delete the file if we have the path
      if file_path && File.exists?(file_path) do
        case File.rm(file_path) do
          :ok ->
            Logger.debug("Deleted DETS file: #{file_path}")
            :ok

          {:error, reason} ->
            Logger.warning("Failed to delete DETS file #{file_path}: #{inspect(reason)}")
            {:error, reason}
        end
      else
        :ok
      end
    rescue
      error -> {:error, error}
    end
  end

  @impl true
  def list_all(table) do
    try do
      case :dets.match_object(table, :"$1") do
        {:error, _reason} -> []
        results when is_list(results) -> results
        result -> [result]
      end
    rescue
      _error -> []
    end
  end

  @impl true
  def clear(table) do
    try do
      case :dets.delete_all_objects(table) do
        :ok -> :ok
        {:error, reason} -> {:error, reason}
      end
    rescue
      error -> {:error, error}
    end
  end

  @impl true
  def size(table) do
    try do
      case :dets.info(table, :size) do
        :undefined -> 0
        size when is_integer(size) -> size
        _other -> 0
      end
    rescue
      _error -> 0
    end
  end

  @impl true
  def table_info(table, key) do
    try do
      case :dets.info(table, key) do
        :undefined -> nil
        info -> info
      end
    rescue
      _error -> nil
    end
  end

  @impl true
  def insert_batch(table, entries) when is_list(entries) do
    try do
      case :dets.insert(table, entries) do
        :ok -> :ok
        {:error, reason} -> {:error, reason}
      end
    rescue
      error -> {:error, error}
    end
  end

  @impl true
  def snapshot(table) do
    try do
      # Force sync to disk before snapshot
      :dets.sync(table)

      # Get all data
      data =
        case :dets.match_object(table, :"$1") do
          {:error, reason} ->
            {:error, reason}

          results when is_list(results) ->
            results

          result ->
            [result]
        end

      case data do
        {:error, reason} ->
          {:error, reason}

        data_list ->
          # Get metadata
          metadata = %{
            type: :dets.info(table, :type),
            size: :dets.info(table, :size),
            filename: :dets.info(table, :filename),
            file_size: :dets.info(table, :file_size),
            no_objects: :dets.info(table, :no_objects),
            timestamp: System.system_time(:second)
          }

          {:ok, %{data: data_list, metadata: metadata}}
      end
    rescue
      error -> {:error, error}
    end
  end

  @impl true
  def restore(table, snapshot) do
    try do
      case snapshot do
        %{data: data} when is_list(data) ->
          # Clear existing data and insert snapshot
          with :ok <- :dets.delete_all_objects(table),
               :ok <- :dets.insert(table, data),
               :ok <- :dets.sync(table) do
            Logger.info("Restored DETS table with #{length(data)} entries")
            :ok
          else
            {:error, reason} -> {:error, reason}
          end

        data when is_list(data) ->
          # Handle simple list format for backward compatibility
          with :ok <- :dets.delete_all_objects(table),
               :ok <- :dets.insert(table, data),
               :ok <- :dets.sync(table) do
            Logger.info("Restored DETS table with #{length(data)} entries")
            :ok
          else
            {:error, reason} -> {:error, reason}
          end

        _ ->
          {:error, :invalid_snapshot_format}
      end
    rescue
      error -> {:error, error}
    end
  end

  # Private helper functions

  defp get_storage_dir(opts) do
    Keyword.get(opts, :storage_dir) ||
      Application.get_env(:presto, :dets_storage_dir) ||
      @default_storage_dir
  end
end

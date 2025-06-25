defmodule Presto.Persistence do
  @moduledoc """
  Pluggable persistence adapter interface for Presto RETE engines.

  This module defines the behavior that persistence adapters must implement
  to provide storage for engine working memory, alpha networks, and state.

  The default implementation uses ETS tables for backward compatibility,
  but adapters can be created for disk persistence (DETS), database storage,
  or distributed caches.

  ## Adapter Interface

  All adapters must implement the `Presto.Persistence.Adapter` behavior
  and provide the following table operations:

  - `create_table/2` - Create a new table with options
  - `insert/2` - Insert a key-value pair
  - `lookup/2` - Lookup values by key  
  - `delete/2` - Delete a key
  - `delete_table/1` - Delete entire table
  - `list_all/1` - List all key-value pairs
  - `clear/1` - Clear all data from table
  - `size/1` - Get table size
  - `table_info/2` - Get table metadata

  ## Usage

  Configure the adapter in your application:

      config :presto, :persistence_adapter, Presto.Persistence.EtsAdapter
      
  Or pass it when starting an engine:

      {:ok, engine} = Presto.RuleEngine.start_link(
        persistence_adapter: Presto.Persistence.DetsAdapter,
        persistence_opts: [dir: "/var/lib/presto"]
      )
  """

  @type table_ref :: term()
  @type table_name :: atom()
  @type table_opts :: keyword()
  @type key :: term()
  @type value :: term()
  @type entry :: {key(), value()}

  @doc """
  Gets the configured persistence adapter module.

  Returns the adapter from application config or defaults to ETS.
  """
  @spec get_adapter() :: module()
  def get_adapter do
    Application.get_env(:presto, :persistence_adapter, Presto.Persistence.EtsAdapter)
  end

  @doc """
  Creates a new table with the given name and options.

  ## Options

  - `:type` - Table type (:set, :ordered_set, :bag, :duplicate_bag)
  - `:access` - Access mode (:public, :protected, :private)
  - `:read_concurrency` - Enable read concurrency (boolean)
  - `:write_concurrency` - Enable write concurrency (boolean)
  - `:adapter_opts` - Adapter-specific options

  ## Examples

      table = Presto.Persistence.create_table(:facts, [
        type: :set,
        access: :public,
        read_concurrency: true
      ])
  """
  @spec create_table(table_name(), table_opts()) :: table_ref()
  def create_table(name, opts \\ []) do
    adapter = get_adapter()
    adapter_opts = Keyword.get(opts, :adapter_opts, [])
    merged_opts = Keyword.merge(opts, adapter_opts)

    adapter.create_table(name, merged_opts)
  end

  @doc """
  Inserts a key-value pair into the table.
  """
  @spec insert(table_ref(), entry()) :: :ok | {:error, term()}
  def insert(table, {key, value} = _entry) do
    get_adapter().insert(table, {key, value})
  end

  @doc """
  Looks up values for the given key.

  Returns a list of values (may be empty if key not found).
  """
  @spec lookup(table_ref(), key()) :: [value()]
  def lookup(table, key) do
    get_adapter().lookup(table, key)
  end

  @doc """
  Deletes a key from the table.
  """
  @spec delete(table_ref(), key()) :: :ok | {:error, term()}
  def delete(table, key) do
    get_adapter().delete(table, key)
  end

  @doc """
  Deletes the entire table.
  """
  @spec delete_table(table_ref()) :: :ok | {:error, term()}
  def delete_table(table) do
    get_adapter().delete_table(table)
  end

  @doc """
  Lists all key-value pairs in the table.
  """
  @spec list_all(table_ref()) :: [entry()]
  def list_all(table) do
    get_adapter().list_all(table)
  end

  @doc """
  Clears all data from the table without deleting the table.
  """
  @spec clear(table_ref()) :: :ok | {:error, term()}
  def clear(table) do
    get_adapter().clear(table)
  end

  @doc """
  Gets the current size (number of entries) of the table.
  """
  @spec size(table_ref()) :: non_neg_integer()
  def size(table) do
    get_adapter().size(table)
  end

  @doc """
  Gets table information/metadata.

  Common info keys: :size, :memory, :type, :protection, :name
  """
  @spec table_info(table_ref(), atom()) :: term()
  def table_info(table, key) do
    get_adapter().table_info(table, key)
  end

  @doc """
  Batch inserts multiple entries for better performance.

  Default implementation calls insert/2 for each entry,
  but adapters can override for optimized batch operations.
  """
  @spec insert_batch(table_ref(), [entry()]) :: :ok | {:error, term()}
  def insert_batch(table, entries) when is_list(entries) do
    adapter = get_adapter()

    if function_exported?(adapter, :insert_batch, 2) do
      adapter.insert_batch(table, entries)
    else
      # Fallback to individual inserts
      Enum.each(entries, fn entry ->
        insert(table, entry)
      end)

      :ok
    end
  end

  @doc """
  Snapshots the table data for backup/recovery.

  Returns serializable representation of all table data.
  """
  @spec snapshot(table_ref()) :: {:ok, term()} | {:error, term()}
  def snapshot(table) do
    adapter = get_adapter()

    if function_exported?(adapter, :snapshot, 1) do
      adapter.snapshot(table)
    else
      # Default implementation using list_all
      data = list_all(table)
      {:ok, data}
    end
  end

  @doc """
  Restores table data from a snapshot.
  """
  @spec restore(table_ref(), term()) :: :ok | {:error, term()}
  def restore(table, snapshot_data) do
    adapter = get_adapter()

    if function_exported?(adapter, :restore, 2) do
      adapter.restore(table, snapshot_data)
    else
      # Default implementation: clear and insert
      with :ok <- clear(table),
           :ok <- insert_batch(table, snapshot_data) do
        :ok
      end
    end
  end
end

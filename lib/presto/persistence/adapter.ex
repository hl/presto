defmodule Presto.Persistence.Adapter do
  @moduledoc """
  Behavior for persistence adapters in Presto.

  This behavior defines the interface that all persistence adapters must implement
  to provide table storage for Presto engines. Adapters can implement different
  storage backends such as ETS, DETS, databases, or distributed caches.

  ## Required Callbacks

  All adapters must implement these callbacks to provide basic table operations:

  - `create_table/2` - Create a new table
  - `insert/2` - Insert key-value pair
  - `lookup/2` - Lookup values by key
  - `delete/2` - Delete a key
  - `delete_table/1` - Delete entire table
  - `list_all/1` - List all entries
  - `clear/1` - Clear table data
  - `size/1` - Get table size
  - `table_info/2` - Get table metadata

  ## Optional Callbacks

  These callbacks provide optimizations and can be implemented for better performance:

  - `insert_batch/2` - Batch insert multiple entries
  - `snapshot/1` - Create table snapshot for backup
  - `restore/2` - Restore table from snapshot

  ## Implementation Example

      defmodule MyAdapter do
        @behaviour Presto.Persistence.Adapter
        
        @impl true
        def create_table(name, opts) do
          # Create your table implementation
        end
        
        @impl true  
        def insert(table, {key, value}) do
          # Insert implementation
        end
        
        # ... implement other required callbacks
      end
  """

  @type table_ref :: term()
  @type table_name :: atom()
  @type table_opts :: keyword()
  @type key :: term()
  @type value :: term()
  @type entry :: {key(), value()}

  @doc """
  Creates a new table with the given name and options.

  ## Parameters

  - `name` - Atom name for the table
  - `opts` - Table options (type, access, concurrency, etc.)

  ## Returns

  Returns a table reference that can be used with other operations.
  The format of this reference is adapter-specific.
  """
  @callback create_table(table_name(), table_opts()) :: table_ref()

  @doc """
  Inserts a key-value pair into the table.

  ## Parameters

  - `table` - Table reference from create_table/2
  - `entry` - Tuple of {key, value} to insert

  ## Returns

  - `:ok` on success
  - `{:error, reason}` on failure
  """
  @callback insert(table_ref(), entry()) :: :ok | {:error, term()}

  @doc """
  Looks up values for the given key.

  ## Parameters

  - `table` - Table reference
  - `key` - Key to lookup

  ## Returns

  List of values associated with the key. Empty list if key not found.
  For :set and :ordered_set tables, this will be at most one value.
  For :bag and :duplicate_bag tables, this may be multiple values.
  """
  @callback lookup(table_ref(), key()) :: [value()]

  @doc """
  Deletes a key and all its associated values from the table.

  ## Parameters

  - `table` - Table reference
  - `key` - Key to delete

  ## Returns

  - `:ok` on success (even if key didn't exist)
  - `{:error, reason}` on failure
  """
  @callback delete(table_ref(), key()) :: :ok | {:error, term()}

  @doc """
  Deletes the entire table and frees associated resources.

  ## Parameters

  - `table` - Table reference to delete

  ## Returns

  - `:ok` on success
  - `{:error, reason}` on failure
  """
  @callback delete_table(table_ref()) :: :ok | {:error, term()}

  @doc """
  Returns all key-value pairs in the table.

  ## Parameters

  - `table` - Table reference

  ## Returns

  List of {key, value} tuples representing all entries in the table.
  Order is not guaranteed unless using an :ordered_set table type.
  """
  @callback list_all(table_ref()) :: [entry()]

  @doc """
  Clears all data from the table without deleting the table structure.

  ## Parameters

  - `table` - Table reference

  ## Returns

  - `:ok` on success
  - `{:error, reason}` on failure
  """
  @callback clear(table_ref()) :: :ok | {:error, term()}

  @doc """
  Returns the current number of entries in the table.

  ## Parameters

  - `table` - Table reference

  ## Returns

  Non-negative integer representing the number of entries.
  """
  @callback size(table_ref()) :: non_neg_integer()

  @doc """
  Returns table metadata/information.

  ## Parameters

  - `table` - Table reference
  - `key` - Information key to retrieve

  Common keys include:
  - `:size` - Number of entries
  - `:memory` - Memory usage in bytes
  - `:type` - Table type (:set, :ordered_set, :bag, :duplicate_bag)
  - `:protection` - Access protection (:public, :protected, :private)
  - `:name` - Table name

  ## Returns

  The requested information value, or `nil` if not available.
  """
  @callback table_info(table_ref(), atom()) :: term()

  @doc """
  Batch inserts multiple entries for better performance.

  This is an optional callback. If not implemented, the default
  behavior will use individual insert/2 calls.

  ## Parameters

  - `table` - Table reference
  - `entries` - List of {key, value} tuples to insert

  ## Returns

  - `:ok` on success
  - `{:error, reason}` on failure
  """
  @callback insert_batch(table_ref(), [entry()]) :: :ok | {:error, term()}

  @doc """
  Creates a snapshot of the table data for backup/recovery.

  This is an optional callback. If not implemented, the default
  behavior will use list_all/1 to get all data.

  ## Parameters

  - `table` - Table reference

  ## Returns

  - `{:ok, snapshot_data}` - Serializable snapshot data
  - `{:error, reason}` on failure
  """
  @callback snapshot(table_ref()) :: {:ok, term()} | {:error, term()}

  @doc """
  Restores table data from a snapshot.

  This is an optional callback. If not implemented, the default
  behavior will clear the table and use insert_batch/2.

  ## Parameters

  - `table` - Table reference
  - `snapshot_data` - Data from snapshot/1

  ## Returns

  - `:ok` on success
  - `{:error, reason}` on failure
  """
  @callback restore(table_ref(), term()) :: :ok | {:error, term()}

  # Make insert_batch, snapshot, and restore optional
  @optional_callbacks [
    insert_batch: 2,
    snapshot: 1,
    restore: 2
  ]
end

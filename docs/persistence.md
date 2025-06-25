# Presto Persistence Layer

The Presto RETE rules engine includes a comprehensive persistence layer that allows you to choose different storage backends for your engine data. This enables scenarios ranging from fast in-memory processing to durable disk-based storage that survives system restarts.

## Overview

The persistence layer provides:

- **Pluggable Adapters**: Choose between ETS (memory), DETS (disk), or custom storage backends
- **Engine Snapshots**: Complete state capture and restoration capabilities
- **Backward Compatibility**: Existing code continues to work unchanged
- **Production Ready**: Robust error handling and comprehensive test coverage

## Quick Start

### Default Configuration (ETS)

By default, Presto uses ETS tables for fast in-memory storage:

```elixir
# No configuration needed - uses ETS by default
{:ok, engine} = Presto.RuleEngine.start_link([])
```

### Disk Persistence (DETS)

To enable disk persistence that survives restarts:

```elixir
# Configure globally
config :presto, :persistence_adapter, Presto.Persistence.DetsAdapter
config :presto, :dets_storage_dir, "/var/lib/presto"

# Or configure per engine
{:ok, engine} = Presto.RuleEngine.start_link([
  persistence_adapter: Presto.Persistence.DetsAdapter,
  adapter_opts: [storage_dir: "/tmp/presto"]
])
```

## Available Adapters

### ETS Adapter (Default)

**Module**: `Presto.Persistence.EtsAdapter`

Fast in-memory storage using Erlang Term Storage.

**Characteristics**:
- ⚡ Extremely fast (O(1) average operations)
- 🔄 High concurrency support
- 💾 Memory-only (data lost on restart)
- 🎯 Perfect for development and high-performance scenarios

**Configuration**:
```elixir
# ETS is the default - no configuration needed
{:ok, engine} = Presto.RuleEngine.start_link([
  # Optional ETS-specific options
  adapter_opts: [
    type: :set,              # :set, :ordered_set, :bag, :duplicate_bag
    access: :public,         # :public, :protected, :private
    read_concurrency: true,  # Enable read optimizations
    write_concurrency: true, # Enable write optimizations
    compressed: false        # Enable compression
  ]
])
```

### DETS Adapter

**Module**: `Presto.Persistence.DetsAdapter`

Disk-based storage using Disk Erlang Term Storage.

**Characteristics**:
- 💾 Persistent storage (survives restarts)
- 🔄 Automatic file management
- ⚖️ Balanced performance (O(log n) operations)
- 🛡️ Data durability and crash recovery

**Configuration**:
```elixir
# Application configuration
config :presto, :persistence_adapter, Presto.Persistence.DetsAdapter
config :presto, :dets_storage_dir, "/var/lib/presto"

# Per-engine configuration
{:ok, engine} = Presto.RuleEngine.start_link([
  persistence_adapter: Presto.Persistence.DetsAdapter,
  adapter_opts: [
    storage_dir: "/var/lib/presto",  # Directory for .dets files
    auto_sync: true,                 # Automatic sync to disk
    sync_interval: 5000,             # Sync interval in milliseconds
    file_size_limit: 2_147_483_648   # 2GB file size limit
  ]
])
```

**File Management**:
- Each table creates a separate `.dets` file
- Files are automatically created and managed
- Example: `:facts` table → `storage_dir/facts.dets`

## Engine Snapshots

Snapshots capture the complete state of a running engine, including facts, rules, and internal network state.

### Creating Snapshots

```elixir
{:ok, engine} = Presto.RuleEngine.start_link([])

# Add some rules and facts
Presto.RuleEngine.add_rule(engine, my_rule)
Presto.RuleEngine.assert_fact(engine, {:person, "Alice", 30})

# Create in-memory snapshot
{:ok, snapshot} = Presto.RuleEngine.create_snapshot(engine)

# Save snapshot to file
:ok = Presto.RuleEngine.save_snapshot_to_file(engine, "/backups/engine_state.snapshot")
```

### Restoring from Snapshots

```elixir
# Create new engine
{:ok, new_engine} = Presto.RuleEngine.start_link([])

# Restore from in-memory snapshot
:ok = Presto.RuleEngine.restore_from_snapshot(new_engine, snapshot)

# Or restore from file
:ok = Presto.RuleEngine.load_snapshot_from_file(new_engine, "/backups/engine_state.snapshot")

# Engine now has all the original rules and facts
rules = Presto.RuleEngine.get_rules(new_engine)
```

### Snapshot Contents

Snapshots include:
- **Facts**: All asserted facts in working memory
- **Rules**: All rule definitions and metadata
- **Alpha Networks**: Compiled pattern matching networks
- **Beta Networks**: Join networks and partial matches
- **Statistics**: Performance and execution statistics
- **Configuration**: Engine configuration and options

### Use Cases for Snapshots

1. **Backup and Recovery**: Regular snapshots for disaster recovery
2. **Development**: Save interesting engine states for testing
3. **Deployment**: Pre-populate production engines with baseline data
4. **Debugging**: Capture problematic states for analysis
5. **Migration**: Move engine state between environments

## Custom Adapters

You can implement custom persistence adapters by creating a module that implements the `Presto.Persistence.Adapter` behaviour.

### Required Callbacks

```elixir
defmodule MyCustomAdapter do
  @behaviour Presto.Persistence.Adapter
  
  @impl true
  def create_table(name, opts) do
    # Create your storage table/collection
  end
  
  @impl true
  def insert(table, {key, value}) do
    # Insert key-value pair
  end
  
  @impl true
  def lookup(table, key) do
    # Return list of values for key
  end
  
  @impl true
  def delete(table, key) do
    # Delete key and all values
  end
  
  @impl true
  def delete_table(table) do
    # Delete entire table
  end
  
  @impl true
  def list_all(table) do
    # Return all {key, value} pairs
  end
  
  @impl true
  def clear(table) do
    # Clear all data from table
  end
  
  @impl true
  def size(table) do
    # Return number of entries
  end
  
  @impl true
  def table_info(table, key) do
    # Return table metadata
  end
end
```

### Optional Callbacks

For better performance, you can implement these optional callbacks:

```elixir
@impl true
def insert_batch(table, entries) do
  # Optimized batch insert
end

@impl true
def snapshot(table) do
  # Optimized snapshot creation
  {:ok, snapshot_data}
end

@impl true
def restore(table, snapshot_data) do
  # Optimized snapshot restoration
end
```

### Adapter Examples

**Redis Adapter** (conceptual):
```elixir
defmodule Presto.Persistence.RedisAdapter do
  @behaviour Presto.Persistence.Adapter
  
  def create_table(name, _opts) do
    # Redis doesn't need explicit table creation
    name
  end
  
  def insert(table, {key, value}) do
    Redix.command(:redix, ["HSET", table, key, :erlang.term_to_binary(value)])
  end
  
  def lookup(table, key) do
    case Redix.command(:redix, ["HGET", table, key]) do
      {:ok, nil} -> []
      {:ok, binary} -> [:erlang.binary_to_term(binary)]
    end
  end
  
  # ... implement other callbacks
end
```

## Configuration Examples

### Development Setup

Fast in-memory processing for development:

```elixir
# config/dev.exs
config :presto, :persistence_adapter, Presto.Persistence.EtsAdapter
```

### Production Setup

Durable disk storage for production:

```elixir
# config/prod.exs
config :presto, :persistence_adapter, Presto.Persistence.DetsAdapter
config :presto, :dets_storage_dir, "/var/lib/presto"

# In your application
def start(_type, _args) do
  children = [
    # Your app components
    {Presto.RuleEngine, [
      name: :production_engine,
      adapter_opts: [
        storage_dir: Application.get_env(:presto, :dets_storage_dir),
        auto_sync: true,
        sync_interval: 5000
      ]
    ]}
  ]
  
  Supervisor.start_link(children, strategy: :one_for_one)
end
```

### Multi-Engine Setup

Different engines with different persistence strategies:

```elixir
defmodule MyApp.EngineManager do
  def start_engines() do
    # Fast engine for real-time processing
    {:ok, fast_engine} = Presto.RuleEngine.start_link([
      name: :fast_engine,
      persistence_adapter: Presto.Persistence.EtsAdapter
    ])
    
    # Durable engine for critical data
    {:ok, durable_engine} = Presto.RuleEngine.start_link([
      name: :durable_engine,
      persistence_adapter: Presto.Persistence.DetsAdapter,
      adapter_opts: [storage_dir: "/var/lib/presto/critical"]
    ])
    
    {:ok, {fast_engine, durable_engine}}
  end
end
```

## Performance Considerations

### ETS vs DETS Performance

| Operation | ETS (Memory) | DETS (Disk) |
|-----------|--------------|-------------|
| Create Table | O(1) | O(1) |
| Insert | O(1) avg | O(log n) |
| Lookup | O(1) avg | O(log n) |
| Delete | O(1) avg | O(log n) |
| Snapshot | O(n) | O(n) + disk I/O |
| Memory Usage | High | Fixed small |
| Persistence | None | Full |

### Optimization Tips

1. **Choose the Right Adapter**:
   - Use ETS for high-performance, ephemeral workloads
   - Use DETS for moderate-performance, persistent workloads
   - Consider custom adapters for specific requirements

2. **Batch Operations**:
   ```elixir
   # Instead of individual inserts
   facts = [{:person, "Alice", 30}, {:person, "Bob", 25}]
   
   # Use bulk operations when available
   Presto.RuleEngine.assert_facts_bulk(engine, facts)
   ```

3. **Snapshot Strategy**:
   - Take snapshots during low-activity periods
   - Use compression for large snapshots
   - Consider incremental snapshots for very large engines

4. **DETS Tuning**:
   ```elixir
   adapter_opts: [
     auto_sync: false,        # Disable auto-sync for bulk operations
     sync_interval: :infinity # Manual sync control
   ]
   
   # Manually sync when appropriate
   :dets.sync(table)
   ```

## Migration Guide

### From ETS-Only to Pluggable Persistence

If you're upgrading from an older version of Presto:

1. **No Code Changes Required**: Existing code continues to work unchanged
2. **Gradual Migration**: You can migrate engines one at a time
3. **Testing**: Use ETS in development, DETS in production

### Migration Example

```elixir
# Before (still works)
{:ok, engine} = Presto.RuleEngine.start_link([])

# After (with explicit configuration)
{:ok, engine} = Presto.RuleEngine.start_link([
  persistence_adapter: Presto.Persistence.DetsAdapter,
  adapter_opts: [storage_dir: "/var/lib/presto"]
])
```

## Troubleshooting

### Common Issues

**DETS File Permissions**:
```bash
# Ensure the storage directory is writable
chmod 755 /var/lib/presto
chown myapp:myapp /var/lib/presto
```

**Large File Sizes**:
```elixir
# Monitor DETS file sizes
file_size = :dets.info(table, :file_size)
IO.puts("DETS file size: #{file_size} bytes")

# Set file size limits
adapter_opts: [file_size_limit: 1_073_741_824]  # 1GB limit
```

**Snapshot Errors**:
```elixir
case Presto.RuleEngine.create_snapshot(engine) do
  {:ok, snapshot} -> 
    # Success
  {:error, reason} -> 
    Logger.error("Snapshot failed: #{inspect(reason)}")
end
```

### Debugging Tools

**Engine State Inspection**:
```elixir
# Check engine statistics
stats = Presto.RuleEngine.get_statistics(engine)

# Get diagnostics
diagnostics = Presto.RuleEngine.get_diagnostics(engine)

# List all facts
facts = Presto.RuleEngine.query_facts(engine, {:_, :_, :_})
```

**Persistence Layer Debugging**:
```elixir
# Check table sizes
table_size = Presto.Persistence.size(table)

# Get table metadata
info = Presto.Persistence.table_info(table, :memory)

# List all entries (use carefully with large tables)
entries = Presto.Persistence.list_all(table)
```

## Best Practices

1. **Environment-Specific Configuration**:
   - Use ETS for development and testing
   - Use DETS or custom adapters for production

2. **Backup Strategy**:
   - Regular snapshots for critical engines
   - Store snapshots in versioned, compressed format
   - Test restoration procedures regularly

3. **Monitoring**:
   - Monitor table sizes and memory usage
   - Set up alerts for persistence failures
   - Track snapshot creation and restoration times

4. **Error Handling**:
   - Always handle persistence errors gracefully
   - Implement fallback strategies for critical operations
   - Log persistence events for troubleshooting

5. **Performance Testing**:
   - Benchmark different adapters with your workload
   - Test snapshot and restoration times
   - Monitor performance impact of persistence choices

The Presto persistence layer provides a robust foundation for production deployments while maintaining the simplicity and performance that makes Presto powerful for development and testing scenarios.
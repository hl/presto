# Presto Logging Configuration

Presto provides structured logging for debugging and monitoring rule engine operations. As a library, Presto's logging is **disabled by default** to avoid interfering with your application's logging.

## Enabling Presto Logging

To enable Presto logging in your host application, add this to your `config/config.exs`:

```elixir
# Enable Presto logging
config :presto, :logging,
  enabled: true,
  level: :info
```

## Log Levels

Presto uses the standard Elixir log levels:

- `:debug` - Detailed debugging information (hot-path operations, fact processing)
- `:info` - General operational information (engine lifecycle, rule compilation)
- `:warn` - Warning conditions 
- `:error` - Error conditions

**Recommended Settings:**

```elixir
# Development - Full debugging
config :presto, :logging, enabled: true, level: :debug

# Production - Operational info only
config :presto, :logging, enabled: true, level: :info

# High Performance - Disabled
config :presto, :logging, enabled: false
```

## Logged Components

When enabled, Presto logs activities from these components:

- `:engine_lifecycle` - Engine start/stop, initialization
- `:rule_engine` - Rule execution tracking
- `:working_memory` - Fact assertion/retraction
- `:rete_network` - Network operations (alpha/beta nodes)
- `:rule_compiler` - Rule compilation and optimization
- `:performance` - Execution timing and statistics

## Example Log Output

```
19:50:57.171 component=engine_lifecycle [info] Engine lifecycle: starting
19:50:57.173 component=rule_compiler [info] Rule compilation: adding_rule for rule adult_rule
19:50:57.344 component=performance [info] Performance: fire_rules completed in 1ms
```

## Environment Variable Override

You can also control logging via environment variables:

```bash
# Enable debug logging temporarily
PRESTO_LOGGING_ENABLED=true PRESTO_LOGGING_LEVEL=debug mix run my_app.exs

# Disable logging
PRESTO_LOGGING_ENABLED=false mix run my_app.exs
```

## Performance Impact

- **Disabled (default)**: Zero performance impact
- **Enabled with `:info`**: Minimal performance impact
- **Enabled with `:debug`**: Some performance impact due to detailed logging

For production systems processing high volumes of facts/rules, keep logging at `:info` level or disabled.
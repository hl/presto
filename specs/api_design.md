# Presto API Design Specification

## Design Philosophy

The Presto API follows Elixir conventions and the "Best Simple System for Now" (BSSN) principle:

- **Start Simple**: Core functionality first, advanced features later
- **Elixir Idiomatic**: Follow OTP patterns and Elixir naming conventions
- **Process-Safe**: All operations safe for concurrent access
- **Explicit Errors**: Clear error handling with `{:ok, result}` | `{:error, reason}` patterns

## Core API

### Engine Management

#### Starting an Engine

```elixir
# Simple start with no rules
{:ok, engine} = Presto.start_link([])

# Start with initial rules
rules = [rule1, rule2, rule3]
{:ok, engine} = Presto.start_link(rules)

# Start with options
{:ok, engine} = Presto.start_link(rules, name: MyRulesEngine, 
                                         max_rule_executions: 1000)

# Start supervised
children = [
  {Presto, [rules, name: MyRulesEngine]}
]
Supervisor.start_link(children, strategy: :one_for_one)
```

#### Engine Operations

```elixir
# Stop engine
:ok = Presto.stop(engine)

# Get engine info
%{
  fact_count: 1234,
  rule_count: 56,
  active_rules: 8,
  network_nodes: 120
} = Presto.info(engine)

# Health check
:ok = Presto.ping(engine)
```

### Rule Definition

#### Rule Structure

```elixir
# Basic rule definition
rule = %Presto.Rule{
  name: "adult_discount",
  
  # Conditions (alpha patterns)
  conditions: [
    {:person, :name, :age},           # Bind variables
    {:order, :name, :total}           # Join on :name variable
  ],
  
  # Guards (additional constraints)
  guards: [
    {:>=, :age, 18},                  # Age must be >= 18
    {:>, :total, 100}                 # Order total > 100
  ],
  
  # Action to execute when rule fires
  action: {:apply_discount, 0.1},
  
  # Optional metadata
  priority: 10,
  enabled: true
}

# Alternative action types
rule_with_function = %Presto.Rule{
  name: "complex_action",
  conditions: [...],
  action: fn bindings -> 
    # Custom function with bound variables
    IO.puts("Processing order for #{bindings.name}")
  end
}

# Action as MFA (Module, Function, Args)
rule_with_mfa = %Presto.Rule{
  name: "mfa_action", 
  conditions: [...],
  action: {MyModule, :process_order, [:name, :total]}
}
```

#### Rule Management

```elixir
# Add rules to running engine
:ok = Presto.add_rule(engine, rule)
:ok = Presto.add_rules(engine, [rule1, rule2, rule3])

# Remove rules
:ok = Presto.remove_rule(engine, "adult_discount")
:ok = Presto.remove_rules(engine, ["rule1", "rule2"])

# Update existing rule
updated_rule = %{rule | priority: 20}
:ok = Presto.update_rule(engine, updated_rule)

# Enable/disable rules
:ok = Presto.enable_rule(engine, "adult_discount")
:ok = Presto.disable_rule(engine, "adult_discount")

# List rules
rules = Presto.list_rules(engine)
active_rules = Presto.list_active_rules(engine)
```

### Fact Management

#### Asserting Facts

```elixir
# Assert single fact
:ok = Presto.assert_fact(engine, {:person, "John", 25})

# Assert multiple facts  
facts = [
  {:person, "John", 25},
  {:person, "Jane", 30},
  {:order, "John", 150}
]
:ok = Presto.assert_facts(engine, facts)

# Assert with callback notification
:ok = Presto.assert_fact(engine, fact, notify: self())
# Receive: {:fact_asserted, fact}
```

#### Retracting Facts

```elixir
# Retract specific fact
:ok = Presto.retract_fact(engine, {:person, "John", 25})

# Retract by pattern (retracts all matching)
:ok = Presto.retract_facts(engine, {:person, "John", :_})

# Retract all facts
:ok = Presto.retract_all_facts(engine)
```

#### Querying Facts

```elixir
# Get all facts
facts = Presto.get_facts(engine)

# Get facts by pattern
persons = Presto.get_facts(engine, {:person, :_, :_})
johns_orders = Presto.get_facts(engine, {:order, "John", :_})

# Count facts
total_count = Presto.count_facts(engine)
person_count = Presto.count_facts(engine, {:person, :_, :_})

# Check if fact exists
true = Presto.fact_exists?(engine, {:person, "John", 25})
```

### Rule Execution Control

#### Execution Modes

```elixir
# Manual execution (rules don't fire automatically)
{:ok, engine} = Presto.start_link(rules, execution_mode: :manual)
:ok = Presto.assert_fact(engine, fact)
{fired_rules, new_facts} = Presto.run_cycle(engine)

# Automatic execution (default - rules fire on fact changes)
{:ok, engine} = Presto.start_link(rules, execution_mode: :automatic)

# Single-step execution
:ok = Presto.assert_fact(engine, fact)
{fired_rule, new_facts} = Presto.step(engine)
```

#### Execution Limits

```elixir
# Set maximum rule executions per cycle
:ok = Presto.set_max_executions(engine, 100)

# Set execution timeout
:ok = Presto.set_execution_timeout(engine, 5000) # 5 seconds

# Run with custom limits
{fired_rules, new_facts} = Presto.run_cycle(engine, 
                                           max_executions: 50,
                                           timeout: 2000)
```

### Event Handling & Monitoring

#### Event Subscription

```elixir
# Subscribe to all events
:ok = Presto.subscribe(engine, self())

# Subscribe to specific event types
:ok = Presto.subscribe(engine, self(), [:rule_fired, :fact_asserted])

# Unsubscribe
:ok = Presto.unsubscribe(engine, self())
```

#### Event Types

```elixir
# Received messages:
{:rule_fired, %{rule: "adult_discount", bindings: %{name: "John", age: 25}}}
{:fact_asserted, {:person, "John", 25}}
{:fact_retracted, {:person, "John", 25}} 
{:rule_added, "new_rule"}
{:rule_removed, "old_rule"}
{:execution_limit_reached, %{cycle: 1, executions: 100}}
{:execution_timeout, %{cycle: 1, timeout: 5000}}
```

#### Performance Monitoring

```elixir
# Get execution statistics
%{
  total_cycles: 42,
  total_rule_executions: 1250,
  average_cycle_time: 15.7, # milliseconds
  memory_usage: %{
    working_memory: 2048,    # KB
    alpha_memories: 1024,    # KB  
    beta_memories: 4096      # KB
  },
  rule_statistics: %{
    "adult_discount" => %{executions: 45, avg_time: 2.1},
    "vip_upgrade" => %{executions: 12, avg_time: 5.8}
  }
} = Presto.get_statistics(engine)

# Reset statistics
:ok = Presto.reset_statistics(engine)
```

## Advanced Features

### Complex Pattern Matching

```elixir
# Nested patterns with guards
rule = %Presto.Rule{
  name: "complex_matching",
  conditions: [
    {:person, :name, %{age: :age, department: :dept}},
    {:order, :name, %{total: :total, items: :items}}
  ],
  guards: [
    {:>=, :age, 21},
    {:==, :dept, "Sales"},
    {:>, :total, 500},
    {:>, {:length, :items}, 3}
  ],
  action: {:premium_processing, []}
}

# Variable binding with transformations
rule = %Presto.Rule{
  name: "computed_values",
  conditions: [
    {:order, :id, :total},
    {:tax_rate, :region, :rate}
  ],
  guards: [
    {:>, :total, 100}
  ],
  # Computed bindings available in action
  bindings: %{
    total_with_tax: {:*, :total, {:+, 1, :rate}}
  },
  action: fn bindings -> 
    IO.puts("Total with tax: #{bindings.total_with_tax}")
  end
}
```

### Batch Operations

```elixir
# Batch fact operations for performance
batch = Presto.start_batch(engine)
|> Presto.batch_assert({:person, "John", 25})
|> Presto.batch_assert({:person, "Jane", 30})
|> Presto.batch_retract({:order, "old_order", :_})

{:ok, results} = Presto.execute_batch(batch)
```

### Rule Templates

```elixir
# Define reusable rule templates
defmodule MyRuleTemplates do
  def discount_rule(age_limit, discount_percent) do
    %Presto.Rule{
      name: "discount_#{age_limit}_#{discount_percent}",
      conditions: [
        {:person, :name, :age},
        {:order, :name, :total}
      ],
      guards: [
        {:>=, :age, age_limit}
      ],
      action: {:apply_discount, discount_percent}
    }
  end
end

# Use templates
senior_discount = MyRuleTemplates.discount_rule(65, 0.15)
adult_discount = MyRuleTemplates.discount_rule(18, 0.05)
```

### Debugging & Introspection

```elixir
# Debug rule execution
:ok = Presto.enable_debug(engine)
:ok = Presto.disable_debug(engine)

# Trace specific rule
:ok = Presto.trace_rule(engine, "adult_discount", self())
# Receive detailed trace messages

# Get network structure
network = Presto.get_network_structure(engine)

# Explain why rule fired or didn't fire
explanation = Presto.explain_rule(engine, "adult_discount", 
                                 current_facts: Presto.get_facts(engine))
```

## Error Handling

### Common Error Patterns

```elixir
# Rule definition errors
{:error, {:invalid_rule, reason}} = Presto.add_rule(engine, invalid_rule)

# Fact assertion errors  
{:error, {:invalid_fact, reason}} = Presto.assert_fact(engine, invalid_fact)

# Engine state errors
{:error, :engine_not_running} = Presto.assert_fact(dead_engine, fact)

# Resource limits
{:error, {:execution_limit_exceeded, details}} = Presto.run_cycle(engine)

# Timeout errors
{:error, {:timeout, partial_results}} = Presto.run_cycle(engine, timeout: 100)
```

### Error Recovery

```elixir
# Graceful error handling
case Presto.assert_fact(engine, fact) do
  :ok -> 
    :ok
  {:error, {:invalid_fact, reason}} ->
    Logger.warning("Invalid fact: #{inspect(reason)}")
    :error
  {:error, :engine_not_running} ->
    # Restart engine or fail gracefully
    restart_engine()
end
```

## Configuration Options

### Engine Configuration

```elixir
config = %{
  # Execution settings
  execution_mode: :automatic,        # :automatic | :manual
  max_rule_executions: 1000,         # per cycle
  execution_timeout: 30_000,         # milliseconds
  
  # Memory settings  
  initial_memory_size: 10_000,       # estimated fact count
  memory_cleanup_interval: 60_000,   # milliseconds
  
  # Concurrency settings
  max_concurrent_rules: 10,          # parallel rule executions
  rule_execution_timeout: 5_000,     # per rule timeout
  
  # Monitoring settings
  enable_statistics: true,
  statistics_interval: 10_000,       # collection interval
  
  # Debug settings
  debug_mode: false,
  trace_execution: false,
  log_level: :info
}

{:ok, engine} = Presto.start_link(rules, config)
```

This API design provides a comprehensive yet approachable interface for building rules engines in Elixir, balancing simplicity for basic use cases with power for advanced scenarios.
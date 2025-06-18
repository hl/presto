# Presto API Design Specification

## Design Philosophy

The Presto API follows Elixir conventions and the "Best Simple System for Now" (BSSN) principle:

- **Start Simple**: Core functionality first, advanced features later
- **Elixir Idiomatic**: Follow OTP patterns and Elixir naming conventions
- **Process-Safe**: All operations safe for concurrent access
- **Explicit Errors**: Clear error handling with `{:ok, result}` | `{:error, reason}` patterns

## Core API

### Engine Management

#### Engine Lifecycle Overview

```mermaid
sequenceDiagram
    participant Client
    participant Presto
    participant Engine as RuleEngine Process
    participant Supervisor
    
    Client->>Presto: start_engine()
    Presto->>Engine: GenServer.start_link()
    Engine-->>Presto: {:ok, pid}
    Presto-->>Client: {:ok, engine}
    
    Note over Client,Engine: Engine is now running
    
    Client->>Presto: get_engine_statistics(engine)
    Presto->>Engine: GenServer.call(:get_statistics)
    Engine-->>Presto: %{total_facts: ..., ...}
    Presto-->>Client: statistics_map
    
    Client->>Presto: stop_engine(engine)
    Presto->>Engine: GenServer.stop()
    Engine-->>Presto: :ok
    Presto-->>Client: :ok
    
    Note over Engine: Process terminated
```

#### Starting an Engine

```elixir
# Simple start with no options
{:ok, engine} = Presto.start_engine()

# Start with options (currently no options supported)
{:ok, engine} = Presto.start_engine([])

# Start supervised
children = [
  {Presto.RuleEngine, []}
]
Supervisor.start_link(children, strategy: :one_for_one)
```

#### Engine Operations

```elixir
# Stop engine
:ok = Presto.stop_engine(engine)

# Get engine statistics
%{
  total_facts: 1234,
  total_rules: 56,
  total_rule_firings: 789,
  last_execution_time: 1500, # microseconds
  fast_path_executions: 45,
  rete_network_executions: 11,
  alpha_nodes_saved_by_sharing: 12
} = Presto.get_engine_statistics(engine)
```

### Rule Definition

#### Rule Lifecycle

```mermaid
flowchart TD
    A[Rule Definition] --> B{Validate Rule}
    B -->|Valid| C[Add to Engine]
    B -->|Invalid| D[Return Error]
    
    C --> E[Analyze Complexity]
    E --> F{Simple Rule?}
    F -->|Yes| G[Mark for Fast-Path]
    F -->|No| H[Build RETE Network]
    
    G --> I[Rule Active]
    H --> J[Share Alpha Nodes]
    J --> I
    
    I --> K[Rule Execution]
    K --> L[Pattern Matching]
    L --> M{Conditions Met?}
    M -->|Yes| N[Execute Action]
    M -->|No| O[Skip Rule]
    
    N --> P[Assert New Facts]
    P --> Q[Update Statistics]
    O --> Q
    
    I --> R[Remove Rule]
    R --> S[Clean Up Network]
    S --> T[Rule Removed]
    
    style A fill:#e1f5fe
    style I fill:#c8e6c9
    style T fill:#ffcdd2
    style D fill:#ffcdd2
```

#### Rule Structure

```elixir
# Basic rule definition
rule = %{
  id: :adult_rule,                      # Required: unique atom identifier
  conditions: [                         # Required: list of conditions
    {:person, :name, :age},             # Pattern: bind variables
    {:age, :>, 18}                      # Test: age must be > 18
  ],
  action: fn bindings ->                # Required: function taking bindings map
    [{:adult, bindings[:name]}]         # Return list of new facts
  end,
  priority: 10                          # Optional: higher priority = earlier execution
}

# Action as anonymous function with explicit bindings
rule_with_function = %{
  id: :complex_action,
  conditions: [
    {:person, :name, :age},
    {:employment, :name, :company},
    {:age, :>, 25}
  ],
  action: fn bindings -> 
    name = bindings[:name]
    company = bindings[:company]
    [{:senior_employee, name, company}]
  end
}
```

#### Rule Management

```mermaid
sequenceDiagram
    participant Client
    participant Presto
    participant Engine
    participant RETENetwork as RETE Network
    
    Client->>Presto: add_rule(engine, rule)
    Presto->>Engine: GenServer.call({:add_rule, rule})
    Engine->>Engine: validate_rule(rule)
    
    alt Rule is valid
        Engine->>Engine: analyse_complexity(rule)
        alt Simple rule (≤2 conditions)
            Engine->>Engine: mark_for_fast_path(rule)
        else Complex rule
            Engine->>RETENetwork: build_network_nodes(rule)
            RETENetwork-->>Engine: nodes_created
            Engine->>Engine: optimise_alpha_sharing()
        end
        Engine-->>Presto: :ok
        Presto-->>Client: :ok
    else Rule is invalid
        Engine-->>Presto: {:error, reason}
        Presto-->>Client: {:error, reason}
    end
    
    Note over Client,RETENetwork: Rule is now active
    
    Client->>Presto: remove_rule(engine, rule_id)
    Presto->>Engine: GenServer.call({:remove_rule, rule_id})
    Engine->>RETENetwork: cleanup_network_nodes(rule_id)
    RETENetwork-->>Engine: cleanup_complete
    Engine-->>Presto: :ok
    Presto-->>Client: :ok
```

```elixir
# Add single rule to running engine
:ok = Presto.add_rule(engine, rule)

# Remove rule
:ok = Presto.remove_rule(engine, :adult_rule)

# Get all rules
rules = Presto.get_rules(engine)
# Returns: %{adult_rule: %{id: :adult_rule, conditions: [...], ...}}
```

### Fact Management

#### Fact Lifecycle and Working Memory

```mermaid
flowchart TD
    A[Assert Fact] --> B[Validate Fact Format]
    B -->|Valid| C[Add to Working Memory]
    B -->|Invalid| D[Return Error]
    
    C --> E[Update Fact Index]
    E --> F[Mark Affected Rules]
    F --> G[Fact Available for Matching]
    
    G --> H{Rule Execution?}
    H -->|Yes| I[Pattern Matching]
    H -->|No| J[Fact Remains in Memory]
    
    I --> K{Fact Matches Conditions?}
    K -->|Yes| L[Rule Fires]
    K -->|No| J
    
    L --> M[Generate New Facts]
    M --> N[Mark as Derived]
    N --> C
    
    J --> O[Retract Fact]
    O --> P[Remove from Working Memory]
    P --> Q[Clean Up Derived Facts]
    Q --> R[Update Fact Index]
    R --> S[Fact Removed]
    
    style A fill:#e1f5fe
    style G fill:#c8e6c9
    style S fill:#ffcdd2
    style D fill:#ffcdd2
    style N fill:#fff3e0
```

#### Asserting Facts

```elixir
# Assert single fact
:ok = Presto.assert_fact(engine, {:person, "John", 25})

# Facts are tuples representing structured information
:ok = Presto.assert_fact(engine, {:employment, "John", "TechCorp"})
:ok = Presto.assert_fact(engine, {:order, "ORDER-123", 150})
```

#### Retracting Facts

```elixir
# Retract specific fact (exact match required)
:ok = Presto.retract_fact(engine, {:person, "John", 25})
```

#### Querying Facts

```elixir
# Get all facts currently in working memory
facts = Presto.get_facts(engine)
# Returns: [{:person, "John", 25}, {:employment, "John", "TechCorp"}, ...]

# Clear all facts
:ok = Presto.clear_facts(engine)
```

### Rule Execution

#### Rule Execution Flow

```mermaid
flowchart TD
    A[fire_rules called] --> B[Get Active Rules]
    B --> C{Any rules to execute?}
    C -->|No| D[Return empty list]
    C -->|Yes| E[Sort by Priority]
    
    E --> F[For each rule]
    F --> G{Rule Strategy?}
    
    G -->|Fast Path| H[Fast Path Execution]
    G -->|RETE Network| I[RETE Network Execution]
    
    H --> J[Match Conditions Directly]
    I --> K[Traverse Alpha/Beta Nodes]
    
    J --> L{Conditions Met?}
    K --> L
    
    L -->|Yes| M[Execute Action Function]
    L -->|No| N[Skip Rule]
    
    M --> O{Action Successful?}
    O -->|Yes| P[Assert New Facts]
    O -->|No| Q[Log Error]
    
    P --> R[Update Statistics]
    Q --> R
    N --> R
    
    R --> S{More Rules?}
    S -->|Yes| F
    S -->|No| T[Return Results]
    
    style A fill:#e1f5fe
    style T fill:#c8e6c9
    style Q fill:#ffcdd2
    style H fill:#fff3e0
    style I fill:#f3e5f5
```

#### Basic Execution

```elixir
# Execute all applicable rules
results = Presto.fire_rules(engine)
# Returns: [{:adult, "John"}, {:senior_employee, "John", "TechCorp"}]

# Execute with options
results = Presto.fire_rules(engine, concurrent: true)        # Parallel execution
results = Presto.fire_rules(engine, auto_chain: true)       # Automatic rule chaining
```

### Performance Monitoring

#### Rule Statistics

```elixir
# Get execution statistics for each rule
%{
  adult_rule: %{
    executions: 45,
    total_time: 1500,      # microseconds
    average_time: 33,      # microseconds
    facts_processed: 120,
    strategy_used: :fast_path,
    complexity: :simple
  },
  senior_rule: %{
    executions: 12,
    total_time: 890,
    average_time: 74,
    facts_processed: 36,
    strategy_used: :rete_network,
    complexity: :moderate
  }
} = Presto.get_rule_statistics(engine)
```

#### Engine Statistics

```elixir
# Get overall engine performance metrics
%{
  total_facts: 1234,
  total_rules: 56,
  total_rule_firings: 789,
  last_execution_time: 1500,
  fast_path_executions: 45,         # Rules executed via fast-path optimisation
  rete_network_executions: 11,      # Rules executed via full RETE network
  alpha_nodes_saved_by_sharing: 12  # Optimization metric
} = Presto.get_engine_statistics(engine)
```

## Advanced Features

### Batch Operations

#### Batch Processing Sequence

```mermaid
sequenceDiagram
    participant Client
    participant Presto
    participant Engine
    participant BatchState as Batch State
    
    Client->>Presto: start_batch(engine)
    Presto->>Engine: GenServer.call(:start_batch)
    Engine->>BatchState: create_batch_context()
    BatchState-->>Engine: batch_id
    Engine-->>Presto: %Batch{id: batch_id, ...}
    Presto-->>Client: batch
    
    loop Batch Operations
        Client->>Presto: batch_assert_fact(batch, fact)
        Presto->>BatchState: add_operation({:assert, fact})
        BatchState-->>Presto: updated_batch
        Presto-->>Client: updated_batch
        
        Client->>Presto: batch_add_rule(batch, rule)
        Presto->>BatchState: add_operation({:add_rule, rule})
        BatchState-->>Presto: updated_batch
        Presto-->>Client: updated_batch
    end
    
    Client->>Presto: execute_batch(batch)
    Presto->>Engine: GenServer.call({:execute_batch, batch})
    
    Engine->>Engine: validate_all_operations(batch)
    alt All operations valid
        Engine->>Engine: execute_operations_atomically(batch)
        Engine->>Engine: update_rete_network()
        Engine->>Engine: fire_affected_rules()
        Engine-->>Presto: {:ok, results}
        Presto-->>Client: results
    else Some operations invalid
        Engine-->>Presto: {:error, failed_operations}
        Presto-->>Client: {:error, failed_operations}
    end
    
    Engine->>BatchState: cleanup_batch(batch_id)
```

Batch operations provide efficient bulk processing of multiple facts and rules:

```elixir
# Start a batch operation
batch = Presto.start_batch(engine)

# Add multiple operations to the batch
batch = batch
  |> Presto.batch_assert_fact({:person, "Alice", 30})
  |> Presto.batch_assert_fact({:person, "Bob", 25})
  |> Presto.batch_retract_fact({:old_person, "Charlie", 65})
  |> Presto.batch_add_rule(new_rule)

# Execute all batched operations at once
results = Presto.execute_batch(batch)
```

### Incremental Processing

#### Incremental Execution Strategy

```mermaid
sequenceDiagram
    participant Client
    participant Engine
    participant FactTracker as Fact Tracker
    participant RuleAnalyzer as Rule Analyzer
    
    Note over Client,RuleAnalyzer: Initial state with existing facts and rules
    
    Client->>Engine: assert_fact(new_fact)
    Engine->>FactTracker: track_new_fact(new_fact)
    FactTracker->>FactTracker: mark_generation(current + 1)
    FactTracker-->>Engine: fact_tracked
    
    Client->>Engine: fire_rules_incremental()
    Engine->>FactTracker: get_facts_since_last_execution()
    FactTracker-->>Engine: [new_facts]
    
    Engine->>RuleAnalyzer: find_affected_rules(new_facts)
    RuleAnalyzer-->>Engine: [affected_rules]
    
    loop For each affected rule
        Engine->>Engine: execute_rule_with_new_facts(rule, new_facts)
        Engine->>Engine: track_derived_facts(results)
    end
    
    Engine->>FactTracker: update_last_execution_marker()
    Engine-->>Client: incremental_results
    
    Note over Client,RuleAnalyzer: Only affected rules executed, significant performance gain
```

For performance-critical applications processing continuous fact streams:

```elixir
# Fire only rules affected by facts added since last incremental execution
incremental_results = Presto.RuleEngine.fire_rules_incremental(engine)
```

### Error Handling in Rule Execution

```elixir
# Execute rules with detailed error reporting
{:ok, results, errors} = Presto.RuleEngine.fire_rules_with_errors(engine)

# errors format: [{:error, rule_id, exception}, ...]
```

### Rule Analysis and Optimization

#### Optimization Decision Tree

```mermaid
flowchart TD
    A[Rule Added] --> B[Analyze Conditions]
    B --> C{Condition Count}
    
    C -->|≤ fast_path_threshold| D[Mark for Fast Path]
    C -->|> fast_path_threshold| E[Build RETE Network]
    
    D --> F[Fast Path Strategy]
    E --> G{Alpha Sharing Enabled?}
    
    G -->|Yes| H[Check for Shared Patterns]
    G -->|No| I[Create Individual Nodes]
    
    H --> J{Shared Pattern Found?}
    J -->|Yes| K[Reuse Alpha Node]
    J -->|No| L[Create New Alpha Node]
    
    K --> M[Update Sharing Statistics]
    L --> M
    I --> N[Full Network Strategy]
    
    M --> N
    F --> O[Ready for Execution]
    N --> O
    
    O --> P{Batching Enabled?}
    P -->|Yes| Q[Group Similar Rules]
    P -->|No| R[Individual Execution]
    
    Q --> S[Batch Execution Strategy]
    R --> T[Direct Execution Strategy]
    
    style D fill:#c8e6c9
    style K fill:#e1f5fe
    style S fill:#fff3e0
```

```elixir
# Analyze individual rule complexity and strategy
analysis = Presto.RuleEngine.analyse_rule(engine, :adult_rule)
# Returns: %{strategy: :fast_path, complexity: :simple, ...}

# Analyze entire rule set
rule_set_analysis = Presto.RuleEngine.analyse_rule_set(engine)

# Configure optimisation settings
:ok = Presto.RuleEngine.configure_optimisation(engine, [
  enable_fast_path: true,
  enable_alpha_sharing: true,
  enable_rule_batching: true,
  fast_path_threshold: 2
])

# Get current optimisation configuration
config = Presto.RuleEngine.get_optimisation_config(engine)
```

### Execution Order Tracking

```elixir
# Get the order in which rules were executed in the last cycle
execution_order = Presto.RuleEngine.get_last_execution_order(engine)
# Returns: [:high_priority_rule, :medium_priority_rule, :low_priority_rule]
```

## Error Handling

### Error Handling Flow

```mermaid
flowchart TD
    A[API Call] --> B{Validate Input}
    B -->|Invalid| C[Return Validation Error]
    B -->|Valid| D[Engine Process Call]
    
    D --> E{Engine Alive?}
    E -->|No| F[Process Exit Error]
    E -->|Yes| G[Execute Operation]
    
    G --> H{Operation Success?}
    H -->|Yes| I[Return Success]
    H -->|No| J[Determine Error Type]
    
    J --> K{Rule Error?}
    K -->|Yes| L[Rule Execution Error]
    K -->|No| M{State Error?}
    M -->|Yes| N[Engine State Error]
    M -->|No| O[System Error]
    
    L --> P[Log Rule Error]
    N --> Q[Log State Error]
    O --> R[Log System Error]
    
    P --> S{Recoverable?}
    Q --> S
    R --> S
    
    S -->|Yes| T[Attempt Recovery]
    S -->|No| U[Return Error]
    
    T --> V{Recovery Success?}
    V -->|Yes| I
    V -->|No| U
    
    style A fill:#e1f5fe
    style I fill:#c8e6c9
    style C fill:#ffcdd2
    style F fill:#ffcdd2
    style U fill:#ffcdd2
    style T fill:#fff3e0
```

### Error Propagation and Recovery

```mermaid
sequenceDiagram
    participant Client
    participant Presto
    participant Engine
    participant Rule as Rule Action
    
    Client->>Presto: fire_rules_with_errors(engine)
    Presto->>Engine: GenServer.call(:fire_rules_with_errors)
    
    Engine->>Engine: execute_rules_safely()
    
    loop For each rule
        Engine->>Rule: execute_action(bindings)
        
        alt Action succeeds
            Rule-->>Engine: new_facts
            Engine->>Engine: assert_facts(new_facts)
        else Action fails
            Rule-->>Engine: exception
            Engine->>Engine: capture_error(rule_id, exception)
            Engine->>Engine: continue_with_next_rule()
        end
    end
    
    Engine->>Engine: compile_results_and_errors()
    Engine-->>Presto: {:ok, results, errors}
    Presto-->>Client: {:ok, results, errors}
    
    Note over Client,Rule: Errors isolated, execution continues
```

### Common Error Patterns

```elixir
# Rule definition errors
{:error, :rule_must_be_map} = Presto.add_rule(engine, "invalid")
{:error, :missing_required_fields} = Presto.add_rule(engine, %{})
{:error, :id_must_be_atom} = Presto.add_rule(engine, %{id: "string_id", conditions: [], action: fn _ -> [] end})
{:error, :conditions_must_be_list} = Presto.add_rule(engine, %{id: :test, conditions: :invalid, action: fn _ -> [] end})
{:error, :action_must_be_function} = Presto.add_rule(engine, %{id: :test, conditions: [], action: "invalid"})

# Engine state errors
** (EXIT) Process not alive - engine process has stopped
```

### Error Recovery

```elixir
# Graceful error handling
case Presto.add_rule(engine, rule) do
  :ok -> 
    Logger.info("Rule added successfully")
  {:error, reason} ->
    Logger.warning("Failed to add rule: #{inspect(reason)}")
    :error
end

# Rule execution with error isolation
{:ok, results, errors} = Presto.RuleEngine.fire_rules_with_errors(engine)
unless Enum.empty?(errors) do
  Logger.error("Rule execution errors: #{inspect(errors)}")
end
```

## Rule Engine Configuration

### Engine Configuration

Currently, the engine has minimal configuration options. Optimization settings can be configured at runtime:

```elixir
# Default optimisation configuration
default_config = %{
  enable_fast_path: false,           # Fast-path execution for simple rules
  enable_alpha_sharing: true,        # Share alpha nodes between rules
  enable_rule_batching: true,        # Batch rule execution for efficiency
  fast_path_threshold: 2,            # Max conditions for fast-path eligibility
  sharing_threshold: 2               # Min rules sharing pattern for alpha node sharing
}

# Update optimisation settings
Presto.RuleEngine.configure_optimisation(engine, [
  enable_fast_path: true,
  fast_path_threshold: 3
])
```

## Implementation Notes

### Rule Execution Strategy

The engine automatically chooses between two execution strategies:

1. **Fast-Path Execution**: For simple rules (≤ 2 conditions), bypasses full RETE network
2. **RETE Network Execution**: For complex rules, uses full alpha/beta network processing

### Fact Lineage Tracking

The engine tracks fact derivation for incremental processing:

- Input facts: Facts directly asserted by users
- Derived facts: Facts produced by rule execution
- Generation numbers: Track fact creation order
- Lineage relationships: Track which facts derived from which

### Performance Characteristics

```mermaid
flowchart LR
    subgraph "Execution Strategies"
        A[Fact Assertion] --> B{Rule Complexity}
        B -->|Simple ≤2 conditions| C[Fast Path O(F)]
        B -->|Complex >2 conditions| D[RETE Network O(F×P)]
        
        C --> E[Direct Pattern Match]
        D --> F[Alpha/Beta Node Traversal]
        
        E --> G[Execute Action]
        F --> H[Shared Alpha Nodes]
        H --> G
        
        G --> I[Assert New Facts]
    end
    
    subgraph "Optimization Benefits"
        J[Alpha Node Sharing] --> K[Reduced Memory O(N)]
        L[Fast Path Execution] --> M[Lower Latency O(1)]
        N[Rule Batching] --> O[Better Throughput]
    end
    
    style C fill:#c8e6c9
    style D fill:#fff3e0
    style K fill:#e1f5fe
    style M fill:#e1f5fe
    style O fill:#e1f5fe
```

- **Fact Assertion**: O(1) for most cases, O(R) where R = rules matching fact type
- **Rule Execution**: O(F) for fast-path rules, O(F×P) for RETE rules where F=facts, P=patterns
- **Memory Usage**: Linear with fact count, shared alpha nodes reduce rule network size

This API provides a solid foundation for building rules engines in Elixir, balancing simplicity for basic use cases with performance optimisations for production scenarios.

## Future Considerations

The following features are planned for future versions but not currently implemented:

- **Pattern-based fact queries**: Query facts by partial patterns
- **Rule enable/disable**: Temporarily enable or disable specific rules
- **Event subscription**: Subscribe to rule firing and fact assertion events
- **Network introspection**: Examine internal RETE network structure for debugging
- **Health checks**: Engine health monitoring and diagnostics
- **Hot rule updates**: Modify rule definitions without engine restart
- **Complex pattern matching**: Nested patterns and advanced guards
- **Rule templates**: Reusable rule generation patterns
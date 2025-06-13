# Presto Architecture Specification

## Overview

Presto implements the RETE algorithm using Elixir's OTP principles with a supervision tree, GenServers for coordination, and ETS tables for high-performance memory management. The architecture balances RETE's memory-intensive approach with Elixir's process-oriented design.

## Module Structure

### Core Library Modules

```
lib/presto/
├── presto.ex                    # Main API module
├── engine.ex                    # Core engine GenServer  
├── rule.ex                      # Rule definition struct
├── network/
│   ├── network.ex              # Network structure management
│   ├── alpha_network.ex        # Alpha network implementation
│   ├── beta_network.ex         # Beta network implementation
│   ├── alpha_node.ex           # Individual alpha node
│   ├── beta_node.ex            # Individual beta node
│   └── compiler.ex             # Rule-to-network compiler
├── memory/
│   ├── working_memory.ex       # Working memory ETS wrapper
│   ├── alpha_memory.ex         # Alpha node memory
│   ├── beta_memory.ex          # Beta node memory
│   └── memory_manager.ex       # Memory lifecycle management
├── execution/
│   ├── executor.ex             # Rule execution coordinator
│   ├── rule_firer.ex           # Individual rule firing
│   ├── conflict_resolver.ex    # Conflict resolution strategies
│   └── scheduler.ex            # Execution scheduling
├── pattern/
│   ├── pattern.ex              # Pattern matching utilities
│   ├── matcher.ex              # Pattern compilation
│   └── guard_evaluator.ex     # Guard condition evaluation
├── monitoring/
│   ├── statistics.ex           # Performance statistics
│   ├── tracer.ex               # Execution tracing
│   └── debugger.ex             # Debug utilities
└── utils/
    ├── token.ex                # Token structure and utilities
    ├── binding.ex              # Variable binding management
    └── id_generator.ex         # Unique ID generation
```

## Component Architecture

### 1. Supervision Tree

```elixir
defmodule Presto.Supervisor do
  use Supervisor
  
  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end
  
  def init(opts) do
    children = [
      # Core engine process
      {Presto.Engine, opts},
      
      # Memory management
      {Presto.Memory.MemoryManager, []},
      
      # Rule execution supervision
      {Task.Supervisor, name: Presto.Execution.Supervisor},
      
      # Statistics collection
      {Presto.Monitoring.Statistics, []},
      
      # Optional: Network component supervision
      {DynamicSupervisor, name: Presto.Network.Supervisor}
    ]
    
    Supervisor.init(children, strategy: :one_for_one)
  end
end
```

### 2. Core Engine (GenServer)

```elixir
defmodule Presto.Engine do
  use GenServer
  
  @type state :: %{
    network: Presto.Network.t(),
    rules: [Presto.Rule.t()],
    execution_mode: :automatic | :manual,
    config: map(),
    subscribers: [pid()]
  }
  
  # Client API
  def start_link(rules, opts \\ [])
  def add_rule(engine, rule)
  def assert_fact(engine, fact)
  def run_cycle(engine, opts \\ [])
  
  # Server callbacks
  def init({rules, opts})
  def handle_call({:add_rule, rule}, _from, state)
  def handle_cast({:assert_fact, fact}, state)
  def handle_info({:rule_fired, result}, state)
end
```

### 3. Network Management

#### Network Structure
```elixir
defmodule Presto.Network do
  @type t :: %__MODULE__{
    alpha_nodes: %{pattern_id() => Presto.AlphaNode.t()},
    beta_nodes: %{node_id() => Presto.BetaNode.t()},
    root_node: node_id(),
    terminal_nodes: %{rule_name() => node_id()},
    node_connections: %{node_id() => [node_id()]},
    memory_tables: %{node_id() => ets_table_name()}
  }
  
  defstruct [
    :alpha_nodes,
    :beta_nodes, 
    :root_node,
    :terminal_nodes,
    :node_connections,
    :memory_tables
  ]
end
```

#### Alpha Network
```elixir
defmodule Presto.AlphaNetwork do
  @moduledoc """
  Manages alpha network - single-condition pattern matching
  """
  
  def create_alpha_node(pattern, conditions)
  def evaluate_fact(alpha_node, fact) 
  def propagate_to_beta(alpha_node, fact)
  
  defp compile_pattern_matcher(pattern)
  defp create_alpha_memory(node_id)
end

defmodule Presto.AlphaNode do
  @type t :: %__MODULE__{
    id: node_id(),
    pattern: pattern(),
    conditions: [condition()],
    matcher_fun: function(),
    memory_table: ets_table_name(),
    beta_connections: [node_id()]
  }
end
```

#### Beta Network  
```elixir
defmodule Presto.BetaNetwork do
  @moduledoc """
  Manages beta network - multi-condition joins
  """
  
  def create_beta_node(left_input, right_input, join_conditions)
  def handle_left_activation(beta_node, token)
  def handle_right_activation(beta_node, fact)
  def perform_join(left_token, right_fact, join_conditions)
  
  defp create_beta_memory(node_id)
  defp check_variable_consistency(bindings1, bindings2)
end

defmodule Presto.BetaNode do
  @type t :: %__MODULE__{
    id: node_id(),
    left_input: node_id(),
    right_input: node_id(), 
    join_conditions: [join_condition()],
    memory_table: ets_table_name(),
    output_connections: [node_id()]
  }
end
```

### 4. Memory Management

#### Working Memory
```elixir
defmodule Presto.WorkingMemory do
  @moduledoc """
  Central fact storage using ETS
  """
  
  def start_link(opts)
  def assert_fact(fact)
  def retract_fact(fact) 
  def get_facts(pattern \\ :_)
  def fact_exists?(fact)
  
  # Internal ETS operations
  defp create_fact_table()
  defp generate_fact_id(fact)
  defp notify_alpha_network(fact, :assert | :retract)
end
```

#### Memory Manager
```elixir
defmodule Presto.Memory.MemoryManager do
  use GenServer
  
  @moduledoc """
  Manages lifecycle of all ETS tables and memory cleanup
  """
  
  def start_link(opts)
  def create_alpha_memory(node_id, opts)
  def create_beta_memory(node_id, opts)
  def cleanup_orphaned_tokens()
  def get_memory_statistics()
  
  # Periodic cleanup
  def handle_info(:cleanup_cycle, state)
end
```

### 5. Rule Execution

#### Execution Coordinator
```elixir
defmodule Presto.Execution.Executor do
  @moduledoc """
  Coordinates rule execution and conflict resolution
  """
  
  def execute_rules(activated_rules, mode \\ :parallel)
  def resolve_conflicts(rule_activations)
  def fire_rule(rule, variable_bindings)
  
  defp apply_conflict_resolution_strategy(activations, strategy)
  defp execute_rule_action(action, bindings)
end
```

#### Rule Firer
```elixir
defmodule Presto.Execution.RuleFirer do
  @moduledoc """
  Handles individual rule execution
  """
  
  def fire_rule_async(rule, bindings, engine_pid)
  def fire_rule_sync(rule, bindings)
  
  defp execute_action({module, function, args}, bindings)
  defp execute_action(function, bindings) when is_function(function)
  defp execute_action(atom_action, bindings) when is_atom(atom_action)
end
```

### 6. Pattern Processing

#### Pattern Compiler
```elixir
defmodule Presto.Pattern.Compiler do
  @moduledoc """
  Compiles rule patterns into efficient matching functions
  """
  
  def compile_rule(rule) 
  def compile_alpha_pattern(pattern, guards)
  def compile_beta_join(left_pattern, right_pattern, join_vars)
  
  defp generate_matcher_function(pattern)
  defp optimize_guard_evaluation(guards)
end
```

#### Pattern Matcher
```elixir
defmodule Presto.Pattern.Matcher do
  @moduledoc """
  Runtime pattern matching utilities
  """
  
  def match_pattern(fact, pattern)
  def evaluate_guards(fact, guards, bindings)
  def extract_variables(fact, pattern)
  
  defp apply_guard_condition(condition, bindings)
end
```

## Data Flow Architecture

### 1. Fact Assertion Flow

```
Fact → Working Memory → Alpha Network → Alpha Memories → Beta Network → Rule Activations
```

**Detailed Steps:**
1. **API Call**: `Presto.assert_fact(engine, fact)`
2. **Engine Process**: Receives fact, validates, forwards to working memory
3. **Working Memory**: Stores fact in ETS, generates fact ID, notifies alpha network
4. **Alpha Network**: Evaluates fact against all alpha nodes
5. **Alpha Memories**: Store matching facts, trigger beta network propagation
6. **Beta Network**: Perform joins, create/update partial matches
7. **Rule Activation**: Complete matches create rule activations
8. **Execution**: Fire activated rules based on conflict resolution

### 2. Rule Addition Flow

```
Rule Definition → Compilation → Network Update → Memory Creation → Connection Establishment
```

**Detailed Steps:**
1. **API Call**: `Presto.add_rule(engine, rule)`
2. **Rule Compilation**: Transform rule into network nodes
3. **Network Update**: Add alpha/beta nodes to network structure
4. **Memory Creation**: Create ETS tables for new nodes
5. **Connection Setup**: Establish data flow between nodes
6. **Backpropagation**: Process existing facts through new rule

### 3. Execution Cycle

```
Rule Activations → Conflict Resolution → Rule Selection → Action Execution → Fact Updates
```

**For Automatic Mode:**
- Triggered by fact assertions/retractions
- Continuous execution until no more rules activate

**For Manual Mode:**
- Triggered by explicit `run_cycle/1` calls
- Single cycle execution with result reporting

## Memory Architecture

### ETS Table Organization

```elixir
# Working Memory
:presto_working_memory          # {fact_id, fact}

# Alpha Memories (one per alpha node)
:presto_alpha_memory_1          # {fact_id, fact}
:presto_alpha_memory_2          # {fact_id, fact}

# Beta Memories (one per beta node)  
:presto_beta_memory_1           # {token_id, token}
:presto_beta_memory_2           # {token_id, token}

# Network Configuration
:presto_network_config          # {key, value}

# Statistics and Monitoring
:presto_statistics              # {metric_name, value}
```

### Memory Access Patterns

**Read-Heavy Operations:**
- Pattern matching (alpha network)
- Variable binding checks (beta network)
- Fact queries

**Write Operations:**
- Fact assertions/retractions  
- Partial match creation/deletion
- Rule activation tracking

**Optimization Strategy:**
- ETS read concurrency for high-frequency reads
- Coordinated writes through GenServer for consistency
- Batch operations for bulk updates

## Concurrency Model

### Process Responsibilities

**Main Engine Process (GenServer):**
- API coordination
- Network state management
- Fact lifecycle coordination
- Rule management

**Memory Manager Process:**
- ETS table lifecycle
- Memory cleanup and optimization
- Statistics collection

**Rule Execution Processes (Task.Supervisor):**
- Parallel rule firing
- Action execution isolation
- Error handling and recovery

**Optional Network Processes:**
- Individual network nodes as processes (for very large networks)
- Distributed network components

### Synchronization Points

**Critical Sections:**
- Network structure updates (rule addition/removal)
- Coordinated fact assertion/retraction
- Conflict resolution and rule selection

**Concurrent Operations:**
- Pattern matching in alpha network
- Parallel rule execution
- Statistics collection and monitoring

## Error Handling Strategy

### Error Isolation

**Network Errors:**
- Invalid rule compilation errors
- Pattern matching failures
- Network inconsistency detection

**Execution Errors:**
- Rule action failures
- Timeout handling
- Resource exhaustion

**Memory Errors:**
- ETS table corruption detection
- Memory cleanup failures
- Orphaned token detection

### Recovery Mechanisms

**Network Recovery:**
- Rebuild network from rule definitions
- Validate network consistency
- Re-propagate facts through rebuilt network

**Execution Recovery:**
- Isolate failed rules
- Continue processing other rules
- Log and report failures

**Memory Recovery:**
- Detect and repair ETS inconsistencies
- Rebuild memories from authoritative sources
- Garbage collect orphaned data

This architecture provides a robust, scalable foundation for implementing RETE in Elixir while leveraging the platform's strengths in concurrency, fault tolerance, and process isolation.
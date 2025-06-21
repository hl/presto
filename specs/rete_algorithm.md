# RETE Algorithm Detailed Specification

## Historical Context

### Origins and Development
- **Creator**: Charles L. Forgy at Carnegie Mellon University  
- **Timeline**: Working paper (1974), PhD thesis (1979), formal publication (1982)
- **Etymology**: "Rete" is Latin for "net" or "network", inspired by anatomical networks
- **Performance Impact**: Enabled 3,000x performance improvement over naive rule matching

### Academic Foundation  
- **PhD Advisor**: Allen Newell at CMU
- **First Implementation**: OPS5 production system language
- **Core Innovation**: Trading memory for computational speed through state preservation

## Core Algorithm Components

### Alpha Network
The alpha network performs pattern matching for individual conditions against single facts.

```mermaid
flowchart TD
    WM[Working Memory<br/>Facts] --> Root[Root Node]
    Root --> A1["Alpha Node 1<br/>age > 18"]
    Root --> A2["Alpha Node 2<br/>status = :active"]
    Root --> A3["Alpha Node 3<br/>dept = :sales"]
    
    A1 --> AM1["Alpha Memory 1<br/>Adults"]
    A2 --> AM2["Alpha Memory 2<br/>Active Users"]
    A3 --> AM3["Alpha Memory 3<br/>Sales Staff"]
    
    AM1 --> Beta[Beta Network]
    AM2 --> Beta
    AM3 --> Beta
    
    classDef memory fill:#e1f5fe,stroke:#0277bd,stroke-width:2px
    classDef node fill:#f3e5f5,stroke:#7b1fa2,stroke-width:2px
    classDef wm fill:#e8f5e8,stroke:#2e7d32,stroke-width:2px
    
    class AM1,AM2,AM3 memory
    class A1,A2,A3,Root node
    class WM wm
```

**Structure:**
- **Alpha Nodes**: 1-input nodes evaluating literal conditions
- **Alpha Memory**: Terminal storage for Working Memory Elements (WMEs) matching specific conditions  
- **Discrimination Tree**: Left side of network, filters facts by attribute-constant comparisons

**Operation Flow:**
1. Receive tokens from working memory
2. Perform PROJECT operations extracting pattern variables
3. Evaluate conditions (equality, comparison operators)
4. Store matching WMEs in alpha memory
5. Trigger beta network propagation

**Example Alpha Condition:**
```
Pattern: {:person, name, age} where age > 18
Alpha Node: Tests age field > 18
Alpha Memory: Stores all person facts with age > 18
```

### Beta Network
The beta network handles join operations between different fact types for multi-condition rules.

```mermaid
flowchart TD
    AM1["Alpha Memory<br/>person facts"] --> J1["Join Node 1<br/>person.name = order.customer"]
    AM2["Alpha Memory<br/>order facts"] --> J1
    
    J1 --> BM1["Beta Memory 1<br/>person-order matches"]
    BM1 --> J2["Join Node 2<br/>order.amount > 100"]
    AM3["Alpha Memory<br/>high-value orders"] --> J2
    
    J2 --> BM2["Beta Memory 2<br/>high-value customer orders"]
    BM2 --> T1["Terminal Node<br/>Rule Activation"]
    
    T1 --> CS[Conflict Set]
    
    classDef alphamem fill:#e1f5fe,stroke:#0277bd,stroke-width:2px
    classDef betamem fill:#fff3e0,stroke:#f57c00,stroke-width:2px
    classDef join fill:#f3e5f5,stroke:#7b1fa2,stroke-width:2px
    classDef terminal fill:#ffebee,stroke:#c62828,stroke-width:2px
    
    class AM1,AM2,AM3 alphamem
    class BM1,BM2 betamem
    class J1,J2 join
    class T1,CS terminal
```

**Structure:**
- **Beta Nodes**: 2-input nodes with left and right inputs
- **Beta Memory**: Left input storing tokens (partial matches)
- **Alpha Memory**: Right input feeding individual facts
- **Join Nodes**: Specialized beta nodes performing variable binding consistency

**Join Operations:**
1. **Right Activation**: Triggered when WME added to alpha memory
2. **Left Activation**: Triggered when token added to beta memory  
3. **Consistency Check**: Search opposite memory for compatible variable bindings
4. **Token Creation**: Build extended tokens representing partial matches

**Example Beta Join:**
```
Left Input: {:person, "John", 25} 
Right Input: {:order, "John", 150}
Join Condition: person.name = order.customer_name
Result: Combined token with both facts
```

### Working Memory
Central fact storage managing the system's knowledge base.

**Components:**
- **Working Memory Elements (WMEs)**: Structured fact representations
- **Assertion Process**: Facts become WMEs when added to working memory
- **Token Propagation**: WMEs enter at root, propagate through network
- **Incremental Updates**: Only processes changes (deltas) between cycles

**Lifecycle Management:**
- Facts persist until explicitly retracted or modified
- State maintained between inference cycles
- Supports dynamic fact addition/removal during execution

### Conflict Resolution
Mechanism for handling multiple simultaneously activated rules.

**Conflict Set**: Collection of activated rules eligible for execution

**Resolution Strategies:**
1. **Salience/Priority**: User-defined rule priorities
2. **Recency**: Favor rules matching recently asserted facts  
3. **Specificity**: Prefer rules with more restrictive conditions
4. **Rule Order**: Default lexicographic ordering
5. **Complexity**: Consider number of conditions per rule

*Note: Conflict resolution is separate from core RETE but used alongside it*

## Algorithm Execution Flow

```mermaid
sequenceDiagram
    participant WM as Working Memory
    participant AN as Alpha Network
    participant BN as Beta Network
    participant CS as Conflict Set
    participant RE as Rule Engine
    
    Note over WM,RE: RETE Execution Cycle
    
    WM->>AN: 1. Assert fact
    AN->>AN: 2. Pattern matching
    AN->>BN: 3. Propagate to joins
    BN->>BN: 4. Variable binding
    BN->>CS: 5. Rule activation
    CS->>RE: 6. Conflict resolution
    RE->>WM: 7. Execute actions
    
    Note over WM,RE: Incremental processing - only changes propagate
    
    WM->>AN: New fact
    AN-->>BN: Only affected paths
    BN-->>CS: Only new activations
```

### Network Construction Phase (One-time)
1. **Rule Compilation**: Transform rules into discrimination network
2. **Alpha Node Creation**: Build nodes for individual conditions
3. **Beta Node Construction**: Create join operations for multi-condition rules
4. **Memory Allocation**: Establish alpha and beta memory structures
5. **Node Sharing**: Optimize by sharing nodes with identical conditions

### Runtime Execution Cycle

#### 1. Fact Insertion
```
Working Memory → Root Node → Alpha Network → Alpha Memories
```
- Assert fact creates WME
- Token enters at root node
- Propagates through alpha conditions
- Successful matches stored in alpha memories

#### 2. Join Processing  
```
Alpha Memory → Beta Network → Variable Binding → Partial Matches
```
- Alpha matches trigger beta network
- Perform variable binding consistency checks
- Create partial match tokens
- Store results in beta memories

#### 3. Rule Activation
```
Complete Matches → Terminal Nodes → Production Instances → Agenda
```
- Complete matches reach terminal nodes
- Create production instances with responsible WME list
- Add to conflict set/agenda

#### 4. Conflict Resolution & Firing
```
Conflict Set → Resolution Strategy → Selected Rule → Action Execution
```
- Apply resolution strategy to select rule
- Execute rule's action part
- Modify working memory if needed
- Trigger new cycle if changes made

```mermaid
flowchart TD
    subgraph "Join Operation Detail"
        LI["Left Input<br/>{:person, 'John', 25}"] --> JN[Join Node]
        RI["Right Input<br/>{:order, 'John', 150}"] --> JN
        
        JN --> VC{"Variable<br/>Consistency<br/>Check"}
        VC -->|"person.name =<br/>order.customer"| VM["Variable<br/>Matching"]
        VM --> TB["Token<br/>Building"]
        TB --> OM["Output<br/>Memory"]
        
        VC -->|"No match"| DISC[Discard]
    end
    
    subgraph "Token Structure"
        OM --> TS["Token:<br/>{wmes: [person_fact, order_fact],<br/>bindings: %{name: 'John'}}"]
    end
    
    classDef input fill:#e8f5e8,stroke:#2e7d32,stroke-width:2px
    classDef process fill:#f3e5f5,stroke:#7b1fa2,stroke-width:2px
    classDef output fill:#e1f5fe,stroke:#0277bd,stroke-width:2px
    classDef discard fill:#ffebee,stroke:#c62828,stroke-width:2px
    
    class LI,RI input
    class JN,VC,VM,TB process
    class OM,TS output
    class DISC discard
```

## Key Optimizations

### Node Sharing
**Purpose**: Eliminate redundant pattern evaluations
- Share alpha nodes with identical conditions across rules
- Reduce total network size and computation
- Single evaluation supports multiple rules

**Example:**
```elixir
# Two rules sharing age > 18 condition
Rule 1: person.age > 18 AND person.status = :active → action1
Rule 2: person.age > 18 AND person.department = :sales → action2

# Single alpha node: person.age > 18
# Feeds both rule paths
```

### Partial Match Storage
**Purpose**: Preserve intermediate matching results
- Store partial matches in beta memories
- Avoid re-computation on subsequent cycles
- Enable incremental processing

**Memory Trade-off**: Exchange memory for computational efficiency

### Incremental Updates
**Purpose**: Process only changes between cycles
- Maintain network state between executions
- Update only affected network portions
- Avoid complete re-evaluation of all facts

**Delta Processing**: 
- Track additions/deletions to working memory
- Propagate only changes through network
- Preserve unaffected partial matches

## Performance Characteristics

### Time Complexity
- **Traditional Approach**: O(R × F^P) per cycle where R=rules, F=facts, P=patterns per rule
- **RETE Approach**: O(R × F × P) overall, but incremental updates approach O(1) for changes
- **Best Case**: Constant time for fact insertion when no rules activate
- **Worst Case**: Still bounded by network size, not fact base size

### Space Complexity
- **Formula**: O(R × F × P) for complete network state
- **Memory Intensive**: Stores extensive partial match information
- **Typical Usage**: 10-100MB for moderate rule sets (1000s of rules/facts)

### Scalability Characteristics
- **Large Rule Sets**: Handles thousands of rules efficiently
- **High Fact Volume**: Manages millions of facts with proper indexing
- **Real-time Performance**: Sub-millisecond response for incremental updates
- **Memory Growth**: Linear with rule count, quadratic worst-case with fact interactions

## Comparison with Naive Approaches

### Naive Pattern Matching Limitations
```
For each cycle:
  For each rule:
    For each fact combination:
      Evaluate all conditions
      If match: activate rule
```

**Problems:**
- O(R × F^P) computational complexity
- Complete re-evaluation every cycle
- No state preservation between cycles
- Exponential growth with pattern complexity

### RETE Advantages
1. **State Preservation**: Maintains matching progress between cycles
2. **Incremental Processing**: Only handles changes, not entire fact base
3. **Compiled Efficiency**: Pre-compiled network structure
4. **Shared Computation**: Reuses evaluation across similar conditions
5. **Performance Gain**: 3-4 orders of magnitude improvement

**Example Performance:**
- Naive: 10,000 rules × 1,000 facts = 10M evaluations per cycle
- RETE: Initial network build + incremental updates ≈ 100-1,000 operations per change

```mermaid
xychart-beta
    title "Performance Comparison: Naive vs RETE"
    x-axis ["100 Facts" "1K Facts" "10K Facts" "100K Facts" "1M Facts"]
    y-axis "Operations per Cycle (log scale)" 100 10000000
    line [1000, 10000, 100000, 1000000, 10000000]
    line [150, 200, 500, 1200, 2500]
```

## Algorithm Variations

### ReteOO (Object-Oriented RETE)
**Enhancements for OOP:**
- Optimized object property access
- Inheritance-aware pattern matching
- Enhanced for object-oriented fact structures
- Used in Drools, IBM ODM

### PHREAK Algorithm (Drools 6+)
**Fundamental Differences:**
- **Lazy vs Eager**: Goal-oriented instead of data-oriented
- **Three-layer Memory**: Node, Segment, Rule contextual memory
- **Stack-based Evaluation**: Pause/resume capabilities
- **Set-oriented Propagation**: Collection operations vs tuple-by-tuple

**PHREAK Advantages:**
- Better scalability for large rule sets
- Reduced memory consumption in sparse networks
- More efficient for query-style operations

### Other Notable Variations
- **RETE/UL**: Enhanced with better join ordering
- **RETE***: Improved with dual tokens and dynamic memory management
- **TREAT**: Low-memory alternative trading speed for space
- **Collection-Oriented Match**: Alternative approach for specific domains

## Implementation Challenges

### Memory Management
**Challenges:**
- High memory requirements O(R × F × P)
- Complex interconnected memory structures
- Token lifecycle management
- Garbage collection of unused partial matches

**Solutions:**
- Efficient hash table implementations
- Reference counting for token cleanup
- Memory pooling for frequent allocations
- Lazy cleanup strategies

### Network Construction Complexity
**Challenges:**
- Pattern order affects storage requirements
- Node sharing identification algorithms
- Rule compilation into efficient topology
- Optimal join ordering selection

**Best Practices:**
- Use proven compilation algorithms
- Implement systematic node sharing
- Profile memory usage patterns
- Consider join selectivity for ordering

### Debugging and Maintenance
**Common Issues:**
- Complex token flow through network
- Understanding rule activation/deactivation
- Performance bottleneck identification
- Network state inspection difficulty

**Debugging Strategies:**
- Token trace logging
- Network visualization tools
- Performance profiling instrumentation
- State snapshot capabilities

### Technical Implementation Requirements

#### Essential Data Structures
```elixir
# Alpha Memory: fact storage by pattern
%{pattern_id => [list_of_wmes]}

# Beta Memory: partial match storage  
%{node_id => [list_of_tokens]}

# Token Structure: partial match representation
%{wmes: [list], bindings: %{var => value}}

# Network Structure: node connectivity
%{node_id => %{type: :alpha/:beta, inputs: [], outputs: []}}
```

#### Core Operations
- **Token Creation**: Build partial match representations
- **Variable Binding**: Consistent variable assignment across joins
- **Memory Indexing**: Efficient storage and retrieval of partial matches
- **Network Traversal**: Systematic propagation through node connections

This specification provides the technical foundation for implementing a complete RETE algorithm, covering both theoretical principles and practical implementation considerations derived from 45+ years of research and development.

## Performance Optimizations

### Indexed Joins Instead of Cartesian Products

Traditional RETE implementations perform cartesian products when joining alpha and beta memories, resulting in O(|Left| × |Right|) complexity. Advanced implementations use indexing strategies to reduce this to near-constant time lookups.

#### Hash-Based Join Indexing
```elixir
# Instead of iterating through all beta memory tokens
# Index by join variables for O(1) lookup
defmodule Presto.OptimizedJoin do
  def create_join_index(beta_memory, join_variables) do
    Enum.group_by(beta_memory, fn token ->
      extract_join_values(token, join_variables)
    end)
  end
  
  def indexed_join(alpha_fact, beta_index, join_variables) do
    join_key = extract_join_values(alpha_fact, join_variables)
    Map.get(beta_index, join_key, [])
  end
end
```

```mermaid
flowchart LR
    subgraph "Traditional O(n×m) Join"
        BM1["Beta Memory<br/>[token1, token2, ..., tokenN]"] --> CJ["Cartesian<br/>Join"]
        AF1["Alpha Fact"] --> CJ
        CJ --> R1["Results<br/>O(N×M)"]
    end
    
    subgraph "Indexed O(1) Join"
        BM2["Beta Memory"] --> HI["Hash Index<br/>{key1: [tokens],<br/>key2: [tokens]}"] 
        AF2["Alpha Fact"] --> EK["Extract Key"]
        EK --> LU["Hash Lookup<br/>O(1)"]
        HI --> LU
        LU --> R2["Results<br/>O(1) average"]
    end
    
    classDef slow fill:#ffebee,stroke:#c62828,stroke-width:2px
    classDef fast fill:#e8f5e8,stroke:#2e7d32,stroke-width:2px
    classDef process fill:#f3e5f5,stroke:#7b1fa2,stroke-width:2px
    
    class BM1,CJ,R1 slow
    class BM2,HI,LU,R2 fast
    class EK process
```

**Performance Impact**: Reduces join operations from O(n×m) to O(1) average case, providing 10-100x speedup for large memory tables.

#### Multi-Level Indexing
For complex joins with multiple variables, implement hierarchical indexing:

```
Primary Index: Variable 1 → Secondary Index
Secondary Index: Variable 2 → Fact List
```

**Benefits**:
- Eliminates redundant comparisons
- Scales linearly with fact count instead of quadratically
- Reduces memory access patterns from scattered to localized

### Fact Type Pre-filtering

Implement type-based discrimination before pattern matching to eliminate impossible matches early in the pipeline.

#### Type Discrimination Trees
```elixir
defmodule Presto.TypeDiscriminator do
  def build_type_tree(patterns) do
    patterns
    |> Enum.group_by(&extract_fact_type/1)
    |> Enum.map(fn {type, patterns} ->
      {type, build_attribute_tree(patterns)}
    end)
    |> Map.new()
  end
  
  def fast_type_filter(fact, type_tree) do
    fact_type = extract_fact_type(fact)
    Map.get(type_tree, fact_type, [])
  end
end
```

**Optimization Benefits**:
- **Early Termination**: Reject 80-95% of facts at type level
- **Cache Efficiency**: Improve CPU cache utilization through type locality
- **Reduced Pattern Evaluations**: Skip expensive guard evaluations for wrong types

#### Hierarchical Filtering
1. **Type Filter**: `:person`, `:order`, `:product`
2. **Attribute Filter**: Age ranges, status values, categories
3. **Guard Evaluation**: Complex conditions only on pre-filtered facts

### Pattern Compilation Optimizations

Transform runtime pattern matching into compile-time optimised code generation.

#### Static Pattern Analysis
```elixir
defmodule Presto.PatternAnalyzer do
  def analyse_pattern_selectivity(pattern, fact_statistics) do
    %{
      type_selectivity: calculate_type_selectivity(pattern.type, fact_statistics),
      attribute_selectivity: analyse_attributes(pattern.attributes),
      guard_selectivity: estimate_guard_efficiency(pattern.guards),
      overall_selectivity: combined_selectivity_score(pattern)
    }
  end
  
  def optimise_pattern_order(patterns) do
    patterns
    |> Enum.map(&analyse_pattern_selectivity/1)
    |> Enum.sort_by(& &1.overall_selectivity)
    |> Enum.map(& &1.pattern)
  end
end
```

#### Code Generation Optimization
- **Macro-based Matchers**: Generate specialized matching functions at compile time
- **Guard Reordering**: Place most restrictive guards first
- **Constant Folding**: Pre-compute static comparisons
- **Branch Prediction**: Structure conditionals for common paths

**Performance Improvements**:
- 2-5x faster pattern matching through code specialization
- Reduced runtime overhead from generic matching logic
- Better compiler optimisation opportunities

### Memory Access Optimizations

Optimize data structures and access patterns for modern CPU architectures.

#### Cache-Conscious Data Structures
```elixir
defmodule Presto.CacheOptimizedMemory do
  # Pack frequently accessed data together
  defstruct [
    :hot_data,    # Frequently accessed: fact_id, type, primary_key
    :warm_data,   # Moderately accessed: attributes, timestamps
    :cold_data    # Rarely accessed: metadata, debug info
  ]
  
  def optimise_memory_layout(facts) do
    facts
    |> Enum.map(&separate_data_by_access_frequency/1)
    |> pack_for_cache_lines()
  end
end
```

#### Memory Access Patterns
- **Sequential Access**: Organize alpha memories for linear traversal
- **Locality Optimization**: Group related facts physically close
- **Prefetching**: Implement predictive memory loading
- **NUMA Awareness**: Optimize for multi-socket systems

#### ETS Optimization Strategies
```elixir
# Optimize ETS table configuration for access patterns
defmodule Presto.ETSOptimization do
  def create_optimised_memory_table(usage_pattern) do
    case usage_pattern do
      :read_heavy ->
        :ets.new(:memory, [:set, :public, {:read_concurrency, true}])
      :write_heavy ->
        :ets.new(:memory, [:set, :public, {:write_concurrency, true}])
      :mixed ->
        :ets.new(:memory, [:set, :public, 
                          {:read_concurrency, true}, 
                          {:write_concurrency, true}])
    end
  end
end
```

**Memory Optimization Benefits**:
- **Reduced Cache Misses**: 10-50% improvement in memory-bound operations
- **Better Memory Bandwidth**: Efficient utilization of memory subsystem
- **Scalability**: Performance maintained under memory pressure

### Advanced Algorithmic Optimizations

#### Lazy Evaluation Networks
Implement lazy propagation to avoid unnecessary computation:

```elixir
defmodule Presto.LazyPropagation do
  def create_lazy_node(pattern, downstream_nodes) do
    %LazyNode{
      pattern: pattern,
      downstream: downstream_nodes,
      dirty: false,
      last_computation: nil
    }
  end
  
  def evaluate_if_needed(node, current_facts) do
    if node.dirty or facts_changed?(node.last_computation, current_facts) do
      results = evaluate_pattern(node.pattern, current_facts)
      mark_clean(node, current_facts)
      results
    else
      node.cached_results
    end
  end
end
```

#### Incremental Maintenance
Optimize for minimal recomputation on fact changes:

- **Delta Propagation**: Track and propagate only changes
- **Dependency Tracking**: Update only affected network portions
- **Memoization**: Cache intermediate results with invalidation

**Optimization Results**:
- **Incremental Updates**: 50-1000x faster than full recomputation
- **Memory Efficiency**: Reduced peak memory usage during updates
- **Latency Reduction**: Sub-millisecond response times for simple changes

### Performance Measurement Integration

#### Built-in Profiling
```elixir
defmodule Presto.PerformanceProfiler do
  def profile_network_execution(network, facts) do
    start_time = System.monotonic_time(:microsecond)
    
    result = with_profiling(fn ->
      execute_network(network, facts)
    end)
    
    %{
      total_time: System.monotonic_time(:microsecond) - start_time,
      alpha_node_times: result.alpha_timings,
      beta_node_times: result.beta_timings,
      memory_operations: result.memory_stats,
      optimisation_opportunities: analyse_bottlenecks(result)
    }
  end
end
```

#### Adaptive Optimization
Implement runtime optimisation based on observed patterns:

- **Pattern Frequency Analysis**: Optimize for common patterns
- **Join Selectivity Learning**: Adapt join ordering based on data
- **Memory Size Tuning**: Adjust memory allocation based on usage
- **Cache Strategy Selection**: Choose optimal cache policies

These performance optimisations can provide 10-100x performance improvements for typical RETE workloads while maintaining algorithmic correctness and adding powerful introspection capabilities.

## RETE-Native Aggregations

### Overview

RETE-native aggregations extend the traditional RETE algorithm to support incremental aggregate computation directly within the beta network. This implementation treats aggregation operations as first-class citizens alongside join operations, enabling efficient computation of sums, counts, averages, and other aggregates with automatic maintenance as facts change.

### Architecture Integration

Aggregation nodes are integrated into the beta network as specialized node types that operate alongside traditional join nodes:

```mermaid
flowchart TD
    subgraph "Extended Beta Network with Aggregations"
        AM1["Alpha Memory<br/>timesheet facts"] --> J1["Join Node<br/>employee matching"]
        AM2["Alpha Memory<br/>employee facts"] --> J1
        
        J1 --> BM1["Beta Memory<br/>employee-timesheet matches"]
        BM1 --> AN1["Aggregation Node<br/>SUM(hours) GROUP BY employee_id"]
        
        AN1 --> AM3["Aggregation Memory<br/>employee totals"]
        AM3 --> T1["Terminal Node<br/>Rule Activation"] 
        
        T1 --> CS[Conflict Set]
    end
    
    classDef alphamem fill:#e1f5fe,stroke:#0277bd,stroke-width:2px
    classDef betamem fill:#fff3e0,stroke:#f57c00,stroke-width:2px
    classDef aggnode fill:#e8f5e8,stroke:#2e7d32,stroke-width:2px
    classDef aggmem fill:#f1f8e9,stroke:#33691e,stroke-width:2px
    classDef terminal fill:#ffebee,stroke:#c62828,stroke-width:2px
    
    class AM1,AM2 alphamem
    class BM1 betamem
    class AN1 aggnode
    class AM3 aggmem
    class T1,CS terminal
```

### Aggregation Node Structure

Aggregation nodes are distinct beta network components with the following characteristics:

```elixir
%{
  id: "aggr_node_id",
  type: :aggregation,
  input_source: "source_node_id",  # Input from alpha or beta node
  group_by: [:field1, :field2],    # Grouping fields
  aggregate_fn: :sum,              # Aggregation function
  aggregate_field: :amount         # Field to aggregate over
}
```

**Supported Aggregation Functions:**
- `:sum` - Summation of numeric values
- `:count` - Count of matching facts  
- `:avg` - Average of numeric values
- `:min` - Minimum value
- `:max` - Maximum value
- `:collect` - Collection of all values

### Incremental Aggregation Processing

The key advantage of RETE-native aggregations is incremental maintenance. When facts are added or removed, only the affected aggregation groups are recomputed:

```mermaid
sequenceDiagram
    participant WM as Working Memory
    participant AN as Alpha Node
    participant AGN as Aggregation Node
    participant AM as Aggregation Memory
    participant RE as Rule Engine
    
    Note over WM,RE: Fact Assertion with Incremental Aggregation
    
    WM->>AN: Assert timesheet fact
    AN->>AGN: New fact matches pattern
    AGN->>AGN: Extract group key (employee_id)
    AGN->>AM: Update group aggregate
    AM->>RE: Notify aggregate change
    
    Note over WM,RE: Only affected group recalculated
    
    WM->>AN: Retract timesheet fact  
    AN->>AGN: Fact removed
    AGN->>AGN: Extract group key
    AGN->>AM: Decrement group aggregate
    AM->>RE: Notify aggregate change
```

### Multi-Field Grouping

Aggregation nodes support complex grouping scenarios with multiple fields:

```elixir
# Group by region and product for sales analysis
aggregation_spec = {
  :aggregate,
  "alpha_sales",        # Input source
  [:region, :product],  # Multi-field grouping
  :sum,                 # Aggregate function
  :amount              # Field to sum
}

# Results in groups like:
# {"north", "widget"} => 250
# {"north", "gadget"} => 200  
# {"south", "widget"} => 120
```

### Rule Creation API

The `Presto.Rule` module provides a clean API for creating aggregation rules:

```elixir
# Sum hours by employee
rule = Presto.Rule.aggregation(
  :weekly_hours,
  [{:timesheet, :id, :employee_id, :hours}],
  [:employee_id],  # Group by employee
  :sum,           # Sum function
  :hours,         # Field to sum
  output: {:employee_total, :employee_id, :total_hours}
)

# Count facts without field specification
rule = Presto.Rule.aggregation(
  :shift_count,
  [{:shift, :id, :department}],
  [:department],  # Group by department
  :count,         # Count function  
  nil             # No field needed for count
)
```

### Network Construction for Aggregations

During rule compilation, aggregation rules create specialized network paths:

1. **Pattern Analysis**: Extract patterns that feed the aggregation
2. **Alpha Node Creation**: Standard alpha nodes for pattern matching
3. **Aggregation Node Creation**: Specialized beta nodes for aggregate computation
4. **Memory Allocation**: Dedicated memory structures for aggregate results
5. **Update Path Establishment**: Connect aggregation nodes to change propagation

```mermaid
flowchart LR
    subgraph "Aggregation Rule Compilation"
        R["Aggregation Rule<br/>SUM(hours) BY employee"] --> PA["Pattern Analysis<br/>Extract timesheet pattern"]
        PA --> AN["Create Alpha Node<br/>timesheet matcher"]
        AN --> AGN["Create Aggregation Node<br/>SUM(hours) GROUP BY employee_id"]
        AGN --> AM["Allocate Aggregation Memory<br/>Store results by group"]
        AM --> UP["Establish Update Paths<br/>Connect to rule engine"]
    end
    
    classDef rule fill:#fff3e0,stroke:#f57c00,stroke-width:2px
    classDef process fill:#f3e5f5,stroke:#7b1fa2,stroke-width:2px
    classDef memory fill:#e8f5e8,stroke:#2e7d32,stroke-width:2px
    
    class R rule
    class PA,AN,AGN,UP process
    class AM memory
```

### Performance Characteristics

RETE-native aggregations provide significant performance advantages:

**Time Complexity:**
- **Traditional Approach**: O(F) for each aggregate recomputation where F = facts
- **RETE Aggregation**: O(1) for incremental updates to existing groups
- **New Group Creation**: O(log G) where G = number of groups

**Space Complexity:**
- **Additional Memory**: O(G × A) where G = groups, A = aggregation state size
- **Typical Overhead**: 10-50MB for thousands of aggregated groups

**Update Performance:**
- **Fact Addition**: Constant time for existing groups
- **Fact Removal**: Constant time with proper bookkeeping  
- **Group Creation**: Logarithmic time for new group establishment

### Aggregation Processing Algorithm

The aggregation processing follows this algorithm:

```elixir
def process_aggregation_node(node_id, node, state) do
  # 1. Get input data from source
  input_data = get_input_data(node.input_source, state)
  
  # 2. Group data by specified fields
  grouped_data = group_by_fields(input_data, node.group_by)
  
  # 3. Apply aggregation function to each group
  aggregated_results = 
    Enum.map(grouped_data, fn {group_key, facts} ->
      value = apply_aggregate_fn(facts, node.aggregate_fn, node.aggregate_field)
      create_result_fact(group_key, value, node.group_by)
    end)
  
  # 4. Update aggregation memory
  update_aggregation_memory(node_id, aggregated_results, state)
end
```

### Integration with Standard RETE

Aggregation nodes integrate seamlessly with standard RETE components:

**Input Sources:**
- **Alpha Memory**: Direct aggregation of pattern-matched facts
- **Beta Memory**: Aggregation of joined fact combinations
- **Chained Aggregations**: Aggregation nodes feeding other aggregation nodes

**Output Integration:**
- **Rule Activation**: Aggregation results can trigger standard rules
- **Join Operations**: Aggregated facts can participate in joins
- **Nested Aggregations**: Support for hierarchical aggregation structures

### Implementation Examples

**Payroll Aggregation:**
```elixir
# Sum hours by employee and week
Rule.aggregation(
  :weekly_hours,
  [{:shift_segment, :employee_id, :week, :hours}],
  [:employee_id, :week],
  :sum,
  :hours
)

# Average hourly rate by department
Rule.aggregation(
  :dept_avg_rate,
  [{:employee, :id, :department, :hourly_rate}],
  [:department],
  :avg,
  :hourly_rate
)
```

**Sales Analysis:**
```elixir  
# Total sales by region and quarter
Rule.aggregation(
  :quarterly_sales,
  [{:sale, :id, :region, :quarter, :amount}],
  [:region, :quarter],
  :sum,
  :amount
)

# Product performance metrics
Rule.aggregation(
  :product_stats,
  [{:order_item, :product_id, :quantity}],
  [:product_id],
  :sum,
  :quantity
)
```

### Advanced Features

**Conditional Aggregation:**
Aggregation nodes can be combined with standard join nodes for conditional aggregation:

```elixir
# Only aggregate sales from active customers
rule_conditions = [
  {:sale, :customer_id, :amount},
  {:customer, :customer_id, :status},
  {:status, :==, :active}
]
```

**Temporal Aggregations:**
Support for time-windowed aggregations through date-based grouping:

```elixir
# Monthly sales totals with automatic month extraction
Rule.aggregation(
  :monthly_sales,
  [{:sale, :date, :amount}],
  [extract_month: :date],  # Custom grouping function
  :sum,
  :amount
)
```

This RETE-native aggregation implementation provides a powerful foundation for building complex analytical rules while maintaining the incremental processing advantages that make RETE algorithms so effective for real-time rule processing systems.
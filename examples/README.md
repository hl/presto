# Presto Examples

This directory contains simplified, focused examples demonstrating Presto's RETE rules engine capabilities. All examples follow **Best Simple System for Now (BSSN)** principles, providing simple solutions for current demonstration needs without over-engineering.

## Quick Start

All examples can be run directly with mix:

```bash
# Basic introduction to Presto
mix run examples/basic_example.exs

# Learn about aggregations
mix run examples/aggregation_example.exs

# See batch processing in action
mix run examples/batch_processing_example.exs

# Explore domain-specific rules (payroll)
mix run -e "Examples.PayrollExample.run_demo()"
```

## Example Overview

### 1. Basic Example (`basic_example.exs`) - **Start Here!**

A gentle introduction to Presto's v0.2 API in ~50 lines. Perfect for beginners.

**Demonstrates:**
- Starting a rules engine
- Creating rules with `Presto.Rule.new/4`
- Using pattern matching and tests
- Batch operations (`add_rules/2`, `assert_facts/2`)
- Firing rules and viewing results

**Key Concepts:**
- Explicit Elixir API (no DSLs)
- RETE pattern matching
- Rule chaining

### 2. Aggregation Example (`aggregation_example.exs`)

Shows how to implement aggregation-like behavior and previews RETE-native aggregations coming in future updates.

**Demonstrates:**
- Grouping and counting patterns
- Simulating aggregation operations
- Business rule logic
- Performance analysis

**Future Features Previewed:**
- `Presto.Rule.aggregation/6` API design
- `:sum`, `:count`, `:avg` operations
- Multi-field grouping

### 3. Batch Processing Example (`batch_processing_example.exs`)

Efficient processing of large datasets using Presto's batch APIs.

**Demonstrates:**
- Bulk fact assertion
- Performance measurement
- Concurrent rule processing
- Result analysis and reporting

**Use Cases:**
- Payroll processing for hundreds/thousands of employees
- Transaction processing
- Data migration and validation

### 4. Payroll Example (`payroll_example.ex`)

Domain-specific example showing real-world business rules for payroll processing.

**Demonstrates:**
- Complex business logic
- Multiple rule interactions
- Practical calculation patterns
- Performance metrics

**Business Rules:**
- Overtime calculations
- Performance bonuses
- Tax deductions
- Holiday pay

## Learning Progression

1. **Start with `basic_example.exs`** - Learn fundamental concepts
2. **Try `aggregation_example.exs`** - Understand data grouping and analysis
3. **Run `batch_processing_example.exs`** - See performance and scale
4. **Explore `payroll_example.ex`** - Apply to real-world scenarios

## Performance Notes

These examples are optimized for learning, not necessarily for maximum performance. For production use:

- Use longer-lived rule engines (don't create/destroy per operation)
- Consider rule compilation and caching strategies
- Profile your specific use case
- Implement proper error handling and monitoring

## API Reference

All examples use the simplified v0.2 API:

### Core Functions
```elixir
# Engine management
{:ok, engine} = Presto.RuleEngine.start_link()

# Rule creation
rule = Presto.Rule.new(id, conditions, action)

# Batch operations
:ok = Presto.RuleEngine.add_rules(engine, rules)
:ok = Presto.RuleEngine.assert_facts(engine, facts)

# Execution
results = Presto.RuleEngine.fire_rules(engine)

# Monitoring
stats = Presto.RuleEngine.get_rule_statistics(engine)
```

### Rule Conditions
```elixir
# Pattern matching
Presto.Rule.pattern(:fact_type, [:field1, :field2, :field3])

# Tests
Presto.Rule.test(:variable, :>, value)
Presto.Rule.test(:variable, :==, value)
```

### Future Aggregations (Preview)
```elixir
# Coming in future updates
Presto.Rule.aggregation(id, patterns, group_by, function, field)
```

## Simplified Architecture

These examples demonstrate Presto's BSSN-simplified architecture:

- **8 focused modules** (down from 11)
- **Direct API calls** (no complex abstractions)
- **Explicit over implicit** (clear function calls vs. DSL magic)
- **Simple solutions** (solve current needs without speculation)

## BSSN Principles Applied

1. **Simplicity First**: Each example solves one specific problem clearly
2. **Quality Standards**: Production-ready code without over-engineering
3. **Explicit APIs**: No DSLs or magic - just Elixir functions
4. **Focused Modules**: Each example has a single, clear purpose

## Previous Examples (Removed)

The following over-engineered examples were removed to align with BSSN principles:

- `enhanced_payroll_demo.exs` - Duplicated functionality
- `payroll_aggregator.ex` - Manual aggregations (superseded by native)
- `massive_scale_payroll/` - 8-file abstraction not needed for demonstration

## Performance Benchmarks

Current optimizations provide:

- **~16μs per fact insertion** (measured)
- **Concurrent ETS access** for better throughput
- **Pattern matching optimizations** via direct tuple access
- **ETS-based statistics** replacing process dictionary overhead

## Getting Help

- Read the main [Presto documentation](../README.md)
- Check the [architecture overview](../docs/OVERVIEW.md)
- Review [performance specifications](../specs/performance.md)
- See [unique features](../specs/unique_features.md) for advanced capabilities

---

**Note**: These examples focus on core RETE functionality. Advanced features like distributed processing, complex aggregations, and enterprise integrations are covered in the main documentation.
#!/usr/bin/env elixir

# Basic Presto RETE Rules Engine Example
# This example demonstrates the core v0.2 API in ~50 lines

IO.puts("=== Basic Presto Example ===\n")

# Start the application
Application.ensure_all_started(:presto)

# 1. Start the rules engine
{:ok, engine} = Presto.RuleEngine.start_link()

# 2. Define rules using the new explicit API
adult_rule = Presto.Rule.new(
  :adult_rule,
  [
    Presto.Rule.pattern(:person, [:name, :age]),
    Presto.Rule.test(:age, :>, 18)
  ],
  fn facts -> 
    IO.puts("  ✓ #{facts[:name]} is an adult (#{facts[:age]} years old)")
    [{:adult, facts[:name]}] 
  end
)

employee_rule = Presto.Rule.new(
  :employee_rule,
  [
    Presto.Rule.pattern(:person, [:name, :age]),
    Presto.Rule.pattern(:employment, [:name, :company]),
    Presto.Rule.test(:age, :>=, 16)
  ],
  fn facts ->
    IO.puts("  ✓ #{facts[:name]} works at #{facts[:company]}")
    [{:employee, facts[:name], facts[:company]}]
  end
)

# 3. Add rules using batch API
IO.puts("Adding rules...")
:ok = Presto.RuleEngine.add_rules(engine, [adult_rule, employee_rule])

# 4. Assert facts using batch API  
IO.puts("\nAsserting facts...")
facts = [
  {:person, "Alice", 25},
  {:person, "Bob", 17},
  {:person, "Carol", 30},
  {:employment, "Alice", "TechCorp"},
  {:employment, "Carol", "StartupInc"}
]

:ok = Presto.RuleEngine.assert_facts(engine, facts)

# 5. Fire rules and see results
IO.puts("\nFiring rules:")
results = Presto.RuleEngine.fire_rules(engine)

IO.puts("\nResults:")
Enum.each(results, fn result ->
  IO.puts("  → #{inspect(result)}")
end)

# 6. Check some statistics
stats = Presto.RuleEngine.get_rule_statistics(engine)
engine_stats = Presto.RuleEngine.get_engine_statistics(engine)

IO.puts("\nEngine Statistics:")
IO.puts("  Rules: #{engine_stats.total_rules}")
IO.puts("  Facts: #{engine_stats.total_facts}")
IO.puts("  Rule Firings: #{engine_stats.total_rule_firings}")

# 7. Clean up
GenServer.stop(engine)

IO.puts("\n=== Example Complete ===")
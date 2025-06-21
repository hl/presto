#!/usr/bin/env elixir

# RETE-Native Aggregations Example
# Demonstrates Presto's powerful aggregation capabilities using the v0.2 API

IO.puts("=== RETE-Native Aggregations Example ===\n")

# Start the application
Application.ensure_all_started(:presto)

# Start the rules engine
{:ok, engine} = Presto.RuleEngine.start_link()

IO.puts("Creating aggregation rules...")

# Note: This example shows the API design for aggregations
# The actual aggregation implementation is part of the RETE-native features
# For now, we'll demonstrate with manual rules that could be aggregations

# 1. Department employee count (would be: count aggregation)
dept_count_rule = Presto.Rule.new(
  :dept_employee_count,
  [
    Presto.Rule.pattern(:employee, [:id, :name, :department])
  ],
  fn facts ->
    # This simulates what a :count aggregation would do
    [{:dept_member, facts[:department], facts[:id]}]
  end
)

# 2. Department salary totals (would be: sum aggregation)  
dept_salary_rule = Presto.Rule.new(
  :dept_salary_total,
  [
    Presto.Rule.pattern(:employee, [:id, :name, :department]),
    Presto.Rule.pattern(:salary, [:id, :amount])
  ],
  fn facts ->
    # This simulates what a :sum aggregation would do
    [{:dept_salary_contribution, facts[:department], facts[:amount]}]
  end
)

# 3. High performer identification (business rule + aggregation concept)
high_performer_rule = Presto.Rule.new(
  :high_performer,
  [
    Presto.Rule.pattern(:employee, [:id, :name, :department]),
    Presto.Rule.pattern(:salary, [:id, :amount]),
    Presto.Rule.test(:amount, :>, 80000)
  ],
  fn facts ->
    IO.puts("  🌟 High performer: #{facts[:name]} (#{facts[:department]}) - $#{facts[:amount]}")
    [{:high_performer, facts[:name], facts[:department], facts[:amount]}]
  end
)

# Add all rules
:ok = Presto.RuleEngine.add_rules(engine, [
  dept_count_rule,
  dept_salary_rule, 
  high_performer_rule
])

IO.puts("\nAsserting employee and salary data...")

# Sample company data
employees = [
  {:employee, 1, "Alice", "Engineering"},
  {:employee, 2, "Bob", "Engineering"}, 
  {:employee, 3, "Carol", "Sales"},
  {:employee, 4, "David", "Sales"},
  {:employee, 5, "Eve", "Marketing"},
  {:employee, 6, "Frank", "Engineering"}
]

salaries = [
  {:salary, 1, 95000},  # Alice - high performer
  {:salary, 2, 75000},  # Bob
  {:salary, 3, 85000},  # Carol - high performer
  {:salary, 4, 70000},  # David
  {:salary, 5, 65000},  # Eve
  {:salary, 6, 105000}  # Frank - high performer
]

# Assert all facts
:ok = Presto.RuleEngine.assert_facts(engine, employees ++ salaries)

IO.puts("\nFiring rules:")
results = Presto.RuleEngine.fire_rules(engine)

# Analyze results to show aggregation-like behavior
IO.puts("\n=== Aggregation Analysis ===")

# Group department members (simulating count aggregation)
dept_members = results
|> Enum.filter(&match?({:dept_member, _, _}, &1))
|> Enum.group_by(fn {:dept_member, dept, _id} -> dept end)

IO.puts("\nDepartment Employee Counts:")
Enum.each(dept_members, fn {dept, members} ->
  count = length(members)
  IO.puts("  #{dept}: #{count} employees")
end)

# Group salary contributions (simulating sum aggregation)
dept_salaries = results
|> Enum.filter(&match?({:dept_salary_contribution, _, _}, &1))
|> Enum.group_by(fn {:dept_salary_contribution, dept, _amount} -> dept end)

IO.puts("\nDepartment Salary Totals:")
Enum.each(dept_salaries, fn {dept, contributions} ->
  total = contributions 
  |> Enum.map(fn {:dept_salary_contribution, _dept, amount} -> amount end)
  |> Enum.sum()
  IO.puts("  #{dept}: $#{total}")
end)

# High performers (direct rule results)
high_performers = results
|> Enum.filter(&match?({:high_performer, _, _, _}, &1))

IO.puts("\nHigh Performers (>$80K):")
Enum.each(high_performers, fn {:high_performer, name, dept, salary} ->
  IO.puts("  #{name} (#{dept}): $#{salary}")
end)

# Show statistics
stats = Presto.RuleEngine.get_engine_statistics(engine)
IO.puts("\nEngine Performance:")
IO.puts("  Facts processed: #{stats.total_facts}")
IO.puts("  Rules executed: #{stats.total_rule_firings}")
IO.puts("  Results generated: #{length(results)}")

# Clean up
GenServer.stop(engine)

IO.puts("\n=== Future RETE-Native Aggregations ===")
IO.puts("In the full implementation, this would use:")
IO.puts("""
  # Count employees by department
  Presto.Rule.aggregation(
    :dept_count,
    [Presto.Rule.pattern(:employee, [:id, :name, :department])],
    [:department],  # Group by
    :count,         # Function
    nil             # Count doesn't need a field
  )
  
  # Sum salaries by department  
  Presto.Rule.aggregation(
    :dept_totals,
    [
      Presto.Rule.pattern(:employee, [:id, :name, :department]),
      Presto.Rule.pattern(:salary, [:id, :amount])
    ],
    [:department],  # Group by
    :sum,           # Function
    :amount         # Field to sum
  )
""")

IO.puts("\n=== Example Complete ===")
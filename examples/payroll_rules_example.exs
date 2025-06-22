#!/usr/bin/env elixir

# Advanced Payroll Rules Example
# This example demonstrates comprehensive payroll processing with time calculation and overtime rules

IO.puts("=== Advanced Payroll Processing Example ===\n")

# Start the application
Application.ensure_all_started(:presto)

# 1. Start the rules engine
{:ok, engine} = Presto.RuleEngine.start_link()

# 2. Define payroll rules using the RETE engine API
IO.puts("Defining payroll rules...")

# Rule 1: Calculate time duration for time entries
time_calculation_rule = %{
  id: :calculate_time_duration,
  conditions: [
    {:time_entry, :id, :data}
  ],
  action: fn facts ->
    data = facts[:data]
    id = facts[:id]

    # Only process if we have datetime fields and NO calculated values
    if Map.has_key?(data, :start_datetime) and Map.has_key?(data, :finish_datetime) and
         is_nil(Map.get(data, :minutes)) and is_nil(Map.get(data, :units)) do
      # Calculate minutes between start and finish
      minutes = DateTime.diff(data.finish_datetime, data.start_datetime, :second) / 60.0
      units = minutes / 60.0

      updated_data = %{
        data
        | minutes: Float.round(minutes, 2),
          units: Float.round(units, 2)
      }

      IO.puts("  ✓ Calculated #{Float.round(units, 1)}h for employee #{data.employee_id}")

      # Return both the updated entry and a processed marker fact
      [
        {:time_entry, id, updated_data},
        {:processed_time_entry, id, updated_data}
      ]
    else
      []
    end
  end,
  priority: 100
}

# Rule 2: Calculate overtime when total units exceed threshold
overtime_threshold = 40.0

overtime_calculation_rule = %{
  id: :calculate_overtime,
  conditions: [
    {:processed_time_entry, :id, :data}
  ],
  action: fn facts ->
    data = facts[:data]
    employee_id = data.employee_id
    units = data.units

    if units > overtime_threshold do
      overtime_units = units - overtime_threshold

      # Calculate week start for grouping
      date = DateTime.to_date(data.start_datetime)
      days_since_monday = Date.day_of_week(date) - 1
      week_start = Date.add(date, -days_since_monday)

      overtime_data = %{
        employee_id: employee_id,
        units: Float.round(overtime_units, 2),
        week_start: week_start,
        type: :overtime,
        source_entry: facts[:id],
        created_at: DateTime.utc_now()
      }

      IO.puts(
        "  ✓ Generated #{Float.round(overtime_units, 1)}h overtime for employee #{employee_id}"
      )

      [{:overtime_entry, {employee_id, week_start}, overtime_data}]
    else
      []
    end
  end,
  priority: 90
}

# 3. Add rules to the engine
IO.puts("\nAdding rules to engine...")
:ok = Presto.RuleEngine.add_rule(engine, time_calculation_rule)
:ok = Presto.RuleEngine.add_rule(engine, overtime_calculation_rule)

# 4. Create sample time entries
IO.puts("\nCreating sample time entries...")

time_entries = [
  {:time_entry, "entry_1",
   %{
     employee_id: "emp_001",
     start_datetime: ~U[2025-01-13 09:00:00Z],
     finish_datetime: ~U[2025-01-13 17:00:00Z],
     minutes: nil,
     units: nil
   }},
  {:time_entry, "entry_2",
   %{
     employee_id: "emp_001",
     start_datetime: ~U[2025-01-14 09:00:00Z],
     finish_datetime: ~U[2025-01-14 17:00:00Z],
     minutes: nil,
     units: nil
   }},
  {:time_entry, "entry_3",
   %{
     employee_id: "emp_002",
     start_datetime: ~U[2025-01-13 08:00:00Z],
     finish_datetime: ~U[2025-01-13 20:00:00Z],
     minutes: nil,
     units: nil
   }},
  {:time_entry, "entry_4",
   %{
     employee_id: "emp_003",
     start_datetime: ~U[2025-01-15 10:00:00Z],
     finish_datetime: ~U[2025-01-15 15:00:00Z],
     minutes: nil,
     units: nil
   }}
]

IO.puts("Sample data: #{length(time_entries)} time entries")

# 5. Assert time entries to working memory
IO.puts("\nAsserting time entries...")

Enum.each(time_entries, fn time_entry ->
  :ok = Presto.RuleEngine.assert_fact(engine, time_entry)
end)

# 6. Fire rules and see results
IO.puts("\nFiring payroll rules:")
results = Presto.RuleEngine.fire_rules(engine)

# 7. Analyze results
IO.puts("\nProcessing Results:")

# Separate the different types of results
processed_entries =
  Enum.filter(results, fn
    {:time_entry, _, %{units: units}} when not is_nil(units) -> true
    _ -> false
  end)

overtime_entries =
  Enum.filter(results, fn
    {:overtime_entry, _, _} -> true
    _ -> false
  end)

IO.puts("  → Processed #{length(processed_entries)} time entries")
IO.puts("  → Generated #{length(overtime_entries)} overtime entries")

# 8. Calculate summary statistics
total_regular_hours =
  processed_entries
  |> Enum.map(fn {:time_entry, _, %{units: units}} -> units end)
  |> Enum.sum()
  |> Float.round(2)

total_overtime_hours =
  overtime_entries
  |> Enum.map(fn {:overtime_entry, _, %{units: units}} -> units end)
  |> Enum.sum()
  |> case do
    0 -> 0.0
    sum -> Float.round(sum, 2)
  end

total_employees =
  processed_entries
  |> Enum.map(fn {:time_entry, _, %{employee_id: emp_id}} -> emp_id end)
  |> Enum.uniq()
  |> length()

employees_with_overtime =
  overtime_entries
  |> Enum.map(fn {:overtime_entry, _, %{employee_id: emp_id}} -> emp_id end)
  |> Enum.uniq()
  |> length()

# 9. Display summary
IO.puts("\n=== Payroll Summary ===")
IO.puts("Total Employees: #{total_employees}")
IO.puts("Regular Hours: #{total_regular_hours}")
IO.puts("Overtime Hours: #{total_overtime_hours}")
IO.puts("Total Hours: #{total_regular_hours + total_overtime_hours}")
IO.puts("Employees with Overtime: #{employees_with_overtime}")

# 10. Show detailed results
if length(overtime_entries) > 0 do
  IO.puts("\n=== Overtime Details ===")

  Enum.each(overtime_entries, fn {:overtime_entry, {emp_id, week_start}, data} ->
    IO.puts("  #{emp_id}: #{data.units}h overtime (week starting #{week_start})")
  end)
end

# 11. Check engine statistics
engine_stats = Presto.RuleEngine.get_engine_statistics(engine)
IO.puts("\n=== Engine Statistics ===")
IO.puts("Rules: #{engine_stats.total_rules}")
IO.puts("Facts: #{engine_stats.total_facts}")
IO.puts("Rule Firings: #{engine_stats.total_rule_firings}")

# 12. Demonstrate custom overtime threshold
IO.puts("\n" <> String.duplicate("=", 50))
IO.puts("CUSTOM THRESHOLD DEMO")
IO.puts(String.duplicate("=", 50))

# Start a new engine with different threshold
{:ok, engine2} = Presto.RuleEngine.start_link()

# Define rule with lower threshold (8 hours daily)
custom_overtime_rule = %{
  id: :calculate_overtime,
  conditions: [
    {:processed_time_entry, :id, :data}
  ],
  action: fn facts ->
    data = facts[:data]
    employee_id = data.employee_id
    units = data.units
    daily_threshold = 8.0

    if units > daily_threshold do
      overtime_units = units - daily_threshold

      date = DateTime.to_date(data.start_datetime)
      days_since_monday = Date.day_of_week(date) - 1
      week_start = Date.add(date, -days_since_monday)

      overtime_data = %{
        employee_id: employee_id,
        units: Float.round(overtime_units, 2),
        week_start: week_start,
        type: :daily_overtime,
        source_entry: facts[:id],
        created_at: DateTime.utc_now()
      }

      IO.puts(
        "  ✓ Daily overtime: #{Float.round(overtime_units, 1)}h for employee #{employee_id}"
      )

      [{:overtime_entry, {employee_id, week_start}, overtime_data}]
    else
      []
    end
  end,
  priority: 90
}

IO.puts("\nUsing 8-hour daily overtime threshold...")
:ok = Presto.RuleEngine.add_rule(engine2, time_calculation_rule)
:ok = Presto.RuleEngine.add_rule(engine2, custom_overtime_rule)

# Assert the same time entries
Enum.each(time_entries, fn time_entry ->
  :ok = Presto.RuleEngine.assert_fact(engine2, time_entry)
end)

IO.puts("\nFiring rules with custom threshold:")
custom_results = Presto.RuleEngine.fire_rules(engine2)

custom_overtime_entries =
  Enum.filter(custom_results, fn
    {:overtime_entry, _, _} -> true
    _ -> false
  end)

custom_overtime_hours =
  custom_overtime_entries
  |> Enum.map(fn {:overtime_entry, _, %{units: units}} -> units end)
  |> Enum.sum()
  |> case do
    0 -> 0.0
    sum -> Float.round(sum, 2)
  end

IO.puts("\nCustom Threshold Results:")
IO.puts("  Overtime entries: #{length(custom_overtime_entries)}")
IO.puts("  Total overtime hours: #{custom_overtime_hours}")

# 13. Clean up
GenServer.stop(engine)
GenServer.stop(engine2)

IO.puts("\n=== Payroll Example Complete ===")

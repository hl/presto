#!/usr/bin/env elixir

# Advanced Overtime Rules Example
# This example demonstrates comprehensive overtime calculation with multi-stage processing

IO.puts("=== Advanced Overtime Processing Example ===\n")

# Start the application
Application.ensure_all_started(:presto)

# 1. Start the rules engine
{:ok, engine} = Presto.RuleEngine.start_link()

# 2. Define overtime calculation rules
IO.puts("Defining overtime calculation rules...")

# Rule 1: Calculate time duration for shifts
time_calculation_rule = %{
  id: :calculate_time_duration,
  conditions: [
    {:time_entry, :id, :data}
  ],
  action: fn facts ->
    data = facts[:data]
    id = facts[:id]

    # Only process if we have datetime fields but no calculated values
    if Map.has_key?(data, :start_dt) and Map.has_key?(data, :finish_dt) and
         is_nil(Map.get(data, :minutes)) and is_nil(Map.get(data, :units)) do
      # Calculate minutes between start and finish
      minutes = DateTime.diff(data.finish_dt, data.start_dt, :second) / 60.0
      units = minutes / 60.0

      updated_data = %{
        data
        | minutes: Float.round(minutes, 2),
          units: Float.round(units, 2)
      }

      IO.puts("  ✓ Calculated #{Float.round(units, 1)}h for shift #{id}")

      # Return the updated entry
      [{:time_entry, id, updated_data}]
    else
      []
    end
  end,
  priority: 100
}

# Rule 2: Aggregate hours by employee and pay code
pay_aggregation_rule = %{
  id: :aggregate_pay_hours,
  conditions: [
    {:time_entry, :id, :data}
  ],
  action: fn facts ->
    data = facts[:data]
    employee_id = data.employee_id
    pay_code = data.pay_code
    units = data.units

    # Only process calculated entries that haven't been paid yet
    if not is_nil(units) and not Map.get(data, :paid, false) do
      aggregate_key = {employee_id, pay_code}

      aggregate_data = %{
        employee_id: employee_id,
        pay_code: pay_code,
        total_hours: Float.round(units, 2),
        entries: [facts[:id]],
        paid: false,
        created_at: DateTime.utc_now()
      }

      IO.puts("  ✓ Aggregated #{Float.round(units, 1)}h for #{employee_id} (#{pay_code})")

      [{:pay_aggregate, aggregate_key, aggregate_data}]
    else
      []
    end
  end,
  priority: 90
}

# Rule 3: Process overtime rules (high priority - basic pay)
overtime_basic_rule = %{
  id: :overtime_basic_pay,
  conditions: [
    {:pay_aggregate, :key, :data},
    {:overtime_rule, :rule_id, :rule_data}
  ],
  action: fn facts ->
    aggregate_data = facts[:data]
    rule_data = facts[:rule_data]

    # Match basic pay overtime rule
    if rule_data.filter_pay_code == "basic_pay" and
         aggregate_data.pay_code == "basic_pay" and
         aggregate_data.total_hours > rule_data.threshold and
         not aggregate_data.paid do
      overtime_hours = aggregate_data.total_hours - rule_data.threshold

      overtime_data = %{
        employee_id: aggregate_data.employee_id,
        units: Float.round(overtime_hours, 2),
        minutes: Float.round(overtime_hours * 60, 2),
        pay_code: rule_data.pay_code,
        threshold: rule_data.threshold,
        source_aggregate: facts[:key],
        created_at: DateTime.utc_now()
      }

      # Mark aggregate as paid
      updated_aggregate_data = Map.put(aggregate_data, :paid, true)

      IO.puts(
        "  ✓ Generated #{Float.round(overtime_hours, 1)}h basic overtime for #{aggregate_data.employee_id}"
      )

      [
        {:overtime_entry, "overtime_#{aggregate_data.employee_id}_#{rule_data.pay_code}",
         overtime_data},
        {:pay_aggregate, facts[:key], updated_aggregate_data}
      ]
    else
      []
    end
  end,
  priority: 80
}

# Rule 4: Process overtime rules (medium priority - special pay)
overtime_special_rule = %{
  id: :overtime_special_pay,
  conditions: [
    {:pay_aggregate, :key, :data},
    {:overtime_rule, :rule_id, :rule_data}
  ],
  action: fn facts ->
    aggregate_data = facts[:data]
    rule_data = facts[:rule_data]

    # Match special pay overtime rule
    if rule_data.filter_pay_code == "special_pay" and
         aggregate_data.pay_code == "special_pay" and
         aggregate_data.total_hours > rule_data.threshold and
         not aggregate_data.paid do
      overtime_hours = aggregate_data.total_hours - rule_data.threshold

      overtime_data = %{
        employee_id: aggregate_data.employee_id,
        units: Float.round(overtime_hours, 2),
        minutes: Float.round(overtime_hours * 60, 2),
        pay_code: rule_data.pay_code,
        threshold: rule_data.threshold,
        source_aggregate: facts[:key],
        created_at: DateTime.utc_now()
      }

      # Mark aggregate as paid
      updated_aggregate_data = Map.put(aggregate_data, :paid, true)

      IO.puts(
        "  ✓ Generated #{Float.round(overtime_hours, 1)}h special overtime for #{aggregate_data.employee_id}"
      )

      [
        {:overtime_entry, "overtime_#{aggregate_data.employee_id}_#{rule_data.pay_code}",
         overtime_data},
        {:pay_aggregate, facts[:key], updated_aggregate_data}
      ]
    else
      []
    end
  end,
  priority: 70
}

# Rule 5: Process overtime rules (low priority - catch-all)
overtime_general_rule = %{
  id: :overtime_general,
  conditions: [
    {:pay_aggregate, :key, :data},
    {:overtime_rule, :rule_id, :rule_data}
  ],
  action: fn facts ->
    aggregate_data = facts[:data]
    rule_data = facts[:rule_data]

    # Match general overtime rule (no filter_pay_code means catch-all)
    if is_nil(rule_data.filter_pay_code) and
         aggregate_data.total_hours > rule_data.threshold and
         not aggregate_data.paid do
      overtime_hours = aggregate_data.total_hours - rule_data.threshold

      overtime_data = %{
        employee_id: aggregate_data.employee_id,
        units: Float.round(overtime_hours, 2),
        minutes: Float.round(overtime_hours * 60, 2),
        pay_code: rule_data.pay_code,
        threshold: rule_data.threshold,
        source_aggregate: facts[:key],
        created_at: DateTime.utc_now()
      }

      # Mark aggregate as paid
      updated_aggregate_data = Map.put(aggregate_data, :paid, true)

      IO.puts(
        "  ✓ Generated #{Float.round(overtime_hours, 1)}h general overtime for #{aggregate_data.employee_id}"
      )

      [
        {:overtime_entry, "overtime_#{aggregate_data.employee_id}_#{rule_data.pay_code}",
         overtime_data},
        {:pay_aggregate, facts[:key], updated_aggregate_data}
      ]
    else
      []
    end
  end,
  priority: 60
}

# 3. Add rules to the engine in priority order
IO.puts("\nAdding rules to engine...")
:ok = Presto.RuleEngine.add_rule(engine, time_calculation_rule)
:ok = Presto.RuleEngine.add_rule(engine, pay_aggregation_rule)
:ok = Presto.RuleEngine.add_rule(engine, overtime_basic_rule)
:ok = Presto.RuleEngine.add_rule(engine, overtime_special_rule)
:ok = Presto.RuleEngine.add_rule(engine, overtime_general_rule)

# 4. Create sample data
IO.puts("\nCreating sample time entries and overtime rules...")

# Sample time entries with different employees and pay codes
time_entries = [
  {:time_entry, "shift_1",
   %{
     employee_id: "emp_001",
     start_dt: ~U[2025-01-01 08:00:00Z],
     # 12 hours
     finish_dt: ~U[2025-01-01 20:00:00Z],
     units: nil,
     minutes: nil,
     pay_code: "basic_pay",
     paid: false
   }},
  {:time_entry, "shift_2",
   %{
     employee_id: "emp_001",
     start_dt: ~U[2025-01-02 08:00:00Z],
     # 8 hours
     finish_dt: ~U[2025-01-02 16:00:00Z],
     units: nil,
     minutes: nil,
     pay_code: "basic_pay",
     paid: false
   }},
  {:time_entry, "shift_3",
   %{
     employee_id: "emp_002",
     start_dt: ~U[2025-01-01 09:00:00Z],
     # 12 hours
     finish_dt: ~U[2025-01-01 21:00:00Z],
     units: nil,
     minutes: nil,
     pay_code: "special_pay",
     paid: false
   }},
  {:time_entry, "shift_4",
   %{
     employee_id: "emp_003",
     start_dt: ~U[2025-01-01 10:00:00Z],
     # 6 hours
     finish_dt: ~U[2025-01-01 16:00:00Z],
     units: nil,
     minutes: nil,
     pay_code: "basic_pay",
     paid: false
   }},
  {:time_entry, "shift_5",
   %{
     employee_id: "emp_003",
     start_dt: ~U[2025-01-02 10:00:00Z],
     # 4 hours
     finish_dt: ~U[2025-01-02 14:00:00Z],
     units: nil,
     minutes: nil,
     pay_code: "other_pay",
     paid: false
   }}
]

# Overtime rules with different priorities and thresholds
overtime_rules = [
  {:overtime_rule, "overtime_basic",
   %{
     # 15 hours threshold for basic pay
     threshold: 15.0,
     filter_pay_code: "basic_pay",
     pay_code: "overtime_basic_pay",
     priority: 1
   }},
  {:overtime_rule, "overtime_special",
   %{
     # 10 hours threshold for special pay
     threshold: 10.0,
     filter_pay_code: "special_pay",
     pay_code: "overtime_special_pay",
     priority: 2
   }},
  {:overtime_rule, "overtime_general",
   %{
     # 8 hours threshold for any other pay
     threshold: 8.0,
     # Catch-all rule
     filter_pay_code: nil,
     pay_code: "overtime_rest",
     priority: 3
   }}
]

IO.puts(
  "Sample data: #{length(time_entries)} time entries, #{length(overtime_rules)} overtime rules"
)

# 5. Assert time entries to working memory
IO.puts("\nAsserting time entries...")

Enum.each(time_entries, fn time_entry ->
  :ok = Presto.RuleEngine.assert_fact(engine, time_entry)
end)

# 6. Assert overtime rules to working memory
IO.puts("Asserting overtime rules...")

Enum.each(overtime_rules, fn overtime_rule ->
  :ok = Presto.RuleEngine.assert_fact(engine, overtime_rule)
end)

# 7. Fire rules and process overtime
IO.puts("\nFiring overtime calculation rules:")
results = Presto.RuleEngine.fire_rules(engine)

# 8. Analyze results
IO.puts("\nProcessing Results:")

# Extract different types of results
calculated_entries =
  Enum.filter(results, fn
    {:time_entry, _, %{units: units}} when not is_nil(units) -> true
    _ -> false
  end)

pay_aggregates =
  Enum.filter(results, fn
    {:pay_aggregate, _, _} -> true
    _ -> false
  end)

overtime_entries =
  Enum.filter(results, fn
    {:overtime_entry, _, _} -> true
    _ -> false
  end)

IO.puts("  → Calculated #{length(calculated_entries)} time entries")
IO.puts("  → Created #{length(pay_aggregates)} pay aggregates")
IO.puts("  → Generated #{length(overtime_entries)} overtime entries")

# 9. Calculate summary statistics
total_regular_hours =
  calculated_entries
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

employees_processed =
  calculated_entries
  |> Enum.map(fn {:time_entry, _, %{employee_id: emp_id}} -> emp_id end)
  |> Enum.uniq()
  |> length()

employees_with_overtime =
  overtime_entries
  |> Enum.map(fn {:overtime_entry, _, %{employee_id: emp_id}} -> emp_id end)
  |> Enum.uniq()
  |> length()

# 10. Display summary
IO.puts("\n=== Overtime Processing Summary ===")
IO.puts("Employees Processed: #{employees_processed}")
IO.puts("Regular Hours: #{total_regular_hours}")
IO.puts("Overtime Hours: #{total_overtime_hours}")
IO.puts("Total Hours: #{total_regular_hours + total_overtime_hours}")
IO.puts("Employees with Overtime: #{employees_with_overtime}")

# 11. Show detailed overtime breakdown
if length(overtime_entries) > 0 do
  IO.puts("\n=== Overtime Details ===")

  Enum.each(overtime_entries, fn {:overtime_entry, _entry_id, data} ->
    IO.puts(
      "  #{data.employee_id}: #{data.units}h overtime -> #{data.pay_code} (threshold: #{data.threshold}h)"
    )
  end)
end

# 12. Show pay aggregate breakdown
if length(pay_aggregates) > 0 do
  IO.puts("\n=== Pay Aggregate Breakdown ===")

  Enum.each(pay_aggregates, fn {:pay_aggregate, {emp_id, pay_code}, data} ->
    paid_status = if data.paid, do: "PAID", else: "UNPAID"
    IO.puts("  #{emp_id} (#{pay_code}): #{data.total_hours}h [#{paid_status}]")
  end)
end

# 13. Demonstrate overtime rule priorities
IO.puts("\n" <> String.duplicate("=", 50))
IO.puts("OVERTIME RULE PRIORITY DEMO")
IO.puts(String.duplicate("=", 50))

# Start a new engine with different rule processing order
{:ok, engine2} = Presto.RuleEngine.start_link()

# Add only basic rules
:ok = Presto.RuleEngine.add_rule(engine2, time_calculation_rule)
:ok = Presto.RuleEngine.add_rule(engine2, pay_aggregation_rule)

# Create a special scenario - employee with multiple pay codes
special_entries = [
  {:time_entry, "special_1",
   %{
     employee_id: "emp_multi",
     start_dt: ~U[2025-01-01 08:00:00Z],
     # 12 hours basic
     finish_dt: ~U[2025-01-01 20:00:00Z],
     units: nil,
     minutes: nil,
     pay_code: "basic_pay",
     paid: false
   }},
  {:time_entry, "special_2",
   %{
     employee_id: "emp_multi",
     start_dt: ~U[2025-01-02 08:00:00Z],
     # 12 hours special
     finish_dt: ~U[2025-01-02 20:00:00Z],
     units: nil,
     minutes: nil,
     pay_code: "special_pay",
     paid: false
   }}
]

# Custom overtime rule with lower threshold
custom_overtime_rule = %{
  id: :custom_overtime,
  conditions: [
    {:pay_aggregate, :key, :data}
  ],
  action: fn facts ->
    aggregate_data = facts[:data]
    # Lower threshold
    threshold = 8.0

    if aggregate_data.total_hours > threshold and not aggregate_data.paid do
      overtime_hours = aggregate_data.total_hours - threshold

      overtime_data = %{
        employee_id: aggregate_data.employee_id,
        units: Float.round(overtime_hours, 2),
        minutes: Float.round(overtime_hours * 60, 2),
        pay_code: "custom_overtime_" <> aggregate_data.pay_code,
        threshold: threshold,
        source_aggregate: facts[:key],
        created_at: DateTime.utc_now()
      }

      updated_aggregate_data = Map.put(aggregate_data, :paid, true)

      IO.puts(
        "  ✓ Custom overtime: #{Float.round(overtime_hours, 1)}h for #{aggregate_data.employee_id} (#{aggregate_data.pay_code})"
      )

      [
        {:overtime_entry, "custom_#{aggregate_data.employee_id}_#{aggregate_data.pay_code}",
         overtime_data},
        {:pay_aggregate, facts[:key], updated_aggregate_data}
      ]
    else
      []
    end
  end,
  priority: 85
}

IO.puts("\nUsing 8-hour custom overtime threshold for all pay codes...")
:ok = Presto.RuleEngine.add_rule(engine2, custom_overtime_rule)

# Assert the special entries
Enum.each(special_entries, fn time_entry ->
  :ok = Presto.RuleEngine.assert_fact(engine2, time_entry)
end)

IO.puts("\nFiring rules with custom overtime processing:")
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

IO.puts("\nCustom Processing Results:")
IO.puts("  Overtime entries: #{length(custom_overtime_entries)}")
IO.puts("  Total overtime hours: #{custom_overtime_hours}")

if length(custom_overtime_entries) > 0 do
  IO.puts("\nCustom Overtime Details:")

  Enum.each(custom_overtime_entries, fn {:overtime_entry, _, data} ->
    IO.puts("    #{data.employee_id}: #{data.units}h -> #{data.pay_code}")
  end)
end

# 14. Check engine statistics
engine_stats = Presto.RuleEngine.get_engine_statistics(engine)
IO.puts("\n=== Engine Statistics ===")
IO.puts("Rules: #{engine_stats.total_rules}")
IO.puts("Facts: #{engine_stats.total_facts}")
IO.puts("Rule Firings: #{engine_stats.total_rule_firings}")

# 15. Clean up
GenServer.stop(engine)
GenServer.stop(engine2)

IO.puts("\n=== Overtime Processing Example Complete ===")

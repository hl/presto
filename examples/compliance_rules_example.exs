#!/usr/bin/env elixir

# Advanced Compliance Rules Example
# This example demonstrates comprehensive compliance monitoring and violation detection

IO.puts("=== Advanced Compliance Monitoring Example ===\n")

# Start the application
Application.ensure_all_started(:presto)

# 1. Start the rules engine
{:ok, engine} = Presto.RuleEngine.start_link()

# 2. Define compliance monitoring rules
IO.puts("Defining compliance monitoring rules...")

# Rule 1: Process time entries and calculate durations
time_processing_rule =
  Presto.Rule.new(
    :process_time_entries,
    [
      Presto.Rule.pattern(:time_entry, [:id, :data])
    ],
    fn facts ->
      data = facts[:data]
      id = facts[:id]

      # Process time entries that need duration calculation
      if Map.has_key?(data, :start_datetime) and Map.has_key?(data, :finish_datetime) and
           is_nil(Map.get(data, :units)) do
        minutes = DateTime.diff(data.finish_datetime, data.start_datetime, :second) / 60.0
        units = minutes / 60.0

        updated_data = %{
          data
          | minutes: Float.round(minutes, 2),
            units: Float.round(units, 2)
        }

        IO.puts("  ✓ Processed #{Float.round(units, 1)}h for employee #{data.employee_id}")

        # Return updated entry and processed marker
        [
          {:time_entry, id, updated_data},
          {:processed_time_entry, id, updated_data}
        ]
      else
        []
      end
    end,
    priority: 100
  )

# Helper function to calculate week start (Monday)
week_start_date = fn date ->
  days_since_monday = Date.day_of_week(date) - 1
  Date.add(date, -days_since_monday)
end

# Rule 2: Aggregate processed time entries into weekly totals
weekly_aggregation_rule =
  Presto.Rule.new(
    :weekly_hours_aggregation,
    [
      Presto.Rule.pattern(:processed_time_entry, [:id, :data])
    ],
    fn facts ->
      data = facts[:data]
      employee_id = data.employee_id
      start_dt = data.start_datetime
      units = data.units

      # Calculate week boundaries (Monday to Sunday)
      date = DateTime.to_date(start_dt)
      week_start = week_start_date.(date)
      week_end = Date.add(week_start, 6)

      # Create weekly aggregation fact
      weekly_data = %{
        employee_id: employee_id,
        week_start: week_start,
        week_end: week_end,
        total_hours: units,
        entries: [facts[:id]],
        aggregated_at: DateTime.utc_now()
      }

      IO.puts(
        "  ✓ Weekly aggregate: #{Float.round(units, 1)}h for #{employee_id} (week #{week_start})"
      )

      [{:weekly_hours, {employee_id, week_start}, weekly_data}]
    end,
    priority: 90
  )

# Rule 3: Check weekly compliance against 48-hour threshold
max_weekly_hours = 48.0

weekly_compliance_rule =
  Presto.Rule.new(
    :weekly_compliance_check,
    [
      Presto.Rule.pattern(:weekly_hours, [:key, :data])
    ],
    fn facts ->
      data = facts[:data]
      key = facts[:key]
      {employee_id, week_start} = key
      total_hours = data.total_hours

      # Check compliance threshold
      {status, reason} =
        if total_hours > max_weekly_hours do
          {:non_compliant, "Exceeded maximum weekly hours (#{total_hours} > #{max_weekly_hours})"}
        else
          {:compliant, "Within weekly hours limit (#{total_hours} <= #{max_weekly_hours})"}
        end

      compliance_data = %{
        employee_id: employee_id,
        week_start: week_start,
        week_end: data.week_end,
        status: status,
        threshold: max_weekly_hours,
        actual_value: total_hours,
        reason: reason,
        checked_at: DateTime.utc_now()
      }

      status_icon = if status == :compliant, do: "✓", else: "✗"
      IO.puts("  #{status_icon} Compliance check: #{employee_id} - #{total_hours}h (#{status})")

      [{:compliance_result, key, compliance_data}]
    end,
    priority: 80
  )

# 3. Add rules to the engine
IO.puts("\nAdding rules to engine...")
:ok = Presto.RuleEngine.add_rule(engine, time_processing_rule)
:ok = Presto.RuleEngine.add_rule(engine, weekly_aggregation_rule)
:ok = Presto.RuleEngine.add_rule(engine, weekly_compliance_rule)

# 4. Create sample time entries
IO.puts("\nCreating sample time entries...")

# Employee 1: Normal working hours (compliant)
# Employee 2: Excessive hours (non-compliant)
time_entries = [
  # Employee 1: Monday-Friday, 8 hours each = 40 hours total (compliant)
  {:time_entry, "emp1_mon",
   %{
     employee_id: "emp_001",
     # Monday
     start_datetime: ~U[2025-01-13 09:00:00Z],
     # 8 hours
     finish_datetime: ~U[2025-01-13 17:00:00Z],
     units: nil
   }},
  {:time_entry, "emp1_tue",
   %{
     employee_id: "emp_001",
     # Tuesday
     start_datetime: ~U[2025-01-14 09:00:00Z],
     # 8 hours
     finish_datetime: ~U[2025-01-14 17:00:00Z],
     units: nil
   }},
  {:time_entry, "emp1_wed",
   %{
     employee_id: "emp_001",
     # Wednesday
     start_datetime: ~U[2025-01-15 09:00:00Z],
     # 8 hours
     finish_datetime: ~U[2025-01-15 17:00:00Z],
     units: nil
   }},
  {:time_entry, "emp1_thu",
   %{
     employee_id: "emp_001",
     # Thursday
     start_datetime: ~U[2025-01-16 09:00:00Z],
     # 8 hours
     finish_datetime: ~U[2025-01-16 17:00:00Z],
     units: nil
   }},
  {:time_entry, "emp1_fri",
   %{
     employee_id: "emp_001",
     # Friday
     start_datetime: ~U[2025-01-17 09:00:00Z],
     # 8 hours
     finish_datetime: ~U[2025-01-17 17:00:00Z],
     units: nil
   }},

  # Employee 2: Monday-Saturday, 10 hours each = 60 hours total (non-compliant)
  {:time_entry, "emp2_mon",
   %{
     employee_id: "emp_002",
     # Monday
     start_datetime: ~U[2025-01-13 08:00:00Z],
     # 10 hours
     finish_datetime: ~U[2025-01-13 18:00:00Z],
     units: nil
   }},
  {:time_entry, "emp2_tue",
   %{
     employee_id: "emp_002",
     # Tuesday
     start_datetime: ~U[2025-01-14 08:00:00Z],
     # 10 hours
     finish_datetime: ~U[2025-01-14 18:00:00Z],
     units: nil
   }},
  {:time_entry, "emp2_wed",
   %{
     employee_id: "emp_002",
     # Wednesday
     start_datetime: ~U[2025-01-15 08:00:00Z],
     # 10 hours
     finish_datetime: ~U[2025-01-15 18:00:00Z],
     units: nil
   }},
  {:time_entry, "emp2_thu",
   %{
     employee_id: "emp_002",
     # Thursday
     start_datetime: ~U[2025-01-16 08:00:00Z],
     # 10 hours
     finish_datetime: ~U[2025-01-16 18:00:00Z],
     units: nil
   }},
  {:time_entry, "emp2_fri",
   %{
     employee_id: "emp_002",
     # Friday
     start_datetime: ~U[2025-01-17 08:00:00Z],
     # 10 hours
     finish_datetime: ~U[2025-01-17 18:00:00Z],
     units: nil
   }},
  {:time_entry, "emp2_sat",
   %{
     employee_id: "emp_002",
     # Saturday
     start_datetime: ~U[2025-01-18 08:00:00Z],
     # 10 hours
     finish_datetime: ~U[2025-01-18 18:00:00Z],
     units: nil
   }},

  # Employee 3: Part-time, mixed days = 24 hours total (compliant)
  {:time_entry, "emp3_tue",
   %{
     employee_id: "emp_003",
     # Tuesday
     start_datetime: ~U[2025-01-14 10:00:00Z],
     # 8 hours
     finish_datetime: ~U[2025-01-14 18:00:00Z],
     units: nil
   }},
  {:time_entry, "emp3_thu",
   %{
     employee_id: "emp_003",
     # Thursday
     start_datetime: ~U[2025-01-16 10:00:00Z],
     # 8 hours
     finish_datetime: ~U[2025-01-16 18:00:00Z],
     units: nil
   }},
  {:time_entry, "emp3_sat",
   %{
     employee_id: "emp_003",
     # Saturday
     start_datetime: ~U[2025-01-18 09:00:00Z],
     # 8 hours
     finish_datetime: ~U[2025-01-18 17:00:00Z],
     units: nil
   }}
]

IO.puts("Sample data: #{length(time_entries)} time entries for 3 employees")

# 5. Assert time entries to working memory
IO.puts("\nAsserting time entries...")

Enum.each(time_entries, fn time_entry ->
  :ok = Presto.RuleEngine.assert_fact(engine, time_entry)
end)

# 6. Fire rules and process compliance
IO.puts("\nFiring compliance monitoring rules:")
results = Presto.RuleEngine.fire_rules(engine)

# 7. Analyze results
IO.puts("\nProcessing Results:")

# Extract different types of results
processed_entries =
  Enum.filter(results, fn
    {:time_entry, _, %{units: units}} when not is_nil(units) -> true
    _ -> false
  end)

weekly_aggregates =
  Enum.filter(results, fn
    {:weekly_hours, _, _} -> true
    _ -> false
  end)

compliance_results =
  Enum.filter(results, fn
    {:compliance_result, _, _} -> true
    _ -> false
  end)

IO.puts("  → Processed #{length(processed_entries)} time entries")
IO.puts("  → Created #{length(weekly_aggregates)} weekly aggregates")
IO.puts("  → Generated #{length(compliance_results)} compliance results")

# 8. Calculate summary statistics
total_employees =
  processed_entries
  |> Enum.map(fn {:time_entry, _, %{employee_id: emp_id}} -> emp_id end)
  |> Enum.uniq()
  |> length()

total_hours =
  processed_entries
  |> Enum.map(fn {:time_entry, _, %{units: units}} -> units end)
  |> Enum.sum()
  |> case do
    sum when is_number(sum) -> Float.round(sum / 1, 2)
    _ -> 0.0
  end

violations =
  compliance_results
  |> Enum.count(fn {:compliance_result, _, %{status: status}} ->
    status == :non_compliant
  end)

employees_with_violations =
  compliance_results
  |> Enum.filter(fn {:compliance_result, _, %{status: status}} ->
    status == :non_compliant
  end)
  |> Enum.map(fn {:compliance_result, {emp_id, _}, _} -> emp_id end)
  |> Enum.uniq()
  |> length()

compliance_rate =
  if length(weekly_aggregates) > 0 do
    ((length(weekly_aggregates) - violations) / length(weekly_aggregates) * 100)
    |> Float.round(1)
  else
    100.0
  end

# 9. Display summary
IO.puts("\n=== Compliance Monitoring Summary ===")
IO.puts("Total Employees: #{total_employees}")
IO.puts("Total Hours Processed: #{total_hours}")
IO.puts("Weekly Aggregates: #{length(weekly_aggregates)}")
IO.puts("Compliance Violations: #{violations}")
IO.puts("Employees with Violations: #{employees_with_violations}")
IO.puts("Compliance Rate: #{compliance_rate}%")

# 10. Show detailed compliance results
if length(compliance_results) > 0 do
  IO.puts("\n=== Detailed Compliance Results ===")

  Enum.each(compliance_results, fn {:compliance_result, {emp_id, week_start}, data} ->
    status_icon = if data.status == :compliant, do: "✓", else: "✗"

    IO.puts(
      "  #{status_icon} #{emp_id}: #{data.actual_value}h (week #{week_start}) - #{data.status}"
    )

    IO.puts("    → #{data.reason}")
  end)
end

# 11. Show weekly breakdown
if length(weekly_aggregates) > 0 do
  IO.puts("\n=== Weekly Hours Breakdown ===")

  Enum.each(weekly_aggregates, fn {:weekly_hours, {emp_id, week_start}, data} ->
    week_end = data.week_end
    hours = data.total_hours
    entries_count = length(data.entries)

    IO.puts(
      "  #{emp_id}: #{hours}h (#{entries_count} entries) - Week #{week_start} to #{week_end}"
    )
  end)
end

# 12. Demonstrate custom compliance threshold
IO.puts("\n" <> String.duplicate("=", 50))
IO.puts("CUSTOM COMPLIANCE THRESHOLD DEMO")
IO.puts(String.duplicate("=", 50))

# Start a new engine with stricter threshold
{:ok, engine2} = Presto.RuleEngine.start_link()

# Create stricter compliance rule (40 hours instead of 48)
strict_max_weekly_hours = 40.0

strict_compliance_rule =
  Presto.Rule.new(
    :strict_weekly_compliance,
    [
      Presto.Rule.pattern(:weekly_hours, [:key, :data])
    ],
    fn facts ->
      data = facts[:data]
      key = facts[:key]
      {employee_id, week_start} = key
      total_hours = data.total_hours

      # Check stricter compliance threshold
      {status, reason} =
        if total_hours > strict_max_weekly_hours do
          {:non_compliant,
           "Exceeded strict weekly hours (#{total_hours} > #{strict_max_weekly_hours})"}
        else
          {:compliant,
           "Within strict weekly hours limit (#{total_hours} <= #{strict_max_weekly_hours})"}
        end

      compliance_data = %{
        employee_id: employee_id,
        week_start: week_start,
        week_end: data.week_end,
        status: status,
        threshold: strict_max_weekly_hours,
        actual_value: total_hours,
        reason: reason,
        checked_at: DateTime.utc_now()
      }

      status_icon = if status == :compliant, do: "✓", else: "✗"
      IO.puts("  #{status_icon} Strict check: #{employee_id} - #{total_hours}h (#{status})")

      [{:strict_compliance_result, key, compliance_data}]
    end,
    priority: 75
  )

IO.puts("\nUsing strict 40-hour weekly threshold...")
:ok = Presto.RuleEngine.add_rule(engine2, time_processing_rule)
:ok = Presto.RuleEngine.add_rule(engine2, weekly_aggregation_rule)
:ok = Presto.RuleEngine.add_rule(engine2, strict_compliance_rule)

# Use same time entries
Enum.each(time_entries, fn time_entry ->
  :ok = Presto.RuleEngine.assert_fact(engine2, time_entry)
end)

IO.puts("\nFiring rules with strict compliance threshold:")
strict_results = Presto.RuleEngine.fire_rules(engine2)

strict_compliance_results =
  Enum.filter(strict_results, fn
    {:strict_compliance_result, _, _} -> true
    _ -> false
  end)

strict_violations =
  Enum.count(strict_compliance_results, fn
    {:strict_compliance_result, _, %{status: status}} -> status == :non_compliant
  end)

strict_compliance_rate =
  if length(strict_compliance_results) > 0 do
    ((length(strict_compliance_results) - strict_violations) / length(strict_compliance_results) *
       100)
    |> Float.round(1)
  else
    100.0
  end

IO.puts("\nStrict Compliance Results:")
IO.puts("  Compliance violations: #{strict_violations}")
IO.puts("  Compliance rate: #{strict_compliance_rate}%")

if length(strict_compliance_results) > 0 do
  IO.puts("\nStrict Compliance Details:")

  Enum.each(strict_compliance_results, fn {:strict_compliance_result, {emp_id, _}, data} ->
    status_icon = if data.status == :compliant, do: "✓", else: "✗"
    IO.puts("    #{status_icon} #{emp_id}: #{data.actual_value}h - #{data.status}")
  end)
end

# 13. Check engine statistics
engine_stats = Presto.RuleEngine.get_engine_statistics(engine)
IO.puts("\n=== Engine Statistics ===")
IO.puts("Rules: #{engine_stats.total_rules}")
IO.puts("Facts: #{engine_stats.total_facts}")
IO.puts("Rule Firings: #{engine_stats.total_rule_firings}")

# 14. Clean up
GenServer.stop(engine)
GenServer.stop(engine2)

IO.puts("\n=== Compliance Monitoring Example Complete ===")

#!/usr/bin/env elixir

# Advanced California Spike Break Rules Example
# This example demonstrates comprehensive jurisdictional compliance monitoring

IO.puts("=== California Spike Break Rules Example ===\n")

# Start the application
Application.ensure_all_started(:presto)

# 1. Start the rules engine
{:ok, engine} = Presto.RuleEngine.start_link()

# 2. Define California spike break compliance rules
IO.puts("Defining California spike break compliance rules...")

# Rule 1: Analyze work sessions for duration and patterns
work_session_analysis_rule = %{
  id: :analyze_work_sessions,
  conditions: [
    {:work_session, :id, :data}
  ],
  action: fn facts ->
    data = facts[:data]
    id = facts[:id]

    # Calculate work session metrics
    if Map.has_key?(data, :start_datetime) and Map.has_key?(data, :end_datetime) do
      start_dt = data.start_datetime
      end_dt = data.end_datetime

      total_hours = DateTime.diff(end_dt, start_dt, :second) / 3600.0

      # Calculate consecutive hours without meal break
      consecutive_hours =
        if Map.get(data, :meal_breaks_taken, 0) == 0 and total_hours > 5 do
          total_hours
        else
          0.0
        end

      analysis_data = Map.merge(data, %{
        total_hours: Float.round(total_hours, 2),
        consecutive_hours_without_meal: Float.round(consecutive_hours, 2),
        analyzed_at: DateTime.utc_now()
      })

      IO.puts(
        "  ✓ Analyzed work session: #{Float.round(total_hours, 1)}h for employee #{data.employee_id}"
      )

      [
        {:work_session, id, analysis_data},
        {:analyzed_work_session, id, analysis_data}
      ]
    else
      []
    end
  end,
  priority: 100
}

# Define jurisdiction-specific thresholds
california_thresholds = %{
  consecutive_hours_threshold: 6,
  extended_day_threshold: 10,
  spike_break_duration: 15,
  extended_break_duration: 10
}

bay_area_thresholds =
  Map.merge(california_thresholds, %{
    tech_crunch_break_interval: 1,
    tech_crunch_break_duration: 5
  })

# Rule 2: Detect consecutive work spike break requirements
consecutive_work_rule = %{
  id: :detect_consecutive_work_breaks,
  conditions: [
    {:analyzed_work_session, :id, :data}
  ],
  action: fn facts ->
    data = facts[:data]
    id = facts[:id]

    consecutive_hours = data.consecutive_hours_without_meal

    if consecutive_hours >= california_thresholds.consecutive_hours_threshold do
      requirement_data = %{
        type: :consecutive_work,
        employee_id: data.employee_id,
        work_session_id: id,
        required_duration_minutes: california_thresholds.spike_break_duration,
        required_by: DateTime.add(data.start_datetime, 5 * 3600, :second),
        reason:
          "Consecutive work exceeds #{california_thresholds.consecutive_hours_threshold} hours",
        created_at: DateTime.utc_now()
      }

      requirement_id = "consecutive_#{data.employee_id}_#{DateTime.to_unix(data.start_datetime)}"

      IO.puts(
        "  ✓ Consecutive work spike break required for #{data.employee_id} (#{Float.round(consecutive_hours, 1)}h)"
      )

      [{:spike_break_requirement, requirement_id, requirement_data}]
    else
      []
    end
  end,
  priority: 90
}

# Rule 3: Detect extended day spike break requirements
extended_day_rule = %{
  id: :detect_extended_day_breaks,
  conditions: [
    {:analyzed_work_session, :id, :data}
  ],
  action: fn facts ->
    data = facts[:data]
    id = facts[:id]

    total_hours = data.total_hours

    if total_hours > california_thresholds.extended_day_threshold do
      extra_hours = total_hours - california_thresholds.extended_day_threshold
      # One break per 2 extra hours
      extra_breaks = ceil(extra_hours / 2)

      requirements =
        Enum.map(1..extra_breaks, fn i ->
          requirement_data = %{
            type: :extended_day,
            employee_id: data.employee_id,
            work_session_id: id,
            required_duration_minutes: california_thresholds.extended_break_duration,
            required_by: DateTime.add(data.start_datetime, 10 * 3600, :second),
            reason: "Extended day break #{i} for #{Float.round(total_hours, 1)} hour day",
            created_at: DateTime.utc_now()
          }

          requirement_id =
            "extended_#{data.employee_id}_#{i}_#{DateTime.to_unix(data.start_datetime)}"

          {:spike_break_requirement, requirement_id, requirement_data}
        end)

      IO.puts(
        "  ✓ Extended day spike breaks required for #{data.employee_id} (#{extra_breaks} breaks)"
      )

      requirements
    else
      []
    end
  end,
  priority: 89
}

# Rule 4: Detect Bay Area tech crunch spike break requirements
bay_area_tech_rule = %{
  id: :detect_bay_area_tech_breaks,
  conditions: [
    {:analyzed_work_session, :id, :data},
    {:jurisdiction, :jurisdiction_id, :jurisdiction_data}
  ],
  action: fn facts ->
    data = facts[:data]
    id = facts[:id]
    jurisdiction_data = facts[:jurisdiction_data]

    # Check if this is Bay Area tech jurisdiction and crunch time
    if jurisdiction_data.region == "bay_area" and
         data.industry == "technology" and
         Map.get(data, :is_crunch_time, false) do
      total_hours = data.total_hours
      breaks_needed = floor(total_hours / bay_area_thresholds.tech_crunch_break_interval)

      requirements =
        Enum.map(1..breaks_needed, fn i ->
          requirement_data = %{
            type: :bay_area_tech_crunch,
            employee_id: data.employee_id,
            work_session_id: id,
            required_duration_minutes: bay_area_thresholds.tech_crunch_break_duration,
            required_by: DateTime.add(data.start_datetime, i * 3600, :second),
            reason: "Bay Area tech crunch break #{i}",
            created_at: DateTime.utc_now()
          }

          requirement_id =
            "bay_area_tech_#{data.employee_id}_#{i}_#{DateTime.to_unix(data.start_datetime)}"

          {:spike_break_requirement, requirement_id, requirement_data}
        end)

      if breaks_needed > 0 do
        IO.puts(
          "  ✓ Bay Area tech crunch spike breaks required for #{data.employee_id} (#{breaks_needed} breaks)"
        )
      end

      requirements
    else
      []
    end
  end,
  priority: 88
}

# Rule 5: Check compliance with spike break requirements
compliance_checking_rule = %{
  id: :check_spike_break_compliance,
  conditions: [
    {:spike_break_requirement, :req_id, :req_data}
  ],
  action: fn facts ->
    req_data = facts[:req_data]
    req_id = facts[:req_id]

    # For this example, assume no breaks were taken (non-compliant)
    # In a real system, this would check against actual break data
    penalty_hours =
      case req_data.type do
        # Enhanced penalty for tech crunch
        :bay_area_tech_crunch -> 1.5
        :extended_day -> 1.25
        :consecutive_work -> 1.0
        _ -> 1.0
      end

    compliance_data = %{
      requirement_id: req_id,
      requirement: req_data,
      status: :non_compliant,
      matching_breaks: [],
      penalty_hours: penalty_hours,
      checked_at: DateTime.utc_now()
    }

    IO.puts(
      "  ✗ Compliance violation: #{req_data.employee_id} - #{req_data.type} (#{penalty_hours}h penalty)"
    )

    [{:spike_break_compliance, req_id, compliance_data}]
  end,
  priority: 80
}

# 3. Add rules to the engine
IO.puts("\nAdding rules to engine...")
:ok = Presto.RuleEngine.add_rule(engine, work_session_analysis_rule)
:ok = Presto.RuleEngine.add_rule(engine, consecutive_work_rule)
:ok = Presto.RuleEngine.add_rule(engine, extended_day_rule)
:ok = Presto.RuleEngine.add_rule(engine, bay_area_tech_rule)
:ok = Presto.RuleEngine.add_rule(engine, compliance_checking_rule)

# 4. Create sample data
IO.puts("\nCreating sample work sessions and jurisdiction data...")

# Sample work sessions with different scenarios
work_sessions = [
  # Employee 1: Tech worker in Bay Area during crunch time (long day + crunch)
  {:work_session, "session_1",
   %{
     employee_id: "emp_001",
     start_datetime: ~U[2025-01-15 08:00:00Z],
     # 12 hours
     end_datetime: ~U[2025-01-15 20:00:00Z],
     industry: "technology",
     # Triggers consecutive work break
     meal_breaks_taken: 0,
     # Triggers tech crunch breaks
     is_crunch_time: true
   }},

  # Employee 2: Entertainment worker in LA (extended day)
  {:work_session, "session_2",
   %{
     employee_id: "emp_002",
     start_datetime: ~U[2025-01-15 09:00:00Z],
     # 13 hours
     end_datetime: ~U[2025-01-15 22:00:00Z],
     industry: "entertainment",
     # Has meal break, so no consecutive work violation
     meal_breaks_taken: 1,
     is_peak_production: true
   }},

  # Employee 3: Agriculture worker in Central Valley (harvest season)
  {:work_session, "session_3",
   %{
     employee_id: "emp_003",
     # June (harvest season)
     start_datetime: ~U[2025-06-15 06:00:00Z],
     # 11 hours
     end_datetime: ~U[2025-06-15 17:00:00Z],
     industry: "agriculture",
     meal_breaks_taken: 1
   }},

  # Employee 4: Retail worker (normal hours, should be compliant)
  {:work_session, "session_4",
   %{
     employee_id: "emp_004",
     start_datetime: ~U[2025-01-15 10:00:00Z],
     # 7 hours
     end_datetime: ~U[2025-01-15 17:00:00Z],
     industry: "retail",
     meal_breaks_taken: 1
   }}
]

# Jurisdiction definitions
bay_area_jurisdiction =
  {:jurisdiction, "bay_area",
   %{
     region: "bay_area",
     city: "san_francisco",
     industry: "technology"
   }}

IO.puts("Sample data: #{length(work_sessions)} work sessions")

# 5. Assert facts to working memory
IO.puts("\nAsserting work sessions...")

Enum.each(work_sessions, fn work_session ->
  :ok = Presto.RuleEngine.assert_fact(engine, work_session)
end)

IO.puts("Asserting jurisdiction data...")
:ok = Presto.RuleEngine.assert_fact(engine, bay_area_jurisdiction)

# 6. Fire rules and process compliance
IO.puts("\nFiring spike break compliance rules:")
results = Presto.RuleEngine.fire_rules(engine)

# 7. Analyze results
IO.puts("\nProcessing Results:")

# Extract different types of results
analyzed_sessions =
  Enum.filter(results, fn
    {:work_session, _, %{analyzed_at: analyzed_at}} when not is_nil(analyzed_at) -> true
    _ -> false
  end)

spike_requirements =
  Enum.filter(results, fn
    {:spike_break_requirement, _, _} -> true
    _ -> false
  end)

compliance_results =
  Enum.filter(results, fn
    {:spike_break_compliance, _, _} -> true
    _ -> false
  end)

IO.puts("  → Analyzed #{length(analyzed_sessions)} work sessions")
IO.puts("  → Generated #{length(spike_requirements)} spike break requirements")
IO.puts("  → Created #{length(compliance_results)} compliance results")

# 8. Calculate summary statistics
total_employees =
  analyzed_sessions
  |> Enum.map(fn {:work_session, _, %{employee_id: emp_id}} -> emp_id end)
  |> Enum.uniq()
  |> length()

total_work_hours =
  analyzed_sessions
  |> Enum.map(fn {:work_session, _, %{total_hours: hours}} -> hours end)
  |> Enum.sum()
  |> then(&(&1 + 0.0))
  |> Float.round(2)

violations = length(compliance_results)

total_penalty_hours =
  compliance_results
  |> Enum.map(fn {:spike_break_compliance, _, %{penalty_hours: hours}} -> hours end)
  |> Enum.sum()
  |> then(&(&1 + 0.0))
  |> Float.round(2)

compliance_rate =
  if length(spike_requirements) > 0 do
    compliant_count = length(spike_requirements) - violations
    (compliant_count / length(spike_requirements) * 100) |> Float.round(1)
  else
    100.0
  end

# 9. Display summary
IO.puts("\n=== California Spike Break Compliance Summary ===")
IO.puts("Total Employees: #{total_employees}")
IO.puts("Total Work Hours: #{total_work_hours}")
IO.puts("Spike Break Requirements: #{length(spike_requirements)}")
IO.puts("Compliance Violations: #{violations}")
IO.puts("Total Penalty Hours: #{total_penalty_hours}")
IO.puts("Compliance Rate: #{compliance_rate}%")

# 10. Show detailed spike break requirements
if length(spike_requirements) > 0 do
  IO.puts("\n=== Spike Break Requirements Details ===")

  Enum.each(spike_requirements, fn {:spike_break_requirement, _req_id, data} ->
    IO.puts("  #{data.employee_id}: #{data.type} - #{data.required_duration_minutes} min")
    IO.puts("    → #{data.reason}")
  end)
end

# 11. Show compliance violations
if length(compliance_results) > 0 do
  IO.puts("\n=== Compliance Violations ===")

  Enum.each(compliance_results, fn {:spike_break_compliance, _, data} ->
    req = data.requirement
    IO.puts("  ✗ #{req.employee_id}: #{req.type} violation")
    IO.puts("    → Penalty: #{data.penalty_hours}h")
    IO.puts("    → Required: #{req.required_duration_minutes} min break")
  end)
end

# 12. Demonstrate different jurisdictions
IO.puts("\n" <> String.duplicate("=", 50))
IO.puts("MULTI-JURISDICTION DEMO")
IO.puts(String.duplicate("=", 50))

# Start new engines for different jurisdictions
{:ok, la_engine} = Presto.RuleEngine.start_link()
{:ok, central_valley_engine} = Presto.RuleEngine.start_link()

# LA County jurisdiction (entertainment industry)
la_county_jurisdiction =
  {:jurisdiction, "la_county",
   %{
     region: "los_angeles_county",
     city: "los_angeles",
     industry: "entertainment"
   }}

# Central Valley jurisdiction (agriculture)
central_valley_jurisdiction =
  {:jurisdiction, "central_valley",
   %{
     region: "central_valley",
     city: "fresno",
     industry: "agriculture"
   }}

# Add basic rules to both engines (skip jurisdiction-specific rules for simplicity)
basic_rules = [
  work_session_analysis_rule,
  consecutive_work_rule,
  extended_day_rule,
  compliance_checking_rule
]

Enum.each(basic_rules, fn rule ->
  :ok = Presto.RuleEngine.add_rule(la_engine, rule)
  :ok = Presto.RuleEngine.add_rule(central_valley_engine, rule)
end)

# Process LA County
IO.puts("\nProcessing LA County Entertainment jurisdiction...")

Enum.each(work_sessions, fn work_session ->
  :ok = Presto.RuleEngine.assert_fact(la_engine, work_session)
end)

:ok = Presto.RuleEngine.assert_fact(la_engine, la_county_jurisdiction)

la_results = Presto.RuleEngine.fire_rules(la_engine)

la_violations =
  Enum.count(la_results, fn
    {:spike_break_compliance, _, _} -> true
    _ -> false
  end)

IO.puts("LA County Results: #{la_violations} violations")

# Process Central Valley  
IO.puts("Processing Central Valley Agriculture jurisdiction...")

Enum.each(work_sessions, fn work_session ->
  :ok = Presto.RuleEngine.assert_fact(central_valley_engine, work_session)
end)

:ok = Presto.RuleEngine.assert_fact(central_valley_engine, central_valley_jurisdiction)

cv_results = Presto.RuleEngine.fire_rules(central_valley_engine)

cv_violations =
  Enum.count(cv_results, fn
    {:spike_break_compliance, _, _} -> true
    _ -> false
  end)

IO.puts("Central Valley Results: #{cv_violations} violations")

# 13. Check engine statistics
engine_stats = Presto.RuleEngine.get_engine_statistics(engine)
IO.puts("\n=== Engine Statistics ===")
IO.puts("Rules: #{engine_stats.total_rules}")
IO.puts("Facts: #{engine_stats.total_facts}")
IO.puts("Rule Firings: #{engine_stats.total_rule_firings}")

# 14. Clean up
GenServer.stop(engine)
GenServer.stop(la_engine)
GenServer.stop(central_valley_engine)

IO.puts("\n=== California Spike Break Compliance Example Complete ===")

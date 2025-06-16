defmodule Presto.Examples.ComplianceRules do
  @moduledoc """
  Compliance rules engine implementation for Presto.

  This example demonstrates how to use the Presto RETE engine for compliance
  monitoring and violation detection. It implements a comprehensive compliance
  system for tracking weekly working hour limits and regulatory requirements.

  Features implemented using the RETE engine:
  1. Time entry processing and duration calculation
  2. Weekly aggregation by Monday-Sunday weeks
  3. Compliance threshold checking against regulations
  4. Violation detection and reporting
  5. Multi-stage compliance workflow processing

  This example demonstrates:
  - Using the actual Presto RETE engine for rule processing
  - Pattern matching on facts to trigger rule execution
  - Incremental processing as new facts are asserted
  - Complex compliance workflows with the engine
  - Proper fact structures for the RETE algorithm
  """

  @behaviour Presto.RuleBehaviour

  @type time_entry :: {:time_entry, any(), map()}
  @type weekly_hours :: {:weekly_hours, any(), map()}
  @type compliance_result :: {:compliance_result, any(), map()}
  @type rule_variables :: map()
  @type rule_spec :: map()
  @type compliance_output :: %{
          weekly_aggregations: [weekly_hours()],
          compliance_results: [compliance_result()],
          summary: map()
        }

  @doc """
  Creates compliance rules for the Presto RETE engine.

  Returns a list of rule definitions that can be added to a Presto engine.
  Each rule uses pattern matching on facts and creates new facts when conditions are met.

  The rule_spec can include:
  - variables: Configuration variables for compliance processing
  - max_weekly_hours: Maximum allowed weekly hours (default 48.0)

  ## Example rule_spec:

      %{
        "variables" => %{
          "max_weekly_hours" => 40.0,
          "compliance_buffer" => 2.0
        }
      }
  """
  @spec create_rules(rule_spec()) :: [map()]
  def create_rules(rule_spec) do
    variables = extract_variables(rule_spec)

    [
      time_processing_rule(),
      weekly_aggregation_rule(),
      weekly_compliance_rule(variables)
    ]
  end

  @doc """
  Rule 1: Process time entries and calculate durations.

  Uses RETE pattern matching to identify time entries that need duration calculation.
  This rule triggers when time entry facts are asserted into working memory.
  """
  @spec time_processing_rule() :: map()
  def time_processing_rule do
    %{
      id: :process_time_entries,
      conditions: [
        {:time_entry, :id, :data}
      ],
      action: fn facts ->
        data = facts[:data]
        id = facts[:id]

        # Process time entries that need calculation
        if Map.has_key?(data, :start_datetime) and Map.has_key?(data, :finish_datetime) and
             Map.get(data, :units) == nil do
          minutes = DateTime.diff(data.finish_datetime, data.start_datetime, :second) / 60.0
          units = minutes / 60.0

          updated_data = %{
            data
            | minutes: Float.round(minutes, 2),
              units: Float.round(units, 2)
          }

          # Return both the updated entry and a processed marker
          [
            {:time_entry, id, updated_data},
            {:processed_time_entry, id, updated_data}
          ]
        else
          # Return empty list if no processing needed
          []
        end
      end,
      priority: 100
    }
  end

  @doc """
  Rule 2: Aggregate processed time entries into weekly totals.

  Uses RETE pattern matching to find processed time entries for weekly aggregation.
  Triggers when processed time entry facts are available.
  """
  @spec weekly_aggregation_rule() :: map()
  def weekly_aggregation_rule do
    %{
      id: :weekly_hours_aggregation,
      conditions: [
        {:processed_time_entry, :id, :data}
      ],
      action: fn facts ->
        data = facts[:data]
        employee_id = data.employee_id
        start_dt = data.start_datetime
        units = data.units

        # Calculate week boundaries
        date = DateTime.to_date(start_dt)
        week_start = week_start_date(date)
        week_end = Date.add(week_start, 6)

        # Create a weekly aggregation fact
        weekly_data = %{
          employee_id: employee_id,
          week_start: week_start,
          week_end: week_end,
          total_hours: units,
          entries: [facts[:id]],
          aggregated_at: DateTime.utc_now()
        }

        [{:weekly_hours, {employee_id, week_start}, weekly_data}]
      end,
      priority: 90
    }
  end

  @doc """
  Rule 3: Check weekly compliance against thresholds.

  Uses RETE pattern matching to find weekly aggregations that need compliance checking.
  Triggers when weekly hour aggregations exceed compliance thresholds.
  """
  @spec weekly_compliance_rule(rule_variables()) :: map()
  def weekly_compliance_rule(variables \\ %{}) do
    max_weekly_hours = Map.get(variables, "max_weekly_hours", 48.0)

    %{
      id: :weekly_compliance_check,
      conditions: [
        {:weekly_hours, :key, :data}
      ],
      action: fn facts ->
        data = facts[:data]
        key = facts[:key]
        {employee_id, week_start} = key

        total_hours = data.total_hours

        # Check if compliance threshold is exceeded
        {status, reason} =
          if total_hours > max_weekly_hours do
            {:non_compliant,
             "Exceeded maximum weekly hours (#{total_hours} > #{max_weekly_hours})"}
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

        [{:compliance_result, key, compliance_data}]
      end,
      priority: 80
    }
  end

  @doc """
  Processes time entries using the Presto RETE engine.

  This function demonstrates how to use the actual Presto engine with compliance rules:
  1. Start the engine
  2. Add compliance processing rules
  3. Assert initial facts (time entries)
  4. Fire rules to process the data
  5. Extract results from working memory

  This showcases the incremental processing capabilities of the RETE algorithm
  as facts are asserted and rules fire in response to pattern matches.
  """
  @spec process_with_engine([time_entry()], rule_spec()) :: compliance_output()
  def process_with_engine(time_entries, rule_spec \\ %{}) do
    # Start the Presto RETE engine
    {:ok, engine} = Presto.start_engine()

    try do
      # Create and add compliance rules to the engine
      variables = extract_variables(rule_spec)

      rules = [
        time_processing_rule(),
        weekly_aggregation_rule(),
        weekly_compliance_rule(variables)
      ]

      Enum.each(rules, &Presto.add_rule(engine, &1))

      # Assert initial facts into working memory
      Enum.each(time_entries, fn {:time_entry, id, data} ->
        Presto.assert_fact(engine, {:time_entry, id, data})
      end)

      # Fire rules to process the data through the RETE network
      rule_results = Presto.fire_rules(engine, concurrent: true)

      # Process aggregations manually since rules don't auto-aggregate in this engine
      max_weekly_hours = Map.get(variables, "max_weekly_hours", 48.0)
      aggregated_results = process_weekly_aggregations(rule_results, max_weekly_hours)

      all_results = rule_results ++ aggregated_results
      extract_compliance_results(all_results)
    after
      # Clean up the engine
      Presto.stop_engine(engine)
    end
  end

  @doc """
  Processes time entries through the complete compliance workflow.

  This is the original implementation that processes data directly without the RETE engine.
  For demonstrations of the actual RETE engine, use process_with_engine/2.
  """
  @spec process_compliance_check([time_entry()], rule_spec()) :: compliance_output()
  def process_compliance_check(time_entries, rule_spec \\ %{}) do
    variables = extract_variables(rule_spec)
    max_weekly_hours = Map.get(variables, "max_weekly_hours", 48.0)

    # Step 0: Process raw time entries to calculate units if needed
    processed_entries = process_raw_time_entries(time_entries)

    # Step 1: Aggregate entries into weekly totals
    weekly_aggregations = aggregate_entries_by_week(processed_entries)

    # Step 2: Check compliance for each weekly aggregation
    compliance_results =
      weekly_aggregations
      |> Enum.map(&check_weekly_compliance(&1, max_weekly_hours))

    # Step 3: Generate summary
    summary = generate_compliance_summary(weekly_aggregations, compliance_results)

    %{
      weekly_aggregations: weekly_aggregations,
      compliance_results: compliance_results,
      summary: summary
    }
  end

  @doc """
  Aggregates time entries into weekly totals grouped by Monday-Sunday weeks.
  """
  def aggregate_entries_by_week(time_entries) do
    time_entries
    |> group_by_employee_and_week()
    |> create_weekly_aggregations()
  end

  @doc """
  Checks a single weekly aggregation for compliance violations.
  """
  def check_weekly_compliance(weekly_aggregation, max_weekly_hours) do
    {:weekly_hours, {employee_id, week_start}, %{total_hours: total_hours} = data} =
      weekly_aggregation

    status = if total_hours > max_weekly_hours, do: :non_compliant, else: :compliant

    reason = generate_compliance_reason(status, total_hours, max_weekly_hours)

    {:compliance_result, {employee_id, week_start},
     %{
       employee_id: employee_id,
       week_start: week_start,
       week_end: data.week_end,
       status: status,
       threshold: max_weekly_hours,
       actual_value: total_hours,
       reason: reason,
       checked_at: DateTime.utc_now()
     }}
  end

  @doc """
  Determines the Monday start date for a given date.
  """
  def week_start_date(date) do
    days_since_monday = Date.day_of_week(date) - 1
    Date.add(date, -days_since_monday)
  end

  @doc """
  Returns all dates in a week (Monday through Sunday) given the Monday start.
  """
  def week_dates(monday_start) do
    0..6
    |> Enum.map(&Date.add(monday_start, &1))
  end

  @doc """
  Validates entries span across week boundaries correctly.
  """
  def validate_week_boundaries(time_entries) do
    time_entries
    |> Enum.map(fn
      {:time_entry, _, %{start_datetime: start_dt}} ->
        date = DateTime.to_date(start_dt)
        week_start = week_start_date(date)
        {date, week_start, Date.day_of_week(week_start)}

      {:processed_time_entry, _, %{start_datetime: start_dt}} ->
        date = DateTime.to_date(start_dt)
        week_start = week_start_date(date)
        {date, week_start, Date.day_of_week(week_start)}
    end)
    # Monday is 1
    |> Enum.all?(fn {_, _, day_of_week} -> day_of_week == 1 end)
  end

  # Private functions for rule implementation

  defp group_by_employee_and_week(time_entries) do
    time_entries
    |> Enum.group_by(fn
      {:time_entry, _, %{employee_id: emp_id, start_datetime: start_dt}} ->
        date = DateTime.to_date(start_dt)
        week_start = week_start_date(date)
        {emp_id, week_start}

      {:processed_time_entry, _, %{employee_id: emp_id, start_datetime: start_dt}} ->
        date = DateTime.to_date(start_dt)
        week_start = week_start_date(date)
        {emp_id, week_start}
    end)
  end

  defp create_weekly_aggregations(grouped_entries) do
    grouped_entries
    |> Enum.map(fn {{emp_id, week_start}, entries} ->
      total_hours = calculate_total_hours(entries)
      # Sunday
      week_end = Date.add(week_start, 6)
      entry_ids = extract_entry_ids(entries)

      {:weekly_hours, {emp_id, week_start},
       %{
         employee_id: emp_id,
         week_start: week_start,
         week_end: week_end,
         total_hours: Float.round(total_hours * 1.0, 2),
         entries: entry_ids,
         aggregated_at: DateTime.utc_now()
       }}
    end)
  end

  defp calculate_total_hours(entries) do
    entries
    |> Enum.map(fn
      {:time_entry, _, %{units: units}} when is_number(units) -> units
      {:time_entry, _, _} -> 0
      {:processed_time_entry, _, %{units: units}} when is_number(units) -> units
      {:processed_time_entry, _, _} -> 0
    end)
    |> Enum.sum()
  end

  defp extract_entry_ids(entries) do
    entries
    |> Enum.map(fn
      {:time_entry, id, _} -> id
      {:processed_time_entry, id, _} -> id
    end)
  end

  @spec process_raw_time_entries([time_entry()]) :: [time_entry()]
  defp process_raw_time_entries(time_entries) do
    time_entries
    |> Enum.map(fn
      {:time_entry, id,
       %{start_datetime: start_dt, finish_datetime: finish_dt, units: nil} = data} ->
        # Calculate duration for entries that don't have units calculated
        minutes = DateTime.diff(finish_dt, start_dt, :second) / 60.0
        units = minutes / 60.0

        updated_data =
          data
          |> Map.put(:minutes, Float.round(minutes, 2))
          |> Map.put(:units, Float.round(units, 2))

        {:time_entry, id, updated_data}

      # Return entries that already have units calculated
      entry ->
        entry
    end)
  end

  defp generate_compliance_reason(:compliant, actual_hours, threshold) do
    "Within weekly hours limit (#{actual_hours} <= #{threshold})"
  end

  defp generate_compliance_reason(:non_compliant, actual_hours, threshold) do
    "Exceeded maximum weekly hours (#{actual_hours} > #{threshold})"
  end

  defp generate_compliance_summary(weekly_aggregations, compliance_results) do
    total_weeks = length(weekly_aggregations)

    total_employees =
      weekly_aggregations
      |> Enum.map(fn {:weekly_hours, {emp_id, _}, _} -> emp_id end)
      |> Enum.uniq()
      |> length()

    violations =
      compliance_results
      |> Enum.count(fn {:compliance_result, _, %{status: status}} -> status == :non_compliant end)

    employees_with_violations =
      compliance_results
      |> Enum.filter(fn {:compliance_result, _, %{status: status}} ->
        status == :non_compliant
      end)
      |> Enum.map(fn {:compliance_result, {emp_id, _}, _} -> emp_id end)
      |> Enum.uniq()
      |> length()

    compliance_rate =
      if total_weeks > 0,
        do: ((total_weeks - violations) / total_weeks * 100) |> Float.round(1),
        else: 100.0

    %{
      total_weeks_checked: total_weeks,
      total_employees: total_employees,
      compliance_violations: violations,
      employees_with_violations: employees_with_violations,
      compliance_rate_percent: compliance_rate,
      summary_generated_at: DateTime.utc_now()
    }
  end

  @spec extract_variables(rule_spec()) :: rule_variables()
  defp extract_variables(rule_spec) do
    case rule_spec do
      # Handle nested JSON format from integration tests
      %{"rules" => rules} when is_list(rules) ->
        rules
        |> Enum.flat_map(fn rule -> Map.get(rule, "variables", %{}) |> Map.to_list() end)
        |> Map.new()

      # Handle direct format
      %{"variables" => variables} ->
        variables

      # Default empty variables
      _ ->
        %{}
    end
  end

  @doc """
  Validates a compliance rule specification.

  Checks that the rule specification contains the required structure
  for compliance processing and has valid variable types.
  """
  @spec valid_rule_spec?(rule_spec()) :: boolean()
  def valid_rule_spec?(rule_spec) do
    case rule_spec do
      # Handle rules_to_run format
      %{"rules_to_run" => rules, "variables" => variables}
      when is_list(rules) and is_map(variables) ->
        validate_rules_format(rules, variables)

      # Handle new JSON format
      %{"rules" => rules} when is_list(rules) ->
        validate_json_format(rules)

      # Invalid format
      _ ->
        false
    end
  end

  defp validate_rules_format(rules, variables) do
    # Check that we have valid compliance rule names
    valid_compliance_rules = ["weekly_compliance"]
    compliance_rules_present = Enum.any?(rules, &(&1 in valid_compliance_rules))

    # Check that variables have valid types for compliance processing
    valid_variables =
      case variables do
        %{"max_weekly_hours" => hours} when is_number(hours) and hours > 0 -> true
        # Empty variables are ok
        %{} -> true
      end

    compliance_rules_present && valid_variables
  end

  defp validate_json_format(rules) do
    Enum.empty?(rules) or Enum.all?(rules, &valid_compliance_rule?/1)
  end

  defp valid_compliance_rule?(%{"name" => name, "type" => "compliance"}) when is_binary(name),
    do: true

  defp valid_compliance_rule?(_), do: false

  @doc """
  Finds all compliance violations within a date range.
  """
  def find_violations_in_range(compliance_results, start_date, end_date) do
    compliance_results
    |> Enum.filter(fn {:compliance_result, {_, week_start}, %{status: status}} ->
      status == :non_compliant and
        Date.compare(week_start, start_date) != :lt and
        Date.compare(week_start, end_date) != :gt
    end)
  end

  @doc """
  Groups compliance results by employee for reporting.
  """
  def group_compliance_by_employee(compliance_results) do
    compliance_results
    |> Enum.group_by(fn {:compliance_result, {emp_id, _}, _} -> emp_id end)
  end

  @doc """
  Calculates compliance statistics for an employee.
  """
  def employee_compliance_stats(compliance_results, employee_id) do
    employee_results =
      compliance_results
      |> Enum.filter(fn
        {:compliance_result, {^employee_id, _}, _} -> true
        _ -> false
      end)

    total_weeks = length(employee_results)

    violations =
      Enum.count(employee_results, fn {:compliance_result, _, %{status: status}} ->
        status == :non_compliant
      end)

    compliance_rate =
      if total_weeks > 0,
        do: ((total_weeks - violations) / total_weeks * 100) |> Float.round(1),
        else: 100.0

    %{
      employee_id: employee_id,
      total_weeks_checked: total_weeks,
      compliance_violations: violations,
      compliance_rate_percent: compliance_rate
    }
  end

  @doc """
  Example data generator for testing and demonstration.
  """
  def generate_example_data do
    time_entries = [
      {:time_entry, "shift_1",
       %{
         employee_id: "emp_001",
         # Monday
         start_datetime: ~U[2025-01-13 09:00:00Z],
         finish_datetime: ~U[2025-01-13 17:00:00Z],
         units: nil
       }},
      {:time_entry, "shift_2",
       %{
         employee_id: "emp_001",
         # Tuesday
         start_datetime: ~U[2025-01-14 09:00:00Z],
         finish_datetime: ~U[2025-01-14 17:00:00Z],
         units: nil
       }},
      {:time_entry, "shift_3",
       %{
         employee_id: "emp_001",
         # Wednesday
         start_datetime: ~U[2025-01-15 09:00:00Z],
         finish_datetime: ~U[2025-01-15 17:00:00Z],
         units: nil
       }},
      {:time_entry, "shift_4",
       %{
         employee_id: "emp_001",
         # Thursday
         start_datetime: ~U[2025-01-16 09:00:00Z],
         finish_datetime: ~U[2025-01-16 17:00:00Z],
         units: nil
       }},
      {:time_entry, "shift_5",
       %{
         employee_id: "emp_001",
         # Friday
         start_datetime: ~U[2025-01-17 09:00:00Z],
         finish_datetime: ~U[2025-01-17 17:00:00Z],
         units: nil
       }},
      {:time_entry, "shift_6",
       %{
         employee_id: "emp_001",
         # Saturday
         start_datetime: ~U[2025-01-18 09:00:00Z],
         finish_datetime: ~U[2025-01-18 17:00:00Z],
         units: nil
       }},
      {:time_entry, "shift_7",
       %{
         employee_id: "emp_002",
         # Monday
         start_datetime: ~U[2025-01-13 08:00:00Z],
         # 12 hours
         finish_datetime: ~U[2025-01-13 20:00:00Z],
         units: nil
       }},
      {:time_entry, "shift_8",
       %{
         employee_id: "emp_002",
         # Tuesday
         start_datetime: ~U[2025-01-14 08:00:00Z],
         # 12 hours
         finish_datetime: ~U[2025-01-14 20:00:00Z],
         units: nil
       }},
      {:time_entry, "shift_9",
       %{
         employee_id: "emp_002",
         # Wednesday
         start_datetime: ~U[2025-01-15 08:00:00Z],
         # 12 hours
         finish_datetime: ~U[2025-01-15 20:00:00Z],
         units: nil
       }},
      {:time_entry, "shift_10",
       %{
         employee_id: "emp_002",
         # Thursday
         start_datetime: ~U[2025-01-16 08:00:00Z],
         # 12 hours
         finish_datetime: ~U[2025-01-16 20:00:00Z],
         units: nil
       }}
    ]

    {time_entries}
  end

  @doc """
  Example rule specification for compliance processing.
  """
  def generate_example_rule_spec do
    %{
      "variables" => %{
        "max_weekly_hours" => 40.0,
        "compliance_buffer" => 2.0
      }
    }
  end

  @doc """
  Demonstrates the Presto RETE engine with compliance rules.
  """
  def run_example do
    IO.puts("=== Presto RETE Engine Compliance Example ===\n")

    {time_entries} = generate_example_data()

    IO.puts("Input Data:")
    IO.puts("Time Entries: #{length(time_entries)} entries")

    IO.puts("\nProcessing with Presto RETE engine...")
    result = process_with_engine(time_entries)

    IO.puts("\nResults from RETE Engine:")
    print_engine_results(result)

    result
  end

  @doc """
  Demonstrates custom compliance configuration with the RETE engine.
  """
  def run_custom_example do
    IO.puts("=== Custom Compliance Rules with RETE Engine ===\n")

    {time_entries} = generate_example_data()
    rule_spec = generate_example_rule_spec()

    IO.puts("Custom Rule Specification:")
    IO.puts("  Max Weekly Hours: #{rule_spec["variables"]["max_weekly_hours"]} hours")
    IO.puts("  Compliance Buffer: #{rule_spec["variables"]["compliance_buffer"]} hours")

    IO.puts("\nProcessing with custom rules using RETE engine...")
    result = process_with_engine(time_entries, rule_spec)

    IO.puts("\nCustom Compliance Results:")
    print_engine_results(result)

    result
  end

  # Helper functions for RETE engine processing

  defp process_weekly_aggregations(rule_results, max_weekly_hours) do
    # Find processed time entries
    processed_entries =
      rule_results
      |> Enum.filter(fn
        {:processed_time_entry, _, _} -> true
        _ -> false
      end)

    # Group by employee and week, then aggregate
    processed_entries
    |> Enum.group_by(fn {:processed_time_entry, _, data} ->
      employee_id = data.employee_id
      start_dt = data.start_datetime
      date = DateTime.to_date(start_dt)
      week_start = week_start_date(date)
      {employee_id, week_start}
    end)
    |> Enum.flat_map(fn {{employee_id, week_start}, entries} ->
      # Calculate total hours for this employee/week
      total_hours =
        entries
        |> Enum.map(fn {:processed_time_entry, _, data} -> data.units end)
        |> Enum.sum()
        |> Float.round(2)

      week_end = Date.add(week_start, 6)

      # Create weekly aggregation
      weekly_data = %{
        employee_id: employee_id,
        week_start: week_start,
        week_end: week_end,
        total_hours: total_hours,
        entries: Enum.map(entries, fn {:processed_time_entry, id, _} -> id end),
        aggregated_at: DateTime.utc_now()
      }

      # Create compliance result
      status = if total_hours > max_weekly_hours, do: :non_compliant, else: :compliant

      reason =
        if status == :non_compliant do
          "Exceeded maximum weekly hours (#{total_hours} > #{max_weekly_hours})"
        else
          "Within weekly hours limit (#{total_hours} <= #{max_weekly_hours})"
        end

      compliance_data = %{
        employee_id: employee_id,
        week_start: week_start,
        week_end: week_end,
        status: status,
        threshold: max_weekly_hours,
        actual_value: total_hours,
        reason: reason,
        checked_at: DateTime.utc_now()
      }

      [
        {:weekly_hours, {employee_id, week_start}, weekly_data},
        {:compliance_result, {employee_id, week_start}, compliance_data}
      ]
    end)
  end

  defp extract_compliance_results(all_facts) do
    weekly_aggregations = extract_facts_by_type(all_facts, :weekly_hours)
    compliance_results = extract_facts_by_type(all_facts, :compliance_result)

    summary = generate_engine_summary(weekly_aggregations, compliance_results)

    %{
      weekly_aggregations: weekly_aggregations,
      compliance_results: compliance_results,
      summary: summary
    }
  end

  defp extract_facts_by_type(facts, type) do
    facts
    |> Enum.filter(fn
      {^type, _, _} -> true
      _ -> false
    end)
  end

  defp generate_engine_summary(weekly_aggregations, compliance_results) do
    total_weeks = length(weekly_aggregations)

    total_employees =
      weekly_aggregations
      |> Enum.map(fn {:weekly_hours, {emp_id, _}, _} -> emp_id end)
      |> Enum.uniq()
      |> length()

    violations =
      compliance_results
      |> Enum.count(fn {:compliance_result, _, %{status: status}} -> status == :non_compliant end)

    employees_with_violations =
      compliance_results
      |> Enum.filter(fn {:compliance_result, _, %{status: status}} ->
        status == :non_compliant
      end)
      |> Enum.map(fn {:compliance_result, {emp_id, _}, _} -> emp_id end)
      |> Enum.uniq()
      |> length()

    compliance_rate =
      if total_weeks > 0,
        do: ((total_weeks - violations) / total_weeks * 100) |> Float.round(1),
        else: 100.0

    %{
      total_weeks_checked: total_weeks,
      total_employees: total_employees,
      compliance_violations: violations,
      employees_with_violations: employees_with_violations,
      compliance_rate_percent: compliance_rate,
      processed_with_engine: true,
      summary_generated_at: DateTime.utc_now()
    }
  end

  defp print_engine_results(result) do
    IO.puts("  Weekly Aggregations: #{length(result.weekly_aggregations)}")
    IO.puts("  Compliance Results: #{length(result.compliance_results)}")
    IO.puts("\nSummary:")
    IO.puts("  Total Weeks Checked: #{result.summary.total_weeks_checked}")
    IO.puts("  Total Employees: #{result.summary.total_employees}")
    IO.puts("  Compliance Violations: #{result.summary.compliance_violations}")
    IO.puts("  Employees with Violations: #{result.summary.employees_with_violations}")
    IO.puts("  Compliance Rate: #{result.summary.compliance_rate_percent}%")
    IO.puts("  Processed with RETE Engine: #{result.summary.processed_with_engine}")
  end
end

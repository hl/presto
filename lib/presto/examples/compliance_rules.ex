defmodule Presto.Examples.ComplianceRules do
  @moduledoc """
  Compliance rules engine implementation for Presto.

  Implements rules for checking compliance against weekly hour thresholds:
  1. Divide time entries into Monday-Sunday weeks
  2. Check if weekly totals exceed compliance thresholds
  3. Generate compliance notices for violations

  This module implements the `Presto.RuleBehaviour` and can be used
  as an example or starting point for custom compliance rule implementations.
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
  Creates compliance rules based on the provided specification.
  """
  @spec create_rules(rule_spec()) :: [map()]
  def create_rules(rule_spec) do
    variables = extract_variables(rule_spec)

    [
      weekly_aggregation_rule(),
      weekly_compliance_rule(variables)
    ]
  end

  @doc """
  Rule 1: Aggregate time entries into weekly totals (Monday-Sunday).

  Pattern: Matches processed time entries 
  Action: Groups entries by employee and week, calculates weekly totals
  """
  @spec weekly_aggregation_rule() :: map()
  def weekly_aggregation_rule do
    %{
      name: :weekly_hours_aggregation,
      pattern: fn facts ->
        # Find processed time entries that need weekly aggregation
        facts
        |> extract_processed_time_entries()
        |> filter_unaggregated_entries(facts)
      end,
      action: fn time_entries ->
        # Aggregate entries into weekly totals
        time_entries
        |> group_by_employee_and_week()
        |> create_weekly_aggregations()
      end
    }
  end

  @doc """
  Rule 2: Check weekly compliance against thresholds.

  Pattern: Matches weekly aggregations that haven't been checked
  Action: Evaluates compliance and creates violation notices
  """
  @spec weekly_compliance_rule(rule_variables()) :: map()
  def weekly_compliance_rule(variables \\ %{}) do
    max_weekly_hours = Map.get(variables, "max_weekly_hours", 48.0)

    %{
      name: :weekly_compliance_check,
      pattern: fn facts ->
        # Find weekly aggregations that need compliance checking
        facts
        |> extract_weekly_aggregations()
        |> filter_unchecked_aggregations(facts)
      end,
      action: fn weekly_aggregations ->
        # Check compliance and create results
        weekly_aggregations
        |> Enum.map(&check_weekly_compliance(&1, max_weekly_hours))
      end
    }
  end

  @doc """
  Processes time entries through the complete compliance workflow.
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
    |> Enum.map(fn {:time_entry, _, %{start_datetime: start_dt}} ->
      date = DateTime.to_date(start_dt)
      week_start = week_start_date(date)
      {date, week_start, Date.day_of_week(week_start)}
    end)
    # Monday is 1
    |> Enum.all?(fn {_, _, day_of_week} -> day_of_week == 1 end)
  end

  # Private functions for rule implementation

  defp extract_processed_time_entries(facts) do
    facts
    |> Enum.filter(fn
      {:time_entry, _, %{minutes: minutes, units: units}}
      when not is_nil(minutes) and not is_nil(units) ->
        true

      _ ->
        false
    end)
  end

  defp filter_unaggregated_entries(time_entries, all_facts) do
    # Find entries that don't have corresponding weekly aggregations
    existing_aggregations = extract_weekly_aggregations(all_facts)
    existing_keys = MapSet.new(existing_aggregations, fn {:weekly_hours, key, _} -> key end)

    time_entries
    |> Enum.reject(fn {:time_entry, _, %{employee_id: emp_id, start_datetime: start_dt}} ->
      date = DateTime.to_date(start_dt)
      week_start = week_start_date(date)
      MapSet.member?(existing_keys, {emp_id, week_start})
    end)
  end

  defp extract_weekly_aggregations(facts) do
    facts
    |> Enum.filter(fn
      {:weekly_hours, _, _} -> true
      _ -> false
    end)
  end

  defp filter_unchecked_aggregations(weekly_aggregations, all_facts) do
    # Find aggregations that don't have corresponding compliance results
    existing_results = extract_compliance_results(all_facts)
    existing_keys = MapSet.new(existing_results, fn {:compliance_result, key, _} -> key end)

    weekly_aggregations
    |> Enum.reject(fn {:weekly_hours, key, _} ->
      MapSet.member?(existing_keys, key)
    end)
  end

  defp extract_compliance_results(facts) do
    facts
    |> Enum.filter(fn
      {:compliance_result, _, _} -> true
      _ -> false
    end)
  end

  defp group_by_employee_and_week(time_entries) do
    time_entries
    |> Enum.group_by(fn {:time_entry, _, %{employee_id: emp_id, start_datetime: start_dt}} ->
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
    end)
    |> Enum.sum()
  end

  defp extract_entry_ids(entries) do
    entries
    |> Enum.map(fn {:time_entry, id, _} -> id end)
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
      # Handle legacy format
      %{"rules_to_run" => rules, "variables" => variables}
      when is_list(rules) and is_map(variables) ->
        validate_legacy_format(rules, variables)

      # Handle new JSON format
      %{"rules" => rules} when is_list(rules) ->
        validate_json_format(rules)

      # Invalid format
      _ ->
        false
    end
  end

  defp validate_legacy_format(rules, variables) do
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
end

defmodule Presto.Examples.PayrollRules do
  @moduledoc """
  Payroll rules engine implementation for Presto.

  Implements two main rules:
  1. Calculate time duration between start and finish datetimes
  2. Calculate overtime when total units exceed threshold

  This module implements the `Presto.RuleBehaviour` and can be used
  as an example or starting point for custom payroll rule implementations.
  """

  @behaviour Presto.RuleBehaviour

  @type time_entry :: {:time_entry, any(), map()}
  @type overtime_entry :: {:overtime_entry, any(), map()}
  @type rule_variables :: map()
  @type rule_spec :: map()
  @type payroll_result :: %{
          processed_entries: [time_entry()],
          overtime_entries: [overtime_entry()],
          summary: map()
        }

  @doc """
  Creates payroll rules based on the provided specification.
  """
  @spec create_rules(rule_spec()) :: [map()]
  def create_rules(rule_spec) do
    variables = extract_variables(rule_spec)

    [
      time_calculation_rule(),
      overtime_calculation_rule(variables)
    ]
  end

  @doc """
  Rule 1: Calculate time duration for time entries.

  Pattern: Matches time entries with start_datetime and finish_datetime but no calculated values
  Action: Calculates minutes and units (hours) and updates the entry
  """
  @spec time_calculation_rule() :: map()
  def time_calculation_rule do
    %{
      name: :calculate_time_duration,
      pattern: fn facts ->
        # Find time entries that need calculation
        facts
        |> Enum.filter(&time_entry_needs_calculation?/1)
      end,
      action: fn matched_facts ->
        # Calculate duration for each matched fact
        matched_facts
        |> Enum.map(&calculate_time_duration/1)
      end
    }
  end

  @doc """
  Rule 2: Calculate overtime when total units exceed threshold.

  Pattern: Matches aggregated time entries by employee where total exceeds threshold
  Action: Creates overtime entries for the excess hours
  """
  @spec overtime_calculation_rule(rule_variables()) :: map()
  def overtime_calculation_rule(variables \\ %{}) do
    overtime_threshold = Map.get(variables, "overtime_threshold", 40.0)

    %{
      name: :calculate_overtime,
      pattern: fn facts ->
        # Group processed time entries by employee and calculate totals
        facts
        |> extract_processed_time_entries()
        |> group_by_employee()
        |> filter_overtime_candidates(overtime_threshold)
      end,
      action: fn overtime_candidates ->
        # Create overtime entries for employees exceeding threshold
        overtime_candidates
        |> Enum.map(fn {employee_id, total_units, week_start} ->
          overtime_units = total_units - overtime_threshold
          create_overtime_entry(employee_id, overtime_units, week_start)
        end)
      end
    }
  end

  @doc """
  Processes a list of time entries through the payroll rules.
  """
  @spec process_time_entries([time_entry()], rule_spec()) :: payroll_result()
  def process_time_entries(time_entries, rule_spec \\ %{}) do
    variables = extract_variables(rule_spec)

    # Step 1: Calculate time durations
    processed_entries =
      time_entries
      |> Enum.map(&calculate_time_duration/1)

    # Step 2: Calculate overtime
    overtime_entries = calculate_overtime_entries(processed_entries, variables)

    # Return both processed entries and overtime entries
    %{
      processed_entries: processed_entries,
      overtime_entries: overtime_entries,
      summary: generate_summary(processed_entries, overtime_entries)
    }
  end

  # Private functions for rule implementation

  @spec time_entry_needs_calculation?(time_entry()) :: boolean()
  defp time_entry_needs_calculation?(
         {:time_entry, _id,
          %{start_datetime: start_dt, finish_datetime: finish_dt, minutes: nil, units: nil}}
       )
       when not is_nil(start_dt) and not is_nil(finish_dt),
       do: true

  defp time_entry_needs_calculation?(_), do: false

  @spec calculate_time_duration(time_entry()) :: time_entry()
  defp calculate_time_duration(
         {:time_entry, id, %{start_datetime: start_dt, finish_datetime: finish_dt} = data}
       ) do
    minutes = calculate_minutes_between(start_dt, finish_dt)
    units = minutes / 60.0

    updated_data =
      data
      |> Map.put(:minutes, minutes)
      |> Map.put(:units, units)

    {:time_entry, id, updated_data}
  end

  @spec calculate_minutes_between(DateTime.t(), DateTime.t()) :: float()
  defp calculate_minutes_between(start_datetime, finish_datetime) do
    (DateTime.diff(finish_datetime, start_datetime, :second) / 60.0)
    |> Float.round(2)
  end

  @spec extract_processed_time_entries([any()]) :: [time_entry()]
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

  @spec group_by_employee([time_entry()]) :: %{any() => [time_entry()]}
  defp group_by_employee(time_entries) do
    time_entries
    |> Enum.group_by(fn {:time_entry, _, %{employee_id: emp_id, start_datetime: start_dt}} ->
      week_start = start_dt |> DateTime.to_date() |> week_start_date()
      {emp_id, week_start}
    end)
  end

  @spec filter_overtime_candidates(%{{any(), Date.t()} => [time_entry()]}, float()) :: [
          {any(), float(), Date.t()}
        ]
  defp filter_overtime_candidates(grouped_entries, threshold) do
    grouped_entries
    |> Enum.map(fn {{employee_id, week_start}, entries} ->
      total_units = calculate_total_units(entries)
      {employee_id, total_units, week_start}
    end)
    |> Enum.filter(fn {_employee_id, total_units, _week_start} ->
      total_units > threshold
    end)
  end

  @spec calculate_total_units([time_entry()]) :: float()
  defp calculate_total_units(entries) do
    entries
    |> Enum.map(fn {:time_entry, _, %{units: units}} -> units end)
    |> Enum.sum()
    |> then(&Float.round(&1 * 1.0, 2))
  end

  @spec week_start_date(Date.t()) :: Date.t()
  defp week_start_date(date) do
    days_since_monday = Date.day_of_week(date) - 1
    Date.add(date, -days_since_monday)
  end

  @spec create_overtime_entry(any(), float(), Date.t()) :: overtime_entry()
  defp create_overtime_entry(employee_id, overtime_units, week_start) do
    {:overtime_entry, {employee_id, week_start},
     %{
       employee_id: employee_id,
       units: Float.round(overtime_units, 2),
       week_start: week_start,
       type: :overtime,
       created_at: DateTime.utc_now()
     }}
  end

  @spec calculate_overtime_entries([time_entry()], rule_variables()) :: [overtime_entry()]
  defp calculate_overtime_entries(processed_entries, variables) do
    threshold = Map.get(variables, "overtime_threshold", 40.0)

    processed_entries
    |> group_by_employee()
    |> filter_overtime_candidates(threshold)
    |> Enum.map(fn {employee_id, total_units, week_start} ->
      overtime_units = total_units - threshold
      create_overtime_entry(employee_id, overtime_units, week_start)
    end)
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

  @spec generate_summary([time_entry()], [overtime_entry()]) :: map()
  defp generate_summary(processed_entries, overtime_entries) do
    total_employees =
      processed_entries
      |> Enum.map(fn {:time_entry, _, %{employee_id: emp_id}} -> emp_id end)
      |> Enum.uniq()
      |> length()

    total_regular_hours =
      processed_entries
      |> Enum.map(fn {:time_entry, _, %{units: units}} -> units end)
      |> Enum.sum()
      |> then(&Float.round(&1 * 1.0, 2))

    total_overtime_hours =
      overtime_entries
      |> Enum.map(fn {:overtime_entry, _, %{units: units}} -> units end)
      |> Enum.sum()
      |> then(&Float.round(&1 * 1.0, 2))

    employees_with_overtime =
      overtime_entries
      |> Enum.map(fn {:overtime_entry, _, %{employee_id: emp_id}} -> emp_id end)
      |> Enum.uniq()
      |> length()

    %{
      total_employees: total_employees,
      total_regular_hours: total_regular_hours,
      total_overtime_hours: total_overtime_hours,
      employees_with_overtime: employees_with_overtime,
      total_hours: total_regular_hours + total_overtime_hours
    }
  end

  @doc """
  Validates a payroll rule specification.

  Checks that the rule specification contains the required structure
  for payroll processing and has valid variable types.
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
    # Check that we have valid payroll rule names
    valid_payroll_rules = ["time_calculation", "overtime_check"]
    payroll_rules_present = Enum.any?(rules, &(&1 in valid_payroll_rules))

    # Check that variables have valid types for payroll processing
    valid_variables =
      case variables do
        %{"overtime_threshold" => threshold} when is_number(threshold) and threshold > 0 -> true
        # Empty variables are ok
        %{} -> true
        _ -> false
      end

    payroll_rules_present && valid_variables
  end

  defp validate_json_format(rules) do
    # Empty rules list is valid
    if Enum.empty?(rules) do
      true
    else
      # Check that all rules have required structure
      Enum.all?(rules, fn rule ->
        case rule do
          %{"name" => name, "type" => "payroll"} when is_binary(name) -> true
          _ -> false
        end
      end)
    end
  end
end

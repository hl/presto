defmodule Presto.Factories do
  @moduledoc """
  Test data factories for payroll and compliance rules engine testing.
  """

  @doc """
  Builds a time entry fact for payroll testing.
  """
  def build_time_entry(id, start_datetime, finish_datetime, employee_id) do
    {:time_entry, id,
     %{
       start_datetime: start_datetime,
       finish_datetime: finish_datetime,
       employee_id: employee_id,
       minutes: nil,
       units: nil
     }}
  end

  @doc """
  Builds a processed time entry with calculated minutes and units.
  """
  def build_processed_time_entry(id, start_datetime, finish_datetime, employee_id) do
    minutes = calculate_minutes(start_datetime, finish_datetime)
    units = minutes / 60.0

    {:processed_time_entry, id,
     %{
       start_datetime: start_datetime,
       finish_datetime: finish_datetime,
       employee_id: employee_id,
       minutes: minutes,
       units: units
     }}
  end

  @doc """
  Builds an overtime entry fact.
  """
  def build_overtime_entry(employee_id, overtime_units, week_start) do
    {:overtime_entry, {employee_id, week_start},
     %{
       employee_id: employee_id,
       units: overtime_units,
       week_start: week_start,
       type: :overtime
     }}
  end

  @doc """
  Builds a weekly hours aggregation fact for compliance testing.
  """
  def build_weekly_hours(employee_id, week_start, total_hours, entry_ids \\ []) do
    week_end = Date.add(week_start, 6)

    {:weekly_hours, {employee_id, week_start},
     %{
       employee_id: employee_id,
       week_start: week_start,
       week_end: week_end,
       total_hours: total_hours,
       entries: entry_ids
     }}
  end

  @doc """
  Builds a compliance result fact.
  """
  def build_compliance_result(
        employee_id,
        week_start,
        status,
        threshold,
        actual_value,
        reason \\ nil
      ) do
    {:compliance_result, {employee_id, week_start},
     %{
       employee_id: employee_id,
       week_start: week_start,
       status: status,
       threshold: threshold,
       actual_value: actual_value,
       reason: reason || generate_reason(status, actual_value, threshold)
     }}
  end

  @doc """
  Builds a payroll rule specification using simplified JSON format.
  """
  def build_payroll_rule_spec(overtime_threshold \\ 40.0) do
    %{
      "rules_to_run" => ["time_calculation", "overtime_check"],
      "variables" => %{
        "overtime_threshold" => overtime_threshold,
        "overtime_multiplier" => 1.5
      }
    }
  end

  @doc """
  Builds a compliance rule specification using simplified JSON format.
  """
  def build_compliance_rule_spec(max_weekly_hours \\ 48.0) do
    %{
      "rules_to_run" => ["weekly_compliance"],
      "variables" => %{
        "max_weekly_hours" => max_weekly_hours
      }
    }
  end

  @doc """
  Returns the Monday of the week containing the given date.
  """
  def week_start(date) do
    days_since_monday = Date.day_of_week(date) - 1
    Date.add(date, -days_since_monday)
  end

  @doc """
  Returns a list of dates representing a full work week (Monday-Friday).
  """
  def work_week_dates(monday) do
    Enum.map(0..4, &Date.add(monday, &1))
  end

  @doc """
  Generates realistic datetime pairs for testing.
  """
  def work_day_hours(date, start_hour \\ 9, duration_hours \\ 8) do
    start_dt = DateTime.new!(date, Time.new!(start_hour, 0, 0), "Etc/UTC")
    finish_dt = DateTime.add(start_dt, duration_hours * 3600, :second)
    {start_dt, finish_dt}
  end

  # Private functions

  defp calculate_minutes(start_datetime, finish_datetime) do
    (DateTime.diff(finish_datetime, start_datetime, :second) / 60)
    |> trunc()
  end

  defp generate_reason(:non_compliant, actual, threshold) do
    "Exceeded maximum weekly hours (#{actual} > #{threshold})"
  end

  defp generate_reason(:compliant, actual, threshold) do
    "Within weekly hours limit (#{actual} <= #{threshold})"
  end
end

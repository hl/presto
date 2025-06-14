defmodule Presto.ComplianceTestHelpers do
  @moduledoc """
  Test helpers specific to compliance rules engine testing.
  """

  alias Presto.Factories

  @doc """
  Creates weekly hours aggregations for compliance testing.
  """
  def create_weekly_aggregations(scenarios) do
    scenarios
    |> Enum.map(fn {employee_id, week_start, hours, entry_ids} ->
      Factories.build_weekly_hours(employee_id, week_start, hours, entry_ids)
    end)
  end

  @doc """
  Creates a compliance scenario with various threshold breaches.
  """
  def create_compliance_scenario(base_monday, threshold \\ 48.0) do
    week1 = base_monday
    week2 = Date.add(base_monday, 7)
    week3 = Date.add(base_monday, 14)

    [
      # Employee 1: Consistently under threshold
      {"emp_001", week1, 40.0, ["entry_1", "entry_2"]},
      {"emp_001", week2, 42.0, ["entry_3", "entry_4"]},

      # Employee 2: One week over threshold
      {"emp_002", week1, 45.0, ["entry_5", "entry_6"]},
      # Over
      {"emp_002", week2, 52.0, ["entry_7", "entry_8", "entry_9"]},
      {"emp_002", week3, 38.0, ["entry_10"]},

      # Employee 3: Consistently over threshold
      # Over
      {"emp_003", week1, 55.0, ["entry_11", "entry_12", "entry_13"]},
      # Over
      {"emp_003", week2, 60.0, ["entry_14", "entry_15", "entry_16"]},

      # Employee 4: Exactly at threshold
      {"emp_004", week1, threshold, ["entry_17", "entry_18"]}
    ]
    |> create_weekly_aggregations()
  end

  @doc """
  Filters aggregations for a specific employee.
  """
  def filter_employee_weeks(aggregations, employee_id) do
    aggregations
    |> Enum.filter(fn
      {:weekly_hours, {^employee_id, _}, _} -> true
      _ -> false
    end)
  end

  @doc """
  Extracts compliance results from a list of facts.
  """
  def extract_compliance_results(facts) do
    facts
    |> Enum.filter(fn
      {:compliance_result, _, _} -> true
      _ -> false
    end)
  end

  @doc """
  Groups compliance results by employee ID.
  """
  def group_compliance_by_employee(compliance_results) do
    compliance_results
    |> Enum.group_by(fn {:compliance_result, {employee_id, _}, _} -> employee_id end)
  end

  @doc """
  Counts compliance violations for an employee.
  """
  def count_violations(compliance_results, employee_id) do
    compliance_results
    |> filter_employee_compliance(employee_id)
    |> Enum.count(fn {:compliance_result, _, %{status: status}} -> status == :non_compliant end)
  end

  @doc """
  Validates that compliance result has correct threshold comparison.
  """
  def compliance_result_valid?(compliance_result, expected_threshold) do
    {:compliance_result, _,
     %{
       status: status,
       threshold: threshold,
       actual_value: actual
     }} = compliance_result

    threshold == expected_threshold and
      case status do
        :compliant -> actual <= threshold
        :non_compliant -> actual > threshold
      end
  end

  @doc """
  Creates week boundary test scenarios.
  """
  def create_week_boundary_scenarios do
    # Test various dates that might cause week calculation issues
    [
      # New Year boundary
      {~D[2024-01-01], "New Year Monday"},
      {~D[2023-12-31], "New Year Sunday"},

      # Month boundary
      {~D[2024-02-26], "February to March"},
      {~D[2024-04-29], "April to May"},

      # Leap year February
      {~D[2024-02-26], "Leap year February"},

      # Year boundary
      {~D[2023-12-25], "End of year"},
      {~D[2024-01-01], "Start of year"}
    ]
  end

  @doc """
  Validates that week boundaries are calculated correctly (Monday to Sunday).
  """
  def week_boundaries_correct?(monday_date) do
    # Monday is day 1
    Date.day_of_week(monday_date) == 1
  end

  @doc """
  Gets all dates in a week given the Monday start date.
  """
  def week_dates(monday_date) do
    0..6
    |> Enum.map(&Date.add(monday_date, &1))
  end

  @doc """
  Creates time entries spanning multiple weeks for boundary testing.
  """
  def create_cross_week_entries(employee_id, friday_date) do
    saturday_date = Date.add(friday_date, 1)

    # Friday late night entry
    friday_start = DateTime.new!(friday_date, ~T[23:00:00], "Etc/UTC")
    # 2 hours into Saturday
    friday_end = DateTime.add(friday_start, 2 * 3600, :second)

    # Saturday normal entry
    saturday_start = DateTime.new!(saturday_date, ~T[09:00:00], "Etc/UTC")
    saturday_end = DateTime.add(saturday_start, 8 * 3600, :second)

    [
      Factories.build_time_entry("cross_week_1", friday_start, friday_end, employee_id),
      Factories.build_time_entry("cross_week_2", saturday_start, saturday_end, employee_id)
    ]
  end

  @doc """
  Aggregates time entries into weekly totals by Monday-Sunday weeks.
  """
  def aggregate_entries_by_week(time_entries) do
    time_entries
    |> Enum.group_by(fn {:time_entry, _, %{start_datetime: start_dt, employee_id: emp_id}} ->
      date = DateTime.to_date(start_dt)
      week_start = Factories.week_start(date)
      {emp_id, week_start}
    end)
    |> Enum.map(fn {{emp_id, week_start}, entries} ->
      total_hours = calculate_total_hours(entries)
      entry_ids = extract_entry_ids(entries)
      Factories.build_weekly_hours(emp_id, week_start, total_hours, entry_ids)
    end)
  end

  @doc """
  Creates partial week scenario (employee started mid-week).
  """
  def create_partial_week_scenario(employee_id, wednesday_date) do
    # Employee starts on Wednesday, works Wed-Fri
    work_days = [
      # Wednesday
      wednesday_date,
      # Thursday
      Date.add(wednesday_date, 1),
      # Friday
      Date.add(wednesday_date, 2)
    ]

    Enum.with_index(work_days, 1)
    |> Enum.map(fn {date, index} ->
      {start_dt, finish_dt} = Factories.work_day_hours(date, 9, 8)

      Factories.build_time_entry(
        "partial_#{employee_id}_#{index}",
        start_dt,
        finish_dt,
        employee_id
      )
    end)
  end

  # Private functions

  defp filter_employee_compliance(compliance_results, employee_id) do
    compliance_results
    |> Enum.filter(fn
      {:compliance_result, {^employee_id, _}, _} -> true
      _ -> false
    end)
  end

  defp calculate_total_hours(entries) do
    entries
    |> Enum.map(fn {:time_entry, _, %{start_datetime: start_dt, finish_datetime: finish_dt}} ->
      DateTime.diff(finish_dt, start_dt, :second) / 3600.0
    end)
    |> Enum.sum()
  end

  defp extract_entry_ids(entries) do
    entries
    |> Enum.map(fn {:time_entry, id, _} -> id end)
  end
end

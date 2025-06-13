defmodule Presto.PayrollTestHelpers do
  @moduledoc """
  Test helpers specific to payroll rules engine testing.
  """

  alias Presto.Factories

  @doc """
  Creates a standard 40-hour work week for an employee.
  """
  def create_standard_work_week(employee_id, monday_date) do
    work_days = Factories.work_week_dates(monday_date)
    
    Enum.with_index(work_days, 1)
    |> Enum.map(fn {date, index} ->
      {start_dt, finish_dt} = Factories.work_day_hours(date, 9, 8)
      Factories.build_time_entry("entry_#{employee_id}_#{index}", start_dt, finish_dt, employee_id)
    end)
  end

  @doc """
  Creates a work week with overtime for an employee.
  """
  def create_overtime_work_week(employee_id, monday_date, extra_hours \\ 10) do
    standard_week = create_standard_work_week(employee_id, monday_date)
    
    # Add overtime on Friday
    friday_date = Date.add(monday_date, 4)
    {start_dt, _} = Factories.work_day_hours(friday_date, 17, 0) # Start at 5 PM
    finish_dt = DateTime.add(start_dt, extra_hours * 3600, :second)
    
    overtime_entry = Factories.build_time_entry(
      "entry_#{employee_id}_overtime", 
      start_dt, 
      finish_dt, 
      employee_id
    )
    
    standard_week ++ [overtime_entry]
  end

  @doc """
  Calculates expected total units for a list of time entries.
  """
  def calculate_expected_total_units(time_entries) do
    time_entries
    |> Enum.map(&calculate_entry_units/1)
    |> Enum.sum()
  end

  @doc """
  Calculates expected overtime for given total units and threshold.
  """
  def calculate_expected_overtime(total_units, threshold) do
    if total_units > threshold do
      total_units - threshold
    else
      0.0
    end
  end

  @doc """
  Extracts time entries for a specific employee from a list of facts.
  """
  def extract_employee_entries(facts, employee_id) do
    facts
    |> Enum.filter(fn
      {:time_entry, _, %{employee_id: ^employee_id}} -> true
      _ -> false
    end)
  end

  @doc """
  Extracts overtime entries from a list of facts.
  """
  def extract_overtime_entries(facts) do
    facts
    |> Enum.filter(fn
      {:overtime_entry, _, _} -> true
      _ -> false
    end)
  end

  @doc """
  Validates that a time entry has been processed (has calculated minutes and units).
  """
  def entry_processed?({:time_entry, _, %{minutes: minutes, units: units}}) do
    not is_nil(minutes) and not is_nil(units)
  end

  def entry_processed?(_), do: false

  @doc """
  Validates time calculation accuracy within tolerance.
  """
  def time_calculation_accurate?(entry, tolerance_minutes \\ 1) do
    {:time_entry, _, %{start_datetime: start_dt, finish_datetime: finish_dt, minutes: minutes}} = entry
    
    expected_minutes = DateTime.diff(finish_dt, start_dt, :second) / 60
    abs(expected_minutes - minutes) <= tolerance_minutes
  end

  @doc """
  Groups time entries by employee ID.
  """
  def group_entries_by_employee(time_entries) do
    time_entries
    |> Enum.group_by(fn {:time_entry, _, %{employee_id: employee_id}} -> employee_id end)
  end

  @doc """
  Creates test scenario with multiple employees having different overtime situations.
  """
  def create_mixed_overtime_scenario(monday_date) do
    %{
      no_overtime: create_standard_work_week("emp_001", monday_date),
      light_overtime: create_overtime_work_week("emp_002", monday_date, 5),
      heavy_overtime: create_overtime_work_week("emp_003", monday_date, 15),
      part_time: create_part_time_week("emp_004", monday_date)
    }
  end

  @doc """
  Creates a part-time work week (20 hours).
  """
  def create_part_time_week(employee_id, monday_date) do
    # Work Monday, Wednesday, Friday - 4 hours each day
    work_days = [
      Date.add(monday_date, 0), # Monday
      Date.add(monday_date, 2), # Wednesday  
      Date.add(monday_date, 4)  # Friday
    ]
    
    Enum.with_index(work_days, 1)
    |> Enum.map(fn {date, index} ->
      {start_dt, finish_dt} = Factories.work_day_hours(date, 9, 4)
      Factories.build_time_entry("entry_#{employee_id}_#{index}", start_dt, finish_dt, employee_id)
    end)
  end

  # Private functions

  defp calculate_entry_units({:time_entry, _, %{start_datetime: start_dt, finish_datetime: finish_dt}}) do
    minutes = DateTime.diff(finish_dt, start_dt, :second) / 60
    minutes / 60.0
  end
end
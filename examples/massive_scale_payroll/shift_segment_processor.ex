defmodule Examples.MassiveScalePayroll.ShiftSegmentProcessor do
  @moduledoc """
  Handles complex time-based shift segmentation for massive scale payroll processing.

  This processor breaks shifts into segments based on different pay periods,
  handling scenarios like:
  - Shifts spanning Christmas Eve → Christmas Day
  - Weekend rate transitions
  - Night shift differentials
  - Holiday pay periods

  The processor operates independently of Presto, preparing data that will
  later be processed by Presto's generic rules engine.
  """

  @type datetime :: DateTime.t()
  @type shift :: %{
          shift_id: String.t(),
          employee_id: String.t(),
          start_datetime: datetime(),
          finish_datetime: datetime(),
          department: String.t(),
          job_code: String.t()
        }

  @type shift_segment :: %{
          segment_id: String.t(),
          original_shift_id: String.t(),
          employee_id: String.t(),
          start_datetime: datetime(),
          finish_datetime: datetime(),
          duration_minutes: non_neg_integer(),
          pay_period: atom(),
          pay_rate_multiplier: float(),
          department: String.t(),
          job_code: String.t()
        }

  # Pay period definitions with their multipliers
  @pay_periods %{
    regular: 1.0,
    weekend_saturday: 1.2,
    weekend_sunday: 1.25,
    night_shift: 1.15,
    christmas_eve: 1.5,
    christmas_day: 2.0,
    boxing_day: 1.8,
    new_years_eve: 1.5,
    new_years_day: 2.0,
    public_holiday: 1.75,
    # Combined night + weekend
    night_weekend: 1.4,
    # Combined night + holiday
    night_holiday: 2.3
  }

  # Holiday definitions (in practice, these would come from configuration)
  @holidays %{
    ~D[2024-12-24] => :christmas_eve,
    ~D[2024-12-25] => :christmas_day,
    ~D[2024-12-26] => :boxing_day,
    ~D[2024-12-31] => :new_years_eve,
    ~D[2025-01-01] => :new_years_day,
    # Australia Day
    ~D[2025-01-26] => :public_holiday,
    # Good Friday
    ~D[2025-04-18] => :public_holiday,
    # Easter Monday
    ~D[2025-04-21] => :public_holiday,
    # ANZAC Day
    ~D[2025-04-25] => :public_holiday,
    # Queen's Birthday
    ~D[2025-06-09] => :public_holiday,
    # Labour Day
    ~D[2025-10-06] => :public_holiday
  }

  # Night shift hours (10 PM to 6 AM)
  @night_start_hour 22
  @night_end_hour 6

  @doc """
  Segments all shifts for an employee into time-based segments.
  """
  @spec segment_employee_shifts([shift()]) :: [shift_segment()]
  def segment_employee_shifts(shifts) do
    shifts
    |> Enum.flat_map(&segment_single_shift/1)
    |> Enum.sort_by(& &1.start_datetime, DateTime)
  end

  @doc """
  Segments a single shift into multiple segments based on pay periods.
  """
  @spec segment_single_shift(shift()) :: [shift_segment()]
  def segment_single_shift(shift) do
    %{
      shift_id: shift_id,
      employee_id: employee_id,
      start_datetime: start_dt,
      finish_datetime: finish_dt,
      department: department,
      job_code: job_code
    } = shift

    # Find all time boundaries within the shift
    boundaries = find_time_boundaries(start_dt, finish_dt)

    # Create segments between each boundary
    boundaries
    |> Enum.chunk_every(2, 1, :discard)
    |> Enum.with_index()
    |> Enum.map(fn {[segment_start, segment_end], index} ->
      create_shift_segment(
        shift_id,
        employee_id,
        segment_start,
        segment_end,
        index + 1,
        department,
        job_code
      )
    end)
  end

  @doc """
  Finds all significant time boundaries within a shift period.
  """
  @spec find_time_boundaries(datetime(), datetime()) :: [datetime()]
  def find_time_boundaries(start_dt, finish_dt) do
    boundaries = [start_dt, finish_dt]

    # Add day boundaries (midnight transitions)
    boundaries = add_day_boundaries(boundaries, start_dt, finish_dt)

    # Add holiday boundaries
    boundaries = add_holiday_boundaries(boundaries, start_dt, finish_dt)

    # Add weekend boundaries
    boundaries = add_weekend_boundaries(boundaries, start_dt, finish_dt)

    # Add night shift boundaries (22:00 and 06:00)
    boundaries = add_night_shift_boundaries(boundaries, start_dt, finish_dt)

    boundaries
    |> Enum.uniq()
    |> Enum.sort(DateTime)
  end

  defp add_day_boundaries(boundaries, start_dt, finish_dt) do
    start_date = DateTime.to_date(start_dt)
    finish_date = DateTime.to_date(finish_dt)

    # Add midnight boundary for each day transition
    Date.range(start_date, finish_date)
    |> Enum.reduce(boundaries, fn date, acc ->
      next_day = Date.add(date, 1)
      midnight = DateTime.new!(next_day, ~T[00:00:00], start_dt.time_zone)

      if DateTime.compare(midnight, start_dt) == :gt and
           DateTime.compare(midnight, finish_dt) == :lt do
        [midnight | acc]
      else
        acc
      end
    end)
  end

  defp add_holiday_boundaries(boundaries, start_dt, finish_dt) do
    # Check for holiday transitions at midnight
    Date.range(DateTime.to_date(start_dt), DateTime.to_date(finish_dt))
    |> Enum.reduce(boundaries, fn date, acc ->
      if Map.has_key?(@holidays, date) do
        holiday_start = DateTime.new!(date, ~T[00:00:00], start_dt.time_zone)

        if DateTime.compare(holiday_start, start_dt) == :gt and
             DateTime.compare(holiday_start, finish_dt) == :lt do
          [holiday_start | acc]
        else
          acc
        end
      else
        acc
      end
    end)
  end

  defp add_weekend_boundaries(boundaries, start_dt, finish_dt) do
    # Add Saturday/Sunday transitions
    current_dt = start_dt
    add_weekend_boundaries_recursive(boundaries, current_dt, finish_dt)
  end

  defp add_weekend_boundaries_recursive(boundaries, current_dt, finish_dt) do
    if DateTime.compare(current_dt, finish_dt) >= :eq do
      boundaries
    else
      current_date = DateTime.to_date(current_dt)

      # Check for Saturday start (Friday → Saturday at midnight)
      # Saturday
      if Date.day_of_week(current_date) == 6 do
        saturday_start = DateTime.new!(current_date, ~T[00:00:00], current_dt.time_zone)

        if DateTime.compare(saturday_start, current_dt) == :gt and
             DateTime.compare(saturday_start, finish_dt) == :lt do
          boundaries = [saturday_start | boundaries]
        end
      end

      # Check for Monday start (Sunday → Monday at midnight)
      # Monday
      if Date.day_of_week(current_date) == 1 do
        monday_start = DateTime.new!(current_date, ~T[00:00:00], current_dt.time_zone)

        if DateTime.compare(monday_start, current_dt) == :gt and
             DateTime.compare(monday_start, finish_dt) == :lt do
          boundaries = [monday_start | boundaries]
        end
      end

      # Add 1 day
      next_dt = DateTime.add(current_dt, 24 * 60 * 60)
      add_weekend_boundaries_recursive(boundaries, next_dt, finish_dt)
    end
  end

  defp add_night_shift_boundaries(boundaries, start_dt, finish_dt) do
    current = start_dt
    add_night_boundaries_recursive(boundaries, current, finish_dt)
  end

  defp add_night_boundaries_recursive(boundaries, current_dt, finish_dt) do
    if DateTime.compare(current_dt, finish_dt) >= :eq do
      boundaries
    else
      current_date = DateTime.to_date(current_dt)

      # Check for 22:00 boundary (night shift start)
      night_start =
        DateTime.new!(current_date, Time.new!(@night_start_hour, 0, 0), current_dt.time_zone)

      boundaries =
        if DateTime.compare(night_start, current_dt) == :gt and
             DateTime.compare(night_start, finish_dt) == :lt do
          [night_start | boundaries]
        else
          boundaries
        end

      # Check for 06:00 boundary (night shift end)
      night_end =
        DateTime.new!(current_date, Time.new!(@night_end_hour, 0, 0), current_dt.time_zone)

      boundaries =
        if DateTime.compare(night_end, current_dt) == :gt and
             DateTime.compare(night_end, finish_dt) == :lt do
          [night_end | boundaries]
        else
          boundaries
        end

      # Check next day's 06:00 as well
      next_day = Date.add(current_date, 1)

      next_night_end =
        DateTime.new!(next_day, Time.new!(@night_end_hour, 0, 0), current_dt.time_zone)

      boundaries =
        if DateTime.compare(next_night_end, current_dt) == :gt and
             DateTime.compare(next_night_end, finish_dt) == :lt do
          [next_night_end | boundaries]
        else
          boundaries
        end

      # Add 1 day
      next_dt = DateTime.add(current_dt, 24 * 60 * 60)
      add_night_boundaries_recursive(boundaries, next_dt, finish_dt)
    end
  end

  defp create_shift_segment(
         shift_id,
         employee_id,
         start_dt,
         end_dt,
         segment_number,
         department,
         job_code
       ) do
    duration_minutes = div(DateTime.diff(end_dt, start_dt, :second), 60)

    # Determine pay period and multiplier
    {pay_period, pay_rate_multiplier} = determine_pay_period_and_rate(start_dt, end_dt)

    %{
      segment_id: "#{shift_id}_seg_#{segment_number}",
      original_shift_id: shift_id,
      employee_id: employee_id,
      start_datetime: start_dt,
      finish_datetime: end_dt,
      duration_minutes: duration_minutes,
      pay_period: pay_period,
      pay_rate_multiplier: pay_rate_multiplier,
      department: department,
      job_code: job_code
    }
  end

  @doc """
  Determines the pay period and rate multiplier for a time segment.
  """
  @spec determine_pay_period_and_rate(datetime(), datetime()) :: {atom(), float()}
  def determine_pay_period_and_rate(start_dt, end_dt) do
    # Use the start time to determine the primary pay period
    # In practice, you might want to use the majority time or midpoint

    start_date = DateTime.to_date(start_dt)
    start_time = DateTime.to_time(start_dt)
    start_hour = start_time.hour
    day_of_week = Date.day_of_week(start_date)

    # Check for holidays first (highest priority)
    holiday_period = Map.get(@holidays, start_date)
    # Saturday or Sunday
    is_weekend = day_of_week in [6, 7]
    is_night = start_hour >= @night_start_hour or start_hour < @night_end_hour

    cond do
      # Holiday combinations
      holiday_period && is_night ->
        {:night_holiday, @pay_periods.night_holiday}

      holiday_period ->
        {holiday_period, Map.get(@pay_periods, holiday_period, @pay_periods.public_holiday)}

      # Weekend combinations
      is_weekend && is_night ->
        {:night_weekend, @pay_periods.night_weekend}

      # Saturday
      day_of_week == 6 ->
        {:weekend_saturday, @pay_periods.weekend_saturday}

      # Sunday
      day_of_week == 7 ->
        {:weekend_sunday, @pay_periods.weekend_sunday}

      # Night shift (non-weekend, non-holiday)
      is_night ->
        {:night_shift, @pay_periods.night_shift}

      # Regular time
      true ->
        {:regular, @pay_periods.regular}
    end
  end

  @doc """
  Converts shift segments into Presto facts for rule processing.
  """
  @spec segments_to_presto_facts([shift_segment()]) :: [tuple()]
  def segments_to_presto_facts(segments) do
    Enum.map(segments, fn segment ->
      {:shift_segment, segment.segment_id,
       %{
         segment_id: segment.segment_id,
         original_shift_id: segment.original_shift_id,
         employee_id: segment.employee_id,
         start_datetime: segment.start_datetime,
         finish_datetime: segment.finish_datetime,
         duration_minutes: segment.duration_minutes,
         pay_period: segment.pay_period,
         pay_rate_multiplier: segment.pay_rate_multiplier,
         department: segment.department,
         job_code: segment.job_code,
         # To be calculated by Presto rules
         units: nil,
         # To be calculated by Presto rules
         base_pay_amount: nil,
         payment_status: :pending
       }}
    end)
  end

  @doc """
  Example: Christmas Eve → Christmas Day shift segmentation.
  """
  def example_christmas_shift do
    shift = %{
      shift_id: "emp_001_shift_christmas",
      employee_id: "emp_001",
      # Christmas Eve 10 PM
      start_datetime: ~U[2024-12-24 22:00:00Z],
      # Christmas Day 6 AM
      finish_datetime: ~U[2024-12-25 06:00:00Z],
      department: "nursing",
      job_code: "rn_level_2"
    }

    segments = segment_single_shift(shift)

    IO.puts("=== Christmas Eve → Christmas Day Shift Segmentation ===")
    IO.puts("Original shift: #{shift.start_datetime} → #{shift.finish_datetime}")
    IO.puts("Segments created:")

    Enum.each(segments, fn segment ->
      IO.puts("  #{segment.segment_id}:")
      IO.puts("    Time: #{segment.start_datetime} → #{segment.finish_datetime}")
      IO.puts("    Duration: #{segment.duration_minutes} minutes")
      IO.puts("    Pay period: #{segment.pay_period}")
      IO.puts("    Rate multiplier: #{segment.pay_rate_multiplier}x")
      IO.puts("")
    end)

    segments
  end
end

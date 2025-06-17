defmodule Examples.MassiveScalePayroll.OvertimeAggregator do
  @moduledoc """
  Aggregates employee results to apply overtime rules and mark segments as paid.

  This module handles the complex overtime calculations that require cross-shift
  analysis:
  - Weekly overtime (>40 hours)
  - Daily overtime (>8 hours) 
  - Consecutive shift overtime
  - Double-time calculations
  - Marking segments as paid by overtime rules

  Uses Presto as a generic rules engine for overtime rule processing,
  while maintaining payroll-specific aggregation logic.
  """

  use GenServer

  @type employee_result :: Examples.MassiveScalePayroll.EmployeeWorker.employee_result()

  @type overtime_calculation :: %{
          employee_id: String.t(),
          period_type: :daily | :weekly | :monthly,
          period_id: String.t(),
          regular_hours: float(),
          overtime_hours: float(),
          double_time_hours: float(),
          affected_segments: [String.t()],
          overtime_rate: float(),
          double_time_rate: float()
        }

  @type aggregation_state :: %{
          run_id: String.t(),
          employee_results: [employee_result()],
          overtime_calculations: [overtime_calculation()],
          paid_segments: MapSet.t(),
          start_time: DateTime.t(),
          status: :active | :finalized
        }

  # Client API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Starts overtime aggregation for a payroll run.
  """
  @spec start_aggregation(pid(), String.t()) :: :ok
  def start_aggregation(aggregator_pid, run_id) do
    GenServer.call(aggregator_pid, {:start_aggregation, run_id})
  end

  @doc """
  Adds an employee result for overtime processing.
  """
  @spec add_employee_result(String.t(), employee_result()) :: :ok
  def add_employee_result(run_id, employee_result) do
    GenServer.call(__MODULE__, {:add_employee_result, run_id, employee_result})
  end

  @doc """
  Finalizes overtime aggregation and returns comprehensive results.
  """
  @spec finalize_aggregation(pid(), String.t()) :: {:ok, map()} | {:error, term()}
  def finalize_aggregation(aggregator_pid, run_id) do
    GenServer.call(aggregator_pid, {:finalize_aggregation, run_id}, 60_000)
  end

  @doc """
  Gets current overtime aggregation status.
  """
  @spec get_aggregation_status(pid(), String.t()) :: {:ok, map()} | {:error, :not_found}
  def get_aggregation_status(aggregator_pid, run_id) do
    GenServer.call(aggregator_pid, {:get_aggregation_status, run_id})
  end

  # Server implementation

  @impl true
  def init(_opts) do
    # Start a dedicated Presto engine for overtime rule processing
    {:ok, overtime_engine} = Presto.start_engine()

    # Load overtime-specific rules
    load_overtime_rules(overtime_engine)

    state = %{
      overtime_engine: overtime_engine,
      active_aggregations: %{},
      completed_aggregations: %{}
    }

    {:ok, state}
  end

  @impl true
  def handle_call({:start_aggregation, run_id}, _from, state) do
    if Map.has_key?(state.active_aggregations, run_id) do
      {:reply, {:error, :already_exists}, state}
    else
      aggregation_state = %{
        run_id: run_id,
        employee_results: [],
        overtime_calculations: [],
        paid_segments: MapSet.new(),
        start_time: DateTime.utc_now(),
        status: :active
      }

      new_state = put_in(state, [:active_aggregations, run_id], aggregation_state)
      {:reply, :ok, new_state}
    end
  end

  @impl true
  def handle_call({:add_employee_result, run_id, employee_result}, _from, state) do
    case Map.get(state.active_aggregations, run_id) do
      nil ->
        {:reply, {:error, :aggregation_not_found}, state}

      aggregation_state ->
        updated_state =
          aggregation_state
          |> Map.update!(:employee_results, &[employee_result | &1])

        new_state = put_in(state, [:active_aggregations, run_id], updated_state)
        {:reply, :ok, new_state}
    end
  end

  @impl true
  def handle_call({:finalize_aggregation, run_id}, _from, state) do
    case Map.get(state.active_aggregations, run_id) do
      nil ->
        {:reply, {:error, :aggregation_not_found}, state}

      aggregation_state ->
        # Process overtime calculations
        case process_overtime_calculations(state.overtime_engine, aggregation_state) do
          {:ok, final_results} ->
            # Mark as completed
            completed_state = Map.put(aggregation_state, :status, :finalized)

            new_state =
              state
              |> Map.update!(:active_aggregations, &Map.delete(&1, run_id))
              |> Map.update!(:completed_aggregations, &Map.put(&1, run_id, completed_state))

            {:reply, {:ok, final_results}, new_state}

          {:error, reason} ->
            {:reply, {:error, reason}, state}
        end
    end
  end

  @impl true
  def handle_call({:get_aggregation_status, run_id}, _from, state) do
    case Map.get(state.active_aggregations, run_id) do
      nil ->
        case Map.get(state.completed_aggregations, run_id) do
          nil ->
            {:reply, {:error, :not_found}, state}

          completed_state ->
            status = generate_aggregation_status(completed_state)
            {:reply, {:ok, status}, state}
        end

      aggregation_state ->
        status = generate_aggregation_status(aggregation_state)
        {:reply, {:ok, status}, state}
    end
  end

  @impl true
  def terminate(_reason, state) do
    if state.overtime_engine do
      Presto.stop_engine(state.overtime_engine)
    end

    :ok
  end

  # Private helper functions

  defp load_overtime_rules(overtime_engine) do
    # Load overtime-specific rules using Presto's rule loading
    # These rules define overtime calculation logic

    overtime_rules = [
      # Weekly overtime rule (>40 hours)
      {:rule, "weekly_overtime_40h", %{priority: 100},
       fn facts ->
         weekly_hours = calculate_weekly_hours_from_facts(facts)

         if weekly_hours > 40.0 do
           overtime_hours = weekly_hours - 40.0
           create_overtime_fact(:weekly, overtime_hours, 1.5)
         else
           []
         end
       end},

      # Daily overtime rule (>8 hours)
      {:rule, "daily_overtime_8h", %{priority: 90},
       fn facts ->
         daily_hours_by_date = calculate_daily_hours_from_facts(facts)

         Enum.flat_map(daily_hours_by_date, fn {date, hours} ->
           if hours > 8.0 do
             overtime_hours = hours - 8.0
             [create_overtime_fact(:daily, overtime_hours, 1.5, date)]
           else
             []
           end
         end)
       end},

      # Double-time rule (>12 hours in a day)
      {:rule, "daily_double_time_12h", %{priority: 80},
       fn facts ->
         daily_hours_by_date = calculate_daily_hours_from_facts(facts)

         Enum.flat_map(daily_hours_by_date, fn {date, hours} ->
           if hours > 12.0 do
             double_time_hours = hours - 12.0
             [create_overtime_fact(:double_time, double_time_hours, 2.0, date)]
           else
             []
           end
         end)
       end}
    ]

    # Load rules into Presto engine
    Enum.each(overtime_rules, fn rule ->
      Presto.add_rule(overtime_engine, rule)
    end)
  end

  defp process_overtime_calculations(overtime_engine, aggregation_state) do
    try do
      # Group employee results by employee for overtime calculation
      employees_grouped = Enum.group_by(aggregation_state.employee_results, & &1.employee_id)

      # Process overtime for each employee
      overtime_results =
        Enum.map(employees_grouped, fn {employee_id, employee_results} ->
          process_employee_overtime(overtime_engine, employee_id, employee_results)
        end)

      # Aggregate final results
      final_results = aggregate_overtime_results(overtime_results, aggregation_state)

      {:ok, final_results}
    rescue
      error -> {:error, {:overtime_processing_failed, error}}
    end
  end

  defp process_employee_overtime(overtime_engine, employee_id, employee_results) do
    # Clear previous facts
    Presto.clear_facts(overtime_engine)

    # Convert employee results to facts for overtime processing
    overtime_facts = convert_to_overtime_facts(employee_id, employee_results)

    # Assert facts into the overtime engine
    Enum.each(overtime_facts, &Presto.assert_fact(overtime_engine, &1))

    # Fire overtime rules
    overtime_rule_results = Presto.fire_rules(overtime_engine)

    # Process results and mark segments as paid
    process_overtime_rule_results(employee_id, overtime_rule_results, employee_results)
  end

  defp convert_to_overtime_facts(employee_id, employee_results) do
    # Convert processed segments to overtime calculation facts
    Enum.flat_map(employee_results, fn employee_result ->
      Enum.map(employee_result.processed_segments, fn {:shift_segment, segment_id, segment_data} ->
        {:segment_for_overtime, segment_id,
         %{
           employee_id: employee_id,
           segment_id: segment_id,
           start_datetime: segment_data.start_datetime,
           finish_datetime: segment_data.finish_datetime,
           hours: segment_data.units || 0.0,
           pay_period: segment_data.pay_period,
           department: segment_data.department,
           original_shift_id: segment_data.original_shift_id
         }}
      end)
    end)
  end

  defp process_overtime_rule_results(employee_id, overtime_rule_results, employee_results) do
    # Extract overtime calculations from rule results
    overtime_calculations =
      Enum.filter(overtime_rule_results, fn
        {:overtime_calculation, _id, _data} -> true
        _ -> false
      end)

    # Determine which segments are affected by overtime
    affected_segments = determine_affected_segments(overtime_calculations, employee_results)

    # Calculate final overtime totals
    {total_overtime_hours, total_double_time_hours} =
      calculate_overtime_totals(overtime_calculations)

    # Generate overtime summary for this employee
    %{
      employee_id: employee_id,
      total_overtime_hours: total_overtime_hours,
      total_double_time_hours: total_double_time_hours,
      overtime_calculations: overtime_calculations,
      affected_segments: affected_segments,
      segments_marked_as_paid: MapSet.new(affected_segments)
    }
  end

  defp determine_affected_segments(overtime_calculations, employee_results) do
    # Logic to determine which specific segments are affected by overtime rules
    # This is where we implement the "mark segments as paid" functionality

    all_segments =
      Enum.flat_map(employee_results, fn employee_result ->
        Enum.map(employee_result.processed_segments, fn {:shift_segment, segment_id, _data} ->
          segment_id
        end)
      end)

    # For now, assume overtime affects the latest segments first (LIFO)
    # In practice, this would be more sophisticated based on overtime rules
    overtime_hours_to_allocate =
      overtime_calculations
      |> Enum.map(fn {:overtime_calculation, _id, data} -> Map.get(data, :hours, 0.0) end)
      |> Enum.sum()

    # Select segments to mark as overtime (simplified logic)
    segments_to_mark =
      Enum.take(
        Enum.reverse(all_segments),
        min(trunc(overtime_hours_to_allocate), length(all_segments))
      )

    segments_to_mark
  end

  defp calculate_overtime_totals(overtime_calculations) do
    {overtime_hours, double_time_hours} =
      Enum.reduce(overtime_calculations, {0.0, 0.0}, fn
        {:overtime_calculation, _id, %{type: :overtime, hours: hours}}, {ot, dt} ->
          {ot + hours, dt}

        {:overtime_calculation, _id, %{type: :double_time, hours: hours}}, {ot, dt} ->
          {ot, dt + hours}

        _, acc ->
          acc
      end)

    {Float.round(overtime_hours, 2), Float.round(double_time_hours, 2)}
  end

  defp aggregate_overtime_results(overtime_results, aggregation_state) do
    total_employees = length(aggregation_state.employee_results)

    total_overtime_hours =
      overtime_results
      |> Enum.map(& &1.total_overtime_hours)
      |> Enum.sum()
      |> Float.round(2)

    total_double_time_hours =
      overtime_results
      |> Enum.map(& &1.total_double_time_hours)
      |> Enum.sum()
      |> Float.round(2)

    employees_with_overtime = Enum.count(overtime_results, &(&1.total_overtime_hours > 0))

    all_paid_segments =
      overtime_results
      |> Enum.flat_map(&MapSet.to_list(&1.segments_marked_as_paid))
      |> MapSet.new()

    %{
      run_id: aggregation_state.run_id,
      aggregation_summary: %{
        total_employees_processed: total_employees,
        total_overtime_hours: total_overtime_hours,
        total_double_time_hours: total_double_time_hours,
        employees_with_overtime: employees_with_overtime,
        overtime_percentage:
          if(total_employees > 0,
            do: Float.round(employees_with_overtime / total_employees * 100, 2),
            else: 0.0
          ),
        segments_marked_as_paid: MapSet.size(all_paid_segments)
      },
      employee_overtime_results: overtime_results,
      paid_segments: all_paid_segments,
      processing_duration_seconds:
        DateTime.diff(DateTime.utc_now(), aggregation_state.start_time, :second),
      generated_at: DateTime.utc_now()
    }
  end

  defp generate_aggregation_status(aggregation_state) do
    %{
      run_id: aggregation_state.run_id,
      status: aggregation_state.status,
      employees_processed: length(aggregation_state.employee_results),
      processing_duration_seconds:
        DateTime.diff(DateTime.utc_now(), aggregation_state.start_time, :second),
      paid_segments_count: MapSet.size(aggregation_state.paid_segments)
    }
  end

  # Helper functions for rule processing

  defp calculate_weekly_hours_from_facts(facts) do
    facts
    |> Enum.filter(fn
      {:segment_for_overtime, _id, _data} -> true
      _ -> false
    end)
    |> Enum.map(fn {:segment_for_overtime, _id, data} -> Map.get(data, :hours, 0.0) end)
    |> Enum.sum()
  end

  defp calculate_daily_hours_from_facts(facts) do
    facts
    |> Enum.filter(fn
      {:segment_for_overtime, _id, _data} -> true
      _ -> false
    end)
    |> Enum.group_by(fn {:segment_for_overtime, _id, data} ->
      DateTime.to_date(data.start_datetime)
    end)
    |> Enum.map(fn {date, segments} ->
      total_hours =
        segments
        |> Enum.map(fn {:segment_for_overtime, _id, data} -> Map.get(data, :hours, 0.0) end)
        |> Enum.sum()

      {date, total_hours}
    end)
  end

  defp create_overtime_fact(type, hours, rate, date \\ nil) do
    {:overtime_calculation, "ot_#{System.unique_integer()}",
     %{
       type: type,
       hours: hours,
       rate: rate,
       date: date,
       generated_at: DateTime.utc_now()
     }}
  end
end

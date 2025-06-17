defmodule Presto.PayrollAggregator do
  @moduledoc """
  Cross-employee aggregation capabilities for payroll processing.

  This module provides functionality to aggregate payroll results across
  all employees in a payroll run, maintaining state and providing
  comprehensive reporting capabilities.
  """

  use GenServer

  @type employee_result :: %{
          employee_id: String.t(),
          processed_entries: [tuple()],
          overtime_entries: [tuple()],
          summary: map(),
          processing_time: non_neg_integer(),
          processed_at: DateTime.t()
        }

  @type aggregation_state :: %{
          payroll_run_id: String.t(),
          employee_results: [employee_result()],
          aggregated_totals: map(),
          start_time: DateTime.t(),
          last_updated: DateTime.t(),
          total_employees_processed: non_neg_integer(),
          expected_employee_count: non_neg_integer() | nil
        }

  @type payroll_report :: %{
          payroll_run_id: String.t(),
          processing_summary: map(),
          employee_summaries: [map()],
          totals: map(),
          performance_metrics: map(),
          generated_at: DateTime.t()
        }

  # Client API

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @spec start_payroll_run(String.t(), non_neg_integer() | nil) :: :ok
  def start_payroll_run(payroll_run_id, expected_employee_count \\ nil) do
    GenServer.call(__MODULE__, {:start_payroll_run, payroll_run_id, expected_employee_count})
  end

  @spec add_employee_result(String.t(), employee_result()) :: :ok
  def add_employee_result(payroll_run_id, employee_result) do
    GenServer.call(__MODULE__, {:add_employee_result, payroll_run_id, employee_result})
  end

  @spec get_current_totals(String.t()) :: {:ok, map()} | {:error, :not_found}
  def get_current_totals(payroll_run_id) do
    GenServer.call(__MODULE__, {:get_current_totals, payroll_run_id})
  end

  @spec get_processing_progress(String.t()) :: {:ok, map()} | {:error, :not_found}
  def get_processing_progress(payroll_run_id) do
    GenServer.call(__MODULE__, {:get_processing_progress, payroll_run_id})
  end

  @spec finalize_payroll_run(String.t()) :: {:ok, payroll_report()} | {:error, term()}
  def finalize_payroll_run(payroll_run_id) do
    GenServer.call(__MODULE__, {:finalize_payroll_run, payroll_run_id})
  end

  @spec get_payroll_report(String.t()) :: {:ok, payroll_report()} | {:error, :not_found}
  def get_payroll_report(payroll_run_id) do
    GenServer.call(__MODULE__, {:get_payroll_report, payroll_run_id})
  end

  @spec cleanup_old_runs(non_neg_integer()) :: :ok
  def cleanup_old_runs(days_to_keep \\ 30) do
    GenServer.cast(__MODULE__, {:cleanup_old_runs, days_to_keep})
  end

  # Server implementation

  @impl true
  def init(_opts) do
    state = %{
      active_runs: %{},
      completed_runs: %{},
      reports: %{}
    }

    {:ok, state}
  end

  @impl true
  def handle_call({:start_payroll_run, payroll_run_id, expected_employee_count}, _from, state) do
    if Map.has_key?(state.active_runs, payroll_run_id) do
      {:reply, {:error, :already_exists}, state}
    else
      aggregation_state = %{
        payroll_run_id: payroll_run_id,
        employee_results: [],
        aggregated_totals: init_aggregated_totals(),
        start_time: DateTime.utc_now(),
        last_updated: DateTime.utc_now(),
        total_employees_processed: 0,
        expected_employee_count: expected_employee_count
      }

      new_state = put_in(state, [:active_runs, payroll_run_id], aggregation_state)
      {:reply, :ok, new_state}
    end
  end

  @impl true
  def handle_call({:add_employee_result, payroll_run_id, employee_result}, _from, state) do
    case Map.get(state.active_runs, payroll_run_id) do
      nil ->
        {:reply, {:error, :payroll_run_not_found}, state}

      aggregation_state ->
        # Add processing timestamp if not present
        employee_result_with_timestamp = Map.put_new(employee_result, :processed_at, DateTime.utc_now())

        # Update aggregation state
        updated_state =
          aggregation_state
          |> Map.update!(:employee_results, fn results -> 
               [employee_result_with_timestamp | results] 
             end)
          |> Map.update!(:total_employees_processed, &(&1 + 1))
          |> Map.put(:last_updated, DateTime.utc_now())
          |> Map.put(:aggregated_totals, update_aggregated_totals(aggregation_state.aggregated_totals, employee_result))

        new_state = put_in(state, [:active_runs, payroll_run_id], updated_state)
        {:reply, :ok, new_state}
    end
  end

  @impl true
  def handle_call({:get_current_totals, payroll_run_id}, _from, state) do
    case Map.get(state.active_runs, payroll_run_id) do
      nil ->
        # Check completed runs
        case Map.get(state.completed_runs, payroll_run_id) do
          nil -> {:reply, {:error, :not_found}, state}
          completed_state -> {:reply, {:ok, completed_state.aggregated_totals}, state}
        end

      aggregation_state ->
        {:reply, {:ok, aggregation_state.aggregated_totals}, state}
    end
  end

  @impl true
  def handle_call({:get_processing_progress, payroll_run_id}, _from, state) do
    case Map.get(state.active_runs, payroll_run_id) do
      nil ->
        {:reply, {:error, :not_found}, state}

      aggregation_state ->
        progress = calculate_processing_progress(aggregation_state)
        {:reply, {:ok, progress}, state}
    end
  end

  @impl true
  def handle_call({:finalize_payroll_run, payroll_run_id}, _from, state) do
    case Map.get(state.active_runs, payroll_run_id) do
      nil ->
        {:reply, {:error, :payroll_run_not_found}, state}

      aggregation_state ->
        # Generate final report
        report = generate_payroll_report(aggregation_state)

        # Move to completed runs
        completed_state = Map.put(aggregation_state, :completed_at, DateTime.utc_now())
        new_state =
          state
          |> Map.update!(:active_runs, &Map.delete(&1, payroll_run_id))
          |> Map.update!(:completed_runs, &Map.put(&1, payroll_run_id, completed_state))
          |> Map.update!(:reports, &Map.put(&1, payroll_run_id, report))

        {:reply, {:ok, report}, new_state}
    end
  end

  @impl true
  def handle_call({:get_payroll_report, payroll_run_id}, _from, state) do
    case Map.get(state.reports, payroll_run_id) do
      nil -> {:reply, {:error, :not_found}, state}
      report -> {:reply, {:ok, report}, state}
    end
  end

  @impl true
  def handle_cast({:cleanup_old_runs, days_to_keep}, state) do
    cutoff_date = DateTime.add(DateTime.utc_now(), -days_to_keep * 24 * 60 * 60, :second)
    
    # Filter out old completed runs
    new_completed_runs =
      Enum.filter(state.completed_runs, fn {_run_id, run_state} ->
        DateTime.compare(Map.get(run_state, :completed_at, cutoff_date), cutoff_date) == :gt
      end)
      |> Enum.into(%{})

    # Filter out old reports
    new_reports =
      Enum.filter(state.reports, fn {_run_id, report} ->
        DateTime.compare(report.generated_at, cutoff_date) == :gt
      end)
      |> Enum.into(%{})

    new_state = %{
      state
      | completed_runs: new_completed_runs,
        reports: new_reports
    }

    {:noreply, new_state}
  end

  # Private functions

  defp init_aggregated_totals do
    %{
      total_employees: 0,
      total_regular_hours: 0.0,
      total_overtime_hours: 0.0,
      total_hours: 0.0,
      employees_with_overtime: 0,
      total_processed_entries: 0,
      total_overtime_entries: 0,
      average_hours_per_employee: 0.0,
      overtime_percentage: 0.0,
      processing_errors: 0
    }
  end

  defp update_aggregated_totals(current_totals, employee_result) do
    employee_summary = Map.get(employee_result, :summary, %{})
    
    regular_hours = Map.get(employee_summary, :total_regular_hours, 0.0)
    overtime_hours = Map.get(employee_summary, :total_overtime_hours, 0.0)
    has_overtime = overtime_hours > 0
    processed_entries_count = length(Map.get(employee_result, :processed_entries, []))
    overtime_entries_count = length(Map.get(employee_result, :overtime_entries, []))

    updated_totals = %{
      total_employees: current_totals.total_employees + 1,
      total_regular_hours: current_totals.total_regular_hours + regular_hours,
      total_overtime_hours: current_totals.total_overtime_hours + overtime_hours,
      total_hours: current_totals.total_hours + regular_hours + overtime_hours,
      employees_with_overtime: current_totals.employees_with_overtime + if(has_overtime, do: 1, else: 0),
      total_processed_entries: current_totals.total_processed_entries + processed_entries_count,
      total_overtime_entries: current_totals.total_overtime_entries + overtime_entries_count,
      processing_errors: current_totals.processing_errors + Map.get(employee_summary, :errors, 0)
    }

    # Calculate derived metrics
    Map.merge(updated_totals, %{
      average_hours_per_employee: if(updated_totals.total_employees > 0, 
                                    do: updated_totals.total_hours / updated_totals.total_employees, 
                                    else: 0.0),
      overtime_percentage: if(updated_totals.total_hours > 0, 
                             do: updated_totals.total_overtime_hours / updated_totals.total_hours * 100.0, 
                             else: 0.0)
    })
  end

  defp calculate_processing_progress(aggregation_state) do
    progress_percentage = if aggregation_state.expected_employee_count do
      min(100.0, aggregation_state.total_employees_processed / aggregation_state.expected_employee_count * 100.0)
    else
      nil
    end

    processing_duration = DateTime.diff(DateTime.utc_now(), aggregation_state.start_time, :second)

    %{
      employees_processed: aggregation_state.total_employees_processed,
      expected_employee_count: aggregation_state.expected_employee_count,
      progress_percentage: progress_percentage,
      processing_duration_seconds: processing_duration,
      last_updated: aggregation_state.last_updated,
      current_totals: aggregation_state.aggregated_totals
    }
  end

  defp generate_payroll_report(aggregation_state) do
    processing_duration = DateTime.diff(DateTime.utc_now(), aggregation_state.start_time, :second)
    
    # Calculate performance metrics
    performance_metrics = %{
      total_processing_time_seconds: processing_duration,
      average_processing_time_per_employee: if(aggregation_state.total_employees_processed > 0, 
                                               do: processing_duration / aggregation_state.total_employees_processed, 
                                               else: 0.0),
      employees_processed_per_minute: if(processing_duration > 0, 
                                        do: aggregation_state.total_employees_processed / (processing_duration / 60.0), 
                                        else: 0.0),
      total_fact_processing_rate: calculate_fact_processing_rate(aggregation_state)
    }

    # Generate individual employee summaries
    employee_summaries = Enum.map(aggregation_state.employee_results, fn employee_result ->
      %{
        employee_id: employee_result.employee_id,
        regular_hours: Map.get(employee_result.summary, :total_regular_hours, 0.0),
        overtime_hours: Map.get(employee_result.summary, :total_overtime_hours, 0.0),
        total_hours: Map.get(employee_result.summary, :total_hours, 0.0),
        processed_entries: length(Map.get(employee_result, :processed_entries, [])),
        overtime_entries: length(Map.get(employee_result, :overtime_entries, [])),
        processing_time_ms: Map.get(employee_result, :processing_time, 0),
        processed_at: employee_result.processed_at
      }
    end)

    # Create comprehensive processing summary
    processing_summary = %{
      payroll_run_id: aggregation_state.payroll_run_id,
      start_time: aggregation_state.start_time,
      completion_time: DateTime.utc_now(),
      total_processing_duration: processing_duration,
      employees_processed: aggregation_state.total_employees_processed,
      expected_employees: aggregation_state.expected_employee_count,
      completion_status: if(aggregation_state.expected_employee_count, 
                           do: if(aggregation_state.total_employees_processed >= aggregation_state.expected_employee_count, 
                                 do: :complete, 
                                 else: :partial), 
                           else: :unknown)
    }

    %{
      payroll_run_id: aggregation_state.payroll_run_id,
      processing_summary: processing_summary,
      employee_summaries: employee_summaries,
      totals: aggregation_state.aggregated_totals,
      performance_metrics: performance_metrics,
      generated_at: DateTime.utc_now()
    }
  end

  defp calculate_fact_processing_rate(aggregation_state) do
    total_facts = aggregation_state.aggregated_totals.total_processed_entries
    processing_duration = DateTime.diff(DateTime.utc_now(), aggregation_state.start_time, :second)
    
    if processing_duration > 0 do
      total_facts / processing_duration
    else
      0.0
    end
  end
end
defmodule Examples.PayrollSystem do
  @moduledoc """
  Complete payroll processing system built using Presto RETE engine.
  
  This example demonstrates how to build a production-ready payroll system
  that can handle 10,000+ employees with complex rule sets using Presto as
  the underlying rules engine.
  
  ## Features
  
  - Cross-employee aggregation and reporting
  - Progress tracking for large payroll runs
  - Bulk rule loading and optimization
  - Performance monitoring and metrics
  - Configurable payroll rules
  
  ## Usage
  
      # Start the payroll system
      {:ok, system} = Examples.PayrollSystem.start_link()
      
      # Start a payroll run
      :ok = Examples.PayrollSystem.start_payroll_run(system, "payroll_2025_01", 10_000)
      
      # Process employees (called once per employee)
      {:ok, result} = Examples.PayrollSystem.process_employee(
        system, 
        "payroll_2025_01", 
        "emp_001", 
        employee_time_entries
      )
      
      # Get progress and finalize
      {:ok, progress} = Examples.PayrollSystem.get_progress(system, "payroll_2025_01")
      {:ok, report} = Examples.PayrollSystem.finalize_run(system, "payroll_2025_01")
  """
  
  use GenServer
  
  alias Presto.Examples.PayrollRules
  
  @type employee_result :: %{
          employee_id: String.t(),
          processed_entries: [tuple()],
          overtime_entries: [tuple()],
          summary: map(),
          processing_time: non_neg_integer(),
          processed_at: DateTime.t()
        }

  @type payroll_run :: %{
          run_id: String.t(),
          expected_employee_count: non_neg_integer() | nil,
          employee_results: [employee_result()],
          totals: map(),
          start_time: DateTime.t(),
          status: :active | :completed
        }

  # Client API

  @doc """
  Starts the payroll system.
  """
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Starts a new payroll run.
  """
  @spec start_payroll_run(GenServer.server(), String.t(), non_neg_integer() | nil) :: :ok
  def start_payroll_run(system, run_id, expected_employee_count \\ nil) do
    GenServer.call(system, {:start_payroll_run, run_id, expected_employee_count})
  end

  @doc """
  Processes a single employee's payroll data.
  """
  @spec process_employee(GenServer.server(), String.t(), String.t(), [tuple()]) :: 
          {:ok, employee_result()} | {:error, term()}
  def process_employee(system, run_id, employee_id, time_entries) do
    GenServer.call(system, {:process_employee, run_id, employee_id, time_entries}, 30_000)
  end

  @doc """
  Gets the current progress of a payroll run.
  """
  @spec get_progress(GenServer.server(), String.t()) :: {:ok, map()} | {:error, :not_found}
  def get_progress(system, run_id) do
    GenServer.call(system, {:get_progress, run_id})
  end

  @doc """
  Gets the current totals for a payroll run.
  """
  @spec get_totals(GenServer.server(), String.t()) :: {:ok, map()} | {:error, :not_found}
  def get_totals(system, run_id) do
    GenServer.call(system, {:get_totals, run_id})
  end

  @doc """
  Finalizes a payroll run and generates a comprehensive report.
  """
  @spec finalize_run(GenServer.server(), String.t()) :: {:ok, map()} | {:error, term()}
  def finalize_run(system, run_id) do
    GenServer.call(system, {:finalize_run, run_id})
  end

  @doc """
  Lists all active payroll runs.
  """
  @spec list_active_runs(GenServer.server()) :: [String.t()]
  def list_active_runs(system) do
    GenServer.call(system, :list_active_runs)
  end

  # Server implementation

  @impl true
  def init(_opts) do
    # Start a dedicated rule engine for payroll processing
    {:ok, engine} = Presto.start_engine()
    
    # Load payroll rules using bulk loading
    load_payroll_rules(engine)
    
    state = %{
      engine: engine,
      active_runs: %{},
      completed_runs: %{},
      reports: %{}
    }
    
    {:ok, state}
  end

  @impl true
  def handle_call({:start_payroll_run, run_id, expected_employee_count}, _from, state) do
    if Map.has_key?(state.active_runs, run_id) do
      {:reply, {:error, :already_exists}, state}
    else
      payroll_run = %{
        run_id: run_id,
        expected_employee_count: expected_employee_count,
        employee_results: [],
        totals: init_totals(),
        start_time: DateTime.utc_now(),
        status: :active
      }
      
      new_state = put_in(state, [:active_runs, run_id], payroll_run)
      {:reply, :ok, new_state}
    end
  end

  @impl true
  def handle_call({:process_employee, run_id, employee_id, time_entries}, _from, state) do
    case Map.get(state.active_runs, run_id) do
      nil ->
        {:reply, {:error, :run_not_found}, state}
      
      payroll_run ->
        case process_employee_with_presto(state.engine, employee_id, time_entries) do
          {:ok, employee_result} ->
            # Update the payroll run with this employee's results
            updated_run = 
              payroll_run
              |> Map.update!(:employee_results, fn results -> [employee_result | results] end)
              |> Map.update!(:totals, fn totals -> update_totals(totals, employee_result) end)
            
            new_state = put_in(state, [:active_runs, run_id], updated_run)
            {:reply, {:ok, employee_result}, new_state}
          
          {:error, reason} ->
            {:reply, {:error, reason}, state}
        end
    end
  end

  @impl true
  def handle_call({:get_progress, run_id}, _from, state) do
    case Map.get(state.active_runs, run_id) do
      nil -> {:reply, {:error, :not_found}, state}
      payroll_run -> 
        progress = calculate_progress(payroll_run)
        {:reply, {:ok, progress}, state}
    end
  end

  @impl true
  def handle_call({:get_totals, run_id}, _from, state) do
    case Map.get(state.active_runs, run_id) do
      nil -> 
        case Map.get(state.completed_runs, run_id) do
          nil -> {:reply, {:error, :not_found}, state}
          completed_run -> {:reply, {:ok, completed_run.totals}, state}
        end
      payroll_run -> 
        {:reply, {:ok, payroll_run.totals}, state}
    end
  end

  @impl true
  def handle_call({:finalize_run, run_id}, _from, state) do
    case Map.get(state.active_runs, run_id) do
      nil ->
        {:reply, {:error, :run_not_found}, state}
      
      payroll_run ->
        # Generate comprehensive report
        report = generate_payroll_report(payroll_run)
        
        # Move to completed runs
        completed_run = Map.put(payroll_run, :status, :completed)
        new_state = 
          state
          |> Map.update!(:active_runs, &Map.delete(&1, run_id))
          |> Map.update!(:completed_runs, &Map.put(&1, run_id, completed_run))
          |> Map.update!(:reports, &Map.put(&1, run_id, report))
        
        {:reply, {:ok, report}, new_state}
    end
  end

  @impl true
  def handle_call(:list_active_runs, _from, state) do
    active_run_ids = Map.keys(state.active_runs)
    {:reply, active_run_ids, state}
  end

  @impl true
  def terminate(_reason, state) do
    Presto.stop_engine(state.engine)
    :ok
  end

  # Private helper functions

  defp load_payroll_rules(engine) do
    case Presto.bulk_load_rules_from_modules(engine, [PayrollRules], %{
      optimization_level: :advanced,
      compile_rules: true,
      validate_rules: true
    }) do
      {:ok, _result} -> :ok
      {:error, reason} -> 
        raise "Failed to load payroll rules: #{inspect(reason)}"
    end
  end

  defp process_employee_with_presto(engine, employee_id, time_entries) do
    start_time = System.monotonic_time(:millisecond)
    
    try do
      # Clear previous facts from the engine
      Presto.clear_facts(engine)
      
      # Assert this employee's time entries
      Enum.each(time_entries, fn time_entry ->
        Presto.assert_fact(engine, time_entry)
      end)
      
      # Fire rules to process the data
      rule_results = Presto.fire_rules(engine, concurrent: true)
      
      # Extract and organise results
      {processed_entries, overtime_entries} = extract_employee_results(rule_results)
      
      # Generate summary
      summary = generate_employee_summary(processed_entries, overtime_entries)
      
      processing_time = System.monotonic_time(:millisecond) - start_time
      
      employee_result = %{
        employee_id: employee_id,
        processed_entries: processed_entries,
        overtime_entries: overtime_entries,
        summary: summary,
        processing_time: processing_time,
        processed_at: DateTime.utc_now()
      }
      
      {:ok, employee_result}
    rescue
      error -> {:error, {:processing_error, error}}
    end
  end

  defp extract_employee_results(rule_results) do
    processed_entries = Enum.filter(rule_results, fn
      {:time_entry, _, data} -> 
        case Map.get(data, :units) do
          nil -> false
          _ -> true
        end
      _ -> false
    end)

    overtime_entries = Enum.filter(rule_results, fn
      {:overtime_entry, _, _} -> true
      _ -> false
    end)

    {processed_entries, overtime_entries}
  end

  defp generate_employee_summary(processed_entries, overtime_entries) do
    total_regular_hours = 
      processed_entries
      |> Enum.map(fn {:time_entry, _, data} -> Map.get(data, :units, 0.0) end)
      |> Enum.sum()
      |> ensure_float()

    total_overtime_hours = 
      overtime_entries
      |> Enum.map(fn {:overtime_entry, _, data} -> Map.get(data, :units, 0.0) end)
      |> Enum.sum()
      |> ensure_float()

    %{
      total_regular_hours: total_regular_hours,
      total_overtime_hours: total_overtime_hours,
      total_hours: total_regular_hours + total_overtime_hours,
      processed_entries_count: length(processed_entries),
      overtime_entries_count: length(overtime_entries),
      has_overtime: total_overtime_hours > 0
    }
  end

  defp ensure_float(value) when is_float(value), do: Float.round(value, 2)
  defp ensure_float(value), do: value * 1.0

  defp init_totals do
    %{
      total_employees: 0,
      total_regular_hours: 0.0,
      total_overtime_hours: 0.0,
      total_hours: 0.0,
      employees_with_overtime: 0,
      average_hours_per_employee: 0.0,
      overtime_percentage: 0.0
    }
  end

  defp update_totals(current_totals, employee_result) do
    summary = employee_result.summary
    
    new_totals = %{
      total_employees: current_totals.total_employees + 1,
      total_regular_hours: current_totals.total_regular_hours + summary.total_regular_hours,
      total_overtime_hours: current_totals.total_overtime_hours + summary.total_overtime_hours,
      total_hours: current_totals.total_hours + summary.total_hours,
      employees_with_overtime: current_totals.employees_with_overtime + 
                               if(summary.has_overtime, do: 1, else: 0)
    }
    
    # Calculate derived metrics
    Map.merge(new_totals, %{
      average_hours_per_employee: if(new_totals.total_employees > 0, 
                                    do: new_totals.total_hours / new_totals.total_employees, 
                                    else: 0.0),
      overtime_percentage: if(new_totals.total_hours > 0, 
                             do: new_totals.total_overtime_hours / new_totals.total_hours * 100.0, 
                             else: 0.0)
    })
  end

  defp calculate_progress(payroll_run) do
    employees_processed = length(payroll_run.employee_results)
    processing_duration = DateTime.diff(DateTime.utc_now(), payroll_run.start_time, :second)
    
    progress_percentage = if payroll_run.expected_employee_count do
      min(100.0, employees_processed / payroll_run.expected_employee_count * 100.0)
    else
      nil
    end
    
    %{
      employees_processed: employees_processed,
      expected_employee_count: payroll_run.expected_employee_count,
      progress_percentage: progress_percentage,
      processing_duration_seconds: processing_duration,
      current_totals: payroll_run.totals
    }
  end

  defp generate_payroll_report(payroll_run) do
    completion_time = DateTime.utc_now()
    processing_duration = DateTime.diff(completion_time, payroll_run.start_time, :second)
    employees_processed = length(payroll_run.employee_results)
    
    # Performance metrics
    performance_metrics = %{
      total_processing_time_seconds: processing_duration,
      average_processing_time_per_employee: if(employees_processed > 0, 
                                               do: processing_duration / employees_processed, 
                                               else: 0.0),
      employees_processed_per_minute: if(processing_duration > 0, 
                                        do: employees_processed / (processing_duration / 60.0), 
                                        else: 0.0)
    }
    
    # Employee summaries
    employee_summaries = Enum.map(payroll_run.employee_results, fn result ->
      %{
        employee_id: result.employee_id,
        total_hours: result.summary.total_hours,
        overtime_hours: result.summary.total_overtime_hours,
        processing_time_ms: result.processing_time,
        processed_at: result.processed_at
      }
    end)
    
    %{
      run_id: payroll_run.run_id,
      start_time: payroll_run.start_time,
      completion_time: completion_time,
      totals: payroll_run.totals,
      performance_metrics: performance_metrics,
      employee_summaries: employee_summaries,
      generated_at: DateTime.utc_now()
    }
  end
end
defmodule Examples.MassiveScalePayroll.PayrollCoordinator do
  @moduledoc """
  Orchestrates massive scale payroll processing using Presto as a generic rules engine.

  This coordinator manages the entire payroll processing pipeline:
  - Batching employees for parallel processing
  - Distributing work across multiple Presto rule engines
  - Coordinating shift segmentation and overtime aggregation
  - Monitoring progress and performance metrics

  Key design principles:
  - Presto remains a generic rules engine
  - Domain-specific logic is in the coordinator and processors
  - Parallel processing with controlled memory usage
  - Real-time progress tracking
  """

  use GenServer

  alias Examples.MassiveScalePayroll.{
    ShiftSegmentProcessor,
    EmployeeWorker,
    OvertimeAggregator,
    PerformanceMonitor
  }

  @type processing_options :: %{
          batch_size: pos_integer(),
          max_concurrency: pos_integer(),
          memory_limit_mb: pos_integer(),
          enable_streaming: boolean()
        }

  @type payroll_run_config :: %{
          run_id: String.t(),
          employee_count: pos_integer(),
          expected_shifts: pos_integer(),
          processing_options: processing_options()
        }

  @default_options %{
    batch_size: 50,
    max_concurrency: 8,
    memory_limit_mb: 4_000,
    enable_streaming: true
  }

  # Client API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Starts a massive scale payroll run.
  """
  @spec start_payroll_run(String.t(), pos_integer(), pos_integer(), processing_options()) ::
          {:ok, pid()} | {:error, term()}
  def start_payroll_run(run_id, employee_count, expected_shifts, options \\ %{}) do
    processing_options = Map.merge(@default_options, options)

    config = %{
      run_id: run_id,
      employee_count: employee_count,
      expected_shifts: expected_shifts,
      processing_options: processing_options
    }

    GenServer.call(__MODULE__, {:start_payroll_run, config})
  end

  @doc """
  Processes payroll data asynchronously.
  """
  @spec process_payroll_async(String.t(), [map()]) :: {:ok, pid()} | {:error, term()}
  def process_payroll_async(run_id, employee_data) do
    GenServer.call(__MODULE__, {:process_payroll_async, run_id, employee_data}, :infinity)
  end

  @doc """
  Gets real-time processing progress.
  """
  @spec get_progress(String.t()) :: {:ok, map()} | {:error, :not_found}
  def get_progress(run_id) do
    GenServer.call(__MODULE__, {:get_progress, run_id})
  end

  @doc """
  Finalises a payroll run and generates comprehensive report.
  """
  @spec finalize_run(String.t()) :: {:ok, map()} | {:error, term()}
  def finalize_run(run_id) do
    GenServer.call(__MODULE__, {:finalize_run, run_id}, :infinity)
  end

  # Server implementation

  @impl true
  def init(_opts) do
    # Start performance monitor
    {:ok, monitor_pid} = PerformanceMonitor.start_link()

    # Start overtime aggregator
    {:ok, aggregator_pid} = OvertimeAggregator.start_link()

    state = %{
      active_runs: %{},
      completed_runs: %{},
      monitor_pid: monitor_pid,
      aggregator_pid: aggregator_pid,
      worker_pool: []
    }

    {:ok, state}
  end

  @impl true
  def handle_call({:start_payroll_run, config}, _from, state) do
    %{run_id: run_id} = config

    if Map.has_key?(state.active_runs, run_id) do
      {:reply, {:error, :already_exists}, state}
    else
      # Initialize worker pool for this run
      worker_pool = initialize_worker_pool(config)

      # Start performance monitoring for this run
      PerformanceMonitor.start_monitoring(state.monitor_pid, run_id, config)

      # Initialize overtime aggregator for this run
      OvertimeAggregator.start_aggregation(state.aggregator_pid, run_id)

      run_state = %{
        config: config,
        worker_pool: worker_pool,
        status: :initialized,
        start_time: DateTime.utc_now(),
        processed_employees: 0,
        current_batch: 0,
        total_batches: 0
      }

      new_state = put_in(state, [:active_runs, run_id], run_state)

      {:reply, {:ok, self()}, new_state}
    end
  end

  @impl true
  def handle_call({:process_payroll_async, run_id, employee_data}, _from, state) do
    case Map.get(state.active_runs, run_id) do
      nil ->
        {:reply, {:error, :run_not_found}, state}

      run_state ->
        # Start async processing
        processing_pid =
          spawn_link(fn ->
            process_employees_in_parallel(run_id, employee_data, run_state)
          end)

        updated_run_state =
          run_state
          |> Map.put(:status, :processing)
          |> Map.put(:processing_pid, processing_pid)
          |> Map.put(:total_batches, calculate_total_batches(employee_data, run_state.config))

        new_state = put_in(state, [:active_runs, run_id], updated_run_state)

        {:reply, {:ok, processing_pid}, new_state}
    end
  end

  @impl true
  def handle_call({:get_progress, run_id}, _from, state) do
    case Map.get(state.active_runs, run_id) do
      nil ->
        {:reply, {:error, :not_found}, state}

      run_state ->
        progress = calculate_progress(run_state)
        performance_metrics = PerformanceMonitor.get_current_metrics(state.monitor_pid, run_id)

        full_progress = Map.merge(progress, %{performance_metrics: performance_metrics})
        {:reply, {:ok, full_progress}, state}
    end
  end

  @impl true
  def handle_call({:finalize_run, run_id}, _from, state) do
    case Map.get(state.active_runs, run_id) do
      nil ->
        {:reply, {:error, :run_not_found}, state}

      run_state ->
        # Finalize overtime aggregation
        {:ok, overtime_results} =
          OvertimeAggregator.finalize_aggregation(
            state.aggregator_pid,
            run_id
          )

        # Get final performance metrics
        {:ok, performance_report} =
          PerformanceMonitor.finalize_monitoring(
            state.monitor_pid,
            run_id
          )

        # Generate comprehensive report
        report = generate_final_report(run_state, overtime_results, performance_report)

        # Move to completed runs
        completed_run_state = Map.put(run_state, :status, :completed)

        new_state =
          state
          |> Map.update!(:active_runs, &Map.delete(&1, run_id))
          |> Map.update!(:completed_runs, &Map.put(&1, run_id, completed_run_state))

        # Cleanup worker pool
        cleanup_worker_pool(run_state.worker_pool)

        {:reply, {:ok, report}, new_state}
    end
  end

  # Private implementation functions

  defp initialize_worker_pool(config) do
    %{max_concurrency: max_concurrency} = config.processing_options

    # Start worker processes, each with their own Presto rule engine
    for worker_id <- 1..max_concurrency do
      {:ok, worker_pid} = EmployeeWorker.start_link(worker_id: worker_id)
      {worker_id, worker_pid}
    end
  end

  defp cleanup_worker_pool(worker_pool) do
    Enum.each(worker_pool, fn {_worker_id, worker_pid} ->
      EmployeeWorker.stop(worker_pid)
    end)
  end

  defp process_employees_in_parallel(run_id, employee_data, run_state) do
    %{config: config, worker_pool: worker_pool} = run_state
    %{batch_size: batch_size} = config.processing_options

    # Step 1: Segment all shifts using the ShiftSegmentProcessor
    segmented_employee_data =
      Enum.map(employee_data, fn employee ->
        segmented_shifts = ShiftSegmentProcessor.segment_employee_shifts(employee.shifts)
        Map.put(employee, :segmented_shifts, segmented_shifts)
      end)

    # Step 2: Process employees in batches across the worker pool
    segmented_employee_data
    |> Enum.chunk_every(batch_size)
    |> Enum.with_index(1)
    |> Task.async_stream(
      fn {employee_batch, batch_number} ->
        process_batch_with_worker(run_id, employee_batch, batch_number, worker_pool)
      end,
      max_concurrency: length(worker_pool),
      timeout: :infinity
    )
    |> Stream.run()

    # Notify coordinator that processing is complete
    send(self(), {:processing_complete, run_id})
  end

  defp process_batch_with_worker(run_id, employee_batch, batch_number, worker_pool) do
    # Select worker in round-robin fashion
    {_worker_id, worker_pid} = Enum.at(worker_pool, rem(batch_number - 1, length(worker_pool)))

    # Process the batch
    {:ok, batch_results} = EmployeeWorker.process_employee_batch(worker_pid, employee_batch)

    # Send results to overtime aggregator
    Enum.each(batch_results, fn employee_result ->
      OvertimeAggregator.add_employee_result(run_id, employee_result)
    end)

    # Update progress
    send(self(), {:batch_complete, run_id, batch_number, length(employee_batch)})

    batch_results
  end

  defp calculate_total_batches(employee_data, config) do
    batch_size = config.processing_options.batch_size
    div(length(employee_data) + batch_size - 1, batch_size)
  end

  defp calculate_progress(run_state) do
    %{
      run_id: run_state.config.run_id,
      employees_processed: run_state.processed_employees,
      total_employees: run_state.config.employee_count,
      current_batch: run_state.current_batch,
      total_batches: run_state.total_batches,
      progress_percentage: calculate_progress_percentage(run_state),
      processing_duration_seconds: calculate_processing_duration(run_state),
      status: run_state.status
    }
  end

  defp calculate_progress_percentage(run_state) do
    if run_state.config.employee_count > 0 do
      min(100.0, run_state.processed_employees / run_state.config.employee_count * 100.0)
    else
      0.0
    end
  end

  defp calculate_processing_duration(run_state) do
    DateTime.diff(DateTime.utc_now(), run_state.start_time, :second)
  end

  defp generate_final_report(run_state, overtime_results, performance_report) do
    %{
      run_id: run_state.config.run_id,
      processing_summary: %{
        start_time: run_state.start_time,
        completion_time: DateTime.utc_now(),
        total_employees: run_state.config.employee_count,
        expected_shifts: run_state.config.expected_shifts,
        processing_options: run_state.config.processing_options
      },
      overtime_results: overtime_results,
      performance_report: performance_report,
      generated_at: DateTime.utc_now()
    }
  end

  @impl true
  def handle_info({:processing_complete, run_id}, state) do
    case Map.get(state.active_runs, run_id) do
      nil ->
        {:noreply, state}

      run_state ->
        updated_run_state = Map.put(run_state, :status, :processing_complete)
        new_state = put_in(state, [:active_runs, run_id], updated_run_state)
        {:noreply, new_state}
    end
  end

  @impl true
  def handle_info({:batch_complete, run_id, batch_number, batch_size}, state) do
    case Map.get(state.active_runs, run_id) do
      nil ->
        {:noreply, state}

      run_state ->
        updated_run_state =
          run_state
          |> Map.update!(:processed_employees, &(&1 + batch_size))
          |> Map.put(:current_batch, batch_number)

        new_state = put_in(state, [:active_runs, run_id], updated_run_state)
        {:noreply, new_state}
    end
  end
end

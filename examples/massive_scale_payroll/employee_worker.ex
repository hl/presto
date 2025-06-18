defmodule Examples.MassiveScalePayroll.EmployeeWorker do
  @moduledoc """
  Individual worker process that uses Presto as a generic rules engine 
  to process employee payroll data.

  Each worker maintains its own Presto rule engine instance to enable
  parallel processing. The worker handles:
  - Loading payroll rules into its Presto engine
  - Processing batches of employees through the rules engine
  - Extracting and formatting results

  Key design principle: Presto remains a generic rules engine.
  All payroll-specific logic is in the rules and result processing.
  """

  use GenServer

  alias Examples.MassiveScalePayroll.{ShiftSegmentProcessor, MassivePayrollRules}

  @type employee_data :: %{
          employee_id: String.t(),
          segmented_shifts: [ShiftSegmentProcessor.shift_segment()],
          employee_info: map()
        }

  @type employee_result :: %{
          employee_id: String.t(),
          processed_segments: [tuple()],
          overtime_eligible_segments: [tuple()],
          summary: map(),
          processing_time_ms: non_neg_integer(),
          errors: [String.t()]
        }

  # Client API

  def start_link(opts) do
    worker_id = Keyword.get(opts, :worker_id, 1)
    GenServer.start_link(__MODULE__, %{worker_id: worker_id}, opts)
  end

  @doc """
  Processes a batch of employees using this worker's Presto engine.
  """
  @spec process_employee_batch(pid(), [employee_data()]) ::
          {:ok, [employee_result()]} | {:error, term()}
  def process_employee_batch(worker_pid, employee_batch) do
    GenServer.call(worker_pid, {:process_employee_batch, employee_batch}, 30_000)
  end

  @doc """
  Gets the current status of this worker.
  """
  @spec get_worker_status(pid()) :: {:ok, map()}
  def get_worker_status(worker_pid) do
    GenServer.call(worker_pid, :get_worker_status)
  end

  @doc """
  Stops the worker and cleans up its Presto engine.
  """
  @spec stop(pid()) :: :ok
  def stop(worker_pid) do
    GenServer.stop(worker_pid, :normal)
  end

  # Server implementation

  @impl true
  def init(%{worker_id: worker_id}) do
    # Start a dedicated Presto rule engine for this worker
    {:ok, presto_engine} = Presto.start_engine()

    # Load payroll rules into the engine
    case load_payroll_rules(presto_engine) do
      :ok ->
        state = %{
          worker_id: worker_id,
          presto_engine: presto_engine,
          status: :ready,
          employees_processed: 0,
          total_processing_time_ms: 0,
          last_batch_size: 0,
          errors: []
        }

        {:ok, state}

      {:error, reason} ->
        Presto.stop_engine(presto_engine)
        {:stop, {:failed_to_load_rules, reason}}
    end
  end

  @impl true
  def handle_call({:process_employee_batch, employee_batch}, _from, state) do
    batch_start_time = System.monotonic_time(:millisecond)

    # Update status to processing
    new_state = Map.put(state, :status, :processing)

    try do
      # Process each employee in the batch
      batch_results =
        Enum.map(employee_batch, fn employee_data ->
          process_single_employee(state.presto_engine, employee_data)
        end)

      # Check for any errors
      {successful_results, failed_results} =
        Enum.split_with(batch_results, fn
          {:ok, _result} -> true
          {:error, _reason} -> false
        end)

      # Extract successful results
      results = Enum.map(successful_results, fn {:ok, result} -> result end)

      # Collect error messages
      error_messages =
        Enum.map(failed_results, fn {:error, reason} ->
          "Employee processing failed: #{inspect(reason)}"
        end)

      batch_processing_time = System.monotonic_time(:millisecond) - batch_start_time

      # Update worker state
      updated_state =
        new_state
        |> Map.put(:status, :ready)
        |> Map.update!(:employees_processed, &(&1 + length(employee_batch)))
        |> Map.update!(:total_processing_time_ms, &(&1 + batch_processing_time))
        |> Map.put(:last_batch_size, length(employee_batch))
        |> Map.update!(:errors, &(&1 ++ error_messages))

      {:reply, {:ok, results}, updated_state}
    rescue
      error ->
        error_message = "Batch processing failed: #{inspect(error)}"

        updated_state =
          new_state
          |> Map.put(:status, :error)
          |> Map.update!(:errors, &[error_message | &1])

        {:reply, {:error, error}, updated_state}
    end
  end

  @impl true
  def handle_call(:get_worker_status, _from, state) do
    status = %{
      worker_id: state.worker_id,
      status: state.status,
      employees_processed: state.employees_processed,
      total_processing_time_ms: state.total_processing_time_ms,
      average_processing_time_per_employee: calculate_average_processing_time(state),
      last_batch_size: state.last_batch_size,
      error_count: length(state.errors),
      recent_errors: Enum.take(state.errors, 5)
    }

    {:reply, {:ok, status}, state}
  end

  @impl true
  def terminate(_reason, state) do
    # Clean up the Presto engine
    if state.presto_engine do
      Presto.stop_engine(state.presto_engine)
    end

    :ok
  end

  # Private helper functions

  defp load_payroll_rules(presto_engine) do
    try do
      # Load rules using Presto's bulk loading feature
      case Presto.bulk_load_rules_from_modules(presto_engine, [MassivePayrollRules], %{
             optimization_level: :advanced,
             compile_rules: true,
             validate_rules: true
           }) do
        {:ok, _result} -> :ok
        {:error, reason} -> {:error, reason}
      end
    rescue
      error -> {:error, {:rule_loading_exception, error}}
    end
  end

  defp process_single_employee(presto_engine, employee_data) do
    employee_start_time = System.monotonic_time(:millisecond)

    %{
      employee_id: employee_id,
      segmented_shifts: segmented_shifts,
      employee_info: employee_info
    } = employee_data

    try do
      # Clear any previous facts from the engine
      Presto.clear_facts(presto_engine)

      # Convert shift segments to Presto facts
      shift_facts = ShiftSegmentProcessor.segments_to_presto_facts(segmented_shifts)

      # Assert employee info as a fact
      employee_fact = {:employee_info, employee_id, employee_info}

      # Assert all facts into the Presto engine
      Presto.assert_fact(presto_engine, employee_fact)
      Enum.each(shift_facts, &Presto.assert_fact(presto_engine, &1))

      # Fire rules to process the data
      # Presto handles all the pattern matching and rule execution
      rule_results = Presto.fire_rules(presto_engine, concurrent: true)

      # Extract and organise results
      {processed_segments, overtime_eligible_segments} = extract_processing_results(rule_results)

      # Generate employee summary
      summary =
        generate_employee_summary(processed_segments, overtime_eligible_segments, employee_info)

      processing_time = System.monotonic_time(:millisecond) - employee_start_time

      employee_result = %{
        employee_id: employee_id,
        processed_segments: processed_segments,
        overtime_eligible_segments: overtime_eligible_segments,
        summary: summary,
        processing_time_ms: processing_time,
        errors: []
      }

      {:ok, employee_result}
    rescue
      error ->
        processing_time = System.monotonic_time(:millisecond) - employee_start_time

        # Return error result with partial information
        error_result = %{
          employee_id: employee_id,
          processed_segments: [],
          overtime_eligible_segments: [],
          summary: %{error: true, processing_time_ms: processing_time},
          processing_time_ms: processing_time,
          errors: ["Processing exception: #{inspect(error)}"]
        }

        {:error, {employee_id, error}}
    end
  end

  defp extract_processing_results(rule_results) do
    # Separate processed segments from overtime-eligible segments
    processed_segments =
      Enum.filter(rule_results, fn
        {:shift_segment, _id, %{units: units}} when not is_nil(units) -> true
        _ -> false
      end)

    overtime_eligible_segments =
      Enum.filter(rule_results, fn
        {:overtime_eligible, _id, _data} -> true
        _ -> false
      end)

    {processed_segments, overtime_eligible_segments}
  end

  defp generate_employee_summary(processed_segments, overtime_eligible_segments, employee_info) do
    # Calculate total regular hours
    total_regular_hours =
      processed_segments
      |> Enum.map(fn {:shift_segment, _id, data} -> Map.get(data, :units, 0.0) end)
      |> Enum.sum()
      |> ensure_float()

    # Calculate total potential overtime hours
    potential_overtime_hours =
      overtime_eligible_segments
      |> Enum.map(fn {:overtime_eligible, _id, data} -> Map.get(data, :hours, 0.0) end)
      |> Enum.sum()
      |> ensure_float()

    # Calculate base pay amounts
    total_base_pay =
      processed_segments
      |> Enum.map(fn {:shift_segment, _id, data} -> Map.get(data, :base_pay_amount, 0.0) end)
      |> Enum.sum()
      |> ensure_float()

    # Extract employee-specific information
    base_hourly_rate = Map.get(employee_info, :base_hourly_rate, 0.0)
    department = Map.get(employee_info, :department, "unknown")
    job_title = Map.get(employee_info, :job_title, "unknown")

    %{
      employee_id: Map.get(employee_info, :employee_id),
      total_regular_hours: total_regular_hours,
      potential_overtime_hours: potential_overtime_hours,
      total_base_pay: total_base_pay,
      base_hourly_rate: base_hourly_rate,
      department: department,
      job_title: job_title,
      processed_segments_count: length(processed_segments),
      overtime_eligible_segments_count: length(overtime_eligible_segments),
      has_overtime_potential: potential_overtime_hours > 0,
      error: false
    }
  end

  defp ensure_float(value) when is_float(value), do: Float.round(value, 2)
  defp ensure_float(value) when is_number(value), do: value * 1.0
  defp ensure_float(_), do: 0.0

  defp calculate_average_processing_time(state) do
    if state.employees_processed > 0 do
      Float.round(state.total_processing_time_ms / state.employees_processed, 2)
    else
      0.0
    end
  end
end

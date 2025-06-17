#!/usr/bin/env elixir

# Enhanced Payroll Demo - Demonstrates the new payroll processing capabilities
# This script shows how to process multiple employees with 1000+ rules efficiently

# Add the lib directory to the code path
Code.prepend_path("lib")

# Ensure Jason is available for JSON parsing
Mix.install([
  {:jason, "~> 1.4"}
])

# Load all required modules
Code.require_file("lib/presto/utils.ex")
Code.require_file("lib/presto/exceptions.ex") 
Code.require_file("lib/presto/logger.ex")
Code.require_file("lib/presto/rule_analyzer.ex")
Code.require_file("lib/presto/working_memory.ex")
Code.require_file("lib/presto/alpha_network.ex")
Code.require_file("lib/presto/beta_network.ex")
Code.require_file("lib/presto/optimisation/compile_time_optimizer.ex")
Code.require_file("lib/presto/optimisation/shared_memory_manager.ex")
Code.require_file("lib/presto/rule_engine.ex")
Code.require_file("lib/presto/bulk_rule_loader.ex")
Code.require_file("lib/presto/payroll_aggregator.ex")
Code.require_file("lib/presto.ex")
Code.require_file("lib/presto/examples/payroll_rules.ex")

defmodule EnhancedPayrollDemo do
  @moduledoc """
  Demonstration of enhanced payroll processing capabilities for large-scale scenarios.
  
  This demo simulates processing 100 employees (scaled down from 10,000 for demo purposes)
  with multiple shifts and complex payroll rules.
  """

  def run_demo do
    IO.puts("=== Enhanced Presto Payroll System Demo ===\n")
    
    # Step 1: Start the rule engine
    {:ok, engine} = Presto.start_engine()
    IO.puts("✓ Rule engine started")

    # Step 2: Load payroll rules using bulk loading
    load_payroll_rules(engine)
    
    # Step 3: Start a payroll run
    payroll_run_id = "demo_payroll_#{System.unique_integer()}"
    employee_count = 100  # Scaled down for demo
    
    Presto.start_payroll_run(payroll_run_id, employee_count)
    IO.puts("✓ Started payroll run: #{payroll_run_id} for #{employee_count} employees")

    # Step 4: Process multiple employees
    process_employees(engine, payroll_run_id, employee_count)

    # Step 5: Show progress and totals
    show_progress(payroll_run_id)

    # Step 6: Finalize and generate report
    generate_final_report(payroll_run_id)

    # Cleanup
    Presto.stop_engine(engine)
    IO.puts("\n✓ Demo completed successfully!")
  end

  defp load_payroll_rules(engine) do
    IO.puts("\n--- Loading Payroll Rules ---")
    
    # Use bulk loading with existing PayrollRules module
    case Presto.bulk_load_rules_from_modules(engine, [Presto.Examples.PayrollRules], %{
      optimization_level: :advanced,
      compile_rules: true,
      validate_rules: true
    }) do
      {:ok, result} ->
        IO.puts("✓ Loaded #{result.successful_loads} rules in #{result.compilation_time_ms}ms")
        IO.puts("  - Optimization level: advanced")
        IO.puts("  - Compilation enabled: yes")
      {:error, reason} ->
        IO.puts("✗ Failed to load rules: #{inspect(reason)}")
    end
  end

  defp process_employees(engine, payroll_run_id, employee_count) do
    IO.puts("\n--- Processing Employees ---")
    
    start_time = System.monotonic_time(:millisecond)
    
    # Process employees in batches for better performance
    batch_size = 10
    batches = div(employee_count, batch_size)
    
    for batch_num <- 1..batches do
      batch_start = (batch_num - 1) * batch_size + 1
      batch_end = min(batch_num * batch_size, employee_count)
      
      IO.write("  Batch #{batch_num}/#{batches}: employees #{batch_start}-#{batch_end}... ")
      
      batch_start_time = System.monotonic_time(:millisecond)
      
      # Process employees in this batch
      for emp_num <- batch_start..batch_end do
        employee_id = "emp_#{String.pad_leading(to_string(emp_num), 4, "0")}"
        time_entries = generate_employee_shifts(employee_id)
        
        # Process this employee's payroll
        case Presto.process_employee_payroll(engine, payroll_run_id, employee_id, time_entries) do
          {:ok, _result} -> :ok
          {:error, reason} -> 
            IO.puts("\n    ✗ Error processing #{employee_id}: #{inspect(reason)}")
        end
      end
      
      batch_time = System.monotonic_time(:millisecond) - batch_start_time
      IO.puts("done (#{batch_time}ms)")
    end
    
    total_time = System.monotonic_time(:millisecond) - start_time
    avg_time_per_employee = total_time / employee_count
    
    IO.puts("✓ Processed #{employee_count} employees in #{total_time}ms")
    IO.puts("  - Average time per employee: #{Float.round(avg_time_per_employee, 2)}ms")
  end

  defp generate_employee_shifts(employee_id) do
    # Generate 12 shifts per week × 4 weeks = 48 shifts per month
    base_date = ~D[2025-01-01]
    
    for week <- 1..4, shift <- 1..12 do
      shift_date = Date.add(base_date, (week - 1) * 7 + rem(shift - 1, 7))
      shift_start = DateTime.new!(shift_date, ~T[09:00:00], "Etc/UTC")
      shift_end = DateTime.add(shift_start, 8 * 60 * 60)  # 8 hour shifts
      
      entry_id = "#{employee_id}_w#{week}_s#{shift}"
      
      {:time_entry, entry_id, %{
        employee_id: employee_id,
        start_datetime: shift_start,
        finish_datetime: shift_end,
        minutes: nil,
        units: nil
      }}
    end
  end

  defp show_progress(payroll_run_id) do
    IO.puts("\n--- Current Progress ---")
    
    case Presto.get_payroll_progress(payroll_run_id) do
      {:ok, progress} ->
        IO.puts("  Employees processed: #{progress.employees_processed}")
        if progress.expected_employee_count do
          IO.puts("  Progress: #{Float.round(progress.progress_percentage, 1)}%")
        end
        IO.puts("  Processing duration: #{progress.processing_duration_seconds}s")
        
      {:error, reason} ->
        IO.puts("  ✗ Could not get progress: #{inspect(reason)}")
    end
    
    case Presto.get_payroll_totals(payroll_run_id) do
      {:ok, totals} ->
        IO.puts("\n--- Current Totals ---")
        IO.puts("  Total employees: #{totals.total_employees}")
        IO.puts("  Total regular hours: #{totals.total_regular_hours}")
        IO.puts("  Total overtime hours: #{totals.total_overtime_hours}")
        IO.puts("  Total hours: #{totals.total_hours}")
        IO.puts("  Employees with overtime: #{totals.employees_with_overtime}")
        IO.puts("  Average hours per employee: #{Float.round(totals.average_hours_per_employee, 2)}")
        IO.puts("  Overtime percentage: #{Float.round(totals.overtime_percentage, 2)}%")
        
      {:error, reason} ->
        IO.puts("  ✗ Could not get totals: #{inspect(reason)}")
    end
  end

  defp generate_final_report(payroll_run_id) do
    IO.puts("\n--- Generating Final Report ---")
    
    case Presto.finalize_payroll_run(payroll_run_id) do
      {:ok, report} ->
        IO.puts("✓ Payroll run finalized")
        IO.puts("\n--- Final Report ---")
        IO.puts("  Payroll Run ID: #{report.payroll_run_id}")
        IO.puts("  Generation Time: #{report.generated_at}")
        
        IO.puts("\n  Processing Summary:")
        IO.puts("    - Total employees processed: #{report.totals.total_employees}")
        IO.puts("    - Total hours: #{report.totals.total_hours}")
        IO.puts("    - Overtime hours: #{report.totals.total_overtime_hours}")
        IO.puts("    - Employees with overtime: #{report.totals.employees_with_overtime}")
        
        IO.puts("\n  Performance Metrics:")
        IO.puts("    - Total processing time: #{report.performance_metrics.total_processing_time_seconds}s")
        IO.puts("    - Avg time per employee: #{Float.round(report.performance_metrics.average_processing_time_per_employee, 3)}s")
        IO.puts("    - Employees per minute: #{Float.round(report.performance_metrics.employees_processed_per_minute, 1)}")
        IO.puts("    - Fact processing rate: #{Float.round(report.performance_metrics.total_fact_processing_rate, 1)} facts/sec")
        
        # Show sample employee data
        IO.puts("\n  Sample Employee Results:")
        report.employee_summaries
        |> Enum.take(5)
        |> Enum.each(fn emp ->
          IO.puts("    #{emp.employee_id}: #{emp.total_hours}h (#{emp.overtime_hours}h OT)")
        end)
        
      {:error, reason} ->
        IO.puts("✗ Failed to finalize payroll run: #{inspect(reason)}")
    end
  end
end

# Run the demo
EnhancedPayrollDemo.run_demo()
#!/usr/bin/env elixir

# Payroll System Demo using Presto RETE Engine
# This demonstrates how to build a complete payroll system using Presto as the rules engine.

defmodule PayrollDemo do
  @moduledoc """
  Comprehensive demonstration of building a payroll system using Presto.
  
  Shows how to:
  - Use Presto as a generic rules engine for payroll processing
  - Handle 10,000+ employees with complex rule sets
  - Aggregate results across employees
  - Monitor progress and generate reports
  """

  def run do
    IO.puts("=== Payroll System Demo using Presto RETE Engine ===\n")
    
    # Load the PayrollSystem example (which uses Presto internally)
    Code.require_file("examples/payroll_system.ex")
    Code.require_file("examples/payroll_aggregator.ex")
    
    # Start the payroll system
    {:ok, system} = Examples.PayrollSystem.start_link()
    IO.puts("✓ Payroll system started (using Presto internally)")
    
    # Demo configuration
    employee_count = 50  # Scaled down for demo
    payroll_run_id = "demo_payroll_#{System.unique_integer()}"
    
    # Start a payroll run
    :ok = Examples.PayrollSystem.start_payroll_run(system, payroll_run_id, employee_count)
    IO.puts("✓ Started payroll run: #{payroll_run_id} for #{employee_count} employees")
    
    # Process employees
    process_employees(system, payroll_run_id, employee_count)
    
    # Show progress
    show_progress(system, payroll_run_id)
    
    # Finalize and show report
    generate_report(system, payroll_run_id)
    
    IO.puts("\n✓ Demo completed successfully!")
    IO.puts("\nThis demonstrates how Presto can be used as a generic rules engine")
    IO.puts("to build domain-specific systems like payroll processing.")
  end

  defp process_employees(system, payroll_run_id, employee_count) do
    IO.puts("\n--- Processing Employees with Presto Rules Engine ---")
    
    start_time = System.monotonic_time(:millisecond)
    
    # Process employees in batches
    batch_size = 10
    batches = div(employee_count, batch_size)
    
    for batch_num <- 1..batches do
      batch_start = (batch_num - 1) * batch_size + 1
      batch_end = min(batch_num * batch_size, employee_count)
      
      IO.write("  Batch #{batch_num}/#{batches}: employees #{batch_start}-#{batch_end}... ")
      
      batch_start_time = System.monotonic_time(:millisecond)
      
      # Process each employee in this batch
      for emp_num <- batch_start..batch_end do
        employee_id = "emp_#{String.pad_leading(to_string(emp_num), 4, "0")}"
        time_entries = generate_employee_time_entries(employee_id)
        
        # Use the PayrollSystem (which uses Presto internally) to process the employee
        case Examples.PayrollSystem.process_employee(system, payroll_run_id, employee_id, time_entries) do
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
    IO.puts("  - Each employee processed through Presto's RETE network")
  end

  defp generate_employee_time_entries(employee_id) do
    # Generate 12 shifts (scaled down from 48 for demo)
    base_date = ~D[2025-01-01]
    
    for shift_num <- 1..12 do
      shift_date = Date.add(base_date, shift_num - 1)
      shift_start = DateTime.new!(shift_date, ~T[09:00:00], "Etc/UTC")
      shift_end = DateTime.add(shift_start, 8 * 60 * 60)  # 8 hour shifts
      
      entry_id = "#{employee_id}_shift_#{shift_num}"
      
      {:time_entry, entry_id, %{
        employee_id: employee_id,
        start_datetime: shift_start,
        finish_datetime: shift_end,
        minutes: nil,
        units: nil
      }}
    end
  end

  defp show_progress(system, payroll_run_id) do
    IO.puts("\n--- Current Progress ---")
    
    case Examples.PayrollSystem.get_progress(system, payroll_run_id) do
      {:ok, progress} ->
        IO.puts("  Employees processed: #{progress.employees_processed}")
        if progress.expected_employee_count do
          IO.puts("  Progress: #{Float.round(progress.progress_percentage, 1)}%")
        end
        IO.puts("  Processing duration: #{progress.processing_duration_seconds}s")
        
      {:error, reason} ->
        IO.puts("  ✗ Could not get progress: #{inspect(reason)}")
    end
    
    case Examples.PayrollSystem.get_totals(system, payroll_run_id) do
      {:ok, totals} ->
        IO.puts("\n--- Current Totals (Aggregated from Presto Results) ---")
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

  defp generate_report(system, payroll_run_id) do
    IO.puts("\n--- Generating Final Report ---")
    
    case Examples.PayrollSystem.finalize_run(system, payroll_run_id) do
      {:ok, report} ->
        IO.puts("✓ Payroll run finalized")
        
        IO.puts("\n--- Final Payroll Report ---")
        IO.puts("  Run ID: #{report.run_id}")
        IO.puts("  Start Time: #{report.start_time}")
        IO.puts("  Completion Time: #{report.completion_time}")
        
        IO.puts("\n  Totals:")
        IO.puts("    - Total employees: #{report.totals.total_employees}")
        IO.puts("    - Total hours: #{report.totals.total_hours}")
        IO.puts("    - Overtime hours: #{report.totals.total_overtime_hours}")
        IO.puts("    - Employees with overtime: #{report.totals.employees_with_overtime}")
        
        IO.puts("\n  Performance Metrics:")
        IO.puts("    - Total processing time: #{report.performance_metrics.total_processing_time_seconds}s")
        IO.puts("    - Avg time per employee: #{Float.round(report.performance_metrics.average_processing_time_per_employee, 3)}s")
        IO.puts("    - Employees per minute: #{Float.round(report.performance_metrics.employees_processed_per_minute, 1)}")
        
        # Show sample employee results
        IO.puts("\n  Sample Employee Results (from Presto processing):")
        report.employee_summaries
        |> Enum.take(5)
        |> Enum.each(fn emp ->
          IO.puts("    #{emp.employee_id}: #{emp.total_hours}h (#{emp.overtime_hours}h OT)")
        end)
        
        IO.puts("\n--- Architecture Summary ---")
        IO.puts("  • Presto RETE Engine: Generic rules engine handling pattern matching")
        IO.puts("  • PayrollSystem: Domain-specific layer built on top of Presto")
        IO.puts("  • PayrollRules: Business logic implemented as Presto rules")
        IO.puts("  • This demonstrates proper separation of concerns:")
        IO.puts("    - Presto = Generic, reusable rules engine")
        IO.puts("    - Examples = Domain-specific applications using Presto")
        
      {:error, reason} ->
        IO.puts("✗ Failed to finalize payroll run: #{inspect(reason)}")
    end
  end
end

# Run the demo
PayrollDemo.run()
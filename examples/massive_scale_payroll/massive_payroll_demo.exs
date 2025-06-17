#!/usr/bin/env elixir

# Massive Scale Payroll Demo - Demonstrates processing 10,000 employees using Presto
# This script showcases the complete massive scale payroll system built on top of Presto

# Add the lib directory to the code path
Code.prepend_path("lib")

# Ensure required dependencies are available
Mix.install([
  {:jason, "~> 1.4"}
])

# Load all Presto core modules
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
Code.require_file("lib/presto.ex")

# Load massive scale payroll system modules
Code.require_file("examples/massive_scale_payroll/shift_segment_processor.ex")
Code.require_file("examples/massive_scale_payroll/massive_payroll_rules.ex")
Code.require_file("examples/massive_scale_payroll/employee_worker.ex")
Code.require_file("examples/massive_scale_payroll/overtime_aggregator.ex")
Code.require_file("examples/massive_scale_payroll/performance_monitor.ex")
Code.require_file("examples/massive_scale_payroll/payroll_coordinator.ex")
Code.require_file("examples/massive_scale_payroll/scalable_payroll_system.ex")

defmodule MassivePayrollDemo do
  @moduledoc """
  Comprehensive demonstration of massive scale payroll processing using Presto.

  This demo simulates processing 10,000 employees with 50 shifts per week,
  demonstrating:
  - Complex time-based shift segmentation (Christmas Eve → Christmas Day)
  - 2,000 payroll rules executed via Presto rule engines  
  - Parallel processing with multiple Presto instances
  - Overtime calculations with segment payment marking
  - Real-time performance monitoring and bottleneck analysis
  - Scalable architecture handling 2.15M shifts per month
  """

  alias Examples.MassiveScalePayroll.{
    ScalablePayrollSystem,
    ShiftSegmentProcessor,
    MassivePayrollRules
  }

  def run_demo do
    IO.puts("=== Massive Scale Payroll Processing Demo ===")
    IO.puts("Demonstrating Presto as a generic rules engine for massive payroll processing\n")

    # Demo configuration
    # Scaled down from 10,000 for demo performance
    demo_employee_count = 1000
    # Scaled down from 50 per week for demo
    demo_shifts_per_employee = 20

    IO.puts("Demo Configuration:")
    IO.puts("  Employees: #{demo_employee_count} (scaled down from 10,000)")

    IO.puts(
      "  Shifts per employee: #{demo_shifts_per_employee} (scaled down from 200+ per month)"
    )

    IO.puts("  Total shifts: #{demo_employee_count * demo_shifts_per_employee}")
    IO.puts("  Payroll rules: 2,000 (via Presto rule engines)")
    IO.puts("  Parallel workers: 8 (each with dedicated Presto engine)")
    IO.puts("")

    # Step 1: Start the scalable payroll system
    {:ok, system_pid} = ScalablePayrollSystem.start_link()
    IO.puts("✓ Massive Scale Payroll System started")
    IO.puts("  - Multiple Presto rule engines initialized")
    IO.puts("  - PayrollCoordinator ready for batch processing")
    IO.puts("  - PerformanceMonitor active")
    IO.puts("  - OvertimeAggregator ready")

    # Step 2: Demonstrate Christmas Eve → Christmas Day segmentation
    demonstrate_christmas_shift_segmentation()

    # Step 3: Generate massive employee dataset
    IO.puts("\n--- Generating Employee Dataset ---")

    employee_data =
      generate_massive_employee_dataset(demo_employee_count, demo_shifts_per_employee)

    IO.puts(
      "✓ Generated #{demo_employee_count} employees with #{demo_employee_count * demo_shifts_per_employee} total shifts"
    )

    # Step 4: Process payroll using Presto rule engines
    run_id = "massive_demo_#{System.unique_integer()}"

    processing_options = %{
      batch_size: 50,
      max_concurrency: 8,
      memory_limit_mb: 4_000,
      enable_streaming: true,
      enable_performance_monitoring: true,
      rule_optimization_level: :advanced
    }

    IO.puts("\n--- Processing Payroll via Presto Rule Engines ---")
    IO.puts("Starting massive payroll processing...")

    start_time = System.monotonic_time(:millisecond)

    case ScalablePayrollSystem.process_massive_payroll(run_id, employee_data, processing_options) do
      {:ok, result} ->
        processing_time = System.monotonic_time(:millisecond) - start_time

        IO.puts("✓ Massive payroll processing completed in #{processing_time}ms")

        # Step 5: Display comprehensive results
        display_processing_results(result, demo_employee_count, processing_time)

        # Step 6: Show Presto integration details
        demonstrate_presto_integration(result)

        # Step 7: Performance analysis
        analyze_performance_metrics(result)

        IO.puts("\n=== Demo Completed Successfully ===")
        IO.puts("This demonstrates how Presto can serve as a generic rules engine")

        IO.puts(
          "for building massive scale domain-specific applications like payroll processing."
        )

      {:error, reason} ->
        IO.puts("✗ Massive payroll processing failed: #{inspect(reason)}")
    end

    # Cleanup
    GenServer.stop(system_pid)
  end

  defp demonstrate_christmas_shift_segmentation do
    IO.puts("\n--- Demonstrating Complex Time-Based Segmentation ---")
    IO.puts("Example: Christmas Eve → Christmas Day shift processing")

    # This demonstrates the complex segmentation that happens before Presto processing
    segments = ShiftSegmentProcessor.example_christmas_shift()

    IO.puts("\nSegmentation demonstrates how complex time-based logic is handled")
    IO.puts("before data is processed by Presto's generic rule engine.")
    IO.puts("Each segment will be processed by Presto rules for pay calculation.")
  end

  defp generate_massive_employee_dataset(employee_count, shifts_per_employee) do
    IO.puts("Generating dataset with complex shift patterns...")

    # Generate diverse employee data
    for emp_num <- 1..employee_count do
      employee_id = "emp_#{String.pad_leading(to_string(emp_num), 4, "0")}"

      # Generate employee info
      employee_info = %{
        employee_id: employee_id,
        # $25-75/hour
        base_hourly_rate: 25.0 + :rand.uniform() * 50.0,
        department: Enum.random(["nursing", "emergency", "icu", "surgery", "general"]),
        job_title: Enum.random(["rn_level_1", "rn_level_2", "supervisor", "specialist"]),
        hire_date: ~D[2020-01-01],
        employment_type: :full_time
      }

      # Generate complex shift patterns
      shifts = generate_complex_shifts(employee_id, shifts_per_employee)

      %{
        employee_id: employee_id,
        employee_info: employee_info,
        shifts: shifts
      }
    end
  end

  defp generate_complex_shifts(employee_id, shift_count) do
    base_date = ~D[2025-01-01]

    for shift_num <- 1..shift_count do
      # Create varied shift patterns including holidays, weekends, night shifts
      # Spread over 30 days
      days_offset = rem(shift_num - 1, 30)
      shift_date = Date.add(base_date, days_offset)

      # Vary shift start times and durations
      {start_hour, duration_hours} =
        case rem(shift_num, 4) do
          # Night shift (10 PM - 6 AM)
          0 -> {22, 8}
          # Early shift (6 AM - 2 PM)  
          1 -> {6, 8}
          # Afternoon shift (2 PM - 10 PM)
          2 -> {14, 8}
          # Long day shift (9 AM - 9 PM)
          3 -> {9, 12}
        end

      start_datetime = DateTime.new!(shift_date, Time.new!(start_hour, 0, 0), "Etc/UTC")
      finish_datetime = DateTime.add(start_datetime, duration_hours * 60 * 60)

      %{
        shift_id: "#{employee_id}_shift_#{shift_num}",
        employee_id: employee_id,
        start_datetime: start_datetime,
        finish_datetime: finish_datetime,
        department: Enum.random(["nursing", "emergency", "icu"]),
        job_code: Enum.random(["rn_level_1", "rn_level_2", "supervisor"])
      }
    end
  end

  defp display_processing_results(result, employee_count, processing_time_ms) do
    IO.puts("\n--- Massive Scale Processing Results ---")

    # Overall summary
    summary = result.summary
    IO.puts("Processing Summary:")
    IO.puts("  Total employees processed: #{employee_count}")

    IO.puts(
      "  Total processing time: #{processing_time_ms}ms (#{Float.round(processing_time_ms / 1000, 2)}s)"
    )

    IO.puts(
      "  Average time per employee: #{Float.round(processing_time_ms / employee_count, 2)}ms"
    )

    IO.puts(
      "  Processing rate: #{Float.round(employee_count / (processing_time_ms / 1000 / 60), 1)} employees/minute"
    )

    # Overtime analysis
    overtime = result.overtime_analysis
    IO.puts("\nOvertime Analysis:")
    IO.puts("  Total overtime hours: #{overtime.total_overtime_hours}")
    IO.puts("  Total double-time hours: #{overtime.total_double_time_hours}")
    IO.puts("  Employees with overtime: #{overtime.employees_with_overtime}")
    IO.puts("  Shift segments marked as paid: #{overtime.segments_marked_as_paid}")

    # Sample employee results
    IO.puts("\nSample Employee Results (first 5):")

    result.employee_results
    |> Enum.take(5)
    |> Enum.each(fn emp ->
      IO.puts(
        "  #{emp.employee_id}: #{emp.total_regular_hours}h regular, #{emp.total_overtime_hours}h OT"
      )

      IO.puts(
        "    Segments processed: #{emp.segments_processed}, Paid segments: #{emp.segments_marked_as_paid}"
      )
    end)
  end

  defp demonstrate_presto_integration(result) do
    IO.puts("\n--- Presto Rules Engine Integration ---")

    presto_stats = result.presto_integration_stats
    IO.puts("Presto Integration Statistics:")
    IO.puts("  Total Presto engines used: #{presto_stats.total_presto_engines_used}")
    IO.puts("  Total rules loaded: #{presto_stats.total_rules_loaded}")
    IO.puts("  Rules executed successfully: #{presto_stats.rules_executed_successfully}")
    IO.puts("  Engine performance: #{presto_stats.presto_engine_performance}")

    IO.puts("\nRule Categories Processed by Presto:")
    IO.puts("  • Segmentation Rules (200): Time-based shift segmentation")
    IO.puts("  • Base Calculation Rules (300): Pay calculations and conversions")
    IO.puts("  • Validation Rules (500): Data integrity and business rule validation")
    IO.puts("  • Overtime Rules (500): Complex overtime eligibility and calculations")
    IO.puts("  • Special Pay Rules (500): Holiday, weekend, and shift differentials")

    IO.puts("\nPRESTO ARCHITECTURE HIGHLIGHTS:")
    IO.puts("  ✓ Presto serves as a generic, reusable rules engine")
    IO.puts("  ✓ Each worker has its own dedicated Presto engine instance")
    IO.puts("  ✓ 2,000 rules loaded and executed via Presto's RETE network")
    IO.puts("  ✓ Domain-specific logic in rules, not hardcoded in application")
    IO.puts("  ✓ Parallel processing with independent Presto engines")
    IO.puts("  ✓ Pattern matching and rule execution handled entirely by Presto")
  end

  defp analyze_performance_metrics(result) do
    performance = result.performance_report

    IO.puts("\n--- Performance Analysis ---")

    if Map.has_key?(performance, :performance_summary) do
      perf_summary = performance.performance_summary
      IO.puts("Performance Metrics:")
      IO.puts("  Peak memory usage: #{perf_summary.peak_memory_usage_mb}MB")

      IO.puts(
        "  Final processing rate: #{perf_summary.final_processing_rate_employees_per_minute} employees/minute"
      )

      IO.puts(
        "  Average processing time per employee: #{perf_summary.average_processing_time_per_employee_ms}ms"
      )

      if Map.has_key?(performance, :rule_performance) do
        rule_perf = performance.rule_performance
        IO.puts("\nRule Engine Performance:")
        IO.puts("  Total unique rules executed: #{rule_perf.total_unique_rules_executed}")
        IO.puts("  Rules executed per second: #{rule_perf.rules_executed_per_second}")

        if length(rule_perf.slowest_rules) > 0 do
          IO.puts("  Slowest rules:")

          rule_perf.slowest_rules
          |> Enum.take(3)
          |> Enum.each(fn {rule_name, stats} ->
            IO.puts("    #{rule_name}: #{stats.average_execution_time_ms}ms avg")
          end)
        end
      end

      # Bottleneck analysis
      if Map.has_key?(result, :bottleneck_analysis) do
        bottlenecks = result.bottleneck_analysis

        if length(bottlenecks) > 0 do
          IO.puts("\nBottleneck Analysis:")

          Enum.each(bottlenecks, fn bottleneck ->
            IO.puts(
              "  #{bottleneck.severity |> to_string() |> String.upcase()}: #{bottleneck.description}"
            )

            IO.puts("    Recommendation: #{bottleneck.recommended_action}")
          end)
        else
          IO.puts("\n✓ No performance bottlenecks detected")
        end
      end
    else
      IO.puts("Performance monitoring was disabled for this run")
    end

    IO.puts("\nSCALABILITY PROJECTION:")
    IO.puts("Based on demo performance, estimated time for full 10,000 employees:")

    # Calculate projection based on demo results
    if Map.has_key?(performance, :performance_summary) do
      demo_rate = performance.performance_summary.final_processing_rate_employees_per_minute

      if demo_rate > 0 do
        projected_time_minutes = 10_000 / demo_rate
        IO.puts("  Estimated processing time: #{Float.round(projected_time_minutes, 1)} minutes")

        IO.puts(
          "  With #{Map.get(result.presto_integration_stats, :total_presto_engines_used, 8)} parallel Presto engines"
        )
      else
        IO.puts("  Unable to calculate projection (insufficient performance data)")
      end
    end
  end
end

# Run the demo
IO.puts("Starting Massive Scale Payroll Demo...")
IO.puts("This demo shows how Presto can handle enterprise-scale payroll processing\n")

MassivePayrollDemo.run_demo()

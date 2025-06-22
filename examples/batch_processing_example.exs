#!/usr/bin/env elixir

# Batch Processing Example
# Demonstrates efficient batch operations with Presto v0.2 API

IO.puts("=== Batch Processing Example ===\n")

# Start the application
Application.ensure_all_started(:presto)

defmodule BatchProcessingDemo do
  def run(employee_count \\ 1000) do
    IO.puts("Processing #{employee_count} employees...")

    # Start engine
    {:ok, engine} = Presto.RuleEngine.start_link()

    # Define business rules using current API format
    overtime_rule = %{
      id: :overtime_eligibility,
      conditions: [
        {:timesheet, :employee_id, :hours_worked},
        {:hours_worked, :>, 40}
      ],
      action: fn facts ->
        overtime_hours = facts[:hours_worked] - 40
        # $25/hour base rate
        overtime_pay = overtime_hours * 1.5 * 25
        [{:overtime, facts[:employee_id], overtime_hours, overtime_pay}]
      end
    }

    bonus_rule = %{
      id: :bonus_eligibility,
      conditions: [
        {:employee, :id, :department, :performance_rating},
        {:performance_rating, :>=, 4.0}
      ],
      action: fn facts ->
        bonus =
          case facts[:department] do
            "Engineering" -> 5000
            "Sales" -> 3000
            _ -> 1000
          end

        [{:bonus, facts[:id], bonus}]
      end
    }

    dept_summary_rule = %{
      id: :department_summary,
      conditions: [
        {:employee, :id, :department, :performance_rating}
      ],
      action: fn facts ->
        [{:dept_employee, facts[:department], facts[:id], facts[:performance_rating]}]
      end
    }

    rules = [overtime_rule, bonus_rule, dept_summary_rule]

    # Add rules individually
    IO.puts("  → Adding #{length(rules)} rules...")

    Enum.each(rules, fn rule ->
      :ok = Presto.RuleEngine.add_rule(engine, rule)
    end)

    # Generate test data
    IO.puts("  → Generating test data...")
    {employees, timesheets} = generate_test_data(employee_count)

    # Assert facts individually
    IO.puts("  → Asserting #{length(employees) + length(timesheets)} facts...")
    start_time = System.monotonic_time(:microsecond)

    Enum.each(employees ++ timesheets, fn fact ->
      :ok = Presto.RuleEngine.assert_fact(engine, fact)
    end)

    assertion_time = System.monotonic_time(:microsecond) - start_time

    # Fire rules and measure performance
    IO.puts("  → Firing rules...")
    fire_start = System.monotonic_time(:microsecond)

    results = Presto.RuleEngine.fire_rules(engine)

    fire_time = System.monotonic_time(:microsecond) - fire_start
    total_time = assertion_time + fire_time

    # Analyze results
    {overtime_count, bonus_count, dept_summary} = analyze_results(results)

    # Show performance metrics
    show_performance_metrics(%{
      employee_count: employee_count,
      total_facts: length(employees) + length(timesheets),
      total_results: length(results),
      assertion_time: assertion_time,
      fire_time: fire_time,
      total_time: total_time,
      overtime_count: overtime_count,
      bonus_count: bonus_count,
      dept_summary: dept_summary
    })

    # Get engine statistics
    stats = Presto.RuleEngine.get_engine_statistics(engine)
    rule_stats = Presto.RuleEngine.get_rule_statistics(engine)

    show_engine_statistics(stats, rule_stats)

    # Clean up
    GenServer.stop(engine)

    IO.puts("\n=== Batch Processing Complete ===")
  end

  defp generate_test_data(count) do
    departments = ["Engineering", "Sales", "Marketing", "Operations"]

    employees =
      for i <- 1..count do
        dept = Enum.at(departments, rem(i, length(departments)))
        # 2.0 to 5.0
        rating = 2.0 + :rand.uniform() * 3.0
        {:employee, i, dept, Float.round(rating, 1)}
      end

    timesheets =
      for i <- 1..count do
        # Some employees work overtime
        # 35-50 hours
        base_hours = 35 + :rand.uniform(15)
        {:timesheet, i, base_hours}
      end

    {employees, timesheets}
  end

  defp analyze_results(results) do
    overtime_results = Enum.filter(results, &match?({:overtime, _, _, _}, &1))
    bonus_results = Enum.filter(results, &match?({:bonus, _, _}, &1))
    dept_results = Enum.filter(results, &match?({:dept_employee, _, _, _}, &1))

    # Department summary
    dept_summary =
      dept_results
      |> Enum.group_by(fn {:dept_employee, dept, _id, _rating} -> dept end)
      |> Enum.map(fn {dept, employees} ->
        count = length(employees)

        ratings_sum =
          employees
          |> Enum.map(fn {:dept_employee, _dept, _id, rating} -> rating end)
          |> Enum.sum()

        avg_rating =
          case ratings_sum do
            0 -> 0.0
            sum -> Float.round(sum / count, 2)
          end

        {dept, count, avg_rating}
      end)

    {length(overtime_results), length(bonus_results), dept_summary}
  end

  defp show_performance_metrics(metrics) do
    IO.puts("\n=== Performance Metrics ===")
    IO.puts("  Employees processed: #{metrics.employee_count}")
    IO.puts("  Total facts: #{metrics.total_facts}")
    IO.puts("  Results generated: #{metrics.total_results}")
    IO.puts("  Assertion time: #{Float.round(metrics.assertion_time / 1000, 2)}ms")
    IO.puts("  Rule execution time: #{Float.round(metrics.fire_time / 1000, 2)}ms")
    IO.puts("  Total time: #{Float.round(metrics.total_time / 1000, 2)}ms")

    IO.puts(
      "  Throughput: #{Float.round(metrics.employee_count / (metrics.total_time / 1_000_000), 0)} employees/second"
    )

    IO.puts("\n=== Business Results ===")
    IO.puts("  Overtime eligible: #{metrics.overtime_count}")
    IO.puts("  Bonus eligible: #{metrics.bonus_count}")
    IO.puts("  Department breakdown:")

    Enum.each(metrics.dept_summary, fn {dept, count, avg_rating} ->
      IO.puts("    #{dept}: #{count} employees (avg rating: #{avg_rating})")
    end)
  end

  defp show_engine_statistics(stats, rule_stats) do
    IO.puts("\n=== Engine Statistics ===")
    IO.puts("  Total rules: #{stats.total_rules}")
    IO.puts("  Total facts: #{stats.total_facts}")
    IO.puts("  Rule firings: #{stats.total_rule_firings}")

    if map_size(rule_stats) > 0 do
      IO.puts("  Rule performance:")

      Enum.each(rule_stats, fn {rule_id, stats} ->
        IO.puts(
          "    #{rule_id}: #{stats.executions} executions, #{Float.round(stats.average_time / 1000, 2)}ms avg"
        )
      end)
    end
  end
end

# Run the demo
employee_count =
  case System.get_env("EMPLOYEE_COUNT") do
    nil -> 1000
    count_str -> String.to_integer(count_str)
  end

BatchProcessingDemo.run(employee_count)

IO.puts("\nTo run with different scale:")
IO.puts("  EMPLOYEE_COUNT=5000 mix run examples/batch_processing_example.exs")

#!/usr/bin/env elixir

# RETE-Native Aggregations Example
# Demonstrates Presto's aggregation capabilities using basic rules

# Start the application
Application.ensure_all_started(:presto)

defmodule Examples.AggregationExample do
  @moduledoc """
  Demonstrates Presto's RETE-native aggregation capabilities.

  Shows built-in aggregation functions, custom aggregations,
  and windowed aggregations for streaming data.
  """

  def run_demo do
    IO.puts("\n=== RETE-Native Aggregations Example ===")

    # Start the engine
    {:ok, engine} = Presto.RuleEngine.start_link()

    # Demonstrate different aggregation types
    demonstrate_basic_aggregations(engine)
    demonstrate_custom_aggregations(engine)
    demonstrate_multi_field_grouping(engine)
    demonstrate_aggregation_performance(engine)

    GenServer.stop(engine)
    IO.puts("\n=== Aggregation Example Complete ===")
  end

  defp demonstrate_basic_aggregations(engine) do
    IO.puts("\n--- Basic Aggregations (Simulated with Rules) ---")

    # Create rules that simulate aggregation behavior
    # Count employees by department
    count_rule =
      Presto.Rule.new(
        :dept_employee_count,
        [
          Presto.Rule.pattern(:employee, [:id, :name, :department])
        ],
        fn facts ->
          [{:employee_in_dept, facts[:department], facts[:name]}]
        end
      )

    # Sum salaries by department (simplified)
    salary_rule =
      Presto.Rule.new(
        :dept_salary_tracking,
        [
          Presto.Rule.pattern(:employee, [:id, :name, :department]),
          Presto.Rule.pattern(:salary, [:id, :amount])
        ],
        fn facts ->
          [{:dept_salary, facts[:department], facts[:amount]}]
        end
      )

    # Salary statistics rule
    high_salary_rule =
      Presto.Rule.new(
        :high_salary,
        [
          Presto.Rule.pattern(:employee, [:id, :name, :department]),
          Presto.Rule.pattern(:salary, [:id, :amount]),
          Presto.Rule.test(:amount, :>, 80000)
        ],
        fn facts ->
          [{:high_earner, facts[:name], facts[:department], facts[:amount]}]
        end
      )

    # Add rules
    :ok = Presto.RuleEngine.add_rule(engine, count_rule)
    :ok = Presto.RuleEngine.add_rule(engine, salary_rule)
    :ok = Presto.RuleEngine.add_rule(engine, high_salary_rule)

    # Sample data
    facts = [
      # Employees
      {:employee, 1, "Alice", "Engineering"},
      {:employee, 2, "Bob", "Engineering"},
      {:employee, 3, "Carol", "Sales"},
      {:employee, 4, "David", "Sales"},
      {:employee, 5, "Eve", "Marketing"},
      {:employee, 6, "Frank", "Engineering"},

      # Salaries
      {:salary, 1, 95000},
      {:salary, 2, 75000},
      {:salary, 3, 85000},
      {:salary, 4, 70000},
      {:salary, 5, 65000},
      {:salary, 6, 105_000}
    ]

    # Assert facts
    Enum.each(facts, fn fact ->
      :ok = Presto.RuleEngine.assert_fact(engine, fact)
    end)

    # Fire rules
    results = Presto.RuleEngine.fire_rules(engine)

    IO.puts("Basic aggregation results:")
    analyze_aggregation_results(results)

    # Simulate department counts by processing results
    show_department_summaries(results)

    # Clear facts for next demo
    :ok = Presto.RuleEngine.clear_facts(engine)
  end

  defp demonstrate_custom_aggregations(engine) do
    IO.puts("\n--- Custom Aggregation Logic ---")

    # Custom rule: salary range analysis (max - min) by department
    salary_analysis_rule =
      Presto.Rule.new(
        :salary_analysis,
        [
          Presto.Rule.pattern(:employee, [:id, :name, :department]),
          Presto.Rule.pattern(:salary, [:id, :amount])
        ],
        fn facts ->
          [{:salary_data, facts[:department], facts[:amount], facts[:name]}]
        end
      )

    # Rule to identify departments with high salary variance
    high_variance_rule =
      Presto.Rule.new(
        :high_variance_dept,
        [
          Presto.Rule.pattern(:employee, [:id, :name, :department]),
          Presto.Rule.pattern(:salary, [:id, :amount]),
          Presto.Rule.test(:amount, :>, 90000)
        ],
        fn facts ->
          [{:potential_high_variance, facts[:department]}]
        end
      )

    # Add rules
    :ok = Presto.RuleEngine.add_rule(engine, salary_analysis_rule)
    :ok = Presto.RuleEngine.add_rule(engine, high_variance_rule)

    # Sample data (same as before)
    facts = [
      {:employee, 1, "Alice", "Engineering"},
      {:employee, 2, "Bob", "Engineering"},
      {:employee, 3, "Carol", "Sales"},
      {:employee, 4, "David", "Sales"},
      {:employee, 5, "Eve", "Marketing"},
      {:employee, 6, "Frank", "Engineering"},
      {:salary, 1, 95000},
      {:salary, 2, 75000},
      {:salary, 3, 85000},
      {:salary, 4, 70000},
      {:salary, 5, 65000},
      {:salary, 6, 105_000}
    ]

    # Assert facts
    Enum.each(facts, fn fact ->
      :ok = Presto.RuleEngine.assert_fact(engine, fact)
    end)

    # Fire rules
    results = Presto.RuleEngine.fire_rules(engine)

    IO.puts("Custom aggregation results:")
    analyze_custom_results(results)

    # Clear facts for next demo
    :ok = Presto.RuleEngine.clear_facts(engine)
  end

  defp demonstrate_multi_field_grouping(engine) do
    IO.puts("\n--- Multi-Field Grouping ---")

    # Group by both department and experience level
    multi_group_rule =
      Presto.Rule.new(
        :dept_experience_tracking,
        [
          Presto.Rule.pattern(:employee, [:id, :name, :department, :experience_level]),
          Presto.Rule.pattern(:salary, [:id, :amount])
        ],
        fn facts ->
          [
            {:dept_exp_salary, facts[:department], facts[:experience_level], facts[:amount],
             facts[:name]}
          ]
        end
      )

    # Senior level rule
    senior_rule =
      Presto.Rule.new(
        :senior_employees,
        [
          Presto.Rule.pattern(:employee, [:id, :name, :department, :experience_level]),
          Presto.Rule.test(:experience_level, :==, "Senior")
        ],
        fn facts ->
          [{:senior_employee, facts[:name], facts[:department]}]
        end
      )

    # Add rules
    :ok = Presto.RuleEngine.add_rule(engine, multi_group_rule)
    :ok = Presto.RuleEngine.add_rule(engine, senior_rule)

    # Sample data with experience levels
    facts = [
      # Employees with experience levels
      {:employee, 1, "Alice", "Engineering", "Senior"},
      {:employee, 2, "Bob", "Engineering", "Junior"},
      {:employee, 3, "Carol", "Sales", "Senior"},
      {:employee, 4, "David", "Sales", "Junior"},
      {:employee, 5, "Eve", "Marketing", "Mid"},
      {:employee, 6, "Frank", "Engineering", "Senior"},

      # Salaries
      {:salary, 1, 110_000},
      {:salary, 2, 65000},
      {:salary, 3, 95000},
      {:salary, 4, 55000},
      {:salary, 5, 70000},
      {:salary, 6, 115_000}
    ]

    # Assert facts
    Enum.each(facts, fn fact ->
      :ok = Presto.RuleEngine.assert_fact(engine, fact)
    end)

    # Fire rules
    results = Presto.RuleEngine.fire_rules(engine)

    IO.puts("Multi-field grouping results:")
    analyze_multi_field_results(results)

    # Clear facts for next demo
    :ok = Presto.RuleEngine.clear_facts(engine)
  end

  defp demonstrate_aggregation_performance(engine) do
    IO.puts("\n--- Performance Test ---")

    # Create a simple performance rule
    perf_rule =
      Presto.Rule.new(
        :perf_tracking,
        [
          Presto.Rule.pattern(:employee, [:id, :name, :department]),
          Presto.Rule.pattern(:salary, [:id, :amount])
        ],
        fn facts ->
          [{:employee_salary, facts[:department], facts[:name], facts[:amount]}]
        end
      )

    high_earner_rule =
      Presto.Rule.new(
        :high_earner_perf,
        [
          Presto.Rule.pattern(:employee, [:id, :name, :department]),
          Presto.Rule.pattern(:salary, [:id, :amount]),
          Presto.Rule.test(:amount, :>, 75000)
        ],
        fn facts ->
          [{:high_earner_perf, facts[:department], facts[:name]}]
        end
      )

    # Add rules
    :ok = Presto.RuleEngine.add_rule(engine, perf_rule)
    :ok = Presto.RuleEngine.add_rule(engine, high_earner_rule)

    # Generate larger test dataset
    large_dataset =
      Enum.flat_map(1..1000, fn i ->
        dept = Enum.random(["Engineering", "Sales", "Marketing", "HR"])
        salary = 50000 + :rand.uniform(100_000)

        [
          {:employee, i, "Employee_#{i}", dept},
          {:salary, i, salary}
        ]
      end)

    # Measure performance
    {time_us, _} =
      :timer.tc(fn ->
        Enum.each(large_dataset, fn fact ->
          :ok = Presto.RuleEngine.assert_fact(engine, fact)
        end)
      end)

    {fire_time_us, results} =
      :timer.tc(fn ->
        Presto.RuleEngine.fire_rules(engine)
      end)

    IO.puts("Performance test with 1000 employees:")
    IO.puts("  Fact assertion time: #{time_us}μs (#{Float.round(time_us / 1000, 2)}ms)")
    IO.puts("  Rule execution time: #{fire_time_us}μs (#{Float.round(fire_time_us / 1000, 2)}ms)")

    IO.puts(
      "  Total time: #{time_us + fire_time_us}μs (#{Float.round((time_us + fire_time_us) / 1000, 2)}ms)"
    )

    IO.puts("  Results generated: #{length(results)}")
    IO.puts("  Facts processed: #{length(large_dataset)}")

    # Show some statistics
    high_earners = Enum.filter(results, &match?({:high_earner_perf, _, _}, &1))
    IO.puts("  High earners found: #{length(high_earners)}")
  end

  defp analyze_aggregation_results(results) do
    # Group results by type
    results
    |> Enum.group_by(fn result -> elem(result, 0) end)
    |> Enum.each(fn {result_type, type_results} ->
      IO.puts("\n#{result_type}:")

      Enum.each(type_results, fn result ->
        IO.puts("  #{inspect(result)}")
      end)
    end)
  end

  defp show_department_summaries(results) do
    IO.puts("\nDepartment Summaries:")

    # Count employees by department
    employee_counts =
      results
      |> Enum.filter(&match?({:employee_in_dept, _, _}, &1))
      |> Enum.group_by(fn {:employee_in_dept, dept, _} -> dept end)
      |> Enum.map(fn {dept, employees} -> {dept, length(employees)} end)

    Enum.each(employee_counts, fn {dept, count} ->
      IO.puts("  #{dept}: #{count} employees")
    end)

    # Show high earners
    high_earners =
      results
      |> Enum.filter(&match?({:high_earner, _, _, _}, &1))
      |> length()

    IO.puts("  High earners (>$80k): #{high_earners}")
  end

  defp analyze_custom_results(results) do
    salary_data = Enum.filter(results, &match?({:salary_data, _, _, _}, &1))
    high_variance = Enum.filter(results, &match?({:potential_high_variance, _}, &1))

    IO.puts("Salary analysis complete:")
    IO.puts("  Salary records processed: #{length(salary_data)}")
    IO.puts("  Departments with potential high variance: #{length(high_variance)}")

    # Show some custom analysis
    dept_salaries =
      salary_data
      |> Enum.group_by(fn {:salary_data, dept, _, _} -> dept end)
      |> Enum.map(fn {dept, salaries} ->
        amounts = Enum.map(salaries, fn {:salary_data, _, amount, _} -> amount end)

        avg =
          if length(amounts) > 0, do: Float.round(Enum.sum(amounts) / length(amounts), 0), else: 0

        {dept, avg}
      end)

    IO.puts("\nAverage salaries by department:")

    Enum.each(dept_salaries, fn {dept, avg} ->
      IO.puts("  #{dept}: $#{avg}")
    end)
  end

  defp analyze_multi_field_results(results) do
    dept_exp_data = Enum.filter(results, &match?({:dept_exp_salary, _, _, _, _}, &1))
    senior_employees = Enum.filter(results, &match?({:senior_employee, _, _}, &1))

    IO.puts("Multi-field analysis:")
    IO.puts("  Department-Experience records: #{length(dept_exp_data)}")
    IO.puts("  Senior employees: #{length(senior_employees)}")

    # Group by department and experience level
    grouped =
      dept_exp_data
      |> Enum.group_by(fn {:dept_exp_salary, dept, exp, _, _} -> {dept, exp} end)
      |> Enum.map(fn {{dept, exp}, records} ->
        salaries = Enum.map(records, fn {:dept_exp_salary, _, _, salary, _} -> salary end)
        count = length(records)
        avg_salary = if count > 0, do: Float.round(Enum.sum(salaries) / count, 0), else: 0
        {{dept, exp}, count, avg_salary}
      end)

    IO.puts("\nDepartment-Experience breakdown:")

    Enum.each(grouped, fn {{dept, exp}, count, avg} ->
      IO.puts("  #{dept} #{exp}: #{count} employees, avg salary $#{avg}")
    end)
  end
end

# Run the demo
Examples.AggregationExample.run_demo()

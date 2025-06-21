#!/usr/bin/env elixir

# RETE-Native Aggregations Example
# Demonstrates Presto's powerful aggregation capabilities using the enhanced API

# Start dependencies
Mix.install([])

# Add lib to path for running as script
Code.append_path("lib")

defmodule Examples.AggregationExample do
  @moduledoc """
  Demonstrates Presto's RETE-native aggregation capabilities.

  Shows built-in aggregation functions, custom aggregations,
  and windowed aggregations for streaming data.
  """

  def run_demo do
    IO.puts("\n=== RETE-Native Aggregations Example ===")

    # Start the engine
    {:ok, engine} = Presto.start_engine()

    # Demonstrate different aggregation types
    demonstrate_basic_aggregations(engine)
    demonstrate_custom_aggregations(engine)
    demonstrate_windowed_aggregations(engine)
    demonstrate_multi_field_grouping(engine)

    # Performance comparison
    demonstrate_aggregation_performance(engine)

    Presto.stop_engine(engine)
    IO.puts("\n=== Aggregation Example Complete ===")
  end

  defp demonstrate_basic_aggregations(engine) do
    IO.puts("\n--- Basic RETE-Native Aggregations ---")

    # Create aggregation rules using the new API
    rules = [
      # Count employees by department
      Presto.Rule.aggregation(
        :dept_employee_count,
        [{:employee, :id, :name, :department}],
        [:department],
        :count,
        nil
      ),

      # Sum salaries by department
      Presto.Rule.aggregation(
        :dept_salary_total,
        [
          {:employee, :id, :name, :department},
          {:salary, :id, :amount}
        ],
        [:department],
        :sum,
        :amount
      ),

      # Average salary by department
      Presto.Rule.aggregation(
        :dept_avg_salary,
        [
          {:employee, :id, :name, :department},
          {:salary, :id, :amount}
        ],
        [:department],
        :avg,
        :amount
      ),

      # Min and max salaries by department
      Presto.Rule.aggregation(
        :dept_min_salary,
        [
          {:employee, :id, :name, :department},
          {:salary, :id, :amount}
        ],
        [:department],
        :min,
        :amount
      ),
      Presto.Rule.aggregation(
        :dept_max_salary,
        [
          {:employee, :id, :name, :department},
          {:salary, :id, :amount}
        ],
        [:department],
        :max,
        :amount
      ),

      # Collect all employees by department
      Presto.Rule.aggregation(
        :dept_employee_list,
        [{:employee, :id, :name, :department}],
        [:department],
        :collect,
        :name
      )
    ]

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
      # Alice
      {:salary, 1, 95000},
      # Bob
      {:salary, 2, 75000},
      # Carol
      {:salary, 3, 85000},
      # David
      {:salary, 4, 70000},
      # Eve
      {:salary, 5, 65000},
      # Frank
      {:salary, 6, 105_000}
    ]

    # Execute batch operation
    results = Presto.execute_batch(engine, rules: rules, facts: facts)

    IO.puts("Basic aggregation results:")
    analyze_aggregation_results(results)
  end

  defp demonstrate_custom_aggregations(engine) do
    IO.puts("\n--- Custom Aggregation Functions ---")

    # Custom aggregation: salary range (max - min) by department
    range_rule =
      Presto.Rule.aggregation(
        :dept_salary_range,
        [
          {:employee, :id, :name, :department},
          {:salary, :id, :amount}
        ],
        [:department],
        fn values ->
          if length(values) > 0 do
            Enum.max(values) - Enum.min(values)
          else
            0
          end
        end,
        :amount
      )

    # Custom aggregation: standard deviation of salaries
    std_dev_rule =
      Presto.Rule.aggregation(
        :dept_salary_std_dev,
        [
          {:employee, :id, :name, :department},
          {:salary, :id, :amount}
        ],
        [:department],
        fn values ->
          if length(values) > 1 do
            mean = Enum.sum(values) / length(values)
            variance = Enum.map(values, &:math.pow(&1 - mean, 2)) |> (Enum.sum() / length(values))
            :math.sqrt(variance) |> Float.round(2)
          else
            0.0
          end
        end,
        :amount
      )

    # Add custom rules to existing engine
    :ok = Presto.add_rules(engine, [range_rule, std_dev_rule])

    # Execute rules
    results = Presto.fire_rules(engine)

    IO.puts("Custom aggregation results:")

    custom_results =
      Enum.filter(results, fn result ->
        elem(result, 0) in [:dept_salary_range, :dept_salary_std_dev]
      end)

    Enum.each(custom_results, fn result ->
      IO.puts("  #{inspect(result)}")
    end)
  end

  defp demonstrate_windowed_aggregations(engine) do
    IO.puts("\n--- Windowed Aggregations (Streaming Data) ---")

    # Clear existing data for fresh demo
    Presto.clear_facts(engine)

    # Windowed moving average for sensor readings
    windowed_avg_rule =
      Presto.Rule.aggregation(
        :sensor_moving_avg,
        [{:sensor_reading, :sensor_id, :timestamp, :value}],
        [:sensor_id],
        :avg,
        :value,
        # Last 5 readings
        window_size: 5
      )

    # Windowed count for activity monitoring
    activity_count_rule =
      Presto.Rule.aggregation(
        :activity_count,
        [{:user_activity, :user_id, :timestamp, :action}],
        [:user_id],
        :count,
        nil,
        # Last 10 activities
        window_size: 10
      )

    :ok = Presto.add_rules(engine, [windowed_avg_rule, activity_count_rule])

    # Simulate streaming sensor data
    sensor_data =
      Enum.map(1..20, fn i ->
        {:sensor_reading, "temp_01", i, 20 + :rand.uniform(10)}
      end)

    # Simulate user activities
    activity_data =
      Enum.map(1..15, fn i ->
        actions = ["login", "view", "click", "logout"]
        {:user_activity, "user_01", i, Enum.random(actions)}
      end)

    # Assert facts in batches to simulate streaming
    :ok = Presto.assert_facts(engine, sensor_data ++ activity_data)
    results = Presto.fire_rules(engine)

    IO.puts("Windowed aggregation results:")

    windowed_results =
      Enum.filter(results, fn result ->
        elem(result, 0) in [:sensor_moving_avg, :activity_count]
      end)

    Enum.each(windowed_results, fn result ->
      IO.puts("  #{inspect(result)}")
    end)
  end

  defp demonstrate_multi_field_grouping(engine) do
    IO.puts("\n--- Multi-Field Grouping ---")

    # Clear for fresh demo
    Presto.clear_facts(engine)

    # Group by both department and experience level
    multi_group_rule =
      Presto.Rule.aggregation(
        :dept_experience_avg_salary,
        [
          {:employee, :id, :name, :department, :experience_level},
          {:salary, :id, :amount}
        ],
        [:department, :experience_level],
        :avg,
        :amount
      )

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
      # Alice - Senior Engineering
      {:salary, 1, 110_000},
      # Bob - Junior Engineering
      {:salary, 2, 65000},
      # Carol - Senior Sales
      {:salary, 3, 95000},
      # David - Junior Sales
      {:salary, 4, 55000},
      # Eve - Mid Marketing
      {:salary, 5, 70000},
      # Frank - Senior Engineering
      {:salary, 6, 115_000}
    ]

    results = Presto.execute_batch(engine, rules: [multi_group_rule], facts: facts)

    IO.puts("Multi-field grouping results:")

    Enum.each(results, fn result ->
      IO.puts("  #{inspect(result)}")
    end)
  end

  defp demonstrate_aggregation_performance(engine) do
    IO.puts("\n--- Aggregation Performance ---")

    # Measure aggregation performance with larger dataset
    large_dataset =
      Enum.flat_map(1..1000, fn i ->
        dept = Enum.random(["Engineering", "Sales", "Marketing", "HR"])

        [
          {:employee, i, "Employee_#{i}", dept},
          {:salary, i, 50000 + :rand.uniform(100_000)}
        ]
      end)

    # Performance rules
    perf_rules = [
      Presto.Rule.aggregation(
        :perf_count,
        [{:employee, :id, :name, :department}],
        [:department],
        :count,
        nil
      ),
      Presto.Rule.aggregation(
        :perf_sum,
        [
          {:employee, :id, :name, :department},
          {:salary, :id, :amount}
        ],
        [:department],
        :sum,
        :amount
      )
    ]

    # Measure time
    {time_us, results} =
      :timer.tc(fn ->
        Presto.execute_batch(engine, rules: perf_rules, facts: large_dataset)
      end)

    IO.puts("Performance test with 1000 employees:")
    IO.puts("  Execution time: #{time_us}μs (#{Float.round(time_us / 1000, 2)}ms)")
    IO.puts("  Results generated: #{length(results)}")
    IO.puts("  Facts processed: #{length(large_dataset)}")
  end

  defp analyze_aggregation_results(results) do
    # Group results by aggregation type
    results
    |> Enum.group_by(fn result -> elem(result, 0) end)
    |> Enum.each(fn {agg_type, agg_results} ->
      IO.puts("\n#{agg_type}:")

      Enum.each(agg_results, fn result ->
        IO.puts("  #{inspect(result)}")
      end)
    end)
  end
end

# Run the demo
Examples.AggregationExample.run_demo()

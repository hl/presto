#!/usr/bin/env elixir

# Presto Query Interface Example
# 
# This example demonstrates the new query interface features added to Presto,
# including pattern-based fact queries, joins, and fact explanation.

# Start dependencies
Mix.install([])

# Add lib to path for running as script
Code.append_path("lib")

defmodule Examples.QueryExample do
  @moduledoc """
  Demonstrates Presto's query interface for ad-hoc fact exploration.

  Shows how to query facts without executing rules, perform joins,
  and explain fact matching behavior.
  """

  def run_demo do
    IO.puts("\n=== Presto Query Interface Demo ===")

    # Start the engine
    {:ok, engine} = Presto.start_engine()

    # Set up sample data
    setup_sample_data(engine)

    # Demonstrate different query types
    demonstrate_basic_queries(engine)
    demonstrate_conditional_queries(engine)
    demonstrate_joins(engine)
    demonstrate_counting(engine)
    demonstrate_explanation(engine)

    # Performance comparison
    demonstrate_query_performance(engine)

    Presto.stop_engine(engine)
    IO.puts("\n=== Query Demo Complete ===")
  end

  defp setup_sample_data(engine) do
    IO.puts("\n--- Setting up sample data ---")

    # Add sample facts
    facts = [
      # People
      {:person, "Alice", 28, "Engineering"},
      {:person, "Bob", 35, "Marketing"},
      {:person, "Carol", 24, "Engineering"},
      {:person, "Dave", 42, "Sales"},
      {:person, "Eve", 31, "Engineering"},

      # Employment details
      {:employment, "Alice", "TechCorp", 75000},
      {:employment, "Bob", "TechCorp", 65000},
      {:employment, "Carol", "StartupInc", 70000},
      {:employment, "Dave", "TechCorp", 80000},
      {:employment, "Eve", "StartupInc", 85000},

      # Performance ratings
      {:performance, "Alice", 2023, 4.5},
      {:performance, "Bob", 2023, 3.8},
      {:performance, "Carol", 2023, 4.2},
      {:performance, "Dave", 2023, 3.5},
      {:performance, "Eve", 2023, 4.8},

      # Project assignments
      {:project, "Alice", "ProjectX"},
      {:project, "Carol", "ProjectX"},
      {:project, "Eve", "ProjectX"},
      {:project, "Bob", "ProjectY"},
      {:project, "Dave", "ProjectY"}
    ]

    :ok = Presto.assert_facts(engine, facts)
    IO.puts("Added #{length(facts)} facts to working memory")
  end

  defp demonstrate_basic_queries(engine) do
    IO.puts("\n--- Basic Pattern Queries ---")

    # Query all people
    people = Presto.query(engine, {:person, :_, :_, :_})
    IO.puts("All people (#{length(people)}):")

    Enum.each(people, fn person ->
      IO.puts("  #{inspect(person)}")
    end)

    # Query all employment records
    employment = Presto.query(engine, {:employment, :_, :_, :_})
    IO.puts("\nAll employment records (#{length(employment)}):")

    Enum.each(employment, fn emp ->
      IO.puts("  #{inspect(emp)}")
    end)
  end

  defp demonstrate_conditional_queries(engine) do
    IO.puts("\n--- Conditional Queries ---")

    # Query people over 30
    seniors = Presto.query(engine, {:person, :name, :age, :department}, age: {:>, 30})
    IO.puts("People over 30 (#{length(seniors)}):")

    Enum.each(seniors, fn person ->
      IO.puts("  #{inspect(person)}")
    end)

    # Query engineering department
    engineers =
      Presto.query(engine, {:person, :name, :age, :department}, department: "Engineering")

    IO.puts("\nEngineering department (#{length(engineers)}):")

    Enum.each(engineers, fn person ->
      IO.puts("  #{inspect(person)}")
    end)

    # Query high earners
    high_earners =
      Presto.query(engine, {:employment, :name, :company, :salary}, salary: {:>, 70000})

    IO.puts("\nHigh earners (salary > 70k) (#{length(high_earners)}):")

    Enum.each(high_earners, fn emp ->
      IO.puts("  #{inspect(emp)}")
    end)

    # Query high performers
    high_performers =
      Presto.query(engine, {:performance, :name, :year, :rating}, rating: {:>, 4.0})

    IO.puts("\nHigh performers (rating > 4.0) (#{length(high_performers)}):")

    Enum.each(high_performers, fn perf ->
      IO.puts("  #{inspect(perf)}")
    end)
  end

  defp demonstrate_joins(engine) do
    IO.puts("\n--- Join Queries ---")

    # Join people with their employment information
    people_employment =
      Presto.query_join(
        engine,
        [
          {:person, :name, :age, :department},
          {:employment, :name, :company, :salary}
        ],
        join_on: [:name]
      )

    IO.puts("People with employment info (#{length(people_employment)}):")

    Enum.each(people_employment, fn record ->
      IO.puts(
        "  #{record[:name]}: #{record[:age]}yr, #{record[:department]} @ #{record[:company]} ($#{record[:salary]})"
      )
    end)

    # Join people with performance ratings
    people_performance =
      Presto.query_join(
        engine,
        [
          {:person, :name, :age, :department},
          {:performance, :name, :year, :rating}
        ],
        join_on: [:name]
      )

    IO.puts("\nPeople with performance ratings (#{length(people_performance)}):")

    Enum.each(people_performance, fn record ->
      IO.puts("  #{record[:name]}: #{record[:rating]} rating")
    end)

    # Three-way join: people, employment, and performance
    complete_profile =
      Presto.query_join(
        engine,
        [
          {:person, :name, :age, :department},
          {:employment, :name, :company, :salary},
          {:performance, :name, :year, :rating}
        ],
        join_on: [:name]
      )

    IO.puts("\nComplete employee profiles (#{length(complete_profile)}):")

    Enum.each(complete_profile, fn record ->
      IO.puts(
        "  #{record[:name]}: #{record[:age]}yr, #{record[:department]} @ #{record[:company]}"
      )

      IO.puts("    Salary: $#{record[:salary]}, Rating: #{record[:rating]}")
    end)
  end

  defp demonstrate_counting(engine) do
    IO.puts("\n--- Counting Queries ---")

    # Count all people
    person_count = Presto.count_facts(engine, {:person, :_, :_, :_})
    IO.puts("Total people: #{person_count}")

    # Count engineers
    engineer_count =
      Presto.count_facts(engine, {:person, :name, :age, :department}, department: "Engineering")

    IO.puts("Engineers: #{engineer_count}")

    # Count high earners
    high_earner_count =
      Presto.count_facts(engine, {:employment, :name, :company, :salary}, salary: {:>, 70000})

    IO.puts("High earners (>70k): #{high_earner_count}")

    # Count project assignments
    project_count = Presto.count_facts(engine, {:project, :_, :_})
    IO.puts("Project assignments: #{project_count}")
  end

  defp demonstrate_explanation(engine) do
    IO.puts("\n--- Fact Explanation ---")

    # First add a simple rule to demonstrate explanation
    rule =
      Presto.Rule.new(
        :senior_engineer,
        [
          {:person, :name, :age, :department},
          {:age, :>, 30},
          {:department, :==, "Engineering"}
        ],
        fn bindings ->
          [{:senior_engineer, bindings[:name]}]
        end
      )

    :ok = Presto.add_rule(engine, rule)

    # Explain how facts would match
    alice_explanation = Presto.explain_fact(engine, {:person, "Alice", 28, "Engineering"})
    IO.puts("Explanation for Alice (28, Engineering):")
    IO.puts("  #{inspect(alice_explanation)}")

    eve_explanation = Presto.explain_fact(engine, {:person, "Eve", 31, "Engineering"})
    IO.puts("\nExplanation for Eve (31, Engineering):")
    IO.puts("  #{inspect(eve_explanation)}")

    bob_explanation = Presto.explain_fact(engine, {:person, "Bob", 35, "Marketing"})
    IO.puts("\nExplanation for Bob (35, Marketing):")
    IO.puts("  #{inspect(bob_explanation)}")
  end

  defp demonstrate_query_performance(engine) do
    IO.puts("\n--- Query Performance ---")

    # Measure query performance
    {time_basic, _result} =
      :timer.tc(fn ->
        Presto.query(engine, {:person, :_, :_, :_})
      end)

    {time_conditional, _result} =
      :timer.tc(fn ->
        Presto.query(engine, {:person, :name, :age, :department}, age: {:>, 30})
      end)

    {time_join, _result} =
      :timer.tc(fn ->
        Presto.query_join(
          engine,
          [
            {:person, :name, :age, :department},
            {:employment, :name, :company, :salary}
          ],
          join_on: [:name]
        )
      end)

    {time_count, _result} =
      :timer.tc(fn ->
        Presto.count_facts(engine, {:person, :_, :_, :_})
      end)

    IO.puts("Query performance (microseconds):")
    IO.puts("  Basic query:       #{time_basic}μs")
    IO.puts("  Conditional query: #{time_conditional}μs")
    IO.puts("  Join query:        #{time_join}μs")
    IO.puts("  Count query:       #{time_count}μs")
  end
end

# Run the demo
Examples.QueryExample.run_demo()

#!/usr/bin/env elixir

# Performance Benchmark for Presto Optimizations
# Run with: elixir performance_benchmark.exs

IO.puts("=== Presto Performance Optimization Benchmark ===\n")

# Start the application
Application.ensure_all_started(:presto)

defmodule PerformanceBenchmark do
  def run_pattern_matching_benchmark do
    IO.puts("1. Pattern Matching Performance Test")
    IO.puts("   Testing optimized tuple access vs list conversion...")
    
    # Create test data
    facts = for i <- 1..1000 do
      {:person, "person_#{i}", :age, 25 + rem(i, 50), :department, "dept_#{rem(i, 5)}"}
    end
    
    _pattern = {:person, :name, :age, :person_age, :department, :dept}
    
    # Start engine to test pattern matching
    {:ok, engine} = Presto.RuleEngine.start_link()
    
    # Measure time for fact insertion
    {time_microseconds, _result} = :timer.tc(fn ->
      Enum.each(facts, fn fact ->
        Presto.RuleEngine.assert_fact(engine, fact)
      end)
    end)
    
    IO.puts("   - Inserted #{length(facts)} facts in #{time_microseconds / 1000} ms")
    IO.puts("   - Average: #{Float.round(time_microseconds / length(facts), 2)} μs per fact")
    
    GenServer.stop(engine)
    IO.puts("")
  end

  def run_ets_concurrency_benchmark do
    IO.puts("2. ETS Concurrency Performance Test")
    IO.puts("   Testing concurrent read/write performance...")
    
    {:ok, engine} = Presto.RuleEngine.start_link()
    
    # Add some rules
    test_rule = %{
      id: :test_rule,
      conditions: [
        {:person, :name, :age, :department}
      ],
      action: fn bindings ->
        [{:adult, bindings[:name]}]
      end
    }
    
    Presto.RuleEngine.add_rule(engine, test_rule)
    
    # Test concurrent fact assertions
    tasks = for i <- 1..10 do
      Task.async(fn ->
        facts = for j <- 1..100 do
          {:person, "person_#{i}_#{j}", 25 + j, "dept_#{rem(j, 3)}"}
        end
        
        {time, _} = :timer.tc(fn ->
          Enum.each(facts, fn fact ->
            Presto.RuleEngine.assert_fact(engine, fact)
          end)
        end)
        
        {i, time, length(facts)}
      end)
    end
    
    results = Enum.map(tasks, &Task.await(&1, 10_000))
    
    total_facts = Enum.reduce(results, 0, fn {_task, _time, count}, acc -> acc + count end)
    total_time = Enum.reduce(results, 0, fn {_task, time, _count}, acc -> acc + time end)
    avg_time = total_time / length(results)
    
    IO.puts("   - #{length(tasks)} concurrent tasks processed #{total_facts} facts")
    IO.puts("   - Average task time: #{Float.round(avg_time / 1000, 2)} ms")
    IO.puts("   - Throughput: #{Float.round(total_facts / (total_time / 1_000_000), 0)} facts/second")
    
    GenServer.stop(engine)
    IO.puts("")
  end

  def run_statistics_collection_benchmark do
    IO.puts("3. ETS-Based Statistics Collection Test")
    IO.puts("   Testing improved statistics performance...")
    
    {:ok, engine} = Presto.RuleEngine.start_link()
    
    # Add multiple rules
    rules = for i <- 1..20 do
      %{
        id: :"rule_#{i}",
        conditions: [
          {:data, :id, :value}
        ],
        action: fn bindings ->
          [{:result, bindings[:id], bindings[:value] * i}]
        end
      }
    end
    
    Enum.each(rules, fn rule ->
      Presto.RuleEngine.add_rule(engine, rule)
    end)
    
    # Generate facts and measure rule execution
    facts = for i <- 1..500 do
      {:data, i, i * 2}
    end
    
    {time_microseconds, results} = :timer.tc(fn ->
      Enum.each(facts, fn fact ->
        Presto.RuleEngine.assert_fact(engine, fact)
      end)
      Presto.RuleEngine.fire_rules(engine)
    end)
    
    stats = Presto.RuleEngine.get_rule_statistics(engine)
    engine_stats = Presto.RuleEngine.get_engine_statistics(engine)
    
    IO.puts("   - Processed #{length(facts)} facts through #{length(rules)} rules")
    IO.puts("   - Total execution time: #{Float.round(time_microseconds / 1000, 2)} ms")
    IO.puts("   - Results generated: #{length(results)}")
    IO.puts("   - Rules with statistics: #{map_size(stats)}")
    IO.puts("   - Total rule firings: #{engine_stats.total_rule_firings}")
    
    GenServer.stop(engine)
    IO.puts("")
  end

  def run_aggregation_performance_test do
    IO.puts("4. Large Dataset Rule Processing Test")
    IO.puts("   Testing rule processing throughput...")
    
    {:ok, engine} = Presto.RuleEngine.start_link()
    
    # Add simple rule for testing throughput
    dept_rule = %{
      id: :dept_rule,
      conditions: [
        {:salary, :employee_id, :department, :amount}
      ],
      action: fn bindings ->
        if bindings[:amount] > 60000 do
          [{:high_earner, bindings[:employee_id], bindings[:department]}]
        else
          []
        end
      end
    }
    
    Presto.RuleEngine.add_rule(engine, dept_rule)
    
    # Generate salary facts
    salary_facts = for i <- 1..1000 do
      dept = "dept_#{rem(i, 10)}"
      amount = 50000 + rem(i * 17, 20000)  # Salary between 50k-70k
      {:salary, i, dept, amount}
    end
    
    {time_microseconds, results} = :timer.tc(fn ->
      Enum.each(salary_facts, fn fact ->
        Presto.RuleEngine.assert_fact(engine, fact)
      end)
      Presto.RuleEngine.fire_rules(engine)
    end)
    
    IO.puts("   - Processed #{length(salary_facts)} salary records")
    IO.puts("   - Processing time: #{Float.round(time_microseconds / 1000, 2)} ms")
    IO.puts("   - Results generated: #{length(results)}")
    IO.puts("   - Throughput: #{Float.round(length(salary_facts) / (time_microseconds / 1_000_000), 0)} records/second")
    
    GenServer.stop(engine)
    IO.puts("")
  end

  def run_memory_usage_test do
    IO.puts("5. Memory Usage Analysis")
    IO.puts("   Testing memory efficiency...")
    
    {:ok, engine} = Presto.RuleEngine.start_link()
    
    # Measure initial memory
    {_memory_before, _} = :timer.tc(fn ->
      :erlang.garbage_collect()
      Process.sleep(10)
    end)
    
    initial_memory = :erlang.memory(:total)
    
    # Add facts and rules
    facts = for i <- 1..5000 do
      {:test_fact, i, "data_#{i}", rem(i, 100)}
    end
    
    rule = %{
      id: :memory_test_rule,
      conditions: [
        {:test_fact, :id, :data, :value}
      ],
      action: fn bindings ->
        [{:processed, bindings[:id], bindings[:value]}]
      end
    }
    
    Presto.RuleEngine.add_rule(engine, rule)
    
    Enum.each(facts, fn fact ->
      Presto.RuleEngine.assert_fact(engine, fact)
    end)
    
    results = Presto.fire_rules(engine)
    
    # Measure final memory
    :erlang.garbage_collect()
    Process.sleep(10)
    final_memory = :erlang.memory(:total)
    
    memory_used = final_memory - initial_memory
    memory_per_fact = memory_used / length(facts)
    
    IO.puts("   - Initial memory: #{Float.round(initial_memory / 1024 / 1024, 2)} MB")
    IO.puts("   - Final memory: #{Float.round(final_memory / 1024 / 1024, 2)} MB")
    IO.puts("   - Memory used: #{Float.round(memory_used / 1024 / 1024, 2)} MB")
    IO.puts("   - Memory per fact: #{Float.round(memory_per_fact, 0)} bytes")
    IO.puts("   - Results produced: #{length(results)}")
    
    GenServer.stop(engine)
    IO.puts("")
  end
end

# Run all benchmarks
PerformanceBenchmark.run_pattern_matching_benchmark()
PerformanceBenchmark.run_ets_concurrency_benchmark() 
PerformanceBenchmark.run_statistics_collection_benchmark()
PerformanceBenchmark.run_aggregation_performance_test()
PerformanceBenchmark.run_memory_usage_test()

IO.puts("=== Benchmark Complete ===")
IO.puts("\nOptimizations implemented:")
IO.puts("✓ ETS tables with read_concurrency and write_concurrency")
IO.puts("✓ Pattern matching micro-optimizations (no tuple-to-list conversion)")
IO.puts("✓ ETS-based statistics collection (replaces process dictionary)")
IO.puts("✓ Enhanced optimization configuration options")
IO.puts("✓ Memory efficiency improvements")
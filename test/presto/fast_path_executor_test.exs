defmodule Presto.FastPathExecutorTest do
  use ExUnit.Case, async: true

  alias Presto.FastPathExecutor
  alias Presto.RuleEngine

  describe "execute_fast_path/2" do
    test "executes simple rule with single fact pattern" do
      # Start rule engine
      engine = start_supervised!(RuleEngine)

      # Add facts to rule engine
      RuleEngine.assert_fact(engine, {:person, "john", %{age: 25}})
      RuleEngine.assert_fact(engine, {:person, "jane", %{age: 17}})

      # Define a simple rule without test conditions for now
      rule = %{
        id: :person_rule,
        conditions: [
          {:person, :name, :data}
        ],
        action: fn bindings ->
          [{:processed_person, bindings[:name]}]
        end
      }

      # Execute fast path
      {:ok, results} = FastPathExecutor.execute_fast_path(rule, engine)

      # Should match both persons
      assert length(results) == 2
      assert {:processed_person, "john"} in results
      assert {:processed_person, "jane"} in results
    end

    test "returns empty results when no facts match the pattern" do
      engine = start_supervised!(RuleEngine)

      # Add facts that don't match the pattern
      RuleEngine.assert_fact(engine, {:employee, "emp1", %{salary: 50_000}})

      rule = %{
        id: :person_rule,
        conditions: [
          {:person, :name, :data}
        ],
        action: fn _bindings -> [{:result, :found}] end
      }

      {:ok, results} = FastPathExecutor.execute_fast_path(rule, engine)

      assert results == []
    end

    test "handles different fact structures" do
      engine = start_supervised!(RuleEngine)

      RuleEngine.assert_fact(engine, {:score, "test1", 85})
      RuleEngine.assert_fact(engine, {:score, "test2", 92})
      RuleEngine.assert_fact(engine, {:score, "test3", 78})

      rule = %{
        id: :score_rule,
        conditions: [
          {:score, :test_name, :score_value}
        ],
        action: fn bindings ->
          [{:high_score, bindings[:test_name], bindings[:score_value]}]
        end
      }

      {:ok, results} = FastPathExecutor.execute_fast_path(rule, engine)

      assert length(results) == 3
      assert {:high_score, "test1", 85} in results
      assert {:high_score, "test2", 92} in results
      assert {:high_score, "test3", 78} in results
    end

    test "handles simple pattern matching without test conditions" do
      engine = start_supervised!(RuleEngine)

      RuleEngine.assert_fact(engine, {:animal, "cat", "fluffy"})
      RuleEngine.assert_fact(engine, {:animal, "dog", "loyal"})

      rule = %{
        id: :animal_rule,
        conditions: [
          {:animal, :type, :trait}
        ],
        action: fn bindings ->
          [{:classified, bindings[:type], bindings[:trait]}]
        end
      }

      {:ok, results} = FastPathExecutor.execute_fast_path(rule, engine)

      assert length(results) == 2
      assert {:classified, "cat", "fluffy"} in results
      assert {:classified, "dog", "loyal"} in results
    end

    test "handles action that raises an error gracefully" do
      engine = start_supervised!(RuleEngine)

      RuleEngine.assert_fact(engine, {:test, "data"})

      rule = %{
        id: :failing_rule,
        conditions: [
          {:test, :value}
        ],
        action: fn _bindings ->
          raise "Test error"
        end
      }

      {:ok, results} = FastPathExecutor.execute_fast_path(rule, engine)

      # Should handle errors gracefully and return empty results
      assert results == []
    end
  end

  describe "execute_batch_fast_path/2" do
    test "executes multiple rules against same fact set efficiently" do
      engine = start_supervised!(RuleEngine)

      RuleEngine.assert_fact(engine, {:score, "math", 85})
      RuleEngine.assert_fact(engine, {:score, "science", 92})

      rule1 = %{
        id: :rule1,
        conditions: [
          {:score, :subject, :value}
        ],
        action: fn bindings ->
          [{:evaluated, bindings[:subject], "passing"}]
        end
      }

      rule2 = %{
        id: :rule2,
        conditions: [
          {:score, :subject, :value}
        ],
        action: fn bindings ->
          [{:graded, bindings[:subject], bindings[:value]}]
        end
      }

      {:ok, results} = FastPathExecutor.execute_batch_fast_path([rule1, rule2], engine)

      # Should have results from both rules for both facts
      assert length(results) == 4
      assert {:evaluated, "math", "passing"} in results
      assert {:evaluated, "science", "passing"} in results
      assert {:graded, "math", 85} in results
      assert {:graded, "science", 92} in results
    end
  end

  describe "pattern matching edge cases" do
    test "handles facts with different tuple sizes" do
      engine = start_supervised!(RuleEngine)

      RuleEngine.assert_fact(engine, {:short, "data"})
      RuleEngine.assert_fact(engine, {:long, "data", "extra", "more"})

      rule = %{
        id: :size_rule,
        conditions: [
          {:short, :value}
        ],
        action: fn bindings ->
          [{:matched, bindings[:value]}]
        end
      }

      {:ok, results} = FastPathExecutor.execute_fast_path(rule, engine)

      assert length(results) == 1
      assert {:matched, "data"} in results
    end

    test "handles wildcard patterns" do
      engine = start_supervised!(RuleEngine)

      RuleEngine.assert_fact(engine, {:data, "test1", "value1"})
      RuleEngine.assert_fact(engine, {:data, "test2", "value2"})

      rule = %{
        id: :wildcard_rule,
        conditions: [
          {:data, :_, :value}
        ],
        action: fn bindings ->
          [{:found_value, bindings[:value]}]
        end
      }

      {:ok, results} = FastPathExecutor.execute_fast_path(rule, engine)

      assert length(results) == 2
      assert {:found_value, "value1"} in results
      assert {:found_value, "value2"} in results
    end
  end
end

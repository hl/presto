defmodule PrestoTest do
  use ExUnit.Case, async: true
  doctest Presto

  describe "RETE algorithm demonstration" do
    test "basic fact assertion and rule matching" do
      # Start the rule engine
      {:ok, engine} = Presto.start_engine()

      # Define a simple rule: if person age > 18, then adult
      rule = %{
        id: :adult_rule,
        conditions: [
          {:person, :name, :age},
          {:age, :>, 18}
        ],
        action: fn facts ->
          [{:adult, facts[:name]}]
        end
      }

      # Add rule to engine
      :ok = Presto.add_rule(engine, rule)

      # Assert facts
      :ok = Presto.assert_fact(engine, {:person, "John", 25})
      :ok = Presto.assert_fact(engine, {:person, "Jane", 16})

      # Fire rules and get results
      results = Presto.fire_rules(engine)

      # Only John should be marked as adult (age > 18)
      assert [{:adult, "John"}] = results

      # Clean up
      Presto.stop_engine(engine)
    end

    test "multiple rules with different conditions" do
      {:ok, engine} = Presto.start_engine()

      # Rule 1: Person with age > 18 is adult
      adult_rule = %{
        id: :adult_rule,
        conditions: [
          {:person, :name, :age},
          {:age, :>, 18}
        ],
        action: fn facts -> [{:adult, facts[:name]}] end
      }

      # Rule 2: Income > 50_000 is high income
      income_rule = %{
        id: :income_rule,
        conditions: [
          {:income, :name, :amount},
          {:amount, :>, 50_000}
        ],
        action: fn facts -> [{:high_income, facts[:name]}] end
      }

      Presto.add_rule(engine, adult_rule)
      Presto.add_rule(engine, income_rule)

      # Assert facts
      Presto.assert_fact(engine, {:person, "Alice", 30})
      Presto.assert_fact(engine, {:income, "Alice", 75_000})
      Presto.assert_fact(engine, {:person, "Bob", 25})
      Presto.assert_fact(engine, {:income, "Bob", 40_000})

      results = Presto.fire_rules(engine)

      # Both should be adults, only Alice should have high income
      assert {:adult, "Alice"} in results
      assert {:adult, "Bob"} in results
      assert {:high_income, "Alice"} in results
      refute {:high_income, "Bob"} in results

      Presto.stop_engine(engine)
    end

    test "fact retraction updates rule results" do
      {:ok, engine} = Presto.start_engine()

      rule = %{
        id: :test_rule,
        conditions: [
          {:person, :name, :age},
          {:age, :>, 21}
        ],
        action: fn facts -> [{:drinking_age, facts[:name]}] end
      }

      Presto.add_rule(engine, rule)

      # Assert and check
      Presto.assert_fact(engine, {:person, "Tom", 25})
      results1 = Presto.fire_rules(engine)
      assert [{:drinking_age, "Tom"}] = results1

      # Retract fact and check again
      Presto.retract_fact(engine, {:person, "Tom", 25})
      results2 = Presto.fire_rules(engine)
      assert [] = results2

      Presto.stop_engine(engine)
    end

    test "concurrent rule evaluation" do
      {:ok, engine} = Presto.start_engine()

      # Add multiple rules that can fire concurrently
      for i <- 1..10 do
        rule = %{
          id: :"rule_#{i}",
          conditions: [
            {:number, :value},
            {:value, :>, i * 10}
          ],
          action: fn facts -> [{:large_number, facts[:value], i}] end
        }

        Presto.add_rule(engine, rule)
      end

      # Assert fact that matches multiple rules
      Presto.assert_fact(engine, {:number, 100})

      results = Presto.fire_rules(engine)

      # Should have 9 results (rules 1-9 match, rule 10 doesn't since 100 > 100 is false)
      assert length(results) == 9

      assert Enum.all?(results, fn {tag, value, _rule_id} ->
               tag == :large_number and value == 100
             end)

      Presto.stop_engine(engine)
    end
  end

  describe "performance characteristics" do
    test "demonstrates RETE algorithm handles increasing fact loads" do
      # This test verifies that the RETE algorithm can correctly process
      # increasing numbers of facts without functional degradation

      {:ok, engine} = Presto.start_engine()

      # Add complex rule with multiple conditions
      complex_rule = %{
        id: :complex_rule,
        conditions: [
          {:person, :name, :age},
          {:employment, :name, :company},
          {:salary, :name, :amount},
          {:age, :>, 25},
          {:amount, :>, 45_000}
        ],
        action: fn facts ->
          [{:qualified_employee, facts[:name], facts[:company]}]
        end
      }

      Presto.add_rule(engine, complex_rule)

      # Process increasing number of facts
      fact_counts = [10, 50, 100]

      results_counts =
        Enum.map(fact_counts, fn count ->
          # Clear working memory
          Presto.clear_facts(engine)

          # Add facts
          for i <- 1..count do
            Presto.assert_fact(engine, {:person, "person_#{i}", 20 + rem(i, 20)})
            Presto.assert_fact(engine, {:employment, "person_#{i}", "company_#{rem(i, 5)}"})
            Presto.assert_fact(engine, {:salary, "person_#{i}", 40_000 + i * 1000})
          end

          # Get results
          results = Presto.fire_rules(engine)
          length(results)
        end)

      # Verify that the algorithm produces consistent results
      # and can handle increasing fact loads without errors
      [count_10, count_50, count_100] = results_counts

      # Results should scale predictably with qualifying facts
      # (people with age > 25 and salary > 45,000)
      assert count_10 > 0
      assert count_50 >= count_10
      assert count_100 >= count_50

      Presto.stop_engine(engine)
    end
  end
end

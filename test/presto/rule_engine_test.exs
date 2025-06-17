defmodule Presto.RuleEngineTest do
  use ExUnit.Case, async: true

  alias Presto.RuleEngine

  setup do
    {:ok, engine} = RuleEngine.start_link([])
    %{engine: engine}
  end

  describe "rule management" do
    test "can add rules to the engine", %{engine: engine} do
      rule = %{
        id: :test_rule,
        conditions: [
          {:person, :name, :age},
          {:age, :>, 18}
        ],
        action: fn facts -> [{:adult, facts[:name]}] end
      }

      :ok = RuleEngine.add_rule(engine, rule)

      rules = RuleEngine.get_rules(engine)
      assert Map.has_key?(rules, :test_rule)
    end

    test "can add multiple rules", %{engine: engine} do
      rules = [
        %{
          id: :adult_rule,
          conditions: [
            {:person, :name, :age},
            {:age, :>, 18}
          ],
          action: fn facts -> [{:adult, facts[:name]}] end
        },
        %{
          id: :senior_rule,
          conditions: [
            {:person, :name, :age},
            {:age, :>, 65}
          ],
          action: fn facts -> [{:senior, facts[:name]}] end
        }
      ]

      for rule <- rules do
        RuleEngine.add_rule(engine, rule)
      end

      engine_rules = RuleEngine.get_rules(engine)
      assert map_size(engine_rules) == 2
      assert Map.has_key?(engine_rules, :adult_rule)
      assert Map.has_key?(engine_rules, :senior_rule)
    end

    test "can remove rules from the engine", %{engine: engine} do
      rule = %{
        id: :test_rule,
        conditions: [
          {:person, :name, :age},
          {:age, :>, 18}
        ],
        action: fn facts -> [{:adult, facts[:name]}] end
      }

      RuleEngine.add_rule(engine, rule)
      assert Map.has_key?(RuleEngine.get_rules(engine), :test_rule)

      :ok = RuleEngine.remove_rule(engine, :test_rule)
      refute Map.has_key?(RuleEngine.get_rules(engine), :test_rule)
    end

    test "validates rule structure", %{engine: engine} do
      invalid_rules = [
        # Missing required fields
        %{id: :invalid1},
        %{conditions: []},
        %{action: fn _ -> [] end},

        # Invalid types
        %{id: "string_id", conditions: [], action: fn _ -> [] end},
        %{id: :test, conditions: "not_list", action: fn _ -> [] end},
        %{id: :test, conditions: [], action: "not_function"}
      ]

      for invalid_rule <- invalid_rules do
        assert {:error, _reason} = RuleEngine.add_rule(engine, invalid_rule)
      end

      # Engine should remain empty
      assert map_size(RuleEngine.get_rules(engine)) == 0
    end
  end

  describe "fact processing" do
    test "can assert and retract facts", %{engine: engine} do
      facts = [
        {:person, "Alice", 25},
        {:person, "Bob", 30}
      ]

      for fact <- facts do
        :ok = RuleEngine.assert_fact(engine, fact)
      end

      stored_facts = RuleEngine.get_facts(engine)

      for fact <- facts do
        assert fact in stored_facts
      end

      # Retract one fact
      RuleEngine.retract_fact(engine, {:person, "Alice", 25})

      remaining_facts = RuleEngine.get_facts(engine)
      assert {:person, "Bob", 30} in remaining_facts
      refute {:person, "Alice", 25} in remaining_facts
    end

    test "can clear all facts", %{engine: engine} do
      facts = [
        {:person, "Alice", 25},
        {:person, "Bob", 30},
        {:company, "TechCorp", "Technology"}
      ]

      for fact <- facts do
        RuleEngine.assert_fact(engine, fact)
      end

      assert length(RuleEngine.get_facts(engine)) == 3

      :ok = RuleEngine.clear_facts(engine)
      assert [] = RuleEngine.get_facts(engine)
    end
  end

  describe "rule execution" do
    test "executes simple rules correctly", %{engine: engine} do
      # Add rule: if person age > 18, then adult
      rule = %{
        id: :adult_rule,
        conditions: [
          {:person, :name, :age},
          {:age, :>, 18}
        ],
        action: fn facts -> [{:adult, facts[:name]}] end
      }

      RuleEngine.add_rule(engine, rule)

      # Add facts
      RuleEngine.assert_fact(engine, {:person, "Alice", 25})
      RuleEngine.assert_fact(engine, {:person, "Bob", 16})

      # Fire rules
      results = RuleEngine.fire_rules(engine)

      # Only Alice should be marked as adult
      assert length(results) == 1
      assert {:adult, "Alice"} in results
    end

    test "executes multiple rules with different conditions", %{engine: engine} do
      rules = [
        %{
          id: :adult_rule,
          conditions: [
            {:person, :name, :age},
            {:age, :>, 18}
          ],
          action: fn facts -> [{:adult, facts[:name]}] end
        },
        %{
          id: :senior_rule,
          conditions: [
            {:person, :name, :age},
            {:age, :>, 65}
          ],
          action: fn facts -> [{:senior, facts[:name]}] end
        },
        %{
          id: :middle_aged_rule,
          conditions: [
            {:person, :name, :age},
            {:age, :>, 40},
            {:age, :<=, 65}
          ],
          action: fn facts -> [{:middle_aged, facts[:name]}] end
        }
      ]

      for rule <- rules do
        RuleEngine.add_rule(engine, rule)
      end

      # Add people of different ages
      people = [
        {:person, "Child", 10},
        {:person, "Young", 25},
        {:person, "Middle", 50},
        {:person, "Senior", 70}
      ]

      for person <- people do
        RuleEngine.assert_fact(engine, person)
      end

      results = RuleEngine.fire_rules(engine)

      # Verify expected classifications
      assert {:adult, "Young"} in results
      assert {:adult, "Middle"} in results
      assert {:adult, "Senior"} in results
      assert {:middle_aged, "Middle"} in results
      assert {:senior, "Senior"} in results

      # Child should not match any rules
      refute {:adult, "Child"} in results
      refute {:senior, "Child"} in results
      refute {:middle_aged, "Child"} in results
    end

    test "executes rules with fact joining", %{engine: engine} do
      # Rule: if person works at tech company, then tech_worker
      rule = %{
        id: :tech_worker_rule,
        conditions: [
          {:person, :name, :age},
          {:employment, :name, :company},
          {:company, :company, :industry},
          {:industry, :==, "Technology"}
        ],
        action: fn facts -> [{:tech_worker, facts[:name], facts[:company]}] end
      }

      RuleEngine.add_rule(engine, rule)

      # Add facts
      facts = [
        {:person, "Alice", 30},
        {:person, "Bob", 25},
        {:employment, "Alice", "TechCorp"},
        {:employment, "Bob", "FinanceInc"},
        {:company, "TechCorp", "Technology"},
        {:company, "FinanceInc", "Finance"}
      ]

      for fact <- facts do
        RuleEngine.assert_fact(engine, fact)
      end

      results = RuleEngine.fire_rules(engine)

      # Only Alice should be identified as tech worker
      # Bob should not match because FinanceInc is Finance industry, not Technology
      assert length(results) == 1
      assert {:tech_worker, "Alice", "TechCorp"} in results
    end

    test "handles rule actions that produce multiple results", %{engine: engine} do
      rule = %{
        id: :benefits_rule,
        conditions: [
          {:employee, :name, :years_service},
          {:years_service, :>, 5}
        ],
        action: fn facts ->
          name = facts[:name]
          years = facts[:years_service]

          [
            {:vacation_eligible, name},
            {:bonus_eligible, name},
            {:seniority_level, name, div(years, 5)}
          ]
        end
      }

      RuleEngine.add_rule(engine, rule)

      RuleEngine.assert_fact(engine, {:employee, "Alice", 12})

      results = RuleEngine.fire_rules(engine)

      assert {:vacation_eligible, "Alice"} in results
      assert {:bonus_eligible, "Alice"} in results
      assert {:seniority_level, "Alice", 2} in results
    end
  end

  describe "incremental processing" do
    test "only processes changed facts", %{engine: engine} do
      rule = %{
        id: :test_rule,
        conditions: [
          {:person, :name, :age},
          {:age, :>, 18}
        ],
        action: fn facts -> [{:adult, facts[:name]}] end
      }

      RuleEngine.add_rule(engine, rule)

      # Add initial facts
      RuleEngine.assert_fact(engine, {:person, "Alice", 25})
      RuleEngine.assert_fact(engine, {:person, "Bob", 30})

      # First rule firing
      results1 = RuleEngine.fire_rules(engine)
      assert length(results1) == 2

      # Add new fact - should only process incrementally
      RuleEngine.assert_fact(engine, {:person, "Charlie", 35})

      # Get incremental results
      incremental_results = RuleEngine.fire_rules_incremental(engine)

      # Should only have result for Charlie (new fact)
      assert [{:adult, "Charlie"}] = incremental_results

      # Full results should include all
      all_results = RuleEngine.fire_rules(engine)
      assert length(all_results) == 3
    end

    test "handles fact retraction incrementally", %{engine: engine} do
      rule = %{
        id: :test_rule,
        conditions: [
          {:person, :name, :age},
          {:age, :>, 18}
        ],
        action: fn facts -> [{:adult, facts[:name]}] end
      }

      RuleEngine.add_rule(engine, rule)

      # Add facts and fire rules
      RuleEngine.assert_fact(engine, {:person, "Alice", 25})
      RuleEngine.assert_fact(engine, {:person, "Bob", 30})

      results = RuleEngine.fire_rules(engine)
      assert length(results) == 2

      # Retract fact
      RuleEngine.retract_fact(engine, {:person, "Alice", 25})

      # Fire rules again - should only have Bob
      results = RuleEngine.fire_rules(engine)
      assert [{:adult, "Bob"}] = results
    end
  end

  describe "rule priorities and ordering" do
    test "executes rules in priority order", %{engine: engine} do
      # Add rules with different priorities
      rules = [
        %{
          id: :low_priority,
          priority: 1,
          conditions: [
            {:person, :name, :age},
            {:age, :>, 18}
          ],
          action: fn facts -> [{:processed_by, :low_priority, facts[:name]}] end
        },
        %{
          id: :high_priority,
          priority: 10,
          conditions: [
            {:person, :name, :age},
            {:age, :>, 18}
          ],
          action: fn facts -> [{:processed_by, :high_priority, facts[:name]}] end
        },
        %{
          id: :medium_priority,
          priority: 5,
          conditions: [
            {:person, :name, :age},
            {:age, :>, 18}
          ],
          action: fn facts -> [{:processed_by, :medium_priority, facts[:name]}] end
        }
      ]

      for rule <- rules do
        RuleEngine.add_rule(engine, rule)
      end

      RuleEngine.assert_fact(engine, {:person, "Alice", 25})

      results = RuleEngine.fire_rules(engine)

      # Should have results from all rules, but execution order should respect priority
      assert length(results) == 3

      # Get execution order
      execution_order = RuleEngine.get_last_execution_order(engine)
      assert execution_order == [:high_priority, :medium_priority, :low_priority]
    end
  end

  describe "concurrent rule execution" do
    test "can execute rules concurrently when safe", %{engine: engine} do
      # Add multiple independent rules
      for i <- 1..5 do
        rule = %{
          id: :"rule_#{i}",
          conditions: [
            {:number, :value},
            {:value, :>, i * 10}
          ],
          action: fn facts ->
            # Simulate some work
            :timer.sleep(10)
            [{:large_number, facts[:value], i}]
          end
        }

        RuleEngine.add_rule(engine, rule)
      end

      RuleEngine.assert_fact(engine, {:number, 100})

      # Measure concurrent execution time
      {time, results} =
        :timer.tc(fn ->
          RuleEngine.fire_rules(engine, concurrent: true)
        end)

      # Should have 5 results (all rules match)
      assert length(results) == 5

      # Concurrent execution should be faster than sequential
      # (5 rules × 10ms sleep = 50ms sequential, but concurrent should be ~10ms)
      # 30ms threshold (allowing for overhead)
      assert time < 30_000
    end

    test "handles rule conflicts safely", %{engine: engine} do
      # Add rules that might conflict (both modify same fact type)
      rules = [
        %{
          id: :rule1,
          conditions: [
            {:person, :name, :age},
            {:age, :>, 18}
          ],
          action: fn facts ->
            [{:status, facts[:name], :adult}]
          end
        },
        %{
          id: :rule2,
          conditions: [
            {:person, :name, :age},
            {:age, :>, 65}
          ],
          action: fn facts ->
            [{:status, facts[:name], :senior}]
          end
        }
      ]

      for rule <- rules do
        RuleEngine.add_rule(engine, rule)
      end

      RuleEngine.assert_fact(engine, {:person, "Alice", 70})

      results = RuleEngine.fire_rules(engine, concurrent: true)

      # Should handle conflicts appropriately (both rules can fire)
      assert {:status, "Alice", :adult} in results
      assert {:status, "Alice", :senior} in results
    end
  end

  describe "error handling" do
    test "handles rule action exceptions gracefully", %{engine: engine} do
      rule = %{
        id: :failing_rule,
        conditions: [
          {:person, :name, :age}
        ],
        action: fn _facts ->
          raise "Intentional error"
        end
      }

      RuleEngine.add_rule(engine, rule)
      RuleEngine.assert_fact(engine, {:person, "Alice", 25})

      # Should not crash, should return error info
      {:ok, results, errors} = RuleEngine.fire_rules_with_errors(engine)

      assert results == []
      assert length(errors) == 1
      assert {:error, :failing_rule, %RuntimeError{}} = hd(errors)
    end

    test "continues processing other rules when one fails", %{engine: engine} do
      rules = [
        %{
          id: :good_rule,
          conditions: [
            {:person, :name, :age},
            {:age, :>, 18}
          ],
          action: fn facts -> [{:adult, facts[:name]}] end
        },
        %{
          id: :bad_rule,
          conditions: [
            {:person, :name, :age}
          ],
          action: fn _facts -> raise "Error" end
        }
      ]

      for rule <- rules do
        RuleEngine.add_rule(engine, rule)
      end

      RuleEngine.assert_fact(engine, {:person, "Alice", 25})

      {:ok, results, errors} = RuleEngine.fire_rules_with_errors(engine)

      # Good rule should succeed
      assert [{:adult, "Alice"}] = results

      # Bad rule should error
      assert length(errors) == 1
    end
  end

  describe "performance monitoring" do
    test "tracks rule execution statistics", %{engine: engine} do
      rule = %{
        id: :test_rule,
        conditions: [
          {:person, :name, :age},
          {:age, :>, 18}
        ],
        action: fn facts ->
          # Add small delay to ensure measurable execution time
          Process.sleep(1)
          [{:adult, facts[:name]}]
        end
      }

      RuleEngine.add_rule(engine, rule)

      # Add facts and fire rules multiple times
      for i <- 1..10 do
        RuleEngine.assert_fact(engine, {:person, "Person#{i}", 20 + i})
      end

      RuleEngine.fire_rules(engine)

      # Get statistics
      stats = RuleEngine.get_rule_statistics(engine)

      assert Map.has_key?(stats, :test_rule)

      rule_stats = stats[:test_rule]
      assert rule_stats.executions >= 1
      assert rule_stats.total_time > 0
      assert rule_stats.average_time > 0
      assert rule_stats.facts_processed >= 10
    end

    test "tracks overall engine performance", %{engine: engine} do
      # Add multiple rules and facts
      for i <- 1..5 do
        rule = %{
          id: :"rule_#{i}",
          conditions: [
            {:number, :value},
            {:value, :>, i * 10}
          ],
          action: fn facts -> [{:large, facts[:value], i}] end
        }

        RuleEngine.add_rule(engine, rule)
      end

      for i <- 1..100 do
        RuleEngine.assert_fact(engine, {:number, i})
      end

      RuleEngine.fire_rules(engine)

      engine_stats = RuleEngine.get_engine_statistics(engine)

      assert engine_stats.total_facts >= 100
      assert engine_stats.total_rules == 5
      assert engine_stats.last_execution_time > 0
      assert engine_stats.total_rule_firings > 0
    end
  end
end

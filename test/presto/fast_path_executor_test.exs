defmodule Presto.FastPathExecutorTest do
  use ExUnit.Case, async: true

  alias Presto.FastPathExecutor
  alias Presto.WorkingMemory

  describe "execute_fast_path/2" do
    test "executes simple rule with single fact pattern" do
      # Start working memory
      {:ok, wm} = WorkingMemory.start_link()

      # Add facts to working memory
      WorkingMemory.assert_fact(wm, {:person, "john", %{age: 25}})
      WorkingMemory.assert_fact(wm, {:person, "jane", %{age: 17}})

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
      {:ok, results} = FastPathExecutor.execute_fast_path(rule, wm)

      # Should match both persons
      assert length(results) == 2
      assert {:processed_person, "john"} in results
      assert {:processed_person, "jane"} in results
    end

    test "returns empty results when no facts match the pattern" do
      {:ok, wm} = WorkingMemory.start_link()

      # Add facts that don't match the pattern
      WorkingMemory.assert_fact(wm, {:employee, "emp1", %{salary: 50000}})

      rule = %{
        id: :person_rule,
        conditions: [
          {:person, :name, :data}
        ],
        action: fn _bindings -> [{:result, :found}] end
      }

      {:ok, results} = FastPathExecutor.execute_fast_path(rule, wm)
      assert results == []
    end

    test "returns empty results when looking for different fact type" do
      {:ok, wm} = WorkingMemory.start_link()

      WorkingMemory.assert_fact(wm, {:person, "young", %{age: 15}})

      # Rule looking for a different fact type
      rule = %{
        id: :employee_rule,
        conditions: [
          {:employee, :name, :data}
        ],
        action: fn bindings -> [{:found_employee, bindings[:name]}] end
      }

      {:ok, results} = FastPathExecutor.execute_fast_path(rule, wm)
      assert results == []
    end

    test "handles different fact structures" do
      {:ok, wm} = WorkingMemory.start_link()

      WorkingMemory.assert_fact(wm, {:employee, "emp1", %{age: 25, salary: 60000}})
      WorkingMemory.assert_fact(wm, {:employee, "emp2", %{age: 30, salary: 40000}})
      WorkingMemory.assert_fact(wm, {:employee, "emp3", %{age: 20, salary: 70000}})

      # Simple pattern matching without test conditions
      rule = %{
        id: :employee_rule,
        conditions: [
          {:employee, :id, :data}
        ],
        action: fn bindings -> 
          [{:processed_employee, bindings[:id], bindings[:data][:age]}] 
        end
      }

      {:ok, results} = FastPathExecutor.execute_fast_path(rule, wm)

      # Should match all employees
      assert length(results) == 3
      assert {:processed_employee, "emp1", 25} in results
      assert {:processed_employee, "emp2", 30} in results
      assert {:processed_employee, "emp3", 20} in results
    end

    test "handles score processing" do
      {:ok, wm} = WorkingMemory.start_link()

      WorkingMemory.assert_fact(wm, {:score, "test1", %{value: 85}})
      WorkingMemory.assert_fact(wm, {:score, "test2", %{value: 95}})
      WorkingMemory.assert_fact(wm, {:score, "test3", %{value: 75}})

      # Simple pattern matching for scores
      rule = %{
        id: :score_rule,
        conditions: [
          {:score, :id, :data}
        ],
        action: fn bindings -> 
          [{:processed_score, bindings[:id], bindings[:data][:value]}] 
        end
      }

      {:ok, results} = FastPathExecutor.execute_fast_path(rule, wm)
      assert length(results) == 3
      assert {:processed_score, "test1", 85} in results
      assert {:processed_score, "test2", 95} in results
      assert {:processed_score, "test3", 75} in results
    end

    test "handles simple pattern matching without test conditions" do
      {:ok, wm} = WorkingMemory.start_link()

      WorkingMemory.assert_fact(wm, {:status, "item1", %{state: "active"}})
      WorkingMemory.assert_fact(wm, {:status, "item2", %{state: "inactive"}})

      # Simple rule without test conditions
      rule = %{
        id: :status_rule,
        conditions: [
          {:status, :id, :data}
        ],
        action: fn bindings -> 
          [{:processed_status, bindings[:id], bindings[:data][:state]}] 
        end
      }

      {:ok, results} = FastPathExecutor.execute_fast_path(rule, wm)
      assert length(results) == 2
      assert {:processed_status, "item1", "active"} in results
      assert {:processed_status, "item2", "inactive"} in results
    end

    test "handles action that raises an error gracefully" do
      {:ok, wm} = WorkingMemory.start_link()

      WorkingMemory.assert_fact(wm, {:person, "john", %{age: 25}})

      # Rule with action that will raise an error
      rule = %{
        id: :failing_rule,
        conditions: [
          {:person, :name, :data}
        ],
        action: fn _bindings -> raise "Action failed" end
      }

      # The FastPathExecutor catches errors in actions and returns empty list
      {:ok, results} = FastPathExecutor.execute_fast_path(rule, wm)
      assert results == []
    end
  end

  describe "compile_rule_for_fast_path/1" do
    test "compiles simple rule with pattern and tests" do
      rule = %{
        id: :test_rule,
        conditions: [
          {:person, :name, :age},
          {:age, :>, 18}
        ],
        action: fn _bindings -> [:result] end
      }

      compiled = FastPathExecutor.compile_rule_for_fast_path(rule)

      assert compiled.id == :test_rule
      assert compiled.pattern == {:person, :name, :age}
      assert compiled.fact_type == :person
      assert is_function(compiled.test_function)
      assert is_function(compiled.action)
    end

    test "compiles rule with no test conditions" do
      rule = %{
        id: :simple_rule,
        conditions: [
          {:employee, :id, :name}
        ],
        action: fn _bindings -> [:result] end
      }

      compiled = FastPathExecutor.compile_rule_for_fast_path(rule)

      # Test function should always return true when no tests
      assert compiled.test_function.(%{}) == true
    end

    test "compiled test function evaluates conditions correctly" do
      rule = %{
        id: :test_rule,
        conditions: [
          {:person, :name, :age},
          {:age, :>, 18}
        ],
        action: fn _bindings -> [:result] end
      }

      compiled = FastPathExecutor.compile_rule_for_fast_path(rule)

      # Test with bindings that should pass
      assert compiled.test_function.(%{age: 25}) == true

      # Test with bindings that should fail
      assert compiled.test_function.(%{age: 15}) == false
    end
  end

  describe "can_batch_rules?/1" do
    test "returns true when all rules use the same fact type" do
      rules = [
        %{
          id: :rule1,
          conditions: [
            {:person, :name, :age},
            {:age, :>, 18}
          ],
          action: fn _bindings -> [] end
        },
        %{
          id: :rule2,
          conditions: [
            {:person, :id, :salary},
            {:salary, :>, 50000}
          ],
          action: fn _bindings -> [] end
        }
      ]

      assert FastPathExecutor.can_batch_rules?(rules) == true
    end

    test "returns false when rules use different fact types" do
      rules = [
        %{
          id: :rule1,
          conditions: [
            {:person, :name, :age}
          ],
          action: fn _bindings -> [] end
        },
        %{
          id: :rule2,
          conditions: [
            {:employee, :id, :salary}
          ],
          action: fn _bindings -> [] end
        }
      ]

      assert FastPathExecutor.can_batch_rules?(rules) == false
    end

    test "returns true for single rule" do
      rules = [
        %{
          id: :rule1,
          conditions: [
            {:person, :name, :age}
          ],
          action: fn _bindings -> [] end
        }
      ]

      assert FastPathExecutor.can_batch_rules?(rules) == true
    end
  end

  describe "execute_batch_fast_path/2" do
    test "executes multiple rules against same fact set efficiently" do
      {:ok, wm} = WorkingMemory.start_link()

      # Add facts
      WorkingMemory.assert_fact(wm, {:person, "john", %{age: 25, salary: 60000}})
      WorkingMemory.assert_fact(wm, {:person, "jane", %{age: 30, salary: 45000}})

      rules = [
        %{
          id: :person_processor,
          conditions: [
            {:person, :name, :data}
          ],
          action: fn bindings -> [{:processed_person, bindings[:name]}] end
        },
        %{
          id: :age_extractor,
          conditions: [
            {:person, :name, :data}
          ],
          action: fn bindings -> [{:age_info, bindings[:name], bindings[:data][:age]}] end
        }
      ]

      {:ok, results} = FastPathExecutor.execute_batch_fast_path(rules, wm)

      # Should get results from both rules for both people
      assert length(results) == 4
      assert {:processed_person, "john"} in results
      assert {:processed_person, "jane"} in results
      assert {:age_info, "john", 25} in results
      assert {:age_info, "jane", 30} in results
    end

    test "handles batch execution with failing action gracefully" do
      {:ok, wm} = WorkingMemory.start_link()

      WorkingMemory.assert_fact(wm, {:person, "john", %{age: 25}})

      rules = [
        %{
          id: :failing_rule,
          conditions: [
            {:person, :name, :data}
          ],
          action: fn _bindings -> raise "Batch action failed" end
        }
      ]

      # The FastPathExecutor catches errors in actions and returns empty list
      {:ok, results} = FastPathExecutor.execute_batch_fast_path(rules, wm)
      assert results == []
    end
  end

  describe "pattern matching edge cases" do
    test "handles facts with different tuple sizes" do
      {:ok, wm} = WorkingMemory.start_link()

      # Add facts with different structures
      WorkingMemory.assert_fact(wm, {:person, "john"})  # 2-tuple
      WorkingMemory.assert_fact(wm, {:person, "jane", %{age: 25}})  # 3-tuple

      rule = %{
        id: :three_element_rule,
        conditions: [
          {:person, :name, :age}  # Expects 3-tuple
        ],
        action: fn bindings -> [{:matched, bindings[:name]}] end
      }

      {:ok, results} = FastPathExecutor.execute_fast_path(rule, wm)

      # Should only match the 3-tuple fact
      assert length(results) == 1
      assert {:matched, "jane"} in results
    end

    test "handles wildcard patterns" do
      {:ok, wm} = WorkingMemory.start_link()

      WorkingMemory.assert_fact(wm, {:event, "evt1", %{type: "click", data: "button1"}})
      WorkingMemory.assert_fact(wm, {:event, "evt2", %{type: "hover", data: "menu"}})

      rule = %{
        id: :any_event_rule,
        conditions: [
          {:event, :_, :data}  # Wildcard for second element
        ],
        action: fn bindings -> [{:event_data, bindings[:data][:data]}] end
      }

      {:ok, results} = FastPathExecutor.execute_fast_path(rule, wm)

      assert length(results) == 2
      assert {:event_data, "button1"} in results
      assert {:event_data, "menu"} in results
    end

    test "handles action that returns empty list" do
      {:ok, wm} = WorkingMemory.start_link()

      WorkingMemory.assert_fact(wm, {:person, "john", %{age: 25}})

      rule = %{
        id: :empty_action_rule,
        conditions: [
          {:person, :name, :age}
        ],
        action: fn _bindings -> [] end  # Returns empty list
      }

      {:ok, results} = FastPathExecutor.execute_fast_path(rule, wm)
      assert results == []
    end

    test "handles action that returns multiple results" do
      {:ok, wm} = WorkingMemory.start_link()

      WorkingMemory.assert_fact(wm, {:person, "john", %{age: 25}})

      rule = %{
        id: :multi_result_rule,
        conditions: [
          {:person, :name, :age}
        ],
        action: fn bindings -> 
          [
            {:adult, bindings[:name]},
            {:person_processed, bindings[:name]},
            {:age_category, "young_adult"}
          ]
        end
      }

      {:ok, results} = FastPathExecutor.execute_fast_path(rule, wm)
      assert length(results) == 3
      assert {:adult, "john"} in results
      assert {:person_processed, "john"} in results
      assert {:age_category, "young_adult"} in results
    end
  end
end
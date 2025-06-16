defmodule Presto.Optimization.CompileTimeOptimizerTest do
  use ExUnit.Case, async: true

  alias Presto.Optimization.CompileTimeOptimizer

  describe "pattern matcher generation" do
    test "generates optimized matcher for constant patterns" do
      pattern = {:person, "John", 25}
      compiled_pattern = CompileTimeOptimizer.generate_pattern_matcher(pattern)

      assert is_binary(compiled_pattern.id)
      assert compiled_pattern.original_pattern == pattern
      assert is_function(compiled_pattern.compiled_matcher)
      # Very selective
      assert compiled_pattern.selectivity_estimate < 0.1

      # Test the compiled matcher
      matcher = compiled_pattern.compiled_matcher
      assert matcher.(pattern) == true
      assert matcher.({:person, "Jane", 25}) == false
    end

    test "generates matcher for selective patterns" do
      # Variable name, constant age
      pattern = {:person, :name, 25}
      compiled_pattern = CompileTimeOptimizer.generate_pattern_matcher(pattern)

      # Moderately selective
      assert compiled_pattern.selectivity_estimate < 0.5

      # Test the compiled matcher
      matcher = compiled_pattern.compiled_matcher
      assert matcher.({:person, "John", 25}) == true
      assert matcher.({:person, "Jane", 25}) == true
      assert matcher.({:person, "Bob", 30}) == false
    end

    test "generates matcher for wildcard patterns" do
      # All variables
      pattern = {:person, :name, :age}
      compiled_pattern = CompileTimeOptimizer.generate_pattern_matcher(pattern)

      # Not selective
      assert compiled_pattern.selectivity_estimate == 1.0

      # Test the compiled matcher
      matcher = compiled_pattern.compiled_matcher
      assert matcher.({:person, "John", 25}) == true
      assert matcher.({:person, "Jane", 30}) == true
    end
  end

  describe "guard optimization" do
    test "optimizes and reorders guard conditions" do
      guards = [
        # More expensive range check
        {:age, :>, 65},
        # Cheaper equality check
        {:name, :==, "John"},
        # Less selective
        {:status, :!=, :inactive}
      ]

      optimized_guards = CompileTimeOptimizer.optimize_guards(guards)

      # Should be reordered by cost and selectivity
      assert length(optimized_guards) == 3

      # First guard should be the cheapest and most selective (equality)
      first_guard = hd(optimized_guards)
      assert first_guard.original_condition == {:name, :==, "John"}

      # Test compiled evaluator
      bindings = %{name: "John", age: 70, status: :active}
      assert first_guard.compiled_evaluator.(bindings) == true

      bindings_false = %{name: "Jane", age: 70, status: :active}
      assert first_guard.compiled_evaluator.(bindings_false) == false
    end

    test "compiles different guard operators correctly" do
      guards = [
        {:age, :>, 18},
        {:score, :<=, 100},
        {:name, :!=, "admin"}
      ]

      optimized_guards = CompileTimeOptimizer.optimize_guards(guards)

      # Test each compiled evaluator
      bindings = %{age: 25, score: 85, name: "user"}

      Enum.each(optimized_guards, fn guard ->
        assert is_function(guard.compiled_evaluator)

        case guard.original_condition do
          {:age, :>, 18} -> assert guard.compiled_evaluator.(bindings) == true
          {:score, :<=, 100} -> assert guard.compiled_evaluator.(bindings) == true
          {:name, :!=, "admin"} -> assert guard.compiled_evaluator.(bindings) == true
        end
      end)
    end
  end

  describe "rule compilation" do
    test "compiles simple rule with optimizations" do
      rule = %{
        id: :test_rule,
        conditions: [
          {:person, :name, :age},
          {:age, :>, 18}
        ],
        action: fn _bindings -> [:adult] end
      }

      config = %{
        enabled: true,
        pattern_cache_size: 100,
        guard_optimization: true,
        network_sharing: false,
        code_generation: true,
        dependency_analysis: false
      }

      [compiled_rule] = CompileTimeOptimizer.compile_rules([rule], config)

      assert compiled_rule.id == :test_rule
      assert compiled_rule.original_rule == rule
      assert is_map(compiled_rule.execution_plan)
      assert is_map(compiled_rule.estimated_performance)
    end

    test "falls back gracefully when optimizations disabled" do
      rule = %{
        id: :simple_rule,
        conditions: [{:fact, :value}],
        action: fn _bindings -> [:result] end
      }

      config = %{enabled: false}

      [compiled_rule] = CompileTimeOptimizer.compile_rules([rule], config)

      assert compiled_rule.id == :simple_rule
      assert compiled_rule.compiled_patterns == []
      assert compiled_rule.optimized_guards == []
    end
  end

  describe "network topology analysis" do
    test "identifies shared patterns across rules" do
      rules = [
        %{
          id: :rule1,
          conditions: [{:person, :name, :age}, {:age, :>, 18}],
          action: fn _ -> [:adult] end
        },
        %{
          id: :rule2,
          conditions: [{:person, :name, :age}, {:age, :>, 65}],
          action: fn _ -> [:senior] end
        }
      ]

      config = %{
        enabled: true,
        network_sharing: true,
        dependency_analysis: true
      }

      compiled_rules = CompileTimeOptimizer.compile_rules(rules, config)
      topology = CompileTimeOptimizer.analyze_network_topology(compiled_rules)

      assert is_map(topology)
      assert Map.has_key?(topology, :shared_alpha_nodes)
      assert Map.has_key?(topology, :shared_beta_nodes)
      assert Map.has_key?(topology, :memory_savings_estimate)
      assert Map.has_key?(topology, :performance_impact)
    end
  end

  describe "pattern analysis" do
    test "correctly assesses pattern complexity" do
      # Test constant pattern (no variables)
      constant_pattern = {"John", 25, "Engineer"}
      constant_analysis = CompileTimeOptimizer.analyze_pattern_structure(constant_pattern)
      assert constant_analysis.complexity == :constant
      assert constant_analysis.variable_count == 0

      # Test selective pattern (more constants than variables)
      # :person and :age are variables, others are constants
      selective_pattern = {:person, "John", 25, "Engineer", :age}
      selective_analysis = CompileTimeOptimizer.analyze_pattern_structure(selective_pattern)
      assert selective_analysis.complexity == :selective
      # :person and :age (2 < 5/2.5)
      assert selective_analysis.variable_count == 2

      # Test wildcard pattern (all variables)
      wildcard_pattern = {:person, :name, :age}
      wildcard_analysis = CompileTimeOptimizer.analyze_pattern_structure(wildcard_pattern)
      assert wildcard_analysis.complexity == :wildcard
      # All three are variables
      assert wildcard_analysis.variable_count == 3
    end

    test "extracts correct index keys" do
      pattern = {:person, "John", :age, "Engineer"}
      index_keys = CompileTimeOptimizer.extract_index_keys(pattern)

      # Should have index keys for constant positions (1, 3)
      assert :field_1 in index_keys
      assert :field_3 in index_keys
      assert length(index_keys) == 2
    end
  end

  describe "integration with rule engine" do
    test "compile-time optimized rules work with standard rule engine" do
      {:ok, engine} = Presto.start_engine()

      # Add a rule that should benefit from compile-time optimization
      rule = %{
        id: :adult_check,
        conditions: [
          {:person, :name, :age},
          {:age, :>, 18}
        ],
        action: fn facts -> [{:adult, facts[:name]}] end
      }

      :ok = Presto.add_rule(engine, rule)

      # Assert facts
      :ok = Presto.assert_fact(engine, {:person, "John", 25})
      :ok = Presto.assert_fact(engine, {:person, "Jane", 16})

      # Fire rules
      results = Presto.fire_rules(engine)

      # Should get result for John (25 > 18) but not Jane (16 <= 18)
      assert {:adult, "John"} in results
      refute {:adult, "Jane"} in results

      Presto.stop_engine(engine)
    end
  end
end

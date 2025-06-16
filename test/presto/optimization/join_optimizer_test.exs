defmodule Presto.Optimization.JoinOptimizerTest do
  use ExUnit.Case, async: true

  alias Presto.Optimization.JoinOptimizer

  describe "selectivity analysis" do
    test "analyzes node selectivity correctly" do
      node = %{
        id: "test_node_1",
        join_keys: [:employee_id],
        left_type: :alpha,
        right_type: :alpha
      }

      input_data = [
        %{employee_id: 1, hours: 8},
        %{employee_id: 2, hours: 7},
        %{employee_id: 3, hours: 9}
      ]

      stats = JoinOptimizer.analyze_node_selectivity(node, input_data)

      assert stats.pattern_id == "test_node_1"
      assert stats.input_size == 3
      assert stats.selectivity > 0.0
      assert stats.estimated_cost > 0.0
      assert is_integer(stats.last_updated)
    end

    test "handles empty input data" do
      node = %{
        id: "empty_node",
        join_keys: [:id],
        left_type: :alpha,
        right_type: :beta
      }

      stats = JoinOptimizer.analyze_node_selectivity(node, [])

      assert stats.input_size == 0
      assert stats.selectivity == 1.0
    end
  end

  describe "join order optimization" do
    test "optimizes join order when beneficial" do
      nodes = [
        %{
          id: "node_1",
          # More selective (2 keys)
          join_keys: [:employee_id, :date],
          left_type: :alpha,
          right_type: :alpha
        },
        %{
          id: "node_2",
          # Less selective (1 key)
          join_keys: [:employee_id],
          left_type: :beta,
          right_type: :alpha
        }
      ]

      config = %{
        enable_join_reordering: true,
        cost_threshold: 1
      }

      plan = JoinOptimizer.optimize_join_order(nodes, config)

      assert plan.optimization_applied == true
      assert length(plan.ordered_nodes) == 2
      assert is_float(plan.estimated_total_cost)
      assert length(plan.selectivity_order) == 2
    end

    test "skips optimization when disabled" do
      nodes = [
        %{id: "node_1", join_keys: [:id], left_type: :alpha, right_type: :alpha}
      ]

      config = %{enable_join_reordering: false}

      plan = JoinOptimizer.optimize_join_order(nodes, config)

      assert plan.optimization_applied == false
      assert plan.ordered_nodes == ["node_1"]
    end

    test "skips optimization when cost threshold not met" do
      nodes = [
        %{id: "simple_node", join_keys: [:id], left_type: :alpha, right_type: :alpha}
      ]

      config = %{
        enable_join_reordering: true,
        # High threshold
        cost_threshold: 1000
      }

      plan = JoinOptimizer.optimize_join_order(nodes, config)

      assert plan.optimization_applied == false
    end
  end

  describe "selectivity learning" do
    test "updates selectivity statistics" do
      node_id = "learning_node"
      input_data = [%{id: 1}, %{id: 2}, %{id: 3}]
      # Only one match
      output_data = [%{id: 1}]

      # Should not raise an error
      assert :ok = JoinOptimizer.update_selectivity_statistics(node_id, input_data, output_data)
    end
  end

  describe "optimization statistics" do
    test "returns optimization statistics" do
      stats = JoinOptimizer.get_optimization_statistics()

      assert is_integer(stats.total_optimizations)
      assert is_float(stats.average_improvement)
      assert is_float(stats.best_case_improvement)
      assert is_float(stats.worst_case_improvement)
    end
  end
end

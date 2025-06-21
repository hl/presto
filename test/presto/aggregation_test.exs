defmodule Presto.AggregationTest do
  use ExUnit.Case, async: true
  alias Presto.Rule

  describe "RETE aggregation functionality" do
    test "sum aggregation groups and sums values correctly" do
      engine = start_supervised!(Presto.RuleEngine)

      # Create a sum aggregation rule for hours by employee
      rule =
        Rule.aggregation(
          :weekly_hours,
          [{:timesheet, :id, :employee_id, :hours}],
          [:employee_id],
          :sum,
          :hours
        )

      :ok = Presto.add_rule(engine, rule)

      # Add timesheet facts
      Presto.assert_fact(engine, {:timesheet, "t1", "emp1", 8})
      Presto.assert_fact(engine, {:timesheet, "t2", "emp1", 6})
      Presto.assert_fact(engine, {:timesheet, "t3", "emp2", 9})
      Presto.assert_fact(engine, {:timesheet, "t4", "emp2", 7})
      Presto.assert_fact(engine, {:timesheet, "t5", "emp1", 4})

      # Fire rules and check aggregation results
      results = Presto.fire_rules(engine)

      # Should have two aggregate results
      assert length(results) == 2

      # Check emp1 total (8 + 6 + 4 = 18)
      assert {:aggregate_result, {"emp1"}, 18} in results

      # Check emp2 total (9 + 7 = 16)
      assert {:aggregate_result, {"emp2"}, 16} in results
    end

    test "count aggregation counts facts by group" do
      engine = start_supervised!(Presto.RuleEngine)

      # Count shifts by department
      rule =
        Rule.aggregation(
          :dept_shift_count,
          [{:shift, :id, :department}],
          [:department],
          :count,
          nil
        )

      :ok = Presto.add_rule(engine, rule)

      # Add shift facts
      Presto.assert_fact(engine, {:shift, "s1", "kitchen"})
      Presto.assert_fact(engine, {:shift, "s2", "kitchen"})
      Presto.assert_fact(engine, {:shift, "s3", "bar"})
      Presto.assert_fact(engine, {:shift, "s4", "kitchen"})
      Presto.assert_fact(engine, {:shift, "s5", "bar"})

      results = Presto.fire_rules(engine)

      assert length(results) == 2
      assert {:aggregate_result, {"kitchen"}, 3} in results
      assert {:aggregate_result, {"bar"}, 2} in results
    end

    test "avg aggregation calculates average correctly" do
      engine = start_supervised!(Presto.RuleEngine)

      rule =
        Rule.aggregation(
          :avg_score,
          [{:score, :student, :value}],
          [:student],
          :avg,
          :value
        )

      :ok = Presto.add_rule(engine, rule)

      # Add scores
      Presto.assert_fact(engine, {:score, "alice", 90})
      Presto.assert_fact(engine, {:score, "alice", 80})
      Presto.assert_fact(engine, {:score, "alice", 85})
      Presto.assert_fact(engine, {:score, "bob", 75})
      Presto.assert_fact(engine, {:score, "bob", 95})

      results = Presto.fire_rules(engine)

      assert length(results) == 2
      assert {:aggregate_result, {"alice"}, 85.0} in results
      assert {:aggregate_result, {"bob"}, 85.0} in results
    end

    test "min/max aggregations find extremes" do
      engine = start_supervised!(Presto.RuleEngine)

      min_rule =
        Rule.aggregation(
          :min_price,
          [{:product, :category, :price}],
          [:category],
          :min,
          :price
        )

      max_rule =
        Rule.aggregation(
          :max_price,
          [{:product, :category, :price}],
          [:category],
          :max,
          :price
        )

      :ok = Presto.add_rule(engine, min_rule)
      :ok = Presto.add_rule(engine, max_rule)

      # Add products
      Presto.assert_fact(engine, {:product, "electronics", 999})
      Presto.assert_fact(engine, {:product, "electronics", 199})
      Presto.assert_fact(engine, {:product, "electronics", 599})
      Presto.assert_fact(engine, {:product, "books", 15})
      Presto.assert_fact(engine, {:product, "books", 25})

      results = Presto.fire_rules(engine)

      # Should have 4 results (2 categories x 2 aggregations)
      assert length(results) == 4

      # Check min values
      assert {:aggregate_result, {"electronics"}, 199} in results
      assert {:aggregate_result, {"books"}, 15} in results

      # Check max values
      assert {:aggregate_result, {"electronics"}, 999} in results
      assert {:aggregate_result, {"books"}, 25} in results
    end

    test "custom output pattern for aggregation results" do
      engine = start_supervised!(Presto.RuleEngine)

      # Use custom output pattern
      rule =
        Rule.aggregation(
          :dept_totals,
          [{:sale, :dept, :amount}],
          [:dept],
          :sum,
          :amount,
          output: {:dept_total, :dept, :value}
        )

      :ok = Presto.add_rule(engine, rule)

      Presto.assert_fact(engine, {:sale, "toys", 50})
      Presto.assert_fact(engine, {:sale, "toys", 30})

      results = Presto.fire_rules(engine)

      # Should use custom output pattern
      assert [{:dept_total, "toys", 80}] = results
    end

    test "multi-field grouping" do
      engine = start_supervised!(Presto.RuleEngine)

      # Group by multiple fields
      rule =
        Rule.aggregation(
          :region_product_sales,
          [{:sale, :region, :product, :amount}],
          [:region, :product],
          :sum,
          :amount
        )

      :ok = Presto.add_rule(engine, rule)

      # Add sales facts
      Presto.assert_fact(engine, {:sale, "north", "widget", 100})
      Presto.assert_fact(engine, {:sale, "north", "widget", 150})
      Presto.assert_fact(engine, {:sale, "north", "gadget", 200})
      Presto.assert_fact(engine, {:sale, "south", "widget", 120})

      results = Presto.fire_rules(engine)

      assert length(results) == 3
      assert {:aggregate_result, {"north", "widget"}, 250} in results
      assert {:aggregate_result, {"north", "gadget"}, 200} in results
      assert {:aggregate_result, {"south", "widget"}, 120} in results
    end

    test "incremental aggregation updates on fact retraction" do
      engine = start_supervised!(Presto.RuleEngine)

      rule =
        Rule.aggregation(
          :total_sales,
          [{:sale, :id, :amount}],
          # No grouping - total aggregate
          [],
          :sum,
          :amount
        )

      :ok = Presto.add_rule(engine, rule)

      # Add sales
      Presto.assert_fact(engine, {:sale, "s1", 100})
      Presto.assert_fact(engine, {:sale, "s2", 200})

      results1 = Presto.fire_rules(engine)
      assert [{:aggregate_result, {}, 300}] = results1

      # Retract one sale
      Presto.retract_fact(engine, {:sale, "s1", 100})

      results2 = Presto.fire_rules(engine)
      assert [{:aggregate_result, {}, 200}] = results2
    end
  end
end

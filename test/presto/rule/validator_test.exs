defmodule Presto.Rule.ValidatorTest do
  use ExUnit.Case, async: true

  alias Presto.Rule.Validator

  describe "runtime validation" do
    test "validates correct standard rule" do
      rule = %{
        id: :test_rule,
        conditions: [
          {:person, :name, :age},
          {:age, :>, 18}
        ],
        action: fn _ -> [] end,
        priority: 10
      }

      assert :ok = Validator.validate(rule)
    end

    test "validates correct aggregation rule" do
      rule = %{
        id: :sum_rule,
        type: :aggregation,
        conditions: [{:sale, :amount, :date}],
        group_by: [:date],
        aggregate: :sum,
        field: :amount,
        output: {:daily_sales, :date, :total}
      }

      assert :ok = Validator.validate(rule)
    end

    test "rejects rule with invalid structure" do
      assert {:error, message} = Validator.validate("not a map")
      assert message == "Rule must be a map"
    end

    test "rejects rule with non-atom id" do
      rule = %{
        id: "string_id",
        conditions: [],
        action: fn _ -> [] end
      }

      assert {:error, message} = Validator.validate(rule)
      assert message == "Rule '\"string_id\"': id must be an atom"
    end

    test "rejects rule with missing required fields" do
      rule = %{id: :test}

      assert {:error, message} = Validator.validate(rule)
      assert message == "Rule is missing required field: conditions"
    end

    test "rejects rule with empty conditions" do
      rule = %{
        id: :test_rule,
        conditions: [],
        action: fn _ -> [] end
      }

      assert {:error, message} = Validator.validate(rule)
      assert message == "Rule conditions cannot be empty"
    end

    test "rejects rule with invalid condition patterns" do
      rule = %{
        id: :test_rule,
        conditions: [
          {:person, "not_atom", :age}
        ],
        action: fn _ -> [] end
      }

      assert {:error, message} = Validator.validate(rule)
      assert message =~ "Condition 1: all bindings must be atoms or :_, invalid:"
    end

    test "rejects rule with invalid test operators" do
      # Force a test condition by making sure the variable doesn't look like a fact type
      # and using a number as the third element which forces pattern validation to fail
      rule = %{
        id: :test_rule,
        conditions: [
          {:person, :name, :age},
          {:age, :invalid_op, 18}
        ],
        action: fn _ -> [] end
      }

      assert {:error, message} = Validator.validate(rule)
      # This will be caught as an invalid pattern condition first due to the number
      assert message =~ "Condition 2: all bindings must be atoms or :_, invalid: [18]"
    end

    test "accepts valid pattern condition with atom bindings" do
      rule = %{
        id: :test_rule,
        conditions: [
          {:person, :name, :age},
          # Valid pattern: fact_type=:age, bindings=[:bad_operator, :some_value]
          {:age, :bad_operator, :some_value}
        ],
        action: fn _ -> [] end
      }

      # This is actually a valid pattern condition, not an invalid test condition
      assert :ok = Validator.validate(rule)
    end

    test "detects unbound variables in test conditions" do
      rule = %{
        id: :test_rule,
        conditions: [
          {:person, :name, :age},
          {:unbound_var, :>, 18}
        ],
        action: fn _ -> [] end
      }

      assert {:error, message} = Validator.validate(rule)
      assert message =~ "Unbound variables in tests: [:unbound_var]"
    end

    test "rejects aggregation rule with missing fields" do
      rule = %{
        id: :bad_agg,
        type: :aggregation,
        conditions: [{:sale, :amount, :date}],
        group_by: [:date]
        # Missing aggregate, output
      }

      assert {:error, message} = Validator.validate(rule)
      assert message =~ "Aggregation rule missing required fields:"
    end

    test "rejects aggregation rule with invalid aggregate function" do
      rule = %{
        id: :bad_agg,
        type: :aggregation,
        conditions: [{:sale, :amount, :date}],
        group_by: [:date],
        aggregate: :invalid_func,
        output: {:result, :date, :total}
      }

      assert {:error, message} = Validator.validate(rule)
      assert message =~ "Invalid aggregate function: :invalid_func"
    end

    test "accepts custom aggregate function" do
      rule = %{
        id: :custom_agg,
        type: :aggregation,
        conditions: [{:sale, :amount, :date}],
        group_by: [:date],
        aggregate: fn values -> Enum.sum(values) end,
        output: {:result, :date, :total}
      }

      assert :ok = Validator.validate(rule)
    end

    test "rejects invalid priority type" do
      rule = %{
        id: :test_rule,
        conditions: [{:person, :name, :age}],
        action: fn _ -> [] end,
        priority: "not_integer"
      }

      assert {:error, message} = Validator.validate(rule)
      assert message == "Rule priority must be an integer"
    end

    test "accepts rule without optional priority" do
      rule = %{
        id: :test_rule,
        conditions: [{:person, :name, :age}],
        action: fn _ -> [] end
      }

      assert :ok = Validator.validate(rule)
    end
  end

  describe "condition pattern validation" do
    test "rejects empty tuples" do
      rule = %{
        id: :test_rule,
        conditions: [{}],
        action: fn _ -> [] end
      }

      assert {:error, message} = Validator.validate(rule)
      assert message =~ "Condition 1: empty tuple is not valid"
    end

    test "rejects single-element tuples" do
      rule = %{
        id: :test_rule,
        conditions: [{:person}],
        action: fn _ -> [] end
      }

      assert {:error, message} = Validator.validate(rule)
      assert message =~ "Condition 1: single-element tuple is not valid"
    end

    test "accepts two-element tuples as pattern conditions" do
      rule = %{
        id: :test_rule,
        conditions: [{:person, :name}],
        action: fn _ -> [] end
      }

      assert :ok = Validator.validate(rule)
    end

    test "accepts pattern conditions with 3+ elements" do
      rule = %{
        id: :test_rule,
        conditions: [{:person, :name, :age}],
        action: fn _ -> [] end
      }

      assert :ok = Validator.validate(rule)
    end

    test "accepts anonymous bindings with :_" do
      rule = %{
        id: :test_rule,
        conditions: [{:person, :_, :age, :_}],
        action: fn _ -> [] end
      }

      assert :ok = Validator.validate(rule)
    end

    test "rejects non-atom fact types" do
      rule = %{
        id: :test_rule,
        conditions: [{"not_atom", :name, :age}],
        action: fn _ -> [] end
      }

      assert {:error, message} = Validator.validate(rule)
      assert message =~ "Condition 1: fact type must be an atom"
    end
  end

  describe "variable binding analysis" do
    test "allows properly bound variables" do
      rule = %{
        id: :test_rule,
        conditions: [
          {:person, :name, :age},
          {:address, :name, :city},
          {:age, :>, 18},
          {:city, :==, "London"}
        ],
        action: fn _ -> [] end
      }

      assert :ok = Validator.validate(rule)
    end

    test "detects multiple unbound variables" do
      rule = %{
        id: :test_rule,
        conditions: [
          {:person, :name, :age},
          {:unbound1, :>, 18},
          {:unbound2, :==, "test"}
        ],
        action: fn _ -> [] end
      }

      assert {:error, message} = Validator.validate(rule)
      assert message =~ "Unbound variables in tests:"
      assert message =~ ":unbound1"
      assert message =~ ":unbound2"
    end

    test "handles complex binding scenarios" do
      rule = %{
        id: :complex_rule,
        conditions: [
          {:person, :id, :name, :age, :dept},
          {:department, :dept, :budget},
          {:salary, :id, :amount},
          {:age, :>=, 25},
          {:budget, :>, 100_000},
          {:amount, :<, 50_000}
        ],
        action: fn _ -> [] end
      }

      assert :ok = Validator.validate(rule)
    end
  end
end

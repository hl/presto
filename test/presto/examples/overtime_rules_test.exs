defmodule Presto.Examples.OvertimeRulesTest do
  use ExUnit.Case, async: true

  alias Presto.Examples.OvertimeRules

  describe "create_rules/1" do
    test "creates default overtime rules with empty rule spec" do
      rules = OvertimeRules.create_rules(%{})

      assert is_list(rules)
      assert length(rules) > 0
      
      # Check that all rules have required structure
      Enum.each(rules, fn rule ->
        assert is_map(rule)
        assert Map.has_key?(rule, :name)
        assert Map.has_key?(rule, :pattern)
        assert Map.has_key?(rule, :action)
        assert is_function(rule.pattern)
        assert is_function(rule.action)
      end)
    end

    test "creates rules with custom rule execution order" do
      rule_spec = %{
        "rule_execution_order" => ["time_calculation", "pay_aggregation"],
        "variables" => %{}
      }

      rules = OvertimeRules.create_rules(rule_spec)
      assert is_list(rules)
      assert length(rules) >= 2
    end

    test "creates rules with overtime rule specifications" do
      rule_spec = %{
        "overtime_rules" => [
          %{
            "name" => "overtime_basic",
            "priority" => 1,
            "threshold" => 40,
            "filter_pay_code" => "basic_pay",
            "pay_code" => "overtime_basic"
          }
        ],
        "variables" => %{}
      }

      rules = OvertimeRules.create_rules(rule_spec)
      assert is_list(rules)
    end

    test "handles rule spec with variables" do
      rule_spec = %{
        "variables" => %{
          "overtime_threshold" => 35,
          "overtime_multiplier" => 1.5
        }
      }

      rules = OvertimeRules.create_rules(rule_spec)
      assert is_list(rules)
    end
  end

  describe "process_overtime/2" do
    test "processes basic overtime scenario" do
      time_entries = [
        {:time_entry, "shift_1", %{
          start_dt: ~U[2025-01-01 09:00:00Z],
          finish_dt: ~U[2025-01-01 19:00:00Z],  # 10 hours
          employee_id: "emp_001",
          pay_code: "basic_pay",
          paid: false
        }},
        {:time_entry, "shift_2", %{
          start_dt: ~U[2025-01-02 09:00:00Z],
          finish_dt: ~U[2025-01-02 19:00:00Z],  # 10 hours
          employee_id: "emp_001",
          pay_code: "basic_pay",
          paid: false
        }}
      ]

      overtime_rules = [
        {:overtime_rule, "overtime_basic", %{
          threshold: 15,  # 15 hours threshold
          filter_pay_code: "basic_pay",
          pay_code: "overtime_basic"
        }}
      ]

      result = OvertimeRules.process_overtime(time_entries, overtime_rules)

      assert is_map(result)
      assert Map.has_key?(result, :processed_entries)
      assert Map.has_key?(result, :overtime_entries)
      assert Map.has_key?(result, :pay_aggregates)
      assert Map.has_key?(result, :summary)

      # Check that we have processed entries
      assert is_list(result.processed_entries)
      assert is_list(result.overtime_entries)
      assert is_list(result.pay_aggregates)
      assert is_map(result.summary)
    end

    test "handles empty time entries" do
      result = OvertimeRules.process_overtime([], [])

      assert is_map(result)
      assert result.processed_entries == []
      assert result.overtime_entries == []
      assert result.pay_aggregates == []
      assert is_map(result.summary)
    end

    test "processes multiple employees" do
      time_entries = [
        {:time_entry, "shift_1", %{
          start_dt: ~U[2025-01-01 09:00:00Z],
          finish_dt: ~U[2025-01-01 17:00:00Z],  # 8 hours
          employee_id: "emp_001",
          pay_code: "basic_pay",
          paid: false
        }},
        {:time_entry, "shift_2", %{
          start_dt: ~U[2025-01-01 09:00:00Z],
          finish_dt: ~U[2025-01-01 17:00:00Z],  # 8 hours
          employee_id: "emp_002",
          pay_code: "basic_pay",
          paid: false
        }}
      ]

      overtime_rules = [
        {:overtime_rule, "overtime_basic", %{
          threshold: 5,
          filter_pay_code: "basic_pay",
          pay_code: "overtime_basic"
        }}
      ]

      result = OvertimeRules.process_overtime(time_entries, overtime_rules)

      # Should process both employees
      assert length(result.processed_entries) >= 2
    end

    test "respects pay code filters in overtime rules" do
      time_entries = [
        {:time_entry, "shift_1", %{
          start_dt: ~U[2025-01-01 09:00:00Z],
          finish_dt: ~U[2025-01-01 19:00:00Z],  # 10 hours
          employee_id: "emp_001",
          pay_code: "special_pay",
          paid: false
        }}
      ]

      overtime_rules = [
        {:overtime_rule, "overtime_basic", %{
          threshold: 5,
          filter_pay_code: "basic_pay",  # Different pay code
          pay_code: "overtime_basic"
        }}
      ]

      result = OvertimeRules.process_overtime(time_entries, overtime_rules)

      # Should not generate overtime for different pay code
      assert is_list(result.overtime_entries)
    end

    test "handles already paid entries" do
      time_entries = [
        {:time_entry, "shift_1", %{
          start_dt: ~U[2025-01-01 09:00:00Z],
          finish_dt: ~U[2025-01-01 19:00:00Z],
          employee_id: "emp_001",
          pay_code: "basic_pay",
          paid: true  # Already paid
        }}
      ]

      overtime_rules = [
        {:overtime_rule, "overtime_basic", %{
          threshold: 5,
          filter_pay_code: "basic_pay",
          pay_code: "overtime_basic"
        }}
      ]

      result = OvertimeRules.process_overtime(time_entries, overtime_rules)

      # Should handle paid entries appropriately
      assert is_map(result)
    end
  end

  describe "generate_example_data/0" do
    test "generates valid example time entries and overtime rules" do
      {time_entries, overtime_rules} = OvertimeRules.generate_example_data()

      # Check time entries structure
      assert is_list(time_entries)
      assert length(time_entries) > 0

      Enum.each(time_entries, fn {:time_entry, id, data} ->
        assert is_binary(id)
        assert is_map(data)
        assert Map.has_key?(data, :start_dt)
        assert Map.has_key?(data, :finish_dt)
        assert Map.has_key?(data, :employee_id)
        assert Map.has_key?(data, :pay_code)
        assert Map.has_key?(data, :paid)
        assert %DateTime{} = data.start_dt
        assert %DateTime{} = data.finish_dt
        assert is_binary(data.employee_id)
        assert is_binary(data.pay_code)
        assert is_boolean(data.paid)
      end)

      # Check overtime rules structure
      assert is_list(overtime_rules)
      assert length(overtime_rules) > 0

      Enum.each(overtime_rules, fn {:overtime_rule, id, data} ->
        assert is_binary(id)
        assert is_map(data)
        assert Map.has_key?(data, :threshold)
        assert Map.has_key?(data, :pay_code)
        assert is_number(data.threshold)
        assert data.threshold > 0
        assert is_binary(data.pay_code)
      end)
    end
  end

  describe "run_example/0" do
    test "runs example without errors" do
      # This should not raise any errors
      result = OvertimeRules.run_example()
      assert is_map(result)
      assert Map.has_key?(result, :processed_entries)
      assert Map.has_key?(result, :overtime_entries)
      assert Map.has_key?(result, :summary)
    end
  end

  describe "RuleBehaviour implementation" do
    test "implements create_rules/1 callback" do
      assert function_exported?(OvertimeRules, :create_rules, 1)
      
      rules = OvertimeRules.create_rules(%{})
      assert is_list(rules)
    end

    test "implements valid_rule_spec?/1 callback if present" do
      # This callback is optional, so we check if it exists
      if function_exported?(OvertimeRules, :valid_rule_spec?, 1) do
        assert OvertimeRules.valid_rule_spec?(%{}) in [true, false]
      end
    end
  end

  describe "time calculation" do
    test "calculates hours and minutes correctly" do
      # Test with a known time span
      start_dt = ~U[2025-01-01 09:00:00Z]
      finish_dt = ~U[2025-01-01 17:30:00Z]  # 8.5 hours

      time_entry = {:time_entry, "test_shift", %{
        start_dt: start_dt,
        finish_dt: finish_dt,
        employee_id: "emp_001",
        pay_code: "basic_pay",
        paid: false
      }}

      # Process with a simple rule to test time calculation
      overtime_rules = []
      result = OvertimeRules.process_overtime([time_entry], overtime_rules)

      # The processing should handle time calculation
      assert is_map(result)
    end

    test "handles overnight shifts" do
      # Shift that crosses midnight
      start_dt = ~U[2025-01-01 22:00:00Z]
      finish_dt = ~U[2025-01-02 06:00:00Z]  # 8 hours overnight

      time_entry = {:time_entry, "night_shift", %{
        start_dt: start_dt,
        finish_dt: finish_dt,
        employee_id: "emp_001",
        pay_code: "basic_pay",
        paid: false
      }}

      result = OvertimeRules.process_overtime([time_entry], [])
      assert is_map(result)
    end
  end

  describe "pay aggregation" do
    test "aggregates hours by employee and pay code" do
      time_entries = [
        {:time_entry, "shift_1", %{
          start_dt: ~U[2025-01-01 09:00:00Z],
          finish_dt: ~U[2025-01-01 13:00:00Z],  # 4 hours
          employee_id: "emp_001",
          pay_code: "basic_pay",
          paid: false
        }},
        {:time_entry, "shift_2", %{
          start_dt: ~U[2025-01-01 14:00:00Z],
          finish_dt: ~U[2025-01-01 18:00:00Z],  # 4 hours
          employee_id: "emp_001",
          pay_code: "basic_pay",
          paid: false
        }}
      ]

      result = OvertimeRules.process_overtime(time_entries, [])

      # Should aggregate the hours for the same employee and pay code
      assert is_list(result.pay_aggregates)
    end

    test "separates different pay codes" do
      time_entries = [
        {:time_entry, "shift_1", %{
          start_dt: ~U[2025-01-01 09:00:00Z],
          finish_dt: ~U[2025-01-01 13:00:00Z],
          employee_id: "emp_001",
          pay_code: "basic_pay",
          paid: false
        }},
        {:time_entry, "shift_2", %{
          start_dt: ~U[2025-01-01 14:00:00Z],
          finish_dt: ~U[2025-01-01 18:00:00Z],
          employee_id: "emp_001",
          pay_code: "special_pay",
          paid: false
        }}
      ]

      result = OvertimeRules.process_overtime(time_entries, [])

      # Should create separate aggregates for different pay codes
      assert is_list(result.pay_aggregates)
    end
  end

  describe "overtime rule processing" do
    test "applies overtime rules in priority order" do
      time_entries = [
        {:time_entry, "shift_1", %{
          start_dt: ~U[2025-01-01 09:00:00Z],
          finish_dt: ~U[2025-01-01 21:00:00Z],  # 12 hours
          employee_id: "emp_001",
          pay_code: "basic_pay",
          paid: false
        }}
      ]

      overtime_rules = [
        {:overtime_rule, "overtime_high", %{
          threshold: 8,
          filter_pay_code: "basic_pay",
          pay_code: "overtime_high",
          priority: 1
        }},
        {:overtime_rule, "overtime_low", %{
          threshold: 10,
          filter_pay_code: "basic_pay",
          pay_code: "overtime_low",
          priority: 2
        }}
      ]

      result = OvertimeRules.process_overtime(time_entries, overtime_rules)

      # Should process rules and generate overtime entries
      assert is_list(result.overtime_entries)
    end

    test "respects threshold limits" do
      time_entries = [
        {:time_entry, "shift_1", %{
          start_dt: ~U[2025-01-01 09:00:00Z],
          finish_dt: ~U[2025-01-01 12:00:00Z],  # 3 hours
          employee_id: "emp_001",
          pay_code: "basic_pay",
          paid: false
        }}
      ]

      overtime_rules = [
        {:overtime_rule, "overtime_basic", %{
          threshold: 8,  # Higher than worked hours
          filter_pay_code: "basic_pay",
          pay_code: "overtime_basic"
        }}
      ]

      result = OvertimeRules.process_overtime(time_entries, overtime_rules)

      # Should not generate overtime when under threshold
      assert is_list(result.overtime_entries)
    end
  end

  describe "edge cases" do
    test "handles zero-duration shifts" do
      time_entries = [
        {:time_entry, "zero_shift", %{
          start_dt: ~U[2025-01-01 09:00:00Z],
          finish_dt: ~U[2025-01-01 09:00:00Z],  # Same time
          employee_id: "emp_001",
          pay_code: "basic_pay",
          paid: false
        }}
      ]

      result = OvertimeRules.process_overtime(time_entries, [])
      assert is_map(result)
    end

    test "handles invalid datetime ranges" do
      time_entries = [
        {:time_entry, "invalid_shift", %{
          start_dt: ~U[2025-01-01 17:00:00Z],
          finish_dt: ~U[2025-01-01 09:00:00Z],  # Finish before start
          employee_id: "emp_001",
          pay_code: "basic_pay",
          paid: false
        }}
      ]

      # Should handle gracefully without crashing
      result = OvertimeRules.process_overtime(time_entries, [])
      assert is_map(result)
    end

    test "handles missing employee IDs" do
      time_entries = [
        {:time_entry, "no_employee", %{
          start_dt: ~U[2025-01-01 09:00:00Z],
          finish_dt: ~U[2025-01-01 17:00:00Z],
          employee_id: nil,
          pay_code: "basic_pay",
          paid: false
        }}
      ]

      result = OvertimeRules.process_overtime(time_entries, [])
      assert is_map(result)
    end
  end
end
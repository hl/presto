defmodule Presto.Examples.PayrollRulesTest do
  use ExUnit.Case, async: true

  alias Presto.Examples.PayrollRules
  alias Presto.Factories
  alias Presto.PayrollTestHelpers

  describe "time calculation rule" do
    test "calculates duration for basic work day" do
      start_dt = ~U[2024-01-01 09:00:00Z]
      finish_dt = ~U[2024-01-01 17:00:00Z]
      
      entry = Factories.build_time_entry("entry_1", start_dt, finish_dt, "emp_001")
      
      rule = PayrollRules.time_calculation_rule()
      matched_facts = rule.pattern.([entry])
      
      assert length(matched_facts) == 1
      assert entry in matched_facts
      
      [result] = rule.action.(matched_facts)
      
      assert {:time_entry, "entry_1", %{
        start_datetime: ^start_dt,
        finish_datetime: ^finish_dt,
        employee_id: "emp_001",
        minutes: 480.0,
        units: 8.0
      }} = result
    end

    test "calculates duration for partial hours" do
      start_dt = ~U[2024-01-01 09:00:00Z]
      finish_dt = ~U[2024-01-01 13:30:00Z] # 4.5 hours
      
      entry = Factories.build_time_entry("entry_1", start_dt, finish_dt, "emp_001")
      
      rule = PayrollRules.time_calculation_rule()
      [result] = rule.action.(rule.pattern.([entry]))
      
      assert {:time_entry, "entry_1", %{minutes: 270.0, units: 4.5}} = result
    end

    test "handles cross-day time entries" do
      start_dt = ~U[2024-01-01 23:00:00Z]
      finish_dt = ~U[2024-01-02 07:00:00Z] # 8 hours across days
      
      entry = Factories.build_time_entry("entry_1", start_dt, finish_dt, "emp_001")
      
      rule = PayrollRules.time_calculation_rule()
      [result] = rule.action.(rule.pattern.([entry]))
      
      assert {:time_entry, "entry_1", %{minutes: 480.0, units: 8.0}} = result
    end

    test "ignores entries that already have calculations" do
      processed_entry = Factories.build_processed_time_entry(
        "entry_1", 
        ~U[2024-01-01 09:00:00Z], 
        ~U[2024-01-01 17:00:00Z], 
        "emp_001"
      )
      
      rule = PayrollRules.time_calculation_rule()
      matched_facts = rule.pattern.([processed_entry])
      
      assert length(matched_facts) == 0
    end

    test "ignores entries without required datetime fields" do
      incomplete_entry = {:time_entry, "entry_1", %{
        employee_id: "emp_001",
        minutes: nil,
        units: nil
      }}
      
      rule = PayrollRules.time_calculation_rule()
      matched_facts = rule.pattern.([incomplete_entry])
      
      assert length(matched_facts) == 0
    end

    test "handles multiple entries in single rule execution" do
      monday = ~D[2024-01-01]
      entries = PayrollTestHelpers.create_standard_work_week("emp_001", monday)
      
      rule = PayrollRules.time_calculation_rule()
      matched_facts = rule.pattern.(entries)
      results = rule.action.(matched_facts)
      
      assert length(results) == 5
      
      # All should be calculated to 8 hours each
      Enum.each(results, fn {:time_entry, _, %{minutes: minutes, units: units}} ->
        assert minutes == 480.0
        assert units == 8.0
      end)
    end
  end

  describe "overtime calculation rule" do
    test "creates overtime entry when threshold exceeded" do
      monday = ~D[2024-01-01]
      processed_entries = PayrollTestHelpers.create_overtime_work_week("emp_001", monday, 10)
      |> Enum.map(fn entry ->
        {:time_entry, id, data} = entry
        minutes = DateTime.diff(data.finish_datetime, data.start_datetime, :second) / 60
        updated_data = data |> Map.put(:minutes, minutes) |> Map.put(:units, minutes / 60.0)
        {:time_entry, id, updated_data}
      end)
      
      variables = %{"overtime_threshold" => 40.0}
      rule = PayrollRules.overtime_calculation_rule(variables)
      
      overtime_candidates = rule.pattern.(processed_entries)
      assert length(overtime_candidates) == 1
      
      [{employee_id, total_units, week_start}] = overtime_candidates
      assert employee_id == "emp_001"
      assert total_units > 40.0
      assert week_start == monday
      
      [overtime_entry] = rule.action.(overtime_candidates)
      
      assert {:overtime_entry, {"emp_001", ^monday}, %{
        employee_id: "emp_001",
        units: overtime_units,
        week_start: ^monday,
        type: :overtime
      }} = overtime_entry
      
      assert overtime_units > 0.0
      assert overtime_units == total_units - 40.0
    end

    test "does not create overtime when under threshold" do
      monday = ~D[2024-01-01]
      processed_entries = PayrollTestHelpers.create_standard_work_week("emp_001", monday)
      |> Enum.map(fn entry ->
        {:time_entry, id, data} = entry
        minutes = DateTime.diff(data.finish_datetime, data.start_datetime, :second) / 60
        updated_data = data |> Map.put(:minutes, minutes) |> Map.put(:units, minutes / 60.0)
        {:time_entry, id, updated_data}
      end)
      
      variables = %{"overtime_threshold" => 40.0}
      rule = PayrollRules.overtime_calculation_rule(variables)
      
      overtime_candidates = rule.pattern.(processed_entries)
      assert length(overtime_candidates) == 0
    end

    test "handles custom overtime threshold" do
      monday = ~D[2024-01-01]
      processed_entries = PayrollTestHelpers.create_standard_work_week("emp_001", monday)
      |> Enum.map(fn entry ->
        {:time_entry, id, data} = entry
        minutes = DateTime.diff(data.finish_datetime, data.start_datetime, :second) / 60
        updated_data = data |> Map.put(:minutes, minutes) |> Map.put(:units, minutes / 60.0)
        {:time_entry, id, updated_data}
      end)
      
      # Lower threshold - should trigger overtime for 40-hour week
      variables = %{"overtime_threshold" => 35.0}
      rule = PayrollRules.overtime_calculation_rule(variables)
      
      overtime_candidates = rule.pattern.(processed_entries)
      assert length(overtime_candidates) == 1
      
      [{_, total_units, _}] = overtime_candidates
      assert total_units == 40.0
      
      [overtime_entry] = rule.action.(overtime_candidates)
      assert {:overtime_entry, _, %{units: 5.0}} = overtime_entry
    end

    test "processes multiple employees separately" do
      monday = ~D[2024-01-01]
      
      # Create mixed scenario with different overtime situations
      scenarios = PayrollTestHelpers.create_mixed_overtime_scenario(monday)
      
      all_entries = scenarios 
      |> Map.values() 
      |> List.flatten()
      |> Enum.map(fn entry ->
        {:time_entry, id, data} = entry
        minutes = DateTime.diff(data.finish_datetime, data.start_datetime, :second) / 60
        updated_data = data |> Map.put(:minutes, minutes) |> Map.put(:units, minutes / 60.0)
        {:time_entry, id, updated_data}
      end)
      
      variables = %{"overtime_threshold" => 40.0}
      rule = PayrollRules.overtime_calculation_rule(variables)
      
      overtime_candidates = rule.pattern.(all_entries)
      
      # Should have overtime for emp_002 and emp_003
      assert length(overtime_candidates) == 2
      
      overtime_entries = rule.action.(overtime_candidates)
      employee_ids = Enum.map(overtime_entries, fn {:overtime_entry, {emp_id, _}, _} -> emp_id end)
      
      assert "emp_002" in employee_ids
      assert "emp_003" in employee_ids
      refute "emp_001" in employee_ids # no overtime
      refute "emp_004" in employee_ids # part time, no overtime
    end
  end

  describe "process_time_entries/2" do
    test "processes complete payroll workflow" do
      monday = ~D[2024-01-01]
      time_entries = PayrollTestHelpers.create_overtime_work_week("emp_001", monday, 10)
      rule_spec = Factories.build_payroll_rule_spec(40.0)
      
      result = PayrollRules.process_time_entries(time_entries, rule_spec)
      
      assert %{
        processed_entries: processed_entries,
        overtime_entries: overtime_entries,
        summary: summary
      } = result
      
      # All entries should be processed
      assert length(processed_entries) == length(time_entries)
      assert Enum.all?(processed_entries, &PayrollTestHelpers.entry_processed?/1)
      
      # Should have one overtime entry
      assert length(overtime_entries) == 1
      
      # Summary should reflect the data
      assert %{
        total_employees: 1,
        total_regular_hours: total_hours,
        total_overtime_hours: overtime_hours,
        employees_with_overtime: 1
      } = summary
      
      assert total_hours > 40.0
      assert overtime_hours > 0.0
      assert summary.total_hours == total_hours + overtime_hours
    end

    test "handles multiple employees in single processing run" do
      monday = ~D[2024-01-01]
      scenarios = PayrollTestHelpers.create_mixed_overtime_scenario(monday)
      
      all_entries = scenarios |> Map.values() |> List.flatten()
      rule_spec = Factories.build_payroll_rule_spec(40.0)
      
      result = PayrollRules.process_time_entries(all_entries, rule_spec)
      
      assert %{summary: summary} = result
      assert summary.total_employees == 4
      assert summary.employees_with_overtime == 2 # emp_002 and emp_003
    end

    test "uses default threshold when no rule spec provided" do
      monday = ~D[2024-01-01]
      time_entries = PayrollTestHelpers.create_overtime_work_week("emp_001", monday, 5)
      
      # Default threshold should be 40.0
      result = PayrollRules.process_time_entries(time_entries)
      
      assert %{overtime_entries: overtime_entries} = result
      assert length(overtime_entries) == 1
      
      [overtime_entry] = overtime_entries
      assert {:overtime_entry, _, %{units: 5.0}} = overtime_entry
    end
  end

  describe "rule specification validation" do
    test "validates correct payroll rule spec" do
      rule_spec = Factories.build_payroll_rule_spec()
      
      assert PayrollRules.valid_rule_spec?(rule_spec) == true
    end

    test "rejects invalid rule spec structure" do
      invalid_specs = [
        %{}, # missing rules
        %{"rules" => "not_a_list"},
        %{"rules" => [%{"name" => "test"}]}, # missing type
        %{"rules" => [%{"name" => "test", "type" => "invalid"}]}, # wrong type
        %{"rules" => [%{"type" => "payroll"}]} # missing name
      ]
      
      Enum.each(invalid_specs, fn spec ->
        assert PayrollRules.valid_rule_spec?(spec) == false
      end)
    end

    test "validates rule specifications using behaviour implementation" do
      # Test that the module properly validates its own supported rules
      valid_spec = %{
        "rules_to_run" => ["time_calculation", "overtime_check"],
        "variables" => %{"overtime_threshold" => 40.0}
      }
      
      assert PayrollRules.valid_rule_spec?(valid_spec) == true
      
      # Test with unsupported rule names  
      invalid_spec = %{
        "rules_to_run" => ["unsupported_rule"],
        "variables" => %{}
      }
      
      assert PayrollRules.valid_rule_spec?(invalid_spec) == false
    end
  end

  describe "edge cases and error handling" do
    test "handles zero-duration entries" do
      start_dt = ~U[2024-01-01 09:00:00Z]
      finish_dt = start_dt # same time
      
      entry = Factories.build_time_entry("entry_1", start_dt, finish_dt, "emp_001")
      
      rule = PayrollRules.time_calculation_rule()
      [result] = rule.action.(rule.pattern.([entry]))
      
      assert {:time_entry, "entry_1", %{minutes: 0.0, units: 0.0}} = result
    end

    test "handles very short durations accurately" do
      start_dt = ~U[2024-01-01 09:00:00Z]
      finish_dt = DateTime.add(start_dt, 90, :second) # 1.5 minutes
      
      entry = Factories.build_time_entry("entry_1", start_dt, finish_dt, "emp_001")
      
      rule = PayrollRules.time_calculation_rule()
      [result] = rule.action.(rule.pattern.([entry]))
      
      assert {:time_entry, "entry_1", %{minutes: 1.5, units: 0.025}} = result
    end

    test "handles entries with minimal overtime" do
      # Create exactly 40.1 hours to test minimal overtime
      monday = ~D[2024-01-01]
      entries = PayrollTestHelpers.create_standard_work_week("emp_001", monday)
      
      # Add 6 minute entry (0.1 hours)
      extra_start = ~U[2024-01-05 17:00:00Z]
      extra_finish = DateTime.add(extra_start, 360, :second) # 6 minutes
      extra_entry = Factories.build_time_entry("extra", extra_start, extra_finish, "emp_001")
      
      all_entries = entries ++ [extra_entry]
      rule_spec = Factories.build_payroll_rule_spec(40.0)
      
      result = PayrollRules.process_time_entries(all_entries, rule_spec)
      
      assert %{overtime_entries: [overtime_entry]} = result
      assert {:overtime_entry, _, %{units: 0.1}} = overtime_entry
    end

    test "groups entries by correct week boundaries" do
      # Create entries spanning two weeks
      friday = ~D[2024-01-05] # End of week 1
      monday = ~D[2024-01-08] # Start of week 2
      
      week1_entry = Factories.build_processed_time_entry(
        "w1", 
        DateTime.new!(friday, ~T[09:00:00], "Etc/UTC"), 
        DateTime.new!(friday, ~T[17:00:00], "Etc/UTC"), 
        "emp_001"
      )
      
      week2_entry = Factories.build_processed_time_entry(
        "w2",
        DateTime.new!(monday, ~T[09:00:00], "Etc/UTC"),
        DateTime.new!(monday, ~T[17:00:00], "Etc/UTC"),
        "emp_001"
      )
      
      entries = [week1_entry, week2_entry]
      variables = %{"overtime_threshold" => 10.0} # Low threshold to trigger
      
      rule = PayrollRules.overtime_calculation_rule(variables)
      overtime_candidates = rule.pattern.(entries)
      
      # Should process each week separately - no overtime since each week has only 8 hours
      assert length(overtime_candidates) == 0
    end
  end

  describe "performance" do
    test "efficiently processes large number of entries" do
      # Create 1000 time entries across 100 employees
      entries = for emp_id <- 1..100, day <- 1..10 do
        date = Date.add(~D[2024-01-01], day - 1)
        start_dt = DateTime.new!(date, ~T[09:00:00], "Etc/UTC")
        finish_dt = DateTime.add(start_dt, 8 * 3600, :second)
        
        Factories.build_time_entry(
          "entry_#{emp_id}_#{day}",
          start_dt,
          finish_dt,
          "emp_#{String.pad_leading(to_string(emp_id), 3, "0")}"
        )
      end
      
      rule_spec = Factories.build_payroll_rule_spec(40.0)
      
      {process_time, result} = :timer.tc(fn ->
        PayrollRules.process_time_entries(entries, rule_spec)
      end)
      
      # Should process 1000 entries in reasonable time (< 1 second)
      assert process_time < 1_000_000 # microseconds
      
      # Verify results
      assert %{
        processed_entries: processed_entries,
        summary: summary
      } = result
      
      assert length(processed_entries) == 1000
      assert summary.total_employees == 100
      
      # Each employee should have overtime (10 days * 8 hours = 80 hours > 40)
      assert summary.employees_with_overtime == 100
    end

    test "overtime calculation performance with many employees" do
      # Create scenario with 500 employees, some with overtime
      processed_entries = for emp_id <- 1..500 do
        hours = if rem(emp_id, 3) == 0, do: 50, else: 35 # Every 3rd employee has overtime
        
        date = ~D[2024-01-01]
        start_dt = DateTime.new!(date, ~T[09:00:00], "Etc/UTC")
        finish_dt = DateTime.add(start_dt, trunc(hours * 3600), :second)
        
        Factories.build_processed_time_entry(
          "entry_#{emp_id}",
          start_dt,
          finish_dt,
          "emp_#{String.pad_leading(to_string(emp_id), 3, "0")}"
        )
      end
      
      variables = %{"overtime_threshold" => 40.0}
      rule = PayrollRules.overtime_calculation_rule(variables)
      
      {pattern_time, overtime_candidates} = :timer.tc(fn ->
        rule.pattern.(processed_entries)
      end)
      
      {action_time, overtime_entries} = :timer.tc(fn ->
        rule.action.(overtime_candidates)
      end)
      
      # Performance should be reasonable
      assert pattern_time < 500_000 # < 0.5 seconds
      assert action_time < 100_000  # < 0.1 seconds
      
      # Verify correctness
      expected_overtime_count = div(500, 3) # Every 3rd employee
      assert length(overtime_entries) == expected_overtime_count
    end
  end
end
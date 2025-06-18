defmodule Presto.Examples.PayrollRulesTest do
  use ExUnit.Case, async: true

  alias Presto.Examples.PayrollRules
  alias Presto.Factories
  alias Presto.PayrollTestHelpers
  alias Presto.RuleEngine

  setup do
    engine = start_supervised!(RuleEngine)
    %{engine: engine}
  end

  describe "time calculation rule" do
    test "calculates duration for basic work day", %{engine: engine} do
      start_dt = ~U[2024-01-01 09:00:00Z]
      finish_dt = ~U[2024-01-01 17:00:00Z]

      entry = Factories.build_time_entry("entry_1", start_dt, finish_dt, "emp_001")
      rule = PayrollRules.time_calculation_rule()

      # Add rule to engine
      RuleEngine.add_rule(engine, rule)

      # Assert the fact
      RuleEngine.assert_fact(engine, entry)

      # Fire rules to execute them
      fire_results = RuleEngine.fire_rules(engine)

      # Assert the generated facts back into working memory
      for fact <- fire_results do
        RuleEngine.assert_fact(engine, fact)
      end

      # Get all facts to see what was produced
      facts = RuleEngine.get_facts(engine)

      # Should have the original entry and the processed entry
      assert length(facts) >= 2

      # Find the updated time entry (with calculated values, not nil)
      updated_entry =
        Enum.find(facts, fn
          {:time_entry, "entry_1", %{minutes: minutes, units: units}}
          when not is_nil(minutes) and not is_nil(units) ->
            true

          _ ->
            false
        end)

      assert {:time_entry, "entry_1",
              %{
                start_datetime: ^start_dt,
                finish_datetime: ^finish_dt,
                employee_id: "emp_001",
                minutes: 480.0,
                units: 8.0
              }} = updated_entry
    end

    test "calculates duration for partial hours", %{engine: engine} do
      start_dt = ~U[2024-01-01 09:00:00Z]
      # 4.5 hours
      finish_dt = ~U[2024-01-01 13:30:00Z]

      entry = Factories.build_time_entry("entry_1", start_dt, finish_dt, "emp_001")
      rule = PayrollRules.time_calculation_rule()

      # Add rule to engine
      RuleEngine.add_rule(engine, rule)

      # Assert the fact
      RuleEngine.assert_fact(engine, entry)

      # Fire rules to execute them
      fire_results = RuleEngine.fire_rules(engine)

      # Assert the generated facts back into working memory
      for fact <- fire_results do
        RuleEngine.assert_fact(engine, fact)
      end

      # Get all facts to find the processed entry
      facts = RuleEngine.get_facts(engine)

      updated_entry =
        Enum.find(facts, fn
          {:time_entry, "entry_1", %{minutes: minutes, units: units}}
          when not is_nil(minutes) and not is_nil(units) ->
            true

          _ ->
            false
        end)

      assert {:time_entry, "entry_1", %{minutes: 270.0, units: 4.5}} = updated_entry
    end

    test "handles cross-day time entries", %{engine: engine} do
      start_dt = ~U[2024-01-01 23:00:00Z]
      # 8 hours across days
      finish_dt = ~U[2024-01-02 07:00:00Z]

      entry = Factories.build_time_entry("entry_1", start_dt, finish_dt, "emp_001")
      rule = PayrollRules.time_calculation_rule()

      # Add rule to engine
      RuleEngine.add_rule(engine, rule)

      # Assert the fact
      RuleEngine.assert_fact(engine, entry)

      # Fire rules to execute them
      fire_results = RuleEngine.fire_rules(engine)

      # Assert the generated facts back into working memory
      for fact <- fire_results do
        RuleEngine.assert_fact(engine, fact)
      end

      # Get all facts to find the processed entry
      facts = RuleEngine.get_facts(engine)

      updated_entry =
        Enum.find(facts, fn
          {:time_entry, "entry_1", %{minutes: minutes, units: units}}
          when not is_nil(minutes) and not is_nil(units) ->
            true

          _ ->
            false
        end)

      assert {:time_entry, "entry_1", %{minutes: 480.0, units: 8.0}} = updated_entry
    end

    test "ignores entries that already have calculations", %{engine: engine} do
      processed_entry =
        Factories.build_processed_time_entry(
          "entry_1",
          ~U[2024-01-01 09:00:00Z],
          ~U[2024-01-01 17:00:00Z],
          "emp_001"
        )

      rule = PayrollRules.time_calculation_rule()

      # Add rule to engine
      RuleEngine.add_rule(engine, rule)

      # Assert the fact
      RuleEngine.assert_fact(engine, processed_entry)

      # Get all facts
      facts = RuleEngine.get_facts(engine)

      # Should only have the original processed entry, no new facts created
      assert length(facts) == 1
      assert processed_entry in facts
    end

    test "ignores entries without required datetime fields", %{engine: engine} do
      incomplete_entry =
        {:time_entry, "entry_1",
         %{
           employee_id: "emp_001",
           minutes: nil,
           units: nil
         }}

      rule = PayrollRules.time_calculation_rule()

      # Add rule to engine
      RuleEngine.add_rule(engine, rule)

      # Assert the fact
      RuleEngine.assert_fact(engine, incomplete_entry)

      # Get all facts
      facts = RuleEngine.get_facts(engine)

      # Should only have the original incomplete entry, no new facts created
      assert length(facts) == 1
      assert incomplete_entry in facts
    end

    test "handles multiple entries in single rule execution", %{engine: engine} do
      monday = ~D[2024-01-01]
      entries = PayrollTestHelpers.create_standard_work_week("emp_001", monday)
      rule = PayrollRules.time_calculation_rule()

      # Add rule to engine
      RuleEngine.add_rule(engine, rule)

      # Assert all facts
      for entry <- entries do
        RuleEngine.assert_fact(engine, entry)
      end

      # Fire rules to execute them
      fire_results = RuleEngine.fire_rules(engine)

      # Assert the generated facts back into working memory
      for fact <- fire_results do
        RuleEngine.assert_fact(engine, fact)
      end

      # Get all facts to find the processed entries
      facts = RuleEngine.get_facts(engine)

      # Find processed entries (those with minutes and units calculated)
      processed_entries =
        Enum.filter(facts, fn
          {:time_entry, _, %{minutes: minutes, units: units}}
          when not is_nil(minutes) and not is_nil(units) ->
            true

          _ ->
            false
        end)

      assert length(processed_entries) == 5

      # All should be calculated to 8 hours each
      Enum.each(processed_entries, fn {:time_entry, _, %{minutes: minutes, units: units}} ->
        assert minutes == 480.0
        assert units == 8.0
      end)
    end
  end

  describe "overtime calculation rule" do
    test "creates overtime entry when threshold exceeded", %{engine: engine} do
      monday = ~D[2024-01-01]

      # Create raw time entries first
      time_entries = PayrollTestHelpers.create_overtime_work_week("emp_001", monday, 10)

      # Add both rules upfront for automatic chaining
      time_rule = PayrollRules.time_calculation_rule()
      RuleEngine.add_rule(engine, time_rule)

      variables = %{"overtime_threshold" => 40.0}
      overtime_rule = PayrollRules.overtime_calculation_rule(variables)
      RuleEngine.add_rule(engine, overtime_rule)

      # Assert raw time entries
      for entry <- time_entries do
        RuleEngine.assert_fact(engine, entry)
      end

      # Use manual chaining for now (automatic chaining needs more work)
      time_results = RuleEngine.fire_rules(engine)

      # Assert time calculation results back into working memory
      for fact <- time_results do
        RuleEngine.assert_fact(engine, fact)
      end

      # Fire rules again to trigger overtime calculations
      all_results = RuleEngine.fire_rules(engine)

      # Filter for overtime entries only
      overtime_results =
        Enum.filter(all_results, fn
          {:overtime_entry, _, _} -> true
          _ -> false
        end)

      # Should have overtime entry
      assert length(overtime_results) == 1

      [overtime_entry] = overtime_results

      assert {:overtime_entry, {"emp_001", ^monday},
              %{
                employee_id: "emp_001",
                units: overtime_units,
                week_start: ^monday,
                type: :overtime
              }} = overtime_entry

      assert overtime_units > 0
    end

    test "does not create overtime when under threshold", %{engine: engine} do
      monday = ~D[2024-01-01]

      # Create raw time entries first (standard work week = 40 hours, no overtime)
      time_entries = PayrollTestHelpers.create_standard_work_week("emp_001", monday)

      # Add time calculation rule to process entries first
      time_rule = PayrollRules.time_calculation_rule()
      RuleEngine.add_rule(engine, time_rule)

      # Assert raw time entries
      for entry <- time_entries do
        RuleEngine.assert_fact(engine, entry)
      end

      # Fire rules to calculate time durations
      time_results = RuleEngine.fire_rules(engine)

      # Assert calculated facts back into working memory
      for fact <- time_results do
        RuleEngine.assert_fact(engine, fact)
      end

      # Add overtime rule
      variables = %{"overtime_threshold" => 40.0}
      overtime_rule = PayrollRules.overtime_calculation_rule(variables)
      RuleEngine.add_rule(engine, overtime_rule)

      # Fire rules to calculate overtime
      all_results = RuleEngine.fire_rules(engine)

      # Filter for overtime entries only
      overtime_results =
        Enum.filter(all_results, fn
          {:overtime_entry, _, _} -> true
          _ -> false
        end)

      # Should have no overtime entries
      assert Enum.empty?(overtime_results)
    end

    test "handles custom overtime threshold", %{engine: engine} do
      monday = ~D[2024-01-01]

      # Create raw time entries first (standard work week = 40 hours)
      time_entries = PayrollTestHelpers.create_standard_work_week("emp_001", monday)

      # Add both rules upfront for automatic chaining
      time_rule = PayrollRules.time_calculation_rule()
      RuleEngine.add_rule(engine, time_rule)

      # Add overtime rule with lower threshold - should trigger overtime for 40-hour week
      variables = %{"overtime_threshold" => 35.0}
      overtime_rule = PayrollRules.overtime_calculation_rule(variables)
      RuleEngine.add_rule(engine, overtime_rule)

      # Assert raw time entries
      for entry <- time_entries do
        RuleEngine.assert_fact(engine, entry)
      end

      # Use manual chaining
      time_results = RuleEngine.fire_rules(engine)

      for fact <- time_results do
        RuleEngine.assert_fact(engine, fact)
      end

      all_results = RuleEngine.fire_rules(engine)

      # Filter for overtime entries only
      overtime_results =
        Enum.filter(all_results, fn
          {:overtime_entry, _, _} -> true
          _ -> false
        end)

      # Should have one overtime entry
      assert length(overtime_results) == 1
      [overtime_entry] = overtime_results
      assert {:overtime_entry, _, %{units: 5.0}} = overtime_entry
    end

    test "processes multiple employees separately", %{engine: engine} do
      monday = ~D[2024-01-01]

      # Create mixed scenario with different overtime situations
      scenarios = PayrollTestHelpers.create_mixed_overtime_scenario(monday)
      all_entries = scenarios |> Map.values() |> List.flatten()

      # Add both rules upfront for automatic chaining
      time_rule = PayrollRules.time_calculation_rule()
      RuleEngine.add_rule(engine, time_rule)

      variables = %{"overtime_threshold" => 40.0}
      overtime_rule = PayrollRules.overtime_calculation_rule(variables)
      RuleEngine.add_rule(engine, overtime_rule)

      # Assert raw time entries
      for entry <- all_entries do
        RuleEngine.assert_fact(engine, entry)
      end

      # Use manual chaining
      time_results = RuleEngine.fire_rules(engine)

      for fact <- time_results do
        RuleEngine.assert_fact(engine, fact)
      end

      all_results = RuleEngine.fire_rules(engine)

      # Filter for overtime entries only
      overtime_results =
        Enum.filter(all_results, fn
          {:overtime_entry, _, _} -> true
          _ -> false
        end)

      # Should have overtime for emp_002 and emp_003
      assert length(overtime_results) == 2

      employee_ids =
        Enum.map(overtime_results, fn {:overtime_entry, {emp_id, _}, _} -> emp_id end)

      assert "emp_002" in employee_ids
      assert "emp_003" in employee_ids
      # no overtime
      refute "emp_001" in employee_ids
      # part time, no overtime
      refute "emp_004" in employee_ids
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
      assert overtime_hours > 0
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
      # emp_002 and emp_003
      assert summary.employees_with_overtime == 2
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
        # missing rules
        %{},
        %{"rules" => "not_a_list"},
        # missing type
        %{"rules" => [%{"name" => "test"}]},
        # wrong type
        %{"rules" => [%{"name" => "test", "type" => "invalid"}]},
        # missing name
        %{"rules" => [%{"type" => "payroll"}]}
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
    test "handles zero-duration entries", %{engine: engine} do
      start_dt = ~U[2024-01-01 09:00:00Z]
      # same time
      finish_dt = start_dt

      entry = Factories.build_time_entry("entry_1", start_dt, finish_dt, "emp_001")

      rule = PayrollRules.time_calculation_rule()
      RuleEngine.add_rule(engine, rule)
      RuleEngine.assert_fact(engine, entry)

      fire_results = RuleEngine.fire_rules(engine)

      for fact <- fire_results do
        RuleEngine.assert_fact(engine, fact)
      end

      facts = RuleEngine.get_facts(engine)

      result =
        Enum.find(facts, fn
          {:time_entry, "entry_1", %{minutes: minutes, units: units}}
          when not is_nil(minutes) and not is_nil(units) ->
            true

          _ ->
            false
        end)

      assert {:time_entry, "entry_1", %{minutes: +0.0, units: +0.0}} = result
    end

    test "handles very short durations accurately", %{engine: engine} do
      start_dt = ~U[2024-01-01 09:00:00Z]
      # 1.5 minutes
      finish_dt = DateTime.add(start_dt, 90, :second)

      entry = Factories.build_time_entry("entry_1", start_dt, finish_dt, "emp_001")

      rule = PayrollRules.time_calculation_rule()
      RuleEngine.add_rule(engine, rule)
      RuleEngine.assert_fact(engine, entry)

      fire_results = RuleEngine.fire_rules(engine)

      for fact <- fire_results do
        RuleEngine.assert_fact(engine, fact)
      end

      facts = RuleEngine.get_facts(engine)

      result =
        Enum.find(facts, fn
          {:time_entry, "entry_1", %{minutes: minutes, units: units}}
          when not is_nil(minutes) and not is_nil(units) ->
            true

          _ ->
            false
        end)

      assert {:time_entry, "entry_1", %{minutes: 1.5, units: 0.03}} = result
    end

    test "handles entries with minimal overtime" do
      # Create exactly 40.1 hours to test minimal overtime
      monday = ~D[2024-01-01]
      entries = PayrollTestHelpers.create_standard_work_week("emp_001", monday)

      # Add 6 minute entry (0.1 hours)
      extra_start = ~U[2024-01-05 17:00:00Z]
      # 6 minutes
      extra_finish = DateTime.add(extra_start, 360, :second)
      extra_entry = Factories.build_time_entry("extra", extra_start, extra_finish, "emp_001")

      all_entries = entries ++ [extra_entry]
      rule_spec = Factories.build_payroll_rule_spec(40.0)

      result = PayrollRules.process_time_entries(all_entries, rule_spec)

      assert %{overtime_entries: [overtime_entry]} = result
      assert {:overtime_entry, _, %{units: 0.1}} = overtime_entry
    end

    test "groups entries by correct week boundaries", %{engine: engine} do
      # Create entries spanning two weeks
      # End of week 1
      friday = ~D[2024-01-05]
      # Start of week 2
      monday = ~D[2024-01-08]

      week1_entry =
        Factories.build_processed_time_entry(
          "w1",
          DateTime.new!(friday, ~T[09:00:00], "Etc/UTC"),
          DateTime.new!(friday, ~T[17:00:00], "Etc/UTC"),
          "emp_001"
        )

      week2_entry =
        Factories.build_processed_time_entry(
          "w2",
          DateTime.new!(monday, ~T[09:00:00], "Etc/UTC"),
          DateTime.new!(monday, ~T[17:00:00], "Etc/UTC"),
          "emp_001"
        )

      # Convert to processed_time_entry facts for the overtime rule
      processed_entries = [
        {:processed_time_entry, "w1", elem(week1_entry, 2)},
        {:processed_time_entry, "w2", elem(week2_entry, 2)}
      ]

      # Add overtime rule with low threshold to trigger
      variables = %{"overtime_threshold" => 10.0}
      rule = PayrollRules.overtime_calculation_rule(variables)
      RuleEngine.add_rule(engine, rule)

      # Assert processed entries
      for entry <- processed_entries do
        RuleEngine.assert_fact(engine, entry)
      end

      # Fire rules to calculate overtime
      overtime_results = RuleEngine.fire_rules(engine)

      # Should process each week separately - no overtime since each week has only 8 hours
      assert Enum.empty?(overtime_results)
    end
  end

  describe "performance" do
    test "efficiently processes large number of entries" do
      # Create 1000 time entries across 100 employees
      entries =
        for emp_id <- 1..100, day <- 1..10 do
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

      {process_time, result} =
        :timer.tc(fn ->
          PayrollRules.process_time_entries(entries, rule_spec)
        end)

      # Should process 1000 entries in reasonable time (< 1 second)
      # microseconds
      assert process_time < 1_000_000

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

    test "overtime calculation performance with many employees", %{engine: engine} do
      # Create scenario with 500 employees, some with overtime
      processed_entries =
        for emp_id <- 1..500 do
          # Every 3rd employee has overtime
          hours = if rem(emp_id, 3) == 0, do: 50, else: 35

          date = ~D[2024-01-01]
          start_dt = DateTime.new!(date, ~T[09:00:00], "Etc/UTC")
          finish_dt = DateTime.add(start_dt, trunc(hours * 3600), :second)

          processed_entry =
            Factories.build_processed_time_entry(
              "entry_#{emp_id}",
              start_dt,
              finish_dt,
              "emp_#{String.pad_leading(to_string(emp_id), 3, "0")}"
            )

          # Convert to processed_time_entry fact
          {:processed_time_entry, elem(processed_entry, 1), elem(processed_entry, 2)}
        end

      variables = %{"overtime_threshold" => 40.0}
      rule = PayrollRules.overtime_calculation_rule(variables)
      RuleEngine.add_rule(engine, rule)

      # Assert all processed entries
      for entry <- processed_entries do
        RuleEngine.assert_fact(engine, entry)
      end

      # Time the rule execution
      {total_time, all_results} =
        :timer.tc(fn ->
          RuleEngine.fire_rules(engine)
        end)

      # Filter for overtime entries only
      overtime_results =
        Enum.filter(all_results, fn
          {:overtime_entry, _, _} -> true
          _ -> false
        end)

      # Performance should be reasonable (< 1 second)
      assert total_time < 1_000_000

      # Verify correctness
      # Every 3rd employee
      expected_overtime_count = div(500, 3)
      assert length(overtime_results) == expected_overtime_count
    end
  end
end

defmodule Presto.Examples.RulesIntegrationTest do
  use ExUnit.Case, async: true

  alias Presto.Examples.{PayrollRules, ComplianceRules}
  alias Presto.Factories
  alias Presto.{PayrollTestHelpers, ComplianceTestHelpers}

  describe "JSON rule specification integration" do
    test "payroll rules with JSON specification" do
      # JSON rule specification for payroll processing
      payroll_spec = %{
        "version" => "1.0",
        "type" => "payroll",
        "rules" => [
          %{
            "name" => "calculate_time_duration",
            "type" => "payroll",
            "pattern" => "time_entry_with_start_finish",
            "action" => "calculate_duration",
            "description" => "Calculate minutes and units for time entries"
          },
          %{
            "name" => "calculate_overtime",
            "type" => "payroll",
            "pattern" => "aggregated_time_entries",
            "action" => "create_overtime_entry",
            "variables" => %{
              "overtime_threshold" => 40.0,
              "overtime_multiplier" => 1.5
            },
            "description" => "Create overtime entries when threshold exceeded"
          }
        ],
        "metadata" => %{
          "created_by" => "HR System",
          "created_at" => "2024-01-01T00:00:00Z",
          "department" => "Human Resources"
        }
      }
      
      # Create test data
      monday = ~D[2024-01-01]
      time_entries = PayrollTestHelpers.create_overtime_work_week("emp_001", monday, 10)
      
      # Process using JSON specification
      result = PayrollRules.process_time_entries(time_entries, payroll_spec)
      
      assert %{
        processed_entries: processed_entries,
        overtime_entries: overtime_entries,
        summary: summary
      } = result
      
      # Verify processing results
      assert length(processed_entries) == length(time_entries)
      assert Enum.all?(processed_entries, &PayrollTestHelpers.entry_processed?/1)
      
      # Verify overtime calculation with threshold from JSON spec
      assert length(overtime_entries) == 1
      [overtime_entry] = overtime_entries
      assert {:overtime_entry, {"emp_001", ^monday}, %{units: overtime_units}} = overtime_entry
      assert overtime_units > 0.0
      
      # Verify summary includes overtime
      assert summary.employees_with_overtime == 1
      assert summary.total_overtime_hours > 0.0
    end

    test "compliance rules with JSON specification" do
      # JSON rule specification for compliance checking
      compliance_spec = %{
        "version" => "1.0",
        "type" => "compliance",
        "rules" => [
          %{
            "name" => "weekly_hours_compliance",
            "type" => "compliance", 
            "pattern" => "weekly_aggregated_hours",
            "action" => "check_compliance_threshold",
            "variables" => %{
              "max_weekly_hours" => 45.0,
              "warning_threshold" => 40.0,
              "region" => "EU"
            },
            "description" => "Check EU compliance for maximum weekly working hours"
          }
        ],
        "metadata" => %{
          "jurisdiction" => "European Union",
          "regulation" => "Working Time Directive 2003/88/EC",
          "effective_date" => "2024-01-01"
        }
      }
      
      # Create test data that will violate the threshold
      monday = ~D[2024-01-01]
      time_entries = Enum.flat_map(0..4, fn day ->
        date = Date.add(monday, day)
        # 10 hours per day = 50 hours total > 45 hour threshold
        [Factories.build_processed_time_entry("entry_#{day}",
          DateTime.new!(date, ~T[08:00:00], "Etc/UTC"),
          DateTime.new!(date, ~T[18:00:00], "Etc/UTC"), "emp_001")]
      end)
      
      # Process using JSON specification
      result = ComplianceRules.process_compliance_check(time_entries, compliance_spec)
      
      assert %{
        weekly_aggregations: aggregations,
        compliance_results: compliance_results,
        summary: summary
      } = result
      
      # Verify aggregation
      assert length(aggregations) == 1
      [aggregation] = aggregations
      assert {:weekly_hours, {"emp_001", ^monday}, %{total_hours: 50.0}} = aggregation
      
      # Verify compliance violation with threshold from JSON spec
      assert length(compliance_results) == 1
      [compliance_result] = compliance_results
      assert {:compliance_result, {"emp_001", ^monday}, %{
        status: :non_compliant,
        threshold: 45.0,
        actual_value: 50.0
      }} = compliance_result
      
      # Verify summary shows violation
      assert summary.compliance_violations == 1
      assert summary.compliance_rate_percent == 0.0
    end

    test "combined payroll and compliance workflow" do
      # Combined rule specification
      combined_spec = %{
        "version" => "1.0",
        "type" => "combined",
        "payroll" => %{
          "rules" => [
            %{
              "name" => "calculate_time_duration",
              "type" => "payroll",
              "variables" => %{}
            },
            %{
              "name" => "calculate_overtime", 
              "type" => "payroll",
              "variables" => %{"overtime_threshold" => 37.5} # UK standard
            }
          ]
        },
        "compliance" => %{
          "rules" => [
            %{
              "name" => "weekly_hours_compliance",
              "type" => "compliance",
              "variables" => %{"max_weekly_hours" => 48.0} # UK maximum
            }
          ]
        },
        "metadata" => %{
          "jurisdiction" => "United Kingdom",
          "regulation" => "Working Time Regulations 1998"
        }
      }
      
      # Create realistic UK work week scenario
      monday = ~D[2024-01-01]
      raw_time_entries = [
        # Monday: 8.5 hours (including 30min unpaid break)
        Factories.build_time_entry("mon",
          DateTime.new!(monday, ~T[09:00:00], "Etc/UTC"),
          DateTime.new!(monday, ~T[17:30:00], "Etc/UTC"), "emp_uk_001"),
        
        # Tuesday: 8.5 hours
        Factories.build_time_entry("tue",
          DateTime.new!(Date.add(monday, 1), ~T[09:00:00], "Etc/UTC"),
          DateTime.new!(Date.add(monday, 1), ~T[17:30:00], "Etc/UTC"), "emp_uk_001"),
        
        # Wednesday: 8.5 hours  
        Factories.build_time_entry("wed",
          DateTime.new!(Date.add(monday, 2), ~T[09:00:00], "Etc/UTC"),
          DateTime.new!(Date.add(monday, 2), ~T[17:30:00], "Etc/UTC"), "emp_uk_001"),
        
        # Thursday: 8.5 hours
        Factories.build_time_entry("thu",
          DateTime.new!(Date.add(monday, 3), ~T[09:00:00], "Etc/UTC"),
          DateTime.new!(Date.add(monday, 3), ~T[17:30:00], "Etc/UTC"), "emp_uk_001"),
        
        # Friday: 9 hours (overtime)
        Factories.build_time_entry("fri",
          DateTime.new!(Date.add(monday, 4), ~T[09:00:00], "Etc/UTC"),
          DateTime.new!(Date.add(monday, 4), ~T[18:00:00], "Etc/UTC"), "emp_uk_001")
      ]
      # Total: 42.5 hours (5 hours overtime over 37.5 threshold, but compliant with 48h max)
      
      # Step 1: Process payroll
      payroll_result = PayrollRules.process_time_entries(raw_time_entries, combined_spec["payroll"])
      
      # Step 2: Process compliance using payroll results
      processed_entries = payroll_result.processed_entries
      compliance_result = ComplianceRules.process_compliance_check(processed_entries, combined_spec["compliance"])
      
      # Verify payroll processing
      assert %{
        overtime_entries: overtime_entries,
        summary: payroll_summary
      } = payroll_result
      
      assert length(overtime_entries) == 1
      [overtime_entry] = overtime_entries
      assert {:overtime_entry, {"emp_uk_001", ^monday}, %{units: 5.0}} = overtime_entry
      assert payroll_summary.total_overtime_hours == 5.0
      
      # Verify compliance checking
      assert %{
        compliance_results: compliance_results,
        summary: compliance_summary
      } = compliance_result
      
      assert length(compliance_results) == 1
      [compliance_result] = compliance_results
      assert {:compliance_result, {"emp_uk_001", ^monday}, %{
        status: :compliant,
        actual_value: 42.5,
        threshold: 48.0
      }} = compliance_result
      
      assert compliance_summary.compliance_violations == 0
      assert compliance_summary.compliance_rate_percent == 100.0
      
      # Combined result
      combined_result = %{
        payroll: payroll_result,
        compliance: compliance_result,
        jurisdiction: combined_spec["metadata"]["jurisdiction"],
        regulation: combined_spec["metadata"]["regulation"]
      }
      
      assert combined_result.jurisdiction == "United Kingdom"
      assert combined_result.payroll.summary.employees_with_overtime == 1
      assert combined_result.compliance.summary.compliance_violations == 0
    end

    test "multi-employee, multi-week scenario with variable thresholds" do
      # Complex rule specification with different thresholds per rule
      complex_spec = %{
        "version" => "2.0", 
        "type" => "enterprise",
        "rules" => [
          %{
            "name" => "standard_payroll_calculation",
            "type" => "payroll",
            "variables" => %{"overtime_threshold" => 40.0}
          },
          %{
            "name" => "weekly_compliance_check",
            "type" => "compliance", 
            "variables" => %{"max_weekly_hours" => 50.0}
          }
        ],
        "employee_overrides" => %{
          "emp_manager_001" => %{
            "overtime_threshold" => 45.0, # Managers have higher threshold
            "max_weekly_hours" => 55.0
          }
        },
        "metadata" => %{
          "company" => "Acme Corp",
          "policy_version" => "2024.1"
        }
      }
      
      # Create scenario with 3 employees over 2 weeks
      base_monday = ~D[2024-01-01]
      week2_monday = Date.add(base_monday, 7)
      
      # Standard employee - overtime in week 1
      standard_emp_entries = PayrollTestHelpers.create_overtime_work_week("emp_standard_001", base_monday, 8) ++
                            PayrollTestHelpers.create_standard_work_week("emp_standard_001", week2_monday)
      
      # Manager - longer hours but within manager thresholds
      manager_entries = PayrollTestHelpers.create_overtime_work_week("emp_manager_001", base_monday, 15) ++
                       PayrollTestHelpers.create_overtime_work_week("emp_manager_001", week2_monday, 12)
      
      # Part-time employee - always under thresholds
      part_time_entries = PayrollTestHelpers.create_part_time_week("emp_parttime_001", base_monday) ++
                         PayrollTestHelpers.create_part_time_week("emp_parttime_001", week2_monday)
      
      all_entries = standard_emp_entries ++ manager_entries ++ part_time_entries
      
      # Process payroll
      payroll_result = PayrollRules.process_time_entries(all_entries, complex_spec)
      
      # Process compliance
      processed_entries = payroll_result.processed_entries
      compliance_result = ComplianceRules.process_compliance_check(processed_entries, complex_spec)
      
      # Verify results
      assert %{summary: payroll_summary} = payroll_result
      assert payroll_summary.total_employees == 3
      
      # Standard employee and manager should have overtime
      assert payroll_summary.employees_with_overtime >= 2
      
      # Verify compliance - all should be compliant with 50h threshold
      assert %{summary: compliance_summary} = compliance_result
      assert compliance_summary.total_employees == 3
      
      # No violations expected with 50-hour threshold
      assert compliance_summary.compliance_violations == 0
      assert compliance_summary.compliance_rate_percent == 100.0
      
      # Verify we have data for both weeks
      assert compliance_summary.total_weeks_checked == 6 # 3 employees × 2 weeks
    end
  end

  describe "rule specification validation and error handling" do
    test "validates payroll rule specifications" do
      valid_specs = [
        Factories.build_payroll_rule_spec(),
        %{"rules" => [%{"name" => "custom_rule", "type" => "payroll"}]},
        %{"rules" => []}  # Empty rules is valid
      ]
      
      invalid_specs = [
        %{}, # Missing rules
        %{"rules" => "not_list"},
        %{"rules" => [%{"name" => "test"}]}, # Missing type
        %{"rules" => [%{"type" => "payroll"}]}, # Missing name
        %{"rules" => [%{"name" => "test", "type" => "invalid"}]} # Wrong type
      ]
      
      Enum.each(valid_specs, fn spec ->
        assert PayrollRules.valid_rule_spec?(spec) == true, 
          "Expected valid spec to pass: #{inspect(spec)}"
      end)
      
      Enum.each(invalid_specs, fn spec ->
        assert PayrollRules.valid_rule_spec?(spec) == false,
          "Expected invalid spec to fail: #{inspect(spec)}"
      end)
    end

    test "validates compliance rule specifications" do
      valid_specs = [
        Factories.build_compliance_rule_spec(),
        %{"rules" => [%{"name" => "custom_rule", "type" => "compliance"}]},
        %{"rules" => []}  # Empty rules is valid
      ]
      
      invalid_specs = [
        %{}, # Missing rules
        %{"rules" => "not_list"},
        %{"rules" => [%{"name" => "test"}]}, # Missing type
        %{"rules" => [%{"type" => "compliance"}]}, # Missing name
        %{"rules" => [%{"name" => "test", "type" => "invalid"}]} # Wrong type
      ]
      
      Enum.each(valid_specs, fn spec ->
        assert ComplianceRules.valid_rule_spec?(spec) == true,
          "Expected valid spec to pass: #{inspect(spec)}"
      end)
      
      Enum.each(invalid_specs, fn spec ->
        assert ComplianceRules.valid_rule_spec?(spec) == false,
          "Expected invalid spec to fail: #{inspect(spec)}"
      end)
    end

    test "handles missing variables gracefully" do
      # Rule spec without variables should use defaults
      minimal_payroll_spec = %{
        "rules" => [
          %{
            "name" => "calculate_overtime",
            "type" => "payroll"
            # No variables section
          }
        ]
      }
      
      monday = ~D[2024-01-01]
      entries = PayrollTestHelpers.create_overtime_work_week("emp_001", monday, 10)
      
      # Should use default 40.0 threshold
      result = PayrollRules.process_time_entries(entries, minimal_payroll_spec)
      
      assert %{overtime_entries: overtime_entries} = result
      assert length(overtime_entries) == 1
    end

    test "handles extra metadata fields" do
      # Rule spec with extra metadata should not affect processing
      detailed_spec = %{
        "version" => "1.0",
        "rules" => [
          %{
            "name" => "calculate_overtime",
            "type" => "payroll",
            "variables" => %{"overtime_threshold" => 35.0},
            "description" => "Custom overtime calculation",
            "author" => "HR Team",
            "last_modified" => "2024-01-01",
            "priority" => "high",
            "tags" => ["overtime", "payroll", "critical"]
          }
        ],
        "metadata" => %{
          "company" => "Test Corp",
          "department" => "Engineering",
          "compliance_officer" => "Jane Doe",
          "review_date" => "2024-12-31"
        }
      }
      
      monday = ~D[2024-01-01]
      entries = PayrollTestHelpers.create_standard_work_week("emp_001", monday)
      
      # Should process normally despite extra metadata
      result = PayrollRules.process_time_entries(entries, detailed_spec)
      
      assert %{summary: summary} = result
      assert summary.total_employees == 1
      
      # Should use the specified 35.0 threshold, triggering overtime for 40-hour week
      assert summary.employees_with_overtime == 1
    end
  end

  describe "performance with complex specifications" do
    test "processes large rule specifications efficiently" do
      # Create a complex rule specification with many metadata fields
      large_spec = %{
        "version" => "3.0",
        "type" => "enterprise",
        "rules" => Enum.map(1..10, fn i ->
          %{
            "name" => "rule_#{i}",
            "type" => if(rem(i, 2) == 0, do: "payroll", else: "compliance"),
            "variables" => %{"threshold_#{i}" => i * 10.0},
            "description" => "Rule number #{i} for testing",
            "metadata" => %{
              "created" => "2024-01-#{String.pad_leading(to_string(i), 2, "0")}",
              "author" => "System #{i}",
              "priority" => rem(i, 3)
            }
          }
        end),
        "global_settings" => %{
          "timezone" => "UTC",
          "currency" => "USD",
          "rounding_precision" => 2,
          "audit_enabled" => true
        },
        "metadata" => %{
          "company" => "Large Corp",
          "total_employees" => 10_000,
          "departments" => Enum.map(1..50, &"Department #{&1}")
        }
      }
      
      # Create moderate dataset
      monday = ~D[2024-01-01]
      time_entries = for emp_id <- 1..10 do
        PayrollTestHelpers.create_standard_work_week("emp_#{String.pad_leading(to_string(emp_id), 3, "0")}", monday)
      end |> List.flatten()
      
      # Process should be fast despite complex specification
      {payroll_time, payroll_result} = :timer.tc(fn ->
        PayrollRules.process_time_entries(time_entries, large_spec)
      end)
      
      {compliance_time, compliance_result} = :timer.tc(fn ->
        ComplianceRules.process_compliance_check(payroll_result.processed_entries, large_spec)
      end)
      
      # Should complete in reasonable time
      assert payroll_time < 500_000    # < 0.5 seconds
      assert compliance_time < 500_000 # < 0.5 seconds
      
      # Results should be correct
      assert payroll_result.summary.total_employees == 10
      assert compliance_result.summary.total_employees == 10
    end
  end
end
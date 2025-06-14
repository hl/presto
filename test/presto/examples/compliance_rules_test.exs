defmodule Presto.Examples.ComplianceRulesTest do
  use ExUnit.Case, async: true

  alias Presto.Examples.ComplianceRules
  alias Presto.Factories
  alias Presto.ComplianceTestHelpers

  describe "weekly aggregation rule" do
    test "aggregates time entries into weekly totals" do
      monday = ~D[2024-01-01]

      # Create 3 days of 8-hour entries
      time_entries = [
        Factories.build_processed_time_entry(
          "entry_1",
          DateTime.new!(monday, ~T[09:00:00], "Etc/UTC"),
          DateTime.new!(monday, ~T[17:00:00], "Etc/UTC"),
          "emp_001"
        ),
        Factories.build_processed_time_entry(
          "entry_2",
          DateTime.new!(Date.add(monday, 1), ~T[09:00:00], "Etc/UTC"),
          DateTime.new!(Date.add(monday, 1), ~T[17:00:00], "Etc/UTC"),
          "emp_001"
        ),
        Factories.build_processed_time_entry(
          "entry_3",
          DateTime.new!(Date.add(monday, 2), ~T[09:00:00], "Etc/UTC"),
          DateTime.new!(Date.add(monday, 2), ~T[17:00:00], "Etc/UTC"),
          "emp_001"
        )
      ]

      rule = ComplianceRules.weekly_aggregation_rule()
      matched_facts = rule.pattern.(time_entries)

      assert length(matched_facts) == 3

      [aggregation] = rule.action.(matched_facts)

      assert {:weekly_hours, {"emp_001", ^monday},
              %{
                employee_id: "emp_001",
                week_start: ^monday,
                week_end: week_end,
                total_hours: 24.0,
                entries: ["entry_1", "entry_2", "entry_3"]
              }} = aggregation

      # Sunday
      assert week_end == Date.add(monday, 6)
    end

    test "groups entries by employee and week correctly" do
      monday = ~D[2024-01-01]
      next_monday = Date.add(monday, 7)

      # Create entries for two employees across two weeks
      time_entries = [
        # Employee 1, Week 1
        Factories.build_processed_time_entry(
          "e1_w1_1",
          DateTime.new!(monday, ~T[09:00:00], "Etc/UTC"),
          DateTime.new!(monday, ~T[17:00:00], "Etc/UTC"),
          "emp_001"
        ),
        Factories.build_processed_time_entry(
          "e1_w1_2",
          DateTime.new!(Date.add(monday, 1), ~T[09:00:00], "Etc/UTC"),
          DateTime.new!(Date.add(monday, 1), ~T[17:00:00], "Etc/UTC"),
          "emp_001"
        ),

        # Employee 1, Week 2  
        Factories.build_processed_time_entry(
          "e1_w2_1",
          DateTime.new!(next_monday, ~T[09:00:00], "Etc/UTC"),
          DateTime.new!(next_monday, ~T[17:00:00], "Etc/UTC"),
          "emp_001"
        ),

        # Employee 2, Week 1
        Factories.build_processed_time_entry(
          "e2_w1_1",
          DateTime.new!(monday, ~T[09:00:00], "Etc/UTC"),
          DateTime.new!(monday, ~T[17:00:00], "Etc/UTC"),
          "emp_002"
        )
      ]

      rule = ComplianceRules.weekly_aggregation_rule()
      aggregations = rule.action.(rule.pattern.(time_entries))

      # 3 unique (employee, week) combinations
      assert length(aggregations) == 3

      # Verify groupings
      keys = Enum.map(aggregations, fn {:weekly_hours, key, _} -> key end)
      assert {"emp_001", monday} in keys
      assert {"emp_001", next_monday} in keys
      assert {"emp_002", monday} in keys
    end

    test "filters out entries that already have aggregations" do
      monday = ~D[2024-01-01]

      time_entry =
        Factories.build_processed_time_entry(
          "entry_1",
          DateTime.new!(monday, ~T[09:00:00], "Etc/UTC"),
          DateTime.new!(monday, ~T[17:00:00], "Etc/UTC"),
          "emp_001"
        )

      existing_aggregation = Factories.build_weekly_hours("emp_001", monday, 40.0, ["entry_1"])

      all_facts = [time_entry, existing_aggregation]

      rule = ComplianceRules.weekly_aggregation_rule()
      matched_facts = rule.pattern.(all_facts)

      # Should not match entries that already have aggregations
      assert length(matched_facts) == 0
    end

    test "handles entries spanning multiple weeks" do
      # Friday of week 1
      friday = ~D[2024-01-05]
      # Saturday of week 1
      saturday = ~D[2024-01-06]
      # Monday of week 2 (new week starts)
      monday = ~D[2024-01-08]

      time_entries = [
        # Friday entry (week 1)
        Factories.build_processed_time_entry(
          "friday",
          DateTime.new!(friday, ~T[09:00:00], "Etc/UTC"),
          DateTime.new!(friday, ~T[17:00:00], "Etc/UTC"),
          "emp_001"
        ),

        # Saturday entry (still week 1 - Sunday ends the week)
        Factories.build_processed_time_entry(
          "saturday",
          DateTime.new!(saturday, ~T[09:00:00], "Etc/UTC"),
          DateTime.new!(saturday, ~T[17:00:00], "Etc/UTC"),
          "emp_001"
        ),

        # Monday entry (week 2)
        Factories.build_processed_time_entry(
          "monday",
          DateTime.new!(monday, ~T[09:00:00], "Etc/UTC"),
          DateTime.new!(monday, ~T[17:00:00], "Etc/UTC"),
          "emp_001"
        )
      ]

      rule = ComplianceRules.weekly_aggregation_rule()
      aggregations = rule.action.(rule.pattern.(time_entries))

      # Should create 2 aggregations (2 different weeks)
      assert length(aggregations) == 2

      week1_start = ComplianceRules.week_start_date(friday)
      week2_start = ComplianceRules.week_start_date(monday)

      keys = Enum.map(aggregations, fn {:weekly_hours, key, _} -> key end)
      assert {"emp_001", week1_start} in keys
      assert {"emp_001", week2_start} in keys
    end
  end

  describe "weekly compliance rule" do
    test "identifies compliant weeks" do
      monday = ~D[2024-01-01]
      weekly_aggregation = Factories.build_weekly_hours("emp_001", monday, 40.0, ["entry_1"])

      variables = %{"max_weekly_hours" => 48.0}
      rule = ComplianceRules.weekly_compliance_rule(variables)

      matched_facts = rule.pattern.([weekly_aggregation])
      assert length(matched_facts) == 1

      [compliance_result] = rule.action.(matched_facts)

      assert {:compliance_result, {"emp_001", ^monday},
              %{
                employee_id: "emp_001",
                week_start: ^monday,
                status: :compliant,
                threshold: 48.0,
                actual_value: 40.0,
                reason: "Within weekly hours limit (40.0 <= 48.0)"
              }} = compliance_result
    end

    test "identifies non-compliant weeks" do
      monday = ~D[2024-01-01]
      weekly_aggregation = Factories.build_weekly_hours("emp_001", monday, 55.0, ["entry_1"])

      variables = %{"max_weekly_hours" => 48.0}
      rule = ComplianceRules.weekly_compliance_rule(variables)

      [compliance_result] = rule.action.(rule.pattern.([weekly_aggregation]))

      assert {:compliance_result, {"emp_001", ^monday},
              %{
                status: :non_compliant,
                threshold: 48.0,
                actual_value: 55.0,
                reason: "Exceeded maximum weekly hours (55.0 > 48.0)"
              }} = compliance_result
    end

    test "handles custom threshold values" do
      monday = ~D[2024-01-01]
      weekly_aggregation = Factories.build_weekly_hours("emp_001", monday, 45.0, ["entry_1"])

      # Lower threshold makes this non-compliant
      variables = %{"max_weekly_hours" => 40.0}
      rule = ComplianceRules.weekly_compliance_rule(variables)

      [compliance_result] = rule.action.(rule.pattern.([weekly_aggregation]))

      assert {:compliance_result, _,
              %{
                status: :non_compliant,
                threshold: 40.0,
                actual_value: 45.0
              }} = compliance_result
    end

    test "uses default threshold when not specified" do
      monday = ~D[2024-01-01]
      weekly_aggregation = Factories.build_weekly_hours("emp_001", monday, 50.0, ["entry_1"])

      # No variables provided - should use default 48.0
      rule = ComplianceRules.weekly_compliance_rule()

      [compliance_result] = rule.action.(rule.pattern.([weekly_aggregation]))

      assert {:compliance_result, _,
              %{
                status: :non_compliant,
                threshold: 48.0
              }} = compliance_result
    end

    test "filters out already checked aggregations" do
      monday = ~D[2024-01-01]
      weekly_aggregation = Factories.build_weekly_hours("emp_001", monday, 50.0, ["entry_1"])

      existing_result =
        Factories.build_compliance_result("emp_001", monday, :non_compliant, 48.0, 50.0)

      all_facts = [weekly_aggregation, existing_result]

      rule = ComplianceRules.weekly_compliance_rule()
      matched_facts = rule.pattern.(all_facts)

      # Should not match aggregations that already have compliance results
      assert length(matched_facts) == 0
    end

    test "processes multiple employees and weeks" do
      monday = ~D[2024-01-01]
      next_monday = Date.add(monday, 7)

      aggregations = [
        # compliant
        Factories.build_weekly_hours("emp_001", monday, 45.0, ["e1_1"]),
        # non-compliant
        Factories.build_weekly_hours("emp_001", next_monday, 55.0, ["e1_2"]),
        # compliant
        Factories.build_weekly_hours("emp_002", monday, 30.0, ["e2_1"]),
        # non-compliant
        Factories.build_weekly_hours("emp_002", next_monday, 60.0, ["e2_2"])
      ]

      variables = %{"max_weekly_hours" => 48.0}
      rule = ComplianceRules.weekly_compliance_rule(variables)

      compliance_results = rule.action.(rule.pattern.(aggregations))
      assert length(compliance_results) == 4

      # Count violations
      violations =
        Enum.count(compliance_results, fn {:compliance_result, _, %{status: status}} ->
          status == :non_compliant
        end)

      assert violations == 2
    end
  end

  describe "process_compliance_check/2" do
    test "processes complete compliance workflow" do
      monday = ~D[2024-01-01]

      # Create entries that will result in compliance violation
      time_entries = [
        Factories.build_processed_time_entry(
          "entry_1",
          DateTime.new!(monday, ~T[09:00:00], "Etc/UTC"),
          # 12 hours
          DateTime.new!(monday, ~T[21:00:00], "Etc/UTC"),
          "emp_001"
        ),
        Factories.build_processed_time_entry(
          "entry_2",
          DateTime.new!(Date.add(monday, 1), ~T[09:00:00], "Etc/UTC"),
          # 12 hours
          DateTime.new!(Date.add(monday, 1), ~T[21:00:00], "Etc/UTC"),
          "emp_001"
        ),
        Factories.build_processed_time_entry(
          "entry_3",
          DateTime.new!(Date.add(monday, 2), ~T[09:00:00], "Etc/UTC"),
          # 12 hours
          DateTime.new!(Date.add(monday, 2), ~T[21:00:00], "Etc/UTC"),
          "emp_001"
        ),
        Factories.build_processed_time_entry(
          "entry_4",
          DateTime.new!(Date.add(monday, 3), ~T[09:00:00], "Etc/UTC"),
          # 12 hours
          DateTime.new!(Date.add(monday, 3), ~T[21:00:00], "Etc/UTC"),
          "emp_001"
        ),
        Factories.build_processed_time_entry(
          "entry_5",
          DateTime.new!(Date.add(monday, 4), ~T[09:00:00], "Etc/UTC"),
          # 6 hours
          DateTime.new!(Date.add(monday, 4), ~T[15:00:00], "Etc/UTC"),
          "emp_001"
        )
      ]

      # Total: 54 hours > 48 hour threshold

      rule_spec = Factories.build_compliance_rule_spec(48.0)

      result = ComplianceRules.process_compliance_check(time_entries, rule_spec)

      assert %{
               weekly_aggregations: aggregations,
               compliance_results: compliance_results,
               summary: summary
             } = result

      # Should have 1 aggregation (all entries in same week)
      assert length(aggregations) == 1
      [aggregation] = aggregations
      assert {:weekly_hours, {"emp_001", ^monday}, %{total_hours: 54.0}} = aggregation

      # Should have 1 compliance result (non-compliant)
      assert length(compliance_results) == 1
      [compliance_result] = compliance_results

      assert {:compliance_result, _, %{status: :non_compliant, actual_value: 54.0}} =
               compliance_result

      # Summary should reflect the violation
      assert %{
               total_weeks_checked: 1,
               total_employees: 1,
               compliance_violations: 1,
               employees_with_violations: 1,
               compliance_rate_percent: +0.0
             } = summary
    end

    test "handles multiple employees with mixed compliance" do
      monday = ~D[2024-01-01]

      # Employee 1: Compliant (40 hours)
      emp1_entries = [
        Factories.build_processed_time_entry(
          "e1_1",
          DateTime.new!(monday, ~T[09:00:00], "Etc/UTC"),
          # 8 hours
          DateTime.new!(monday, ~T[17:00:00], "Etc/UTC"),
          "emp_001"
        ),
        Factories.build_processed_time_entry(
          "e1_2",
          DateTime.new!(Date.add(monday, 1), ~T[09:00:00], "Etc/UTC"),
          # 8 hours
          DateTime.new!(Date.add(monday, 1), ~T[17:00:00], "Etc/UTC"),
          "emp_001"
        ),
        Factories.build_processed_time_entry(
          "e1_3",
          DateTime.new!(Date.add(monday, 2), ~T[09:00:00], "Etc/UTC"),
          # 8 hours
          DateTime.new!(Date.add(monday, 2), ~T[17:00:00], "Etc/UTC"),
          "emp_001"
        ),
        Factories.build_processed_time_entry(
          "e1_4",
          DateTime.new!(Date.add(monday, 3), ~T[09:00:00], "Etc/UTC"),
          # 8 hours
          DateTime.new!(Date.add(monday, 3), ~T[17:00:00], "Etc/UTC"),
          "emp_001"
        ),
        Factories.build_processed_time_entry(
          "e1_5",
          DateTime.new!(Date.add(monday, 4), ~T[09:00:00], "Etc/UTC"),
          # 8 hours
          DateTime.new!(Date.add(monday, 4), ~T[17:00:00], "Etc/UTC"),
          "emp_001"
        )
      ]

      # Employee 2: Non-compliant (60 hours)
      emp2_entries = [
        Factories.build_processed_time_entry(
          "e2_1",
          DateTime.new!(monday, ~T[08:00:00], "Etc/UTC"),
          # 12 hours
          DateTime.new!(monday, ~T[20:00:00], "Etc/UTC"),
          "emp_002"
        ),
        Factories.build_processed_time_entry(
          "e2_2",
          DateTime.new!(Date.add(monday, 1), ~T[08:00:00], "Etc/UTC"),
          # 12 hours
          DateTime.new!(Date.add(monday, 1), ~T[20:00:00], "Etc/UTC"),
          "emp_002"
        ),
        Factories.build_processed_time_entry(
          "e2_3",
          DateTime.new!(Date.add(monday, 2), ~T[08:00:00], "Etc/UTC"),
          # 12 hours
          DateTime.new!(Date.add(monday, 2), ~T[20:00:00], "Etc/UTC"),
          "emp_002"
        ),
        Factories.build_processed_time_entry(
          "e2_4",
          DateTime.new!(Date.add(monday, 3), ~T[08:00:00], "Etc/UTC"),
          # 12 hours
          DateTime.new!(Date.add(monday, 3), ~T[20:00:00], "Etc/UTC"),
          "emp_002"
        ),
        Factories.build_processed_time_entry(
          "e2_5",
          DateTime.new!(Date.add(monday, 4), ~T[08:00:00], "Etc/UTC"),
          # 12 hours
          DateTime.new!(Date.add(monday, 4), ~T[20:00:00], "Etc/UTC"),
          "emp_002"
        )
      ]

      all_entries = emp1_entries ++ emp2_entries
      rule_spec = Factories.build_compliance_rule_spec(48.0)

      result = ComplianceRules.process_compliance_check(all_entries, rule_spec)

      assert %{summary: summary} = result
      assert summary.total_employees == 2
      assert summary.compliance_violations == 1
      assert summary.employees_with_violations == 1
      assert summary.compliance_rate_percent == 50.0
    end

    test "uses default threshold when no rule spec provided" do
      monday = ~D[2024-01-01]

      _time_entries = [
        Factories.build_processed_time_entry(
          "entry_1",
          DateTime.new!(monday, ~T[09:00:00], "Etc/UTC"),
          # 10 hours per day
          DateTime.new!(monday, ~T[19:00:00], "Etc/UTC"),
          "emp_001"
        )
      ]

      # Replicate for 5 days = 50 hours total
      all_entries =
        Enum.flat_map(0..4, fn day ->
          date = Date.add(monday, day)

          [
            Factories.build_processed_time_entry(
              "entry_#{day}",
              DateTime.new!(date, ~T[09:00:00], "Etc/UTC"),
              DateTime.new!(date, ~T[19:00:00], "Etc/UTC"),
              "emp_001"
            )
          ]
        end)

      # Default threshold should be 48.0
      result = ComplianceRules.process_compliance_check(all_entries)

      assert %{compliance_results: [compliance_result]} = result

      assert {:compliance_result, _, %{status: :non_compliant, threshold: 48.0}} =
               compliance_result
    end
  end

  describe "week boundary calculations" do
    test "correctly identifies Monday as week start" do
      test_dates = [
        # Monday
        {~D[2024-01-01], 1},
        # Tuesday  
        {~D[2024-01-02], 2},
        # Sunday
        {~D[2024-01-07], 7},
        # Monday (next week)
        {~D[2024-01-08], 1}
      ]

      Enum.each(test_dates, fn {date, expected_day} ->
        actual_day = Date.day_of_week(date)

        assert actual_day == expected_day,
               "Date #{date} should be day #{expected_day}, got #{actual_day}"
      end)
    end

    test "week_start_date returns correct Monday" do
      test_cases = [
        # Monday -> Monday
        {~D[2024-01-01], ~D[2024-01-01]},
        # Wednesday -> Previous Monday
        {~D[2024-01-03], ~D[2024-01-01]},
        # Sunday -> Previous Monday
        {~D[2024-01-07], ~D[2024-01-01]},
        # Monday -> Monday
        {~D[2024-01-08], ~D[2024-01-08]}
      ]

      Enum.each(test_cases, fn {input_date, expected_monday} ->
        actual_monday = ComplianceRules.week_start_date(input_date)

        assert actual_monday == expected_monday,
               "Week start for #{input_date} should be #{expected_monday}, got #{actual_monday}"

        assert Date.day_of_week(actual_monday) == 1, "Week start should always be Monday"
      end)
    end

    test "week_dates returns correct Monday-Sunday sequence" do
      monday = ~D[2024-01-01]
      week_dates = ComplianceRules.week_dates(monday)

      assert length(week_dates) == 7

      expected_dates = [
        # Monday
        ~D[2024-01-01],
        # Tuesday
        ~D[2024-01-02],
        # Wednesday
        ~D[2024-01-03],
        # Thursday
        ~D[2024-01-04],
        # Friday
        ~D[2024-01-05],
        # Saturday
        ~D[2024-01-06],
        # Sunday
        ~D[2024-01-07]
      ]

      assert week_dates == expected_dates
    end

    test "validates entries across year boundary" do
      # Test week spanning New Year
      # Monday before New Year
      december_monday = ~D[2023-12-25]

      # Friday
      time_entries = ComplianceTestHelpers.create_cross_week_entries("emp_001", ~D[2023-12-29])

      result = ComplianceRules.aggregate_entries_by_week(time_entries)

      # Should create 2 separate weekly aggregations
      assert length(result) == 2

      keys = Enum.map(result, fn {:weekly_hours, key, _} -> key end)
      assert {"emp_001", december_monday} in keys
      # Next Monday (New Year)
      assert {"emp_001", ~D[2024-01-01]} in keys
    end

    test "handles leap year February correctly" do
      # Test February 29, 2024 (leap year)
      # Thursday
      leap_day = ~D[2024-02-29]
      expected_monday = ~D[2024-02-26]

      actual_monday = ComplianceRules.week_start_date(leap_day)
      assert actual_monday == expected_monday
    end
  end

  describe "compliance analysis and reporting" do
    test "finds violations within date range" do
      base_monday = ~D[2024-01-01]

      compliance_results = [
        Factories.build_compliance_result("emp_001", base_monday, :non_compliant, 48.0, 55.0),
        Factories.build_compliance_result(
          "emp_001",
          Date.add(base_monday, 7),
          :compliant,
          48.0,
          40.0
        ),
        Factories.build_compliance_result(
          "emp_001",
          Date.add(base_monday, 14),
          :non_compliant,
          48.0,
          60.0
        ),
        Factories.build_compliance_result("emp_002", base_monday, :non_compliant, 48.0, 50.0)
      ]

      # Find violations in first two weeks
      violations =
        ComplianceRules.find_violations_in_range(
          compliance_results,
          base_monday,
          Date.add(base_monday, 10)
        )

      # Two violations in range
      assert length(violations) == 2
    end

    test "groups compliance results by employee" do
      base_monday = ~D[2024-01-01]

      compliance_results = [
        Factories.build_compliance_result("emp_001", base_monday, :compliant, 48.0, 40.0),
        Factories.build_compliance_result(
          "emp_001",
          Date.add(base_monday, 7),
          :non_compliant,
          48.0,
          55.0
        ),
        Factories.build_compliance_result("emp_002", base_monday, :non_compliant, 48.0, 60.0)
      ]

      grouped = ComplianceRules.group_compliance_by_employee(compliance_results)

      assert Map.has_key?(grouped, "emp_001")
      assert Map.has_key?(grouped, "emp_002")
      assert length(Map.get(grouped, "emp_001")) == 2
      assert length(Map.get(grouped, "emp_002")) == 1
    end

    test "calculates employee compliance statistics" do
      base_monday = ~D[2024-01-01]

      compliance_results = [
        Factories.build_compliance_result("emp_001", base_monday, :compliant, 48.0, 40.0),
        Factories.build_compliance_result(
          "emp_001",
          Date.add(base_monday, 7),
          :non_compliant,
          48.0,
          55.0
        ),
        Factories.build_compliance_result(
          "emp_001",
          Date.add(base_monday, 14),
          :compliant,
          48.0,
          35.0
        ),
        Factories.build_compliance_result(
          "emp_001",
          Date.add(base_monday, 21),
          :non_compliant,
          48.0,
          52.0
        )
      ]

      stats = ComplianceRules.employee_compliance_stats(compliance_results, "emp_001")

      assert %{
               employee_id: "emp_001",
               total_weeks_checked: 4,
               compliance_violations: 2,
               compliance_rate_percent: 50.0
             } = stats
    end
  end

  describe "rule specification validation" do
    test "validates correct compliance rule spec" do
      rule_spec = Factories.build_compliance_rule_spec()

      assert ComplianceRules.valid_rule_spec?(rule_spec) == true
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
        %{"rules" => [%{"type" => "compliance"}]}
      ]

      Enum.each(invalid_specs, fn spec ->
        assert ComplianceRules.valid_rule_spec?(spec) == false
      end)
    end

    test "validates rule specifications using behaviour implementation" do
      # Test that the module properly validates its own supported rules
      valid_spec = %{
        "rules_to_run" => ["weekly_compliance"],
        "variables" => %{"max_weekly_hours" => 48.0}
      }

      assert ComplianceRules.valid_rule_spec?(valid_spec) == true

      # Test with unsupported rule names
      invalid_spec = %{
        "rules_to_run" => ["unsupported_compliance_rule"],
        "variables" => %{}
      }

      assert ComplianceRules.valid_rule_spec?(invalid_spec) == false
    end
  end

  describe "edge cases and error handling" do
    test "handles empty time entries list" do
      result = ComplianceRules.process_compliance_check([])

      assert %{
               weekly_aggregations: [],
               compliance_results: [],
               summary: %{
                 total_weeks_checked: 0,
                 total_employees: 0,
                 compliance_violations: 0,
                 employees_with_violations: 0,
                 compliance_rate_percent: 100.0
               }
             } = result
    end

    test "handles single day entry" do
      monday = ~D[2024-01-01]

      time_entry =
        Factories.build_processed_time_entry(
          "entry_1",
          DateTime.new!(monday, ~T[09:00:00], "Etc/UTC"),
          DateTime.new!(monday, ~T[17:00:00], "Etc/UTC"),
          "emp_001"
        )

      result = ComplianceRules.process_compliance_check([time_entry])

      assert %{
               weekly_aggregations: [aggregation],
               compliance_results: [compliance_result]
             } = result

      assert {:weekly_hours, _, %{total_hours: 8.0}} = aggregation
      assert {:compliance_result, _, %{status: :compliant}} = compliance_result
    end

    test "handles exact threshold boundary" do
      monday = ~D[2024-01-01]

      # Create exactly 48 hours (6 days * 8 hours)
      time_entries =
        Enum.map(0..5, fn day ->
          date = Date.add(monday, day)

          Factories.build_processed_time_entry(
            "entry_#{day}",
            DateTime.new!(date, ~T[09:00:00], "Etc/UTC"),
            DateTime.new!(date, ~T[17:00:00], "Etc/UTC"),
            "emp_001"
          )
        end)

      rule_spec = Factories.build_compliance_rule_spec(48.0)
      result = ComplianceRules.process_compliance_check(time_entries, rule_spec)

      assert %{compliance_results: [compliance_result]} = result

      assert {:compliance_result, _, %{status: :compliant, actual_value: 48.0}} =
               compliance_result
    end

    test "handles partial week for new employee" do
      # Employee starts on Wednesday
      wednesday = ~D[2024-01-03]
      entries = ComplianceTestHelpers.create_partial_week_scenario("emp_001", wednesday)

      rule_spec = Factories.build_compliance_rule_spec(48.0)
      result = ComplianceRules.process_compliance_check(entries, rule_spec)

      assert %{
               weekly_aggregations: [aggregation],
               compliance_results: [compliance_result]
             } = result

      # Should aggregate 3 days * 8 hours = 24 hours
      assert {:weekly_hours, _, %{total_hours: 24.0}} = aggregation
      assert {:compliance_result, _, %{status: :compliant}} = compliance_result
    end
  end

  describe "performance" do
    test "efficiently processes large datasets" do
      # Create 100 employees with 4 weeks of data each
      time_entries =
        for emp_id <- 1..100, week <- 0..3, day <- 0..6 do
          base_monday = ~D[2024-01-01]
          date = Date.add(base_monday, week * 7 + day)

          # Skip weekends for realism
          if Date.day_of_week(date) <= 5 do
            start_dt = DateTime.new!(date, ~T[09:00:00], "Etc/UTC")
            finish_dt = DateTime.add(start_dt, 8 * 3600, :second)

            Factories.build_processed_time_entry(
              "entry_#{emp_id}_#{week}_#{day}",
              start_dt,
              finish_dt,
              "emp_#{String.pad_leading(to_string(emp_id), 3, "0")}"
            )
          else
            nil
          end
        end
        |> Enum.reject(&is_nil/1)

      rule_spec = Factories.build_compliance_rule_spec(48.0)

      {process_time, result} =
        :timer.tc(fn ->
          ComplianceRules.process_compliance_check(time_entries, rule_spec)
        end)

      # Should process in reasonable time (< 2 seconds)
      # microseconds
      assert process_time < 2_000_000

      # Verify results
      assert %{summary: summary} = result
      assert summary.total_employees == 100
      # 100 employees * 4 weeks
      assert summary.total_weeks_checked == 400
    end
  end
end

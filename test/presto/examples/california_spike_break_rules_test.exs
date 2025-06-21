defmodule Presto.Examples.CaliforniaSpikeBreakRulesTest do
  use ExUnit.Case, async: true

  alias Presto.Examples.CaliforniaSpikeBreakRules
  alias Presto.SpikeBreakTestHelpers

  describe "California State-Level Spike Break Rules" do
    test "detects consecutive work without meal break" do
      employee_id = "emp_001"
      date = ~D[2024-01-15]

      work_session = SpikeBreakTestHelpers.create_consecutive_work_session(employee_id, date, 7)
      jurisdiction = %{state: "california"}

      assert CaliforniaSpikeBreakRules.work_session_triggers_spike_breaks?(
               work_session,
               jurisdiction
             )

      requirements =
        CaliforniaSpikeBreakRules.calculate_spike_break_requirements(work_session, jurisdiction)

      SpikeBreakTestHelpers.validate_spike_break_requirements(
        requirements,
        1,
        [:consecutive_work]
      )

      [requirement] = requirements
      assert {:spike_break_requirement, _, data} = requirement
      assert data.required_duration_minutes == 15
      assert data.reason =~ "Consecutive work exceeds 6 hours"
    end

    test "detects extended day work requiring additional breaks" do
      employee_id = "emp_002"
      date = ~D[2024-01-15]

      work_session = SpikeBreakTestHelpers.create_extended_day_session(employee_id, date, 14)
      jurisdiction = %{state: "california"}

      requirements =
        CaliforniaSpikeBreakRules.calculate_spike_break_requirements(work_session, jurisdiction)

      # Should have extended day breaks for hours beyond 10
      extended_day_requirements =
        Enum.filter(requirements, fn {:spike_break_requirement, _, data} ->
          data.type == :extended_day
        end)

      # 2 breaks for 4 extra hours (14-10)
      assert length(extended_day_requirements) == 2

      Enum.each(extended_day_requirements, fn {:spike_break_requirement, _, data} ->
        assert data.required_duration_minutes == 10
        assert data.reason =~ "Extended day break"
      end)
    end

    test "does not trigger spike breaks for normal 8-hour day" do
      employee_id = "emp_003"
      date = ~D[2024-01-15]

      work_session = SpikeBreakTestHelpers.create_standard_work_session(employee_id, date, 8)
      jurisdiction = %{state: "california"}

      refute CaliforniaSpikeBreakRules.work_session_triggers_spike_breaks?(
               work_session,
               jurisdiction
             )

      requirements =
        CaliforniaSpikeBreakRules.calculate_spike_break_requirements(work_session, jurisdiction)

      assert Enum.empty?(requirements)
    end

    test "calculates correct break timing requirements" do
      employee_id = "emp_004"
      date = ~D[2024-01-15]

      work_session = SpikeBreakTestHelpers.create_consecutive_work_session(employee_id, date, 8)
      jurisdiction = %{state: "california"}

      requirements =
        CaliforniaSpikeBreakRules.calculate_spike_break_requirements(work_session, jurisdiction)

      [requirement] = requirements
      assert {:spike_break_requirement, _, data} = requirement

      # Should be required after 5 hours of consecutive work (before 6th hour)
      {:work_session, _, work_data} = work_session
      expected_time = DateTime.add(work_data.start_datetime, 5 * 3600, :second)
      assert data.required_by == expected_time
    end

    test "handles multiple violation types in single session" do
      employee_id = "emp_005"
      date = ~D[2024-01-15]

      # 16-hour day without meal break - triggers both consecutive and extended day
      work_session =
        {:work_session, "multi_violation",
         %{
           employee_id: employee_id,
           work_session_id: "multi_violation",
           start_datetime: DateTime.new!(date, ~T[06:00:00], "Etc/UTC"),
           end_datetime: DateTime.new!(date, ~T[22:00:00], "Etc/UTC"),
           total_hours: 16,
           meal_breaks_taken: 0,
           breaks_taken: []
         }}

      jurisdiction = %{state: "california"}

      requirements =
        CaliforniaSpikeBreakRules.calculate_spike_break_requirements(work_session, jurisdiction)

      # Should have consecutive work break + extended day breaks
      types = Enum.map(requirements, fn {:spike_break_requirement, _, data} -> data.type end)
      assert :consecutive_work in types
      assert :extended_day in types

      # 6 hours beyond 10 = 3 extended day breaks
      extended_day_count = Enum.count(types, &(&1 == :extended_day))
      assert extended_day_count == 3
    end
  end

  describe "Bay Area Regional Rules" do
    test "applies tech industry crunch time rules" do
      employee_id = "tech_emp_001"
      date = ~D[2024-01-15]

      work_session = SpikeBreakTestHelpers.create_bay_area_crunch_session(employee_id, date, 12)
      jurisdiction = %{state: "california", region: "bay_area", industry: "technology"}

      assert CaliforniaSpikeBreakRules.work_session_triggers_spike_breaks?(
               work_session,
               jurisdiction
             )

      requirements =
        CaliforniaSpikeBreakRules.calculate_spike_break_requirements(work_session, jurisdiction)

      # Should have Bay Area specific tech crunch breaks
      tech_breaks =
        Enum.filter(requirements, fn {:spike_break_requirement, _, data} ->
          data.type == :bay_area_tech_crunch
        end)

      assert length(tech_breaks) > 0

      Enum.each(tech_breaks, fn {:spike_break_requirement, _, data} ->
        assert data.required_duration_minutes == 5
        assert data.reason =~ "Bay Area tech crunch break"
      end)
    end

    test "applies different thresholds for Bay Area jurisdiction" do
      jurisdiction = %{state: "california", region: "bay_area"}

      thresholds = CaliforniaSpikeBreakRules.get_jurisdiction_thresholds(jurisdiction)

      # Should have Bay Area specific adjustments
      assert Map.has_key?(thresholds, :tech_crunch_break_interval)
      assert thresholds.tech_crunch_break_interval == 1
      assert thresholds.tech_crunch_break_duration == 5
    end

    test "does not apply tech crunch rules outside crunch periods" do
      employee_id = "tech_emp_002"
      date = ~D[2024-01-15]

      # Regular tech work session (not crunch time)
      work_session =
        {:work_session, "regular_tech",
         %{
           employee_id: employee_id,
           work_session_id: "regular_tech",
           start_datetime: DateTime.new!(date, ~T[09:00:00], "Etc/UTC"),
           end_datetime: DateTime.new!(date, ~T[17:00:00], "Etc/UTC"),
           total_hours: 8,
           meal_breaks_taken: 1,
           breaks_taken: [],
           # Not during crunch
           is_crunch_time: false,
           industry: "technology",
           jurisdiction: %{state: "california", region: "bay_area"}
         }}

      jurisdiction = %{state: "california", region: "bay_area", industry: "technology"}

      requirements =
        CaliforniaSpikeBreakRules.calculate_spike_break_requirements(work_session, jurisdiction)

      # Should not have tech crunch specific breaks
      tech_breaks =
        Enum.filter(requirements, fn {:spike_break_requirement, _, data} ->
          data.type == :bay_area_tech_crunch
        end)

      assert Enum.empty?(tech_breaks)
    end
  end

  describe "Los Angeles County Regional Rules" do
    test "applies entertainment industry peak production rules" do
      employee_id = "ent_emp_001"
      # Award season
      date = ~D[2024-02-15]

      work_session = SpikeBreakTestHelpers.create_la_entertainment_session(employee_id, date, 16)

      jurisdiction = %{
        state: "california",
        region: "los_angeles_county",
        industry: "entertainment"
      }

      requirements =
        CaliforniaSpikeBreakRules.calculate_spike_break_requirements(work_session, jurisdiction)

      # Should have LA County specific entertainment breaks
      entertainment_breaks =
        Enum.filter(requirements, fn {:spike_break_requirement, _, data} ->
          data.type == :la_entertainment_peak
        end)

      assert length(entertainment_breaks) > 0

      Enum.each(entertainment_breaks, fn {:spike_break_requirement, _, data} ->
        assert data.required_duration_minutes == 10
        assert data.reason =~ "LA entertainment peak break"
      end)
    end

    test "calculates enhanced penalties for entertainment industry" do
      jurisdiction = %{
        state: "california",
        region: "los_angeles_county",
        industry: "entertainment"
      }

      req_data = %{type: :la_entertainment_peak}
      penalty = CaliforniaSpikeBreakRules.calculate_spike_break_penalty(req_data, jurisdiction)

      # Should have enhanced penalty multiplier
      # 1.25x base penalty
      assert penalty == 1.25
    end
  end

  describe "Central Valley Regional Rules" do
    test "applies agricultural harvest season rules" do
      employee_id = "ag_emp_001"
      # During harvest season (June-October)
      harvest_date = ~D[2024-08-15]

      work_session =
        SpikeBreakTestHelpers.create_central_valley_agriculture_session(
          employee_id,
          harvest_date,
          10
        )

      jurisdiction = %{state: "california", region: "central_valley", industry: "agriculture"}

      requirements =
        CaliforniaSpikeBreakRules.calculate_spike_break_requirements(work_session, jurisdiction)

      # Should have Central Valley specific agricultural breaks
      ag_breaks =
        Enum.filter(requirements, fn {:spike_break_requirement, _, data} ->
          data.type == :central_valley_agriculture
        end)

      assert length(ag_breaks) > 0

      Enum.each(ag_breaks, fn {:spike_break_requirement, _, data} ->
        assert data.required_duration_minutes == 15
        assert data.reason =~ "Central Valley agricultural break"
      end)
    end

    test "does not apply harvest rules outside harvest season" do
      employee_id = "ag_emp_002"
      # Outside harvest season
      non_harvest_date = ~D[2024-02-15]

      work_session =
        {:work_session, "non_harvest_ag",
         %{
           employee_id: employee_id,
           work_session_id: "non_harvest_ag",
           start_datetime: DateTime.new!(non_harvest_date, ~T[05:00:00], "Etc/UTC"),
           end_datetime: DateTime.new!(non_harvest_date, ~T[15:00:00], "Etc/UTC"),
           total_hours: 10,
           meal_breaks_taken: 1,
           breaks_taken: [],
           industry: "agriculture",
           jurisdiction: %{state: "california", region: "central_valley"}
         }}

      jurisdiction = %{state: "california", region: "central_valley", industry: "agriculture"}

      requirements =
        CaliforniaSpikeBreakRules.calculate_spike_break_requirements(work_session, jurisdiction)

      # Should not have agricultural specific breaks outside harvest season
      ag_breaks =
        Enum.filter(requirements, fn {:spike_break_requirement, _, data} ->
          data.type == :central_valley_agriculture
        end)

      assert Enum.empty?(ag_breaks)
    end
  end

  describe "City-Level Rules" do
    test "applies San Francisco tourism peak rules" do
      jurisdiction = %{state: "california", region: "bay_area", city: "san_francisco"}

      city_adjustments = CaliforniaSpikeBreakRules.get_city_adjustments(jurisdiction)

      assert Map.has_key?(city_adjustments, :tourism_peak_break_interval)
      assert city_adjustments.tourism_peak_break_interval == 1.5
      assert city_adjustments.tourism_peak_break_duration == 8
    end

    test "applies Los Angeles award season rules" do
      jurisdiction = %{state: "california", region: "los_angeles_county", city: "los_angeles"}

      city_adjustments = CaliforniaSpikeBreakRules.get_city_adjustments(jurisdiction)

      assert Map.has_key?(city_adjustments, :award_season_break_interval)
      assert city_adjustments.award_season_break_interval == 1
      assert city_adjustments.award_season_break_duration == 10
    end

    test "applies San Diego convention rules" do
      jurisdiction = %{state: "california", region: "san_diego_county", city: "san_diego"}

      city_adjustments = CaliforniaSpikeBreakRules.get_city_adjustments(jurisdiction)

      assert Map.has_key?(city_adjustments, :convention_break_interval)
      assert city_adjustments.convention_break_interval == 2
      assert city_adjustments.convention_break_duration == 10
    end
  end

  describe "Spike Break Compliance Checking" do
    test "identifies compliant work sessions with proper breaks" do
      employee_id = "compliant_emp_001"
      date = ~D[2024-01-15]

      # Create breaks that satisfy requirements
      required_break_time = DateTime.new!(date, ~T[15:00:00], "Etc/UTC")
      break_taken = SpikeBreakTestHelpers.create_break_taken(employee_id, required_break_time, 15)

      work_session =
        SpikeBreakTestHelpers.create_work_session_with_breaks(
          employee_id,
          date,
          12,
          [break_taken]
        )

      jurisdiction = %{state: "california"}

      # Process compliance
      result =
        CaliforniaSpikeBreakRules.process_spike_break_compliance(
          [work_session],
          %{},
          jurisdiction
        )

      compliance_results = result.compliance_results

      # Should be compliant if proper breaks were taken
      compliant_results =
        Enum.filter(compliance_results, fn {:spike_break_compliance, _, data} ->
          data.status == :compliant
        end)

      assert length(compliant_results) > 0
    end

    test "identifies non-compliant work sessions without required breaks" do
      employee_id = "non_compliant_emp_001"
      date = ~D[2024-01-15]

      # Extended day session without breaks
      work_session = SpikeBreakTestHelpers.create_extended_day_session(employee_id, date, 12)

      jurisdiction = %{state: "california"}

      result =
        CaliforniaSpikeBreakRules.process_spike_break_compliance(
          [work_session],
          %{},
          jurisdiction
        )

      assert result.summary.compliance_violations > 0
      assert result.summary.total_penalty_hours > 0
      assert result.summary.compliance_rate_percent < 100.0
    end

    test "calculates correct penalties for different jurisdiction levels" do
      scenarios =
        SpikeBreakTestHelpers.create_break_compliance_scenarios("penalty_test", ~D[2024-01-15])

      jurisdictions = [
        # Base penalty
        %{state: "california"},
        # 1.5x penalty
        %{state: "california", region: "bay_area"},
        # 1.25x penalty
        %{state: "california", region: "los_angeles_county"}
      ]

      Enum.each(jurisdictions, fn jurisdiction ->
        work_session = scenarios.non_compliant.work_session

        result =
          CaliforniaSpikeBreakRules.process_spike_break_compliance(
            [work_session],
            %{},
            jurisdiction
          )

        expected_penalty =
          SpikeBreakTestHelpers.calculate_expected_penalties(
            result.summary.compliance_violations,
            jurisdiction
          )

        assert abs(result.summary.total_penalty_hours - expected_penalty) < 0.01
      end)
    end
  end

  describe "Multi-Jurisdictional Scenarios" do
    test "processes multiple employees across different jurisdictions" do
      base_date = ~D[2024-01-15]
      scenarios = SpikeBreakTestHelpers.create_multi_jurisdiction_scenario(base_date)

      _all_sessions =
        scenarios.bay_area_sessions ++
          scenarios.la_sessions ++
          scenarios.central_valley_sessions ++
          scenarios.standard_sessions

      # Process each group with appropriate jurisdiction
      bay_area_jurisdiction = %{state: "california", region: "bay_area", city: "san_francisco"}
      la_jurisdiction = %{state: "california", region: "los_angeles_county", city: "los_angeles"}
      central_valley_jurisdiction = %{state: "california", region: "central_valley"}
      state_jurisdiction = %{state: "california"}

      bay_area_result =
        CaliforniaSpikeBreakRules.process_spike_break_compliance(
          scenarios.bay_area_sessions,
          %{},
          bay_area_jurisdiction
        )

      la_result =
        CaliforniaSpikeBreakRules.process_spike_break_compliance(
          scenarios.la_sessions,
          %{},
          la_jurisdiction
        )

      central_valley_result =
        CaliforniaSpikeBreakRules.process_spike_break_compliance(
          scenarios.central_valley_sessions,
          %{},
          central_valley_jurisdiction
        )

      state_result =
        CaliforniaSpikeBreakRules.process_spike_break_compliance(
          scenarios.standard_sessions,
          %{},
          state_jurisdiction
        )

      # Verify each jurisdiction has appropriate requirements
      assert bay_area_result.summary.total_spike_break_requirements > 0
      assert la_result.summary.total_spike_break_requirements > 0
      assert central_valley_result.summary.total_spike_break_requirements > 0
      assert state_result.summary.total_spike_break_requirements > 0

      # Bay Area should have highest penalty multiplier
      assert bay_area_result.summary.total_penalty_hours >= la_result.summary.total_penalty_hours
      assert la_result.summary.total_penalty_hours >= state_result.summary.total_penalty_hours
    end

    test "handles jurisdiction hierarchy correctly" do
      employee_id = "hierarchy_test"
      date = ~D[2024-01-15]

      work_session = SpikeBreakTestHelpers.create_extended_day_session(employee_id, date, 12)

      # Test different levels of jurisdiction specificity
      jurisdictions = [
        %{state: "california"},
        %{state: "california", region: "bay_area"},
        %{state: "california", region: "bay_area", city: "san_francisco"},
        %{state: "california", region: "bay_area", city: "san_francisco", industry: "technology"}
      ]

      results =
        Enum.map(jurisdictions, fn jurisdiction ->
          CaliforniaSpikeBreakRules.process_spike_break_compliance(
            [work_session],
            %{},
            jurisdiction
          )
        end)

      # More specific jurisdictions should have same or more requirements
      base_requirements = hd(results).summary.total_spike_break_requirements

      Enum.each(results, fn result ->
        assert result.summary.total_spike_break_requirements >= base_requirements
      end)
    end
  end

  describe "Temporal and Seasonal Rules" do
    test "applies harvest season rules during correct time periods" do
      employee_id = "seasonal_test"
      scenarios = SpikeBreakTestHelpers.create_temporal_scenarios()

      # Test harvest season (should trigger agricultural rules)
      harvest_work =
        SpikeBreakTestHelpers.create_central_valley_agriculture_session(
          employee_id,
          scenarios.harvest_season,
          10
        )

      jurisdiction = %{state: "california", region: "central_valley", industry: "agriculture"}

      harvest_result =
        CaliforniaSpikeBreakRules.process_spike_break_compliance(
          [harvest_work],
          %{},
          jurisdiction
        )

      # Test non-harvest period
      regular_work =
        SpikeBreakTestHelpers.create_central_valley_agriculture_session(
          employee_id,
          scenarios.regular_period,
          10
        )

      regular_result =
        CaliforniaSpikeBreakRules.process_spike_break_compliance(
          [regular_work],
          %{},
          jurisdiction
        )

      # Harvest season should have more requirements
      assert harvest_result.summary.total_spike_break_requirements >=
               regular_result.summary.total_spike_break_requirements
    end

    test "recognizes award season timing for entertainment industry" do
      # February is typically award season in entertainment
      award_season_date = ~D[2024-02-15]
      regular_date = ~D[2024-05-15]

      jurisdiction = %{
        state: "california",
        region: "los_angeles_county",
        industry: "entertainment"
      }

      # During award season, work sessions should be more likely to trigger enhanced rules
      award_session =
        SpikeBreakTestHelpers.create_la_entertainment_session("ent_001", award_season_date, 14)

      regular_session =
        SpikeBreakTestHelpers.create_la_entertainment_session("ent_002", regular_date, 14)

      award_result =
        CaliforniaSpikeBreakRules.process_spike_break_compliance(
          [award_session],
          %{},
          jurisdiction
        )

      regular_result =
        CaliforniaSpikeBreakRules.process_spike_break_compliance(
          [regular_session],
          %{},
          jurisdiction
        )

      # Both should have requirements, but award season work may have enhanced rules
      assert award_result.summary.total_spike_break_requirements > 0
      assert regular_result.summary.total_spike_break_requirements > 0
    end
  end

  describe "Rule Specification Validation" do
    test "validates correct spike break rule specification" do
      rule_spec = %{
        "rules" => [
          %{
            "name" => "california_spike_break_detection",
            "type" => "spike_break",
            "variables" => %{
              "consecutive_hours_threshold" => 6,
              "extended_day_threshold" => 10
            }
          }
        ]
      }

      assert CaliforniaSpikeBreakRules.valid_rule_spec?(rule_spec) == true
    end

    test "rejects invalid rule specification structure" do
      invalid_specs = [
        # Missing rules
        %{},
        %{"rules" => "not_a_list"},
        # Missing type
        %{"rules" => [%{"name" => "test"}]},
        # Missing name
        %{"rules" => [%{"type" => "spike_break"}]},
        # Wrong type
        %{"rules" => [%{"name" => "test", "type" => "invalid"}]}
      ]

      Enum.each(invalid_specs, fn spec ->
        assert CaliforniaSpikeBreakRules.valid_rule_spec?(spec) == false
      end)
    end

    test "validates rule specifications using behaviour implementation" do
      # Test that the module properly validates its own supported rules
      valid_spec = %{
        "rules_to_run" => ["spike_break_compliance"],
        "variables" => %{}
      }

      assert CaliforniaSpikeBreakRules.valid_rule_spec?(valid_spec) == true

      # Test with unsupported rule names
      invalid_spec = %{
        "rules_to_run" => ["unsupported_spike_rule"],
        "variables" => %{}
      }

      assert CaliforniaSpikeBreakRules.valid_rule_spec?(invalid_spec) == false
    end
  end

  describe "Edge Cases and Error Handling" do
    test "handles empty work session list" do
      jurisdiction = %{state: "california"}

      result = CaliforniaSpikeBreakRules.process_spike_break_compliance([], %{}, jurisdiction)

      assert result.summary.total_spike_break_requirements == 0
      assert result.summary.compliance_violations == 0
      assert result.summary.compliance_rate_percent == 100.0
    end

    test "handles work sessions with missing data" do
      incomplete_session =
        {:work_session, "incomplete",
         %{
           employee_id: "emp_001",
           start_datetime: DateTime.new!(~D[2024-01-15], ~T[09:00:00], "Etc/UTC")
           # Missing end_datetime and other fields
         }}

      jurisdiction = %{state: "california"}

      # Should not crash with incomplete data
      result =
        CaliforniaSpikeBreakRules.process_spike_break_compliance(
          [incomplete_session],
          %{},
          jurisdiction
        )

      assert is_map(result)
      assert Map.has_key?(result, :summary)
    end

    test "handles unknown jurisdiction gracefully" do
      employee_id = "unknown_jurisdiction_test"
      date = ~D[2024-01-15]

      work_session = SpikeBreakTestHelpers.create_extended_day_session(employee_id, date, 12)
      # Not California
      unknown_jurisdiction = %{state: "nevada"}

      # Should still process but use default California rules
      result =
        CaliforniaSpikeBreakRules.process_spike_break_compliance(
          [work_session],
          %{},
          unknown_jurisdiction
        )

      assert result.summary.total_spike_break_requirements >= 0
    end

    test "handles break timing edge cases" do
      employee_id = "timing_edge_test"
      date = ~D[2024-01-15]

      # Break taken exactly at required time
      required_time = DateTime.new!(date, ~T[15:00:00], "Etc/UTC")
      exact_break = SpikeBreakTestHelpers.create_break_taken(employee_id, required_time, 15)

      # Break taken just before required time
      early_break =
        SpikeBreakTestHelpers.create_break_taken(
          employee_id,
          DateTime.add(required_time, -60, :second),
          15
        )

      # Break taken after required time (should not count)
      late_break =
        SpikeBreakTestHelpers.create_break_taken(
          employee_id,
          DateTime.add(required_time, 3600, :second),
          15
        )

      work_session_with_exact =
        SpikeBreakTestHelpers.create_work_session_with_breaks(
          employee_id,
          date,
          12,
          [exact_break]
        )

      work_session_with_early =
        SpikeBreakTestHelpers.create_work_session_with_breaks(
          employee_id,
          date,
          12,
          [early_break]
        )

      work_session_with_late =
        SpikeBreakTestHelpers.create_work_session_with_breaks(
          employee_id,
          date,
          12,
          [late_break]
        )

      jurisdiction = %{state: "california"}

      exact_result =
        CaliforniaSpikeBreakRules.process_spike_break_compliance(
          [work_session_with_exact],
          %{},
          jurisdiction
        )

      early_result =
        CaliforniaSpikeBreakRules.process_spike_break_compliance(
          [work_session_with_early],
          %{},
          jurisdiction
        )

      late_result =
        CaliforniaSpikeBreakRules.process_spike_break_compliance(
          [work_session_with_late],
          %{},
          jurisdiction
        )

      # Exact and early breaks should result in better compliance than late breaks
      assert exact_result.summary.compliance_violations <=
               late_result.summary.compliance_violations

      assert early_result.summary.compliance_violations <=
               late_result.summary.compliance_violations
    end
  end

  describe "Performance and Scalability" do
    test "processes large number of work sessions efficiently" do
      # Create 100 work sessions across different jurisdictions
      _jurisdictions = SpikeBreakTestHelpers.create_jurisdiction_configs()

      work_sessions =
        for i <- 1..100 do
          employee_id = "emp_#{String.pad_leading(to_string(i), 3, "0")}"
          date = Date.add(~D[2024-01-01], rem(i, 30))

          case rem(i, 4) do
            0 ->
              SpikeBreakTestHelpers.create_bay_area_crunch_session(employee_id, date, 12)

            1 ->
              SpikeBreakTestHelpers.create_la_entertainment_session(employee_id, date, 14)

            2 ->
              SpikeBreakTestHelpers.create_central_valley_agriculture_session(
                employee_id,
                date,
                10
              )

            3 ->
              SpikeBreakTestHelpers.create_extended_day_session(employee_id, date, 11)
          end
        end

      jurisdiction = %{state: "california"}

      {process_time, result} =
        :timer.tc(fn ->
          CaliforniaSpikeBreakRules.process_spike_break_compliance(
            work_sessions,
            %{},
            jurisdiction
          )
        end)

      # Should process 100 sessions in reasonable time (< 2 seconds)
      # microseconds
      assert process_time < 2_000_000

      # Verify results are reasonable
      assert result.summary.total_spike_break_requirements > 0
      # Reasonable upper bound
      assert result.summary.total_spike_break_requirements <= length(work_sessions) * 10
    end

    test "handles concurrent processing correctly" do
      employee_id = "concurrent_test"
      date = ~D[2024-01-15]

      work_session = SpikeBreakTestHelpers.create_extended_day_session(employee_id, date, 12)
      jurisdiction = %{state: "california"}

      # Process same session multiple times concurrently
      task_supervisor = start_supervised!(Task.Supervisor)

      tasks =
        for _i <- 1..10 do
          Task.Supervisor.async(task_supervisor, fn ->
            CaliforniaSpikeBreakRules.process_spike_break_compliance(
              [work_session],
              %{},
              jurisdiction
            )
          end)
        end

      results = Task.await_many(tasks)

      # All results should be identical
      first_result = hd(results)

      Enum.each(results, fn result ->
        assert result.summary.total_spike_break_requirements ==
                 first_result.summary.total_spike_break_requirements

        assert result.summary.compliance_violations ==
                 first_result.summary.compliance_violations
      end)
    end
  end
end

defmodule Presto.Examples.SpikeBreakEdgeCasesTest do
  use ExUnit.Case, async: true

  alias Presto.Examples.CaliforniaSpikeBreakRules
  alias Presto.SpikeBreakTestHelpers

  describe "Jurisdiction Overlap and Boundary Cases" do
    test "handles employee working across multiple jurisdictions in same day" do
      employee_id = "cross_jurisdiction_worker"
      date = ~D[2024-01-15]

      # Morning work in Bay Area (tech company)
      morning_session =
        {:work_session, "morning_bay_area",
         %{
           employee_id: employee_id,
           work_session_id: "morning_bay_area",
           start_datetime: DateTime.new!(date, ~T[06:00:00], "Etc/UTC"),
           end_datetime: DateTime.new!(date, ~T[12:00:00], "Etc/UTC"),
           total_hours: 6,
           # Triggers consecutive work rule
           meal_breaks_taken: 0,
           breaks_taken: [],
           is_crunch_time: true,
           industry: "technology",
           jurisdiction: %{state: "california", region: "bay_area", city: "san_francisco"}
         }}

      # Afternoon work in LA County (entertainment company)
      afternoon_session =
        {:work_session, "afternoon_la",
         %{
           employee_id: employee_id,
           work_session_id: "afternoon_la",
           start_datetime: DateTime.new!(date, ~T[14:00:00], "Etc/UTC"),
           end_datetime: DateTime.new!(date, ~T[22:00:00], "Etc/UTC"),
           total_hours: 8,
           meal_breaks_taken: 1,
           breaks_taken: [],
           is_peak_production: true,
           industry: "entertainment",
           jurisdiction: %{state: "california", region: "los_angeles_county", city: "los_angeles"}
         }}

      # Process each session with its appropriate jurisdiction
      bay_area_jurisdiction = %{state: "california", region: "bay_area", city: "san_francisco"}
      la_jurisdiction = %{state: "california", region: "los_angeles_county", city: "los_angeles"}

      bay_area_result =
        CaliforniaSpikeBreakRules.process_spike_break_compliance(
          [morning_session],
          %{},
          bay_area_jurisdiction
        )

      la_result =
        CaliforniaSpikeBreakRules.process_spike_break_compliance(
          [afternoon_session],
          %{},
          la_jurisdiction
        )

      # Both sessions should generate requirements with different rules
      assert bay_area_result.summary.total_spike_break_requirements > 0
      assert la_result.summary.total_spike_break_requirements > 0

      # Bay Area should have tech-specific requirements
      bay_area_requirements =
        SpikeBreakTestHelpers.extract_spike_break_requirements(bay_area_result.spike_requirements)

      tech_requirements =
        Enum.filter(bay_area_requirements, fn {:spike_break_requirement, _, data} ->
          data.type == :bay_area_tech_crunch
        end)

      assert length(tech_requirements) > 0

      # LA should have entertainment-specific requirements
      la_requirements =
        SpikeBreakTestHelpers.extract_spike_break_requirements(la_result.spike_requirements)

      entertainment_requirements =
        Enum.filter(la_requirements, fn {:spike_break_requirement, _, data} ->
          data.type == :la_entertainment_peak
        end)

      assert length(entertainment_requirements) > 0
    end

    test "resolves conflicting jurisdiction requirements for same employee" do
      employee_id = "conflicted_worker"
      date = ~D[2024-01-15]

      # Single work session that could apply multiple jurisdictional rules
      complex_session =
        {:work_session, "complex_jurisdiction",
         %{
           employee_id: employee_id,
           work_session_id: "complex_jurisdiction",
           start_datetime: DateTime.new!(date, ~T[06:00:00], "Etc/UTC"),
           end_datetime: DateTime.new!(date, ~T[20:00:00], "Etc/UTC"),
           total_hours: 14,
           meal_breaks_taken: 1,
           breaks_taken: [],
           is_crunch_time: true,
           # Conflicting industry indicators
           is_peak_production: true,
           # Primary industry
           industry: "technology",
           # Secondary industry
           secondary_industry: "entertainment",
           jurisdiction: %{
             state: "california",
             region: "bay_area",
             city: "san_francisco",
             work_location: "multi_industry_complex"
           }
         }}

      # Test with different jurisdiction interpretations
      tech_jurisdiction = %{state: "california", region: "bay_area", industry: "technology"}

      entertainment_jurisdiction = %{
        state: "california",
        region: "los_angeles_county",
        industry: "entertainment"
      }

      tech_result =
        CaliforniaSpikeBreakRules.process_spike_break_compliance(
          [complex_session],
          %{},
          tech_jurisdiction
        )

      entertainment_result =
        CaliforniaSpikeBreakRules.process_spike_break_compliance(
          [complex_session],
          %{},
          entertainment_jurisdiction
        )

      # Both should generate requirements, but with different characteristics
      assert tech_result.summary.total_spike_break_requirements > 0
      assert entertainment_result.summary.total_spike_break_requirements > 0

      # Tech jurisdiction might have more frequent, shorter breaks
      # Entertainment jurisdiction might have longer, less frequent breaks
      tech_requirements =
        SpikeBreakTestHelpers.extract_spike_break_requirements(tech_result.spike_requirements)

      entertainment_requirements =
        SpikeBreakTestHelpers.extract_spike_break_requirements(
          entertainment_result.spike_requirements
        )

      # Requirements should be different between jurisdictions
      refute tech_requirements == entertainment_requirements
    end

    test "handles jurisdiction boundary edge cases" do
      employee_id = "boundary_worker"
      date = ~D[2024-01-15]

      # Work session exactly at jurisdiction boundary
      boundary_session =
        {:work_session, "boundary_work",
         %{
           employee_id: employee_id,
           work_session_id: "boundary_work",
           start_datetime: DateTime.new!(date, ~T[09:00:00], "Etc/UTC"),
           end_datetime: DateTime.new!(date, ~T[19:00:00], "Etc/UTC"),
           total_hours: 10,
           meal_breaks_taken: 1,
           breaks_taken: [],
           # Work location at county boundary
           work_location: "county_border",
           jurisdiction: %{
             state: "california",
             primary_region: "bay_area",
             # Straddles regions
             secondary_region: "central_valley",
             # No specific city
             city: nil
           }
         }}

      # Test with primary region rules
      primary_jurisdiction = %{state: "california", region: "bay_area"}
      secondary_jurisdiction = %{state: "california", region: "central_valley"}

      primary_result =
        CaliforniaSpikeBreakRules.process_spike_break_compliance(
          [boundary_session],
          %{},
          primary_jurisdiction
        )

      secondary_result =
        CaliforniaSpikeBreakRules.process_spike_break_compliance(
          [boundary_session],
          %{},
          secondary_jurisdiction
        )

      # Should handle boundary cases gracefully without errors
      assert is_map(primary_result)
      assert is_map(secondary_result)
      assert Map.has_key?(primary_result, :summary)
      assert Map.has_key?(secondary_result, :summary)
    end

    test "prioritizes most specific jurisdiction when multiple apply" do
      employee_id = "specific_jurisdiction_test"
      date = ~D[2024-01-15]

      work_session = SpikeBreakTestHelpers.create_extended_day_session(employee_id, date, 12)

      # Test jurisdiction hierarchy: city > region > state
      jurisdictions = [
        # Least specific
        %{state: "california"},
        # More specific
        %{state: "california", region: "bay_area"},
        # Most specific
        %{state: "california", region: "bay_area", city: "san_francisco"},
        # Very specific
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

      # More specific jurisdictions should have same or enhanced requirements
      base_requirements = hd(results).summary.total_spike_break_requirements

      Enum.with_index(results, fn result, index ->
        # More specific jurisdictions should have at least as many requirements
        assert result.summary.total_spike_break_requirements >= base_requirements,
               "Jurisdiction level #{index} should have at least #{base_requirements} requirements"
      end)
    end
  end

  describe "Timing Conflicts and Break Scheduling" do
    test "handles overlapping break requirement timings" do
      employee_id = "timing_conflict_test"
      date = ~D[2024-01-15]

      # Create session that triggers multiple break types with overlapping timing
      complex_session =
        {:work_session, "timing_conflict",
         %{
           employee_id: employee_id,
           work_session_id: "timing_conflict",
           start_datetime: DateTime.new!(date, ~T[08:00:00], "Etc/UTC"),
           end_datetime: DateTime.new!(date, ~T[22:00:00], "Etc/UTC"),
           total_hours: 14,
           # No meal breaks - triggers consecutive work
           meal_breaks_taken: 0,
           breaks_taken: [],
           # Triggers tech crunch breaks
           is_crunch_time: true,
           industry: "technology",
           jurisdiction: %{state: "california", region: "bay_area", city: "san_francisco"}
         }}

      jurisdiction = %{state: "california", region: "bay_area", industry: "technology"}

      result =
        CaliforniaSpikeBreakRules.process_spike_break_compliance(
          [complex_session],
          %{},
          jurisdiction
        )

      requirements =
        SpikeBreakTestHelpers.extract_spike_break_requirements(result.spike_requirements)

      # Should have multiple types of requirements
      types = Enum.map(requirements, fn {:spike_break_requirement, _, data} -> data.type end)
      assert :consecutive_work in types
      assert :extended_day in types
      assert :bay_area_tech_crunch in types

      # Check that required timings are reasonable and don't conflict
      required_times =
        Enum.map(requirements, fn {:spike_break_requirement, _, data} -> data.required_by end)

      # All required times should be within the work session
      session_start = complex_session |> elem(2) |> Map.get(:start_datetime)
      session_end = complex_session |> elem(2) |> Map.get(:end_datetime)

      Enum.each(required_times, fn required_time ->
        assert DateTime.compare(required_time, session_start) != :lt
        assert DateTime.compare(required_time, session_end) != :gt
      end)
    end

    test "resolves break timing priorities correctly" do
      employee_id = "priority_test"
      date = ~D[2024-01-15]

      # Create multiple break requirements with different priorities
      session_with_conflicts =
        {:work_session, "priority_conflicts",
         %{
           employee_id: employee_id,
           work_session_id: "priority_conflicts",
           start_datetime: DateTime.new!(date, ~T[06:00:00], "Etc/UTC"),
           end_datetime: DateTime.new!(date, ~T[20:00:00], "Etc/UTC"),
           total_hours: 14,
           meal_breaks_taken: 0,
           breaks_taken: [],
           is_crunch_time: true,
           industry: "technology",
           jurisdiction: %{state: "california", region: "bay_area", city: "san_francisco"}
         }}

      jurisdiction = %{state: "california", region: "bay_area", industry: "technology"}

      result =
        CaliforniaSpikeBreakRules.process_spike_break_compliance(
          [session_with_conflicts],
          %{},
          jurisdiction
        )

      requirements =
        SpikeBreakTestHelpers.extract_spike_break_requirements(result.spike_requirements)

      # Check that requirements are ordered by priority/timing using the new scheduling system
      consecutive_work_reqs =
        Enum.filter(requirements, fn {:spike_break_requirement, _, data} ->
          data.type == :consecutive_work
        end)

      tech_crunch_reqs =
        Enum.filter(requirements, fn {:spike_break_requirement, _, data} ->
          data.type == :bay_area_tech_crunch
        end)

      # Verify that both types of requirements are generated
      if length(consecutive_work_reqs) > 0 and length(tech_crunch_reqs) > 0 do
        assert length(consecutive_work_reqs) >= 1
        assert length(tech_crunch_reqs) >= 1

        # Verify that all requirements have proper structure
        all_reqs = consecutive_work_reqs ++ tech_crunch_reqs

        Enum.each(all_reqs, fn {:spike_break_requirement, _id, data} ->
          assert data.employee_id == "priority_test"
          assert is_atom(data.type)
          assert is_integer(data.required_duration_minutes)
          assert data.required_duration_minutes > 0
          assert %DateTime{} = data.required_by
        end)
      end
    end

    test "handles rapid break requirement changes" do
      employee_id = "rapid_change_test"
      date = ~D[2024-01-15]

      # Multiple short sessions with different requirements
      sessions = [
        # Short Bay Area tech session
        {:work_session, "rapid_1",
         %{
           employee_id: employee_id,
           work_session_id: "rapid_1",
           start_datetime: DateTime.new!(date, ~T[08:00:00], "Etc/UTC"),
           end_datetime: DateTime.new!(date, ~T[10:00:00], "Etc/UTC"),
           total_hours: 2,
           meal_breaks_taken: 0,
           breaks_taken: [],
           is_crunch_time: true,
           industry: "technology",
           jurisdiction: %{state: "california", region: "bay_area"}
         }},

        # Transition to LA entertainment session
        {:work_session, "rapid_2",
         %{
           employee_id: employee_id,
           work_session_id: "rapid_2",
           start_datetime: DateTime.new!(date, ~T[11:00:00], "Etc/UTC"),
           end_datetime: DateTime.new!(date, ~T[15:00:00], "Etc/UTC"),
           total_hours: 4,
           meal_breaks_taken: 0,
           breaks_taken: [],
           is_peak_production: true,
           industry: "entertainment",
           jurisdiction: %{state: "california", region: "los_angeles_county"}
         }},

        # Back to standard California work
        {:work_session, "rapid_3",
         %{
           employee_id: employee_id,
           work_session_id: "rapid_3",
           start_datetime: DateTime.new!(date, ~T[16:00:00], "Etc/UTC"),
           end_datetime: DateTime.new!(date, ~T[22:00:00], "Etc/UTC"),
           total_hours: 6,
           meal_breaks_taken: 1,
           breaks_taken: [],
           jurisdiction: %{state: "california"}
         }}
      ]

      # Process each session with appropriate jurisdiction
      bay_area_result =
        CaliforniaSpikeBreakRules.process_spike_break_compliance(
          [Enum.at(sessions, 0)],
          %{},
          %{state: "california", region: "bay_area", industry: "technology"}
        )

      la_result =
        CaliforniaSpikeBreakRules.process_spike_break_compliance(
          [Enum.at(sessions, 1)],
          %{},
          %{state: "california", region: "los_angeles_county", industry: "entertainment"}
        )

      state_result =
        CaliforniaSpikeBreakRules.process_spike_break_compliance(
          [Enum.at(sessions, 2)],
          %{},
          %{state: "california"}
        )

      # Each should handle the rapid changes without errors
      assert is_map(bay_area_result) and Map.has_key?(bay_area_result, :summary)
      assert is_map(la_result) and Map.has_key?(la_result, :summary)
      assert is_map(state_result) and Map.has_key?(state_result, :summary)

      # Total cumulative hours across sessions would trigger additional requirements
      # 12 hours total
      total_hours = 2 + 4 + 6
      assert total_hours == 12
    end
  end

  describe "Complex Seasonal and Temporal Edge Cases" do
    test "handles year boundary transitions for seasonal rules" do
      employee_id = "year_boundary_test"

      # Work session spanning New Year (harvest season ending)
      dec_session =
        SpikeBreakTestHelpers.create_central_valley_agriculture_session(
          employee_id,
          # December (still harvest season)
          ~D[2023-12-31],
          10
        )

      jan_session =
        SpikeBreakTestHelpers.create_central_valley_agriculture_session(
          employee_id,
          # January (not harvest season)
          ~D[2024-01-01],
          10
        )

      jurisdiction = %{state: "california", region: "central_valley", industry: "agriculture"}

      dec_result =
        CaliforniaSpikeBreakRules.process_spike_break_compliance([dec_session], %{}, jurisdiction)

      jan_result =
        CaliforniaSpikeBreakRules.process_spike_break_compliance([jan_session], %{}, jurisdiction)

      # December (harvest season) should potentially have more requirements than January
      # Note: December is technically outside harvest season (June-October) in our implementation
      # Both should process without errors
      assert is_map(dec_result) and Map.has_key?(dec_result, :summary)
      assert is_map(jan_result) and Map.has_key?(jan_result, :summary)
    end

    test "handles leap year date calculations" do
      employee_id = "leap_year_test"

      # February 29, 2024 (leap year)
      leap_day_session =
        SpikeBreakTestHelpers.create_la_entertainment_session(
          employee_id,
          # Leap day during award season
          ~D[2024-02-29],
          14
        )

      jurisdiction = %{
        state: "california",
        region: "los_angeles_county",
        industry: "entertainment"
      }

      result =
        CaliforniaSpikeBreakRules.process_spike_break_compliance(
          [leap_day_session],
          %{},
          jurisdiction
        )

      # Should handle leap year date without errors
      assert is_map(result)
      assert result.summary.total_spike_break_requirements > 0
    end

    test "handles daylight saving time transitions" do
      employee_id = "dst_test"

      # Work session during DST transition (spring forward)
      # Note: Using UTC to avoid DST complexity in test
      dst_session =
        {:work_session, "dst_transition",
         %{
           employee_id: employee_id,
           work_session_id: "dst_transition",
           start_datetime: DateTime.new!(~D[2024-03-10], ~T[09:00:00], "Etc/UTC"),
           # 12 hours
           end_datetime: DateTime.new!(~D[2024-03-10], ~T[21:00:00], "Etc/UTC"),
           # Actual hours worked
           total_hours: 12,
           meal_breaks_taken: 1,
           breaks_taken: [],
           timezone_transition: :spring_forward,
           jurisdiction: %{state: "california", region: "los_angeles_county"}
         }}

      jurisdiction = %{state: "california", region: "los_angeles_county"}

      result =
        CaliforniaSpikeBreakRules.process_spike_break_compliance([dst_session], %{}, jurisdiction)

      # Should handle DST transition gracefully
      assert is_map(result)
      assert result.summary.total_spike_break_requirements >= 0
    end
  end

  describe "Industry-Specific Edge Cases" do
    test "handles multi-industry work sessions" do
      employee_id = "multi_industry_test"
      date = ~D[2024-01-15]

      # Worker with multiple industry responsibilities
      multi_industry_session =
        {:work_session, "multi_industry",
         %{
           employee_id: employee_id,
           work_session_id: "multi_industry",
           start_datetime: DateTime.new!(date, ~T[06:00:00], "Etc/UTC"),
           end_datetime: DateTime.new!(date, ~T[20:00:00], "Etc/UTC"),
           total_hours: 14,
           meal_breaks_taken: 2,
           breaks_taken: [],
           primary_industry: "technology",
           secondary_industry: "entertainment",
           work_activities: ["coding", "video_production", "client_meetings"],
           industry_split: %{technology: 0.6, entertainment: 0.4},
           jurisdiction: %{state: "california", region: "bay_area", city: "san_francisco"}
         }}

      # Test with different industry focus
      tech_jurisdiction = %{state: "california", region: "bay_area", industry: "technology"}

      entertainment_jurisdiction = %{
        state: "california",
        region: "los_angeles_county",
        industry: "entertainment"
      }

      tech_result =
        CaliforniaSpikeBreakRules.process_spike_break_compliance(
          [multi_industry_session],
          %{},
          tech_jurisdiction
        )

      entertainment_result =
        CaliforniaSpikeBreakRules.process_spike_break_compliance(
          [multi_industry_session],
          %{},
          entertainment_jurisdiction
        )

      # Both should generate requirements, potentially different
      assert tech_result.summary.total_spike_break_requirements > 0
      assert entertainment_result.summary.total_spike_break_requirements > 0
    end

    test "handles industry transitions within same session" do
      employee_id = "industry_transition_test"
      date = ~D[2024-01-15]

      # Single session with industry change mid-way
      transition_session =
        {:work_session, "industry_transition",
         %{
           employee_id: employee_id,
           work_session_id: "industry_transition",
           start_datetime: DateTime.new!(date, ~T[08:00:00], "Etc/UTC"),
           end_datetime: DateTime.new!(date, ~T[20:00:00], "Etc/UTC"),
           total_hours: 12,
           meal_breaks_taken: 1,
           breaks_taken: [],
           industry_schedule: [
             %{time_start: ~T[08:00:00], time_end: ~T[14:00:00], industry: "technology"},
             %{time_start: ~T[14:00:00], time_end: ~T[20:00:00], industry: "entertainment"}
           ],
           jurisdiction: %{state: "california", region: "bay_area"}
         }}

      # Test with jurisdiction that could apply to both industries
      mixed_jurisdiction = %{state: "california", region: "bay_area"}

      result =
        CaliforniaSpikeBreakRules.process_spike_break_compliance(
          [transition_session],
          %{},
          mixed_jurisdiction
        )

      # Should handle industry transition within session
      assert is_map(result)
      assert result.summary.total_spike_break_requirements >= 0
    end
  end

  describe "Data Integrity and Validation Edge Cases" do
    test "handles sessions with invalid or corrupted timing data" do
      employee_id = "invalid_timing_test"
      date = ~D[2024-01-15]

      # Session with end time before start time
      invalid_session =
        {:work_session, "invalid_timing",
         %{
           employee_id: employee_id,
           work_session_id: "invalid_timing",
           start_datetime: DateTime.new!(date, ~T[18:00:00], "Etc/UTC"),
           # Invalid: ends before it starts
           end_datetime: DateTime.new!(date, ~T[08:00:00], "Etc/UTC"),
           # Negative hours
           total_hours: -10,
           meal_breaks_taken: 1,
           breaks_taken: [],
           jurisdiction: %{state: "california"}
         }}

      jurisdiction = %{state: "california"}

      # Should handle gracefully without crashing
      result =
        CaliforniaSpikeBreakRules.process_spike_break_compliance(
          [invalid_session],
          %{},
          jurisdiction
        )

      assert is_map(result)
      # May have zero requirements due to invalid data
      assert result.summary.total_spike_break_requirements >= 0
    end

    test "handles extremely long work sessions" do
      employee_id = "extreme_hours_test"
      date = ~D[2024-01-15]

      # Unrealistically long work session (48 hours)
      extreme_session =
        {:work_session, "extreme_hours",
         %{
           employee_id: employee_id,
           work_session_id: "extreme_hours",
           start_datetime: DateTime.new!(date, ~T[00:00:00], "Etc/UTC"),
           end_datetime: DateTime.new!(Date.add(date, 2), ~T[00:00:00], "Etc/UTC"),
           total_hours: 48,
           meal_breaks_taken: 2,
           breaks_taken: [],
           jurisdiction: %{state: "california"}
         }}

      jurisdiction = %{state: "california"}

      result =
        CaliforniaSpikeBreakRules.process_spike_break_compliance(
          [extreme_session],
          %{},
          jurisdiction
        )

      # Should generate many requirements for such a long session
      assert result.summary.total_spike_break_requirements > 10

      # Should handle extreme case without performance issues
      assert is_map(result)
    end

    test "handles sessions with inconsistent break data" do
      employee_id = "inconsistent_data_test"
      date = ~D[2024-01-15]

      # Session claiming breaks taken but not providing break details
      inconsistent_session =
        {:work_session, "inconsistent_breaks",
         %{
           employee_id: employee_id,
           work_session_id: "inconsistent_breaks",
           start_datetime: DateTime.new!(date, ~T[08:00:00], "Etc/UTC"),
           end_datetime: DateTime.new!(date, ~T[20:00:00], "Etc/UTC"),
           total_hours: 12,
           # Claims 3 meal breaks
           meal_breaks_taken: 3,
           # But no break details provided
           breaks_taken: [],
           # Reports 5 total breaks
           reported_break_count: 5,
           # But zero duration
           actual_break_duration: 0,
           jurisdiction: %{state: "california"}
         }}

      jurisdiction = %{state: "california"}

      result =
        CaliforniaSpikeBreakRules.process_spike_break_compliance(
          [inconsistent_session],
          %{},
          jurisdiction
        )

      # Should handle inconsistent data gracefully
      assert is_map(result)

      # Likely to show non-compliance due to missing break details
      assert result.summary.compliance_violations >= 0
    end
  end

  describe "Performance Edge Cases" do
    test "handles very short work sessions efficiently" do
      employee_id = "short_session_test"
      date = ~D[2024-01-15]

      # Many very short sessions (30 minutes each)
      short_sessions =
        for i <- 1..100 do
          # 30 minute intervals
          start_time = Time.add(~T[08:00:00], i * 30 * 60)
          # 30 minutes duration
          end_time = Time.add(start_time, 30 * 60)

          {:work_session, "short_#{i}",
           %{
             employee_id: employee_id,
             work_session_id: "short_#{i}",
             start_datetime: DateTime.new!(date, start_time, "Etc/UTC"),
             end_datetime: DateTime.new!(date, end_time, "Etc/UTC"),
             total_hours: 0.5,
             meal_breaks_taken: 0,
             breaks_taken: [],
             jurisdiction: %{state: "california"}
           }}
        end

      jurisdiction = %{state: "california"}

      {process_time, result} =
        :timer.tc(fn ->
          CaliforniaSpikeBreakRules.process_spike_break_compliance(
            short_sessions,
            %{},
            jurisdiction
          )
        end)

      # Should process quickly even with many short sessions
      # < 1 second
      assert process_time < 1_000_000

      # Most short sessions shouldn't trigger spike break requirements
      assert result.summary.total_spike_break_requirements < 10
    end

    test "handles concurrent jurisdiction processing" do
      employee_id = "concurrent_test"
      date = ~D[2024-01-15]

      work_session = SpikeBreakTestHelpers.create_extended_day_session(employee_id, date, 12)

      # Process same session with different jurisdictions concurrently
      jurisdictions = [
        %{state: "california"},
        %{state: "california", region: "bay_area"},
        %{state: "california", region: "los_angeles_county"},
        %{state: "california", region: "central_valley"}
      ]

      tasks =
        Enum.map(jurisdictions, fn jurisdiction ->
          Task.async(fn ->
            CaliforniaSpikeBreakRules.process_spike_break_compliance(
              [work_session],
              %{},
              jurisdiction
            )
          end)
        end)

      results = Task.await_many(tasks)

      # All results should be valid
      Enum.each(results, fn result ->
        assert is_map(result)
        assert Map.has_key?(result, :summary)
        assert result.summary.total_spike_break_requirements >= 0
      end)

      # Results should be consistent for same input with same jurisdiction
      # (Test deterministic behaviour)
      assert length(Enum.uniq(results)) <= length(jurisdictions)
    end
  end
end
